package dedup

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"dxcluster/spot"

	"github.com/zeebo/xxh3"
)

// FrequencyChangeOptions is validated at construction; runtime values belong
// to the required dedupe YAML. No zero-valued option supplies a default.
type FrequencyChangeOptions struct {
	State                   string
	CWRTTYToleranceHz       int64
	FTToleranceHz           int64
	OtherSkimmerToleranceHz int64
	HumanToleranceHz        int64
	MaxChanges              int
}

type frequencyEntry struct {
	when         time.Time
	anchorMinute int64
	anchorHz     int64
	snr          int
	changes      int
	hasReport    bool
}

type frequencyShard struct {
	cache      map[[32]byte]frequencyEntry
	peak       int
	processed  uint64
	duplicates uint64
	cohorts    [ShadowClassCount]ShadowCounters
}

// frequencyChanges owns only candidate maps and fixed counters. The historical
// shard mutex owns each corresponding baseline/candidate pair. Both maps expire
// by their own when field, under one sweep clock, with no separately retained
// per-key diagnostic or statistics state.
type frequencyChanges struct {
	options   FrequencyChangeOptions
	startedAt time.Time
	shards    [secondaryShardCount]frequencyShard
	lifeMu    sync.Mutex
	started   bool
	stopped   bool
	done      chan struct{}
	// pairHook is a package-test seam to force a pause between paired
	// decisions. It is immutable after callers start, and nil in runtime use.
	pairHook func()
}

// NewSecondaryDeduperWithFrequencyChanges builds only the engines selected by
// state. Off preserves the historical entry representation; on allocates no
// baseline maps. Configuration-level averaging validation belongs to config.
func NewSecondaryDeduperWithFrequencyChanges(window time.Duration, prefer bool, keyMode SecondaryKeyMode, options FrequencyChangeOptions) (*SecondaryDeduper, error) {
	if err := validateFrequencyOptions(options); err != nil {
		return nil, err
	}
	if options.State == "off" {
		return NewSecondaryDeduperWithKey(window, prefer, keyMode), nil
	}
	d := &SecondaryDeduper{window: window, preferStronger: prefer, keyMode: keyMode,
		shards: make([]secondaryShard, secondaryShardCount), cleanupInterval: time.Minute,
		shutdown: make(chan struct{}), changes: &frequencyChanges{options: options, startedAt: time.Now().UTC(), done: make(chan struct{})}}
	for i := range d.shards {
		if options.State == "shadow" {
			d.shards[i].cache = make(map[[32]byte]secondaryEntry)
		}
		d.changes.shards[i].cache = make(map[[32]byte]frequencyEntry)
	}
	return d, nil
}

func validateFrequencyOptions(o FrequencyChangeOptions) error {
	if o.State != "off" && o.State != "shadow" && o.State != "on" {
		return fmt.Errorf("invalid secondary frequency-change state %q", o.State)
	}
	if o.CWRTTYToleranceHz <= 0 || o.FTToleranceHz <= 0 || o.OtherSkimmerToleranceHz <= 0 || o.HumanToleranceHz <= 0 || o.MaxChanges <= 0 {
		return fmt.Errorf("secondary frequency-change tolerances and max changes must be positive")
	}
	return nil
}

// Evaluate returns immutable, pre-decision anchor evidence only for on. Shadow
// retains baseline output and diagnostic bytes; its candidate evidence cannot
// leak into asynchronous fanout. No returned value borrows a cache entry.
func (d *SecondaryDeduper) Evaluate(s *spot.Spot) (bool, spot.SecondaryDedupeEvidence) {
	if d == nil || d.changes == nil {
		return d.ShouldForward(s), spot.SecondaryDedupeEvidence{}
	}
	return d.evaluateChanges(s)
}

func (d *SecondaryDeduper) evaluateChanges(s *spot.Spot) (bool, spot.SecondaryDedupeEvidence) {
	if d.window <= 0 || s == nil {
		return true, spot.SecondaryDedupeEvidence{}
	}
	key := secondaryFrequencyKey(s, s.DEMetadata.ADIF, d.keyMode)
	// Placement deliberately excludes mode. Different modes of one historical
	// key share the pair transaction, even though candidate equality differs.
	index := uint32(xxh3.Hash(key[:])) & (secondaryShardCount - 1)
	shard := &d.shards[index]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	f := d.changes
	cs := &f.shards[index]
	if s.DEMetadata.ADIF <= 0 {
		if f.options.State == "shadow" {
			cs.cohorts[frequencyClass(s, "")].BaselineForwarded++
		}
		return true, spot.SecondaryDedupeEvidence{}
	}
	modeID, mode := byte(0), ""
	if spot.IsSkimmerSource(s.SourceType) {
		modeID, mode = spot.SecondaryModeIdentity(s.ModeNorm)
	}
	class := frequencyClass(s, mode)
	baseForward := false
	if f.options.State == "shadow" {
		baseForward = d.baselineLocked(shard, key, s)
		if f.pairHook != nil {
			f.pairHook()
		}
	}
	key[21] = modeID
	forward, reason, evidence := d.candidateLocked(cs, key, modeID, class, s)
	if f.options.State == "shadow" {
		cs.cohorts[class].observe(baseForward, forward, reason)
		return baseForward, spot.SecondaryDedupeEvidence{}
	}
	return forward, evidence
}

// secondaryFrequencyKey preserves every historical key byte while packing
// ordinary ASCII bands directly. NormalizeUpper allocates for every "20m"
// band; avoiding that temporary is required by the candidate hot-path budget.
// The Unicode fallback deliberately keeps historical uppercasing semantics.
func secondaryFrequencyKey(s *spot.Spot, deDXCC int, mode SecondaryKeyMode) [32]byte {
	s.EnsureNormalized()
	var key [32]byte
	band := s.BandNorm
	if band == "" {
		band = spot.FreqToBand(s.Frequency)
	}
	band = strings.TrimSpace(band)
	ascii := true
	for i := 0; i < len(band); i++ {
		if band[i] >= 128 {
			ascii = false
			break
		}
	}
	if ascii {
		for i := 0; i < len(band) && i < 4; i++ {
			key[i] = toUpperASCII(band[i])
		}
	} else {
		binary.LittleEndian.PutUint32(key[:4], bandKeyNumeric(s))
	}
	binary.LittleEndian.PutUint16(key[4:6], uint16(deDXCC))
	if mode == SecondaryKeyCQZone {
		binary.LittleEndian.PutUint16(key[6:8], fixedCQZone(s))
	} else {
		grid := fixedGrid2(s)
		key[6], key[7] = grid[0], grid[1]
	}
	writeFixedCallNormalized(key[8:20], s.DXCallNorm)
	key[20] = secondarySourceClass(s)
	return key
}

// baselineLocked is the historical decision verbatim, except the caller owns
// locking and increments counters before the candidate observes the same spot.
func (d *SecondaryDeduper) baselineLocked(shard *secondaryShard, key [32]byte, s *spot.Spot) bool {
	shard.processedCount++
	dup, last := isSecondaryDuplicateLocked(shard.cache, key, s.Time, d.window)
	upgradeToReported := s.HasReport && !last.hasReport
	stronger := d.preferStronger && s.Report > last.snr
	if dup && !upgradeToReported && !stronger {
		shard.duplicateCount++
		return false
	}
	shard.cache[key] = secondaryEntry{when: s.Time, snr: s.Report, hasReport: s.HasReport}
	d.updateShardPeakLocked(shard)
	return true
}

type frequencyReason uint8

const (
	frequencyRepeat frequencyReason = iota
	frequencyNewEntry
	frequencyReportException
	frequencyAdmission
	frequencyEarlierMinute
	frequencyLimitBlocked
)

func (d *SecondaryDeduper) candidateLocked(shard *frequencyShard, key [32]byte, mode byte, class int, s *spot.Spot) (bool, frequencyReason, spot.SecondaryDedupeEvidence) {
	shard.processed++
	hz, valid := wholeHz(s.Frequency)
	if !valid {
		// Invalid frequencies are outside the ingest contract. Preserve a
		// metadata-only decision rather than introducing a new validation drop.
		return d.invalidFrequencyLocked(shard, key, s)
	}
	entry, exists := shard.cache[key]
	age := s.Time.Sub(entry.when)
	if age < 0 {
		age = -age
	}
	if !exists || age >= d.window {
		shard.cache[key] = frequencyEntry{when: s.Time, anchorMinute: anchorMinuteUnix(s.Time), anchorHz: hz, snr: s.Report, hasReport: s.HasReport}
		shard.updatePeak()
		return true, frequencyNewEntry, spot.SecondaryDedupeEvidence{Enabled: true, ModeID: mode}
	}
	evidence := spot.SecondaryDedupeEvidence{Enabled: true, HasAnchor: true, AnchorHz: entry.anchorHz, ModeID: mode}
	outside := distanceHz(hz, entry.anchorHz) > d.toleranceHz(class)
	if (s.HasReport && !entry.hasReport) || (d.preferStronger && s.Report > entry.snr) {
		entry.when, entry.snr, entry.hasReport = s.Time, s.Report, s.HasReport
		if outside {
			entry.anchorHz, entry.anchorMinute = hz, anchorMinuteUnix(s.Time)
		}
		shard.cache[key] = entry
		return true, frequencyReportException, evidence
	}
	reason := frequencyRepeat
	if outside {
		if s.Time.Unix() < entry.anchorMinute {
			reason = frequencyEarlierMinute
		} else if entry.changes >= d.changes.options.MaxChanges {
			reason = frequencyLimitBlocked
		} else {
			entry.anchorHz, entry.anchorMinute = hz, anchorMinuteUnix(s.Time)
			entry.snr, entry.hasReport = s.Report, s.HasReport
			entry.changes++
			shard.cache[key] = entry
			return true, frequencyAdmission, evidence
		}
	}
	shard.duplicates++
	return false, reason, evidence
}

func (d *SecondaryDeduper) invalidFrequencyLocked(shard *frequencyShard, key [32]byte, s *spot.Spot) (bool, frequencyReason, spot.SecondaryDedupeEvidence) {
	entry, exists := shard.cache[key]
	age := s.Time.Sub(entry.when)
	if age < 0 {
		age = -age
	}
	if !exists || age >= d.window {
		shard.cache[key] = frequencyEntry{when: s.Time, anchorMinute: anchorMinuteUnix(s.Time), snr: s.Report, hasReport: s.HasReport}
		shard.updatePeak()
		return true, frequencyNewEntry, spot.SecondaryDedupeEvidence{}
	}
	if (s.HasReport && !entry.hasReport) || (d.preferStronger && s.Report > entry.snr) {
		entry.when, entry.snr, entry.hasReport = s.Time, s.Report, s.HasReport
		shard.cache[key] = entry
		return true, frequencyReportException, spot.SecondaryDedupeEvidence{}
	}
	shard.duplicates++
	return false, frequencyRepeat, spot.SecondaryDedupeEvidence{}
}

// anchorMinuteUnix is exactly Time.Truncate(time.Minute).Unix(). Ordinary
// calendar dates use integer floor-to-minute arithmetic, including pre-epoch
// seconds. Extreme time.Time values keep the standard-library path to avoid
// changing its internal epoch-overflow semantics.
func anchorMinuteUnix(at time.Time) int64 {
	seconds := at.Unix()
	// UTC 0001-01-01 through 9999-12-31, safely inside time.Time's internal
	// epoch range. This is an arithmetic fast path, not a date admission rule.
	if seconds < -62135596800 || seconds > 253402300799 {
		return at.Truncate(time.Minute).Unix()
	}
	remainder := seconds % 60
	if remainder < 0 {
		remainder += 60
	}
	return seconds - remainder
}

func wholeHz(kHz float64) (int64, bool) {
	hz := math.Round(kHz * 1000)
	if math.IsNaN(hz) || math.IsInf(hz, 0) || hz <= 0 || hz >= float64(math.MaxInt64) {
		return 0, false
	}
	return int64(hz), true
}

func distanceHz(a, b int64) int64 {
	if a < b {
		return b - a
	}
	return a - b
}

func (d *SecondaryDeduper) toleranceHz(class int) int64 {
	o := d.changes.options
	switch class {
	case ShadowHuman:
		return o.HumanToleranceHz
	case ShadowCWRTTY:
		return o.CWRTTYToleranceHz
	case ShadowFT:
		return o.FTToleranceHz
	default:
		return o.OtherSkimmerToleranceHz
	}
}
