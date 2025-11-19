package spot

import (
	"math"
	"strings"
	"sync"
	"time"
)

// CorrectionSettings captures the knobs that govern whether a consensus-based
// call correction should happen. The values ultimately come from config.yaml,
// but the struct is deliberately defined here so the algorithm can be unit-tested
// without importing the config package (which would create a cycle).
type CorrectionSettings struct {
	// MinConsensusReports is the number of *other* unique spotters that must
	// agree on the same DX call before we consider overriding the subject spot.
	MinConsensusReports int
	// MinAdvantage is the minimum delta (candidate supporters - subject supporters)
	// required before a correction can happen.
	MinAdvantage int
	// MinConfidencePercent enforces that the alternate call must represent at least
	// this percentage of all unique spotters currently reporting that frequency.
	MinConfidencePercent int
	// RecencyWindow bounds how old the supporting spots can be. Anything older
	// than this duration is ignored so stale data never drives a correction.
	RecencyWindow time.Duration
	// MaxEditDistance bounds how different the alternate call can be compared to
	// the subject call. A value of 2 typically allows single-character typos.
	MaxEditDistance int
}

var correctionEligibleModes = map[string]struct{}{
	"CW":   {},
	"RTTY": {},
	"SSB":  {},
}

// IsCallCorrectionCandidate returns true if the given mode is eligible for
// consensus-based call correction. Only CW, RTTY, and SSB are considered
// because other digital modes already embed their own error correction.
func IsCallCorrectionCandidate(mode string) bool {
	_, ok := correctionEligibleModes[strings.ToUpper(strings.TrimSpace(mode))]
	return ok
}

// frequencyToleranceKHz defines how close two frequencies must be to be considered
// the "same" signal. We only care about agreement on essentially identical frequencies,
// so we allow a half-kilohertz wiggle room to absorb rounding differences between
// data sources.
const frequencyToleranceKHz = 0.5

// SuggestCallCorrection analyzes recent spots on the same frequency and determines
// whether there is overwhelming evidence that the subject spot's DX call should
// be corrected. IMPORTANT: This function only suggests a correction. The caller
// is responsible for deciding whether to apply it and for updating any caches or
// deduplication structures. Nothing in the core pipeline calls this yet—it's a
// building block for future work.
//
// Parameters:
//   - subject: the spot we are evaluating.
//   - others: a slice of other recent spots (e.g., from a ring buffer or stat tracker).
//   - settings: consensus thresholds (min reporters, freshness).
//   - now: the time reference used to evaluate recency. Passing it as an argument
//     rather than calling time.Now() simplifies deterministic testing.
//
// Returns:
//   - correctedCall: the most likely callsign if consensus is met.
//   - supporters: how many unique spotters contributed to the correction.
//   - ok: true if a correction is recommended, false otherwise.
func SuggestCallCorrection(subject *Spot, others []*Spot, settings CorrectionSettings, now time.Time) (correctedCall string, supporters int, correctedConfidence int, subjectConfidence int, totalReporters int, ok bool) {
	if subject == nil {
		return "", 0, 0, 0, 0, false
	}

	cfg := normalizeCorrectionSettings(settings)
	subjectCall := strings.ToUpper(strings.TrimSpace(subject.DXCall))
	if subjectCall == "" {
		return "", 0, 0, 0, 0, false
	}
	subjectReporter := strings.ToUpper(strings.TrimSpace(subject.DECall))
	subjectVotes := map[string]struct{}{}
	allReporters := map[string]struct{}{}
	if subjectReporter != "" {
		subjectVotes[subjectReporter] = struct{}{}
		allReporters[subjectReporter] = struct{}{}
	}

	type candidate struct {
		reporters map[string]struct{}
		lastSeen  time.Time
	}

	candidates := make(map[string]*candidate)

	for _, other := range others {
		if other == nil {
			continue
		}
		otherCall := strings.ToUpper(strings.TrimSpace(other.DXCall))
		if otherCall == "" {
			continue
		}
		// Normalize spotter identifier.
		reporter := strings.ToUpper(strings.TrimSpace(other.DECall))
		if reporter == "" {
			continue
		}
		allReporters[reporter] = struct{}{}
		if reporter == subjectReporter {
			if otherCall == subjectCall {
				subjectVotes[reporter] = struct{}{}
			}
			continue
		}
		// Require frequency overlap within the tolerance window.
		if math.Abs(other.Frequency-subject.Frequency) > frequencyToleranceKHz {
			continue
		}
		// Enforce recency: the supporting spot must be newer than (now - recency window).
		if now.Sub(other.Time) > cfg.RecencyWindow {
			continue
		}

		if otherCall == subjectCall {
			subjectVotes[reporter] = struct{}{}
			continue
		}

		stats, exists := candidates[otherCall]
		if !exists {
			stats = &candidate{
				reporters: make(map[string]struct{}),
			}
			candidates[otherCall] = stats
		}
		stats.reporters[reporter] = struct{}{}
		if other.Time.After(stats.lastSeen) {
			stats.lastSeen = other.Time
		}
	}

	var (
		bestCall       string
		bestCount      int
		bestTime       time.Time
		bestConfidence int
	)
	subjectCount := len(subjectVotes)
	totalReporters = len(allReporters)
	if totalReporters == 0 {
		return "", 0, 0, 0, 0, false
	}
	subjectConfidence = len(subjectVotes) * 100 / totalReporters

	for call, stats := range candidates {
		count := len(stats.reporters)
		if count < cfg.MinConsensusReports {
			continue
		}
		if count < subjectCount+cfg.MinAdvantage {
			continue
		}
		if cfg.MaxEditDistance >= 0 {
			if distance := levenshtein(subjectCall, call); distance > cfg.MaxEditDistance {
				continue
			}
		}
		confidence := count * 100 / totalReporters
		if confidence < cfg.MinConfidencePercent {
			continue
		}
		// Prefer the candidate with the most unique spotters. In a tie, take the
		// most recent one so we gravitate toward the freshest consensus.
		if count > bestCount || (count == bestCount && stats.lastSeen.After(bestTime)) {
			bestCall = call
			bestCount = count
			bestTime = stats.lastSeen
			bestConfidence = confidence
		}
	}

	if bestCall == "" {
		return "", 0, 0, subjectConfidence, totalReporters, false
	}
	return bestCall, bestCount, bestConfidence, subjectConfidence, totalReporters, true
}

// normalizeCorrectionSettings fills in safe defaults so callers can omit config
// while unit tests can deliberately pass tiny values.
func normalizeCorrectionSettings(settings CorrectionSettings) CorrectionSettings {
	cfg := settings
	if cfg.MinConsensusReports <= 0 {
		cfg.MinConsensusReports = 4
	}
	if cfg.MinAdvantage <= 0 {
		cfg.MinAdvantage = 1
	}
	if cfg.MinConfidencePercent <= 0 {
		cfg.MinConfidencePercent = 70
	}
	if cfg.RecencyWindow <= 0 {
		cfg.RecencyWindow = 45 * time.Second
	}
	if cfg.MaxEditDistance <= 0 {
		cfg.MaxEditDistance = 2
	}
	return cfg
}

// CorrectionIndex maintains a time-bounded, frequency-bucketed view of recent
// spots so consensus checks can run without scanning the entire ring buffer.
type CorrectionIndex struct {
	mu      sync.Mutex
	buckets map[int]*correctionBucket
}

type correctionBucket struct {
	spots []*Spot
}

// NewCorrectionIndex constructs an empty index.
func NewCorrectionIndex() *CorrectionIndex {
	return &CorrectionIndex{
		buckets: make(map[int]*correctionBucket),
	}
}

// Add inserts a spot into the appropriate bucket and prunes stale entries.
func (ci *CorrectionIndex) Add(s *Spot, now time.Time, window time.Duration) {
	if ci == nil || s == nil {
		return
	}
	if window <= 0 {
		window = 45 * time.Second
	}

	key := bucketKey(s.Frequency)

	ci.mu.Lock()
	defer ci.mu.Unlock()

	bucket := ci.buckets[key]
	if bucket == nil {
		bucket = &correctionBucket{}
		ci.buckets[key] = bucket
	}

	bucket.spots = pruneAndAppend(bucket.spots, s, now, window)
}

// Candidates retrieves nearby spots (within +/- 0.5 kHz) for the given subject.
func (ci *CorrectionIndex) Candidates(subject *Spot, now time.Time, window time.Duration) []*Spot {
	if ci == nil || subject == nil {
		return nil
	}
	if window <= 0 {
		window = 45 * time.Second
	}

	key := bucketKey(subject.Frequency)
	minKey := key - 5
	maxKey := key + 5

	ci.mu.Lock()
	defer ci.mu.Unlock()

	var results []*Spot
	for k := minKey; k <= maxKey; k++ {
		bucket := ci.buckets[k]
		if bucket == nil {
			continue
		}
		bucket.spots = prune(bucket.spots, now, window)
		results = append(results, bucket.spots...)
	}
	return results
}

func bucketKey(freq float64) int {
	return int(math.Round(freq * 10))
}

func prune(spots []*Spot, now time.Time, window time.Duration) []*Spot {
	if len(spots) == 0 {
		return spots
	}
	cutoff := now.Add(-window)
	dst := spots[:0]
	for _, s := range spots {
		if s == nil {
			continue
		}
		if s.Time.Before(cutoff) {
			continue
		}
		dst = append(dst, s)
	}
	return dst
}

func pruneAndAppend(spots []*Spot, s *Spot, now time.Time, window time.Duration) []*Spot {
	spots = prune(spots, now, window)
	return append(spots, s)
}

func levenshtein(a, b string) int {
	if a == b {
		return 0
	}
	r1 := []rune(a)
	r2 := []rune(b)
	len1 := len(r1)
	len2 := len(r2)
	if len1 == 0 {
		return len2
	}
	if len2 == 0 {
		return len1
	}
	prev := make([]int, len2+1)
	cur := make([]int, len2+1)
	for j := 0; j <= len2; j++ {
		prev[j] = j
	}
	for i := 1; i <= len1; i++ {
		cur[0] = i
		for j := 1; j <= len2; j++ {
			cost := 0
			if r1[i-1] != r2[j-1] {
				cost = 1
			}
			insert := cur[j-1] + 1
			delete := prev[j] + 1
			replace := prev[j-1] + cost
			cur[j] = min(insert, min(delete, replace))
		}
		prev, cur = cur, prev
	}
	return prev[len2]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
