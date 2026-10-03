package spot

import (
	"math"
	"sync"
	"time"
	"unsafe"
)

// HarmonicSettings controls how harmonic detection behaves.
type HarmonicSettings struct {
	Enabled              bool
	RecencyWindow        time.Duration
	MaxHarmonicMultiple  int
	FrequencyToleranceHz float64
	MinReportDelta       int
	MinReportDeltaStep   float64
}

// harmonicEntry stores a recently seen "fundamental" spot for comparison.
type harmonicEntry struct {
	frequency float64
	report    int
	at        time.Time
}

// harmonicRecency couples the existing acceptance timestamp to one expiry
// position. Updating time in either direction repairs that same heap item.
type harmonicRecency struct {
	at    time.Time
	index int
}

// HarmonicDetector tracks recent fundamentals per DX call and decides whether
// a new spot is likely a harmonic that should be dropped.
type HarmonicDetector struct {
	settings HarmonicSettings
	mu       sync.Mutex
	entries  map[string][]harmonicEntry
	lastSeen map[string]harmonicRecency
	expiry   []string // exactly one item per lastSeen owner; protected by mu

	sweepQuit chan struct{}
}

// NewHarmonicDetector constructs a harmonic detector with configured thresholds.
// Key aspects: Initializes entry maps and stores settings.
// Upstream: main startup.
// Downstream: map allocation.
// NewHarmonicDetector creates a detector with the provided settings.
func NewHarmonicDetector(settings HarmonicSettings) *HarmonicDetector {
	return &HarmonicDetector{
		settings: settings,
		entries:  make(map[string][]harmonicEntry),
		lastSeen: make(map[string]harmonicRecency),
	}
}

// ShouldDrop reports whether a spot is a harmonic that should be dropped.
// Key aspects: Checks recency, report deltas, and harmonic multiples.
// Upstream: processOutputSpots harmonic suppression stage.
// Downstream: detectHarmonic and cleanup/prune.
// ShouldDrop returns true if the given spot appears to be a harmonic of a lower
// frequency fundamental. The second return value is the fundamental frequency
// that triggered the drop (in kHz) for logging purposes, and the third value is
// how many fundamentals corroborated that decision.
func (hd *HarmonicDetector) ShouldDrop(s *Spot, now time.Time) (bool, float64, int, int) {
	if hd == nil || !hd.settings.Enabled || s == nil {
		return false, 0, 0, 0
	}
	if !IsCallCorrectionCandidate(s.Mode) {
		return false, 0, 0, 0
	}

	s.EnsureNormalized()
	call := s.DXCallNorm
	if call == "" {
		return false, 0, 0, 0
	}

	hd.mu.Lock()
	defer hd.mu.Unlock()

	hd.cleanup(now)
	hd.prune(call, now)
	if fundamental, corroborators, delta := hd.detectHarmonic(call, s); fundamental > 0 {
		return true, fundamental, corroborators, delta
	}

	hd.entries[call] = append(hd.entries[call], harmonicEntry{
		frequency: s.Frequency,
		report:    s.Report,
		at:        s.Time,
	})
	hd.setLastSeen(call, now)
	// Prevent map growth: if the slice is empty after pruning, drop the key.
	if len(hd.entries[call]) == 0 {
		hd.removeCall(call)
		hd.compactExpiry()
	}
	return false, 0, 0, 0
}

// Purpose: Check candidate fundamentals for a harmonic match.
// Key aspects: Evaluates harmonic multiples and report delta thresholds.
// Upstream: ShouldDrop.
// Downstream: math.Abs and settings thresholds.
func (hd *HarmonicDetector) detectHarmonic(call string, s *Spot) (float64, int, int) {
	candidates := hd.entries[call]
	if len(candidates) == 0 {
		return 0, 0, 0
	}

	minDelta := hd.settings.MinReportDelta
	stepDelta := hd.settings.MinReportDeltaStep
	toleranceKHz := hd.settings.FrequencyToleranceHz / 1000.0

	var fundamental float64
	var corroborators int
	var deltaDB int
	for _, entry := range candidates {
		if entry.frequency <= 0 || s.Frequency <= entry.frequency {
			continue
		}
		reportDelta := entry.report - s.Report
		if minDelta > 0 && reportDelta < minDelta {
			continue
		}
		for mult := 2; mult <= hd.settings.MaxHarmonicMultiple; mult++ {
			expected := entry.frequency * float64(mult)
			if math.Abs(expected-s.Frequency) <= toleranceKHz {
				requiredDelta := float64(minDelta)
				if stepDelta > 0 && mult > 2 {
					requiredDelta += stepDelta * float64(mult-2)
				}
				if requiredDelta > 0 && float64(reportDelta) < requiredDelta {
					continue
				}
				if entry.at.IsZero() || s.Time.Sub(entry.at) <= hd.settings.RecencyWindow {
					if fundamental == 0 {
						fundamental = entry.frequency
						corroborators = 1
						deltaDB = reportDelta
					} else if math.Abs(entry.frequency-fundamental) <= toleranceKHz {
						corroborators++
					}
				}
				break
			}
		}
	}
	if deltaDB < 0 {
		deltaDB = -deltaDB
	}
	return fundamental, corroborators, deltaDB
}

// Purpose: Prune stale fundamental entries for a callsign.
// Key aspects: Retains only entries within the recency window.
// Upstream: ShouldDrop.
// Downstream: map deletes and slice filtering.
func (hd *HarmonicDetector) prune(call string, now time.Time) {
	window := hd.settings.RecencyWindow
	slice := hd.entries[call]
	if len(slice) == 0 {
		return
	}
	cutoff := now.Add(-window)
	dst := slice[:0]
	for _, entry := range slice {
		if entry.at.After(cutoff) {
			dst = append(dst, entry)
		}
	}
	if len(dst) == 0 {
		hd.removeCall(call)
		hd.compactExpiry()
		return
	}
	hd.entries[call] = dst
	hd.setLastSeen(call, now)
}

// Purpose: Drop inactive calls beyond the recency window.
// Key aspects: Removes entries when lastSeen is too old.
// Upstream: ShouldDrop and StartCleanup.
// Downstream: map deletes.
// cleanup drops inactive calls entirely when their last seen time is outside the recency window.
func (hd *HarmonicDetector) cleanup(now time.Time) {
	if len(hd.expiry) == 0 {
		return
	}
	cutoff := now.Add(-hd.settings.RecencyWindow)
	for len(hd.expiry) != 0 {
		call := hd.expiry[0]
		if !hd.lastSeen[call].at.Before(cutoff) {
			break
		}
		hd.removeCall(call)
	}
	hd.compactExpiry()
}

// All expiry helpers require hd.mu. Heap order changes only the order in which
// wholly inactive calls are removed; it never reorders a call's fundamentals.
// In particular, equality survives global cleanup, while prune keeps its
// existing strict entry.at.After(cutoff) boundary.
func (hd *HarmonicDetector) setLastSeen(call string, at time.Time) {
	if current, ok := hd.lastSeen[call]; ok {
		current.at = at
		hd.lastSeen[call] = current
		// Match the refreshed recency owner's string header. Keeping the first
		// equal callsign here could retain an obsolete input-frame allocation.
		hd.expiry[current.index] = call
		hd.fixExpiry(current.index)
		return
	}
	index := len(hd.expiry)
	hd.lastSeen[call] = harmonicRecency{at: at, index: index}
	hd.expiry = append(hd.expiry, call)
	hd.upExpiry(index)
}

func (hd *HarmonicDetector) removeCall(call string) {
	current, ok := hd.lastSeen[call]
	if ok {
		last := len(hd.expiry) - 1
		hd.swapExpiry(current.index, last)
		hd.expiry[last] = "" // retired capacity must not retain callsign backing
		hd.expiry = hd.expiry[:last]
		delete(hd.lastSeen, call)
		if current.index < last {
			hd.fixExpiry(current.index)
		}
	}
	delete(hd.entries, call)
}

func (hd *HarmonicDetector) expiryLess(a, b int) bool {
	return hd.lastSeen[hd.expiry[a]].at.Before(hd.lastSeen[hd.expiry[b]].at)
}

func (hd *HarmonicDetector) swapExpiry(a, b int) {
	if a == b {
		return
	}
	hd.expiry[a], hd.expiry[b] = hd.expiry[b], hd.expiry[a]
	left, right := hd.lastSeen[hd.expiry[a]], hd.lastSeen[hd.expiry[b]]
	left.index, right.index = a, b
	hd.lastSeen[hd.expiry[a]], hd.lastSeen[hd.expiry[b]] = left, right
}

func (hd *HarmonicDetector) fixExpiry(index int) {
	if index > 0 && hd.expiryLess(index, (index-1)/2) {
		hd.upExpiry(index)
		return
	}
	hd.downExpiry(index)
}

func (hd *HarmonicDetector) upExpiry(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if !hd.expiryLess(index, parent) {
			return
		}
		hd.swapExpiry(index, parent)
		index = parent
	}
}

func (hd *HarmonicDetector) downExpiry(index int) {
	for index < len(hd.expiry)/2 {
		child := 2*index + 1
		if child+1 < len(hd.expiry) && hd.expiryLess(child+1, child) {
			child++
		}
		if !hd.expiryLess(child, index) {
			return
		}
		hd.swapExpiry(index, child)
		index = child
	}
}

// Compact only excess index backing. During replacement both arrays are
// owned; the new capacity is max(8, 2*live), then the old owner is released.
// Empty indexes release all backing. The existing maps and entry slices keep
// their original lifetime and allocation behavior.
func (hd *HarmonicDetector) compactExpiry() {
	live := len(hd.expiry)
	if live == 0 {
		hd.expiry = nil
		return
	}
	if cap(hd.expiry) <= max(8, 4*live) {
		return
	}
	next := make([]string, live, max(8, 2*live))
	copy(next, hd.expiry)
	hd.expiry = next
}

// HarmonicRetentionStats reports owned expiry backing, separately from the
// existing maps, fundamental slices and Go runtime overhead. Scalar reads do
// not traverse history or allocate an observation per call.
type HarmonicRetentionStats struct {
	Calls, RecencyEntries, ExpiryEntries, ExpiryCapacity int
	ExpiryBackingBytes                                   uint64
}

func (hd *HarmonicDetector) RetentionStats() HarmonicRetentionStats {
	if hd == nil {
		return HarmonicRetentionStats{}
	}
	hd.mu.Lock()
	defer hd.mu.Unlock()
	return HarmonicRetentionStats{
		Calls: len(hd.entries), RecencyEntries: len(hd.lastSeen),
		ExpiryEntries: len(hd.expiry), ExpiryCapacity: cap(hd.expiry),
		ExpiryBackingBytes: uint64(cap(hd.expiry)) * uint64(unsafe.Sizeof(string(""))),
	}
}

// StartCleanup starts periodic cleanup of harmonic entries.
// Key aspects: Guards against multiple starts and uses quit channel.
// Upstream: main startup.
// Downstream: cleanup and time.NewTicker.
// StartCleanup starts a periodic sweep to evict inactive calls even when new spots are sparse.
func (hd *HarmonicDetector) StartCleanup(interval time.Duration) {
	if hd == nil {
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	startPeriodicCleanup(&hd.mu, &hd.sweepQuit, interval, func() {
		hd.mu.Lock()
		hd.cleanup(time.Now().UTC())
		hd.mu.Unlock()
	})
}

// StopCleanup stops the periodic cleanup goroutine.
// Key aspects: Closes quit channel and clears it.
// Upstream: main shutdown.
// Downstream: channel close only.
// StopCleanup stops the periodic cleanup goroutine.
func (hd *HarmonicDetector) StopCleanup() {
	if hd == nil {
		return
	}
	stopPeriodicCleanup(&hd.mu, &hd.sweepQuit)
}
