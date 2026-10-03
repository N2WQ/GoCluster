// Frozen pre-v16 harmonic behavior oracle. Only type/constructor names differ.
// Source SHA-256: 7f312e4fb54259df2910c4c5833569d86b377d8895b2f06a06f5d3d7efc78fa3
// This compatibility reference is supplemented by literal boundary tests.
package spot

import (
	"math"
	"sync"
	"time"
)

// v16ReferenceSettings controls how harmonic detection behaves.
type v16ReferenceSettings struct {
	Enabled              bool
	RecencyWindow        time.Duration
	MaxHarmonicMultiple  int
	FrequencyToleranceHz float64
	MinReportDelta       int
	MinReportDeltaStep   float64
}

// v16ReferenceEntry stores a recently seen "fundamental" spot for comparison.
type v16ReferenceEntry struct {
	frequency float64
	report    int
	at        time.Time
}

// v16ReferenceDetector tracks recent fundamentals per DX call and decides whether
// a new spot is likely a harmonic that should be dropped.
type v16ReferenceDetector struct {
	settings v16ReferenceSettings
	mu       sync.Mutex
	entries  map[string][]v16ReferenceEntry
	lastSeen map[string]time.Time

	sweepQuit chan struct{}
}

// newV16ReferenceDetector constructs a harmonic detector with configured thresholds.
// Key aspects: Initializes entry maps and stores settings.
// Upstream: main startup.
// Downstream: map allocation.
// newV16ReferenceDetector creates a detector with the provided settings.
func newV16ReferenceDetector(settings v16ReferenceSettings) *v16ReferenceDetector {
	return &v16ReferenceDetector{
		settings: settings,
		entries:  make(map[string][]v16ReferenceEntry),
		lastSeen: make(map[string]time.Time),
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
func (hd *v16ReferenceDetector) ShouldDrop(s *Spot, now time.Time) (bool, float64, int, int) {
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

	hd.entries[call] = append(hd.entries[call], v16ReferenceEntry{
		frequency: s.Frequency,
		report:    s.Report,
		at:        s.Time,
	})
	hd.lastSeen[call] = now
	// Prevent map growth: if the slice is empty after pruning, drop the key.
	if len(hd.entries[call]) == 0 {
		delete(hd.entries, call)
		delete(hd.lastSeen, call)
	}
	return false, 0, 0, 0
}

// Purpose: Check candidate fundamentals for a harmonic match.
// Key aspects: Evaluates harmonic multiples and report delta thresholds.
// Upstream: ShouldDrop.
// Downstream: math.Abs and settings thresholds.
func (hd *v16ReferenceDetector) detectHarmonic(call string, s *Spot) (float64, int, int) {
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
func (hd *v16ReferenceDetector) prune(call string, now time.Time) {
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
		delete(hd.entries, call)
		delete(hd.lastSeen, call)
		return
	}
	hd.entries[call] = dst
	hd.lastSeen[call] = now
}

// Purpose: Drop inactive calls beyond the recency window.
// Key aspects: Removes entries when lastSeen is too old.
// Upstream: ShouldDrop and StartCleanup.
// Downstream: map deletes.
// cleanup drops inactive calls entirely when their last seen time is outside the recency window.
func (hd *v16ReferenceDetector) cleanup(now time.Time) {
	if len(hd.lastSeen) == 0 {
		return
	}
	cutoff := now.Add(-hd.settings.RecencyWindow)
	for call, last := range hd.lastSeen {
		if last.Before(cutoff) {
			delete(hd.lastSeen, call)
			delete(hd.entries, call)
		}
	}
}

// StartCleanup starts periodic cleanup of harmonic entries.
// Key aspects: Guards against multiple starts and uses quit channel.
// Upstream: main startup.
// Downstream: cleanup and time.NewTicker.
// StartCleanup starts a periodic sweep to evict inactive calls even when new spots are sparse.
func (hd *v16ReferenceDetector) StartCleanup(interval time.Duration) {
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
func (hd *v16ReferenceDetector) StopCleanup() {
	if hd == nil {
		return
	}
	stopPeriodicCleanup(&hd.mu, &hd.sweepQuit)
}
