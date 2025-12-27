package spot

import (
	"math"
	"sync"
	"time"
)

// FrequencyAverager collects recent frequency reports per callsign and
// returns the average within a sliding window.
type FrequencyAverager struct {
	mu       sync.Mutex
	entries  map[string][]freqSample
	lastSeen map[string]time.Time
	opCount  int

	sweepQuit chan struct{}
}

type freqSample struct {
	freq float64
	at   time.Time
}

// Purpose: Construct a frequency averager with empty state.
// Key aspects: Initializes maps for samples and last-seen timestamps.
// Upstream: main startup.
// Downstream: map allocation.
// NewFrequencyAverager creates an empty averager.
func NewFrequencyAverager() *FrequencyAverager {
	return &FrequencyAverager{
		entries:  make(map[string][]freqSample),
		lastSeen: make(map[string]time.Time),
	}
}

// Purpose: Update history and compute average frequency within a window.
// Key aspects: Filters by recency and tolerance, returns average/corroborators/total.
// Upstream: processOutputSpots frequency averaging.
// Downstream: cleanup and math.Abs.
// Average updates the history for the given call and returns the average
// frequency (in kHz) across all reports within the provided window that
// sit within the supplied tolerance of the current report. The returned
// counts include the current report.
//   - return #1: averaged frequency in kHz
//   - return #2: number of corroborating reports within tolerance
//   - return #3: total reports within the recency window
func (fa *FrequencyAverager) Average(call string, freq float64, now time.Time, window time.Duration, tolerance float64) (float64, int, int) {
	if call == "" {
		return freq, 1, 1
	}

	fa.mu.Lock()
	defer fa.mu.Unlock()
	fa.cleanup(now, window)

	list := fa.entries[call]
	pruned := list[:0]
	sum := freq
	corroborators := 1
	cutoff := now.Add(-window)

	for _, sample := range list {
		if sample.at.Before(cutoff) {
			continue
		}
		pruned = append(pruned, sample)
		if math.Abs(sample.freq-freq) <= tolerance {
			sum += sample.freq
			corroborators++
		}
	}

	pruned = append(pruned, freqSample{freq: freq, at: now})
	// Even if pruned is empty (should not happen after append), keep logic to clean map.
	if len(pruned) == 0 {
		delete(fa.entries, call)
		delete(fa.lastSeen, call)
	} else {
		// Reclaim excess capacity when the slice has shrunk significantly.
		if cap(pruned) > len(pruned)*2 {
			newSlice := make([]freqSample, len(pruned))
			copy(newSlice, pruned)
			pruned = newSlice
		}
		fa.entries[call] = pruned
		fa.lastSeen[call] = now
	}

	total := len(pruned)
	return sum / float64(corroborators), corroborators, total
}

// Purpose: Drop inactive calls to bound memory usage.
// Key aspects: Runs every N ops to keep cleanup lightweight.
// Upstream: Average and StartCleanup.
// Downstream: map deletes for entries/lastSeen.
// cleanup drops inactive calls to prevent unbounded map growth as calls churn.
// It runs cheaply on every call; for larger maps you could adjust the cadence.
func (fa *FrequencyAverager) cleanup(now time.Time, window time.Duration) {
	fa.opCount++
	if fa.opCount%128 != 0 {
		return
	}
	cutoff := now.Add(-window)
	for call, last := range fa.lastSeen {
		if last.Before(cutoff) {
			delete(fa.lastSeen, call)
			delete(fa.entries, call)
		}
	}
}

// Purpose: Start a periodic cleanup goroutine.
// Key aspects: Guards against multiple starts and uses a quit channel.
// Upstream: main startup.
// Downstream: cleanup and time.NewTicker.
// StartCleanup launches a periodic sweep to drop inactive calls even when
// traffic is sparse, keeping memory bounded over long runtimes.
func (fa *FrequencyAverager) StartCleanup(interval, window time.Duration) {
	if fa == nil {
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	fa.mu.Lock()
	if fa.sweepQuit != nil {
		fa.mu.Unlock()
		return
	}
	fa.sweepQuit = make(chan struct{})
	fa.mu.Unlock()

	// Purpose: Periodically invoke cleanup until StopCleanup is called.
	// Key aspects: Ticker-driven loop with quit channel.
	// Upstream: StartCleanup.
	// Downstream: cleanup and ticker.Stop.
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fa.mu.Lock()
				fa.cleanup(time.Now(), window)
				fa.mu.Unlock()
			case <-fa.sweepQuit:
				return
			}
		}
	}()
}

// Purpose: Stop the periodic cleanup goroutine.
// Key aspects: Closes quit channel and clears it.
// Upstream: main shutdown.
// Downstream: channel close only.
// StopCleanup stops the periodic sweep goroutine.
func (fa *FrequencyAverager) StopCleanup() {
	if fa == nil {
		return
	}
	fa.mu.Lock()
	defer fa.mu.Unlock()
	if fa.sweepQuit != nil {
		close(fa.sweepQuit)
		fa.sweepQuit = nil
	}
}
