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
}

type freqSample struct {
	freq float64
	at   time.Time
}

// NewFrequencyAverager creates an empty averager.
func NewFrequencyAverager() *FrequencyAverager {
	return &FrequencyAverager{
		entries:  make(map[string][]freqSample),
		lastSeen: make(map[string]time.Time),
	}
}

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
