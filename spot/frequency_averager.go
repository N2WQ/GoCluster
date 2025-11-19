package spot

import (
	"math"
	"sync"
	"time"
)

// FrequencyAverager collects recent frequency reports per callsign and
// returns the average within a sliding window.
type FrequencyAverager struct {
	mu      sync.Mutex
	entries map[string][]freqSample
}

type freqSample struct {
	freq float64
	at   time.Time
}

// NewFrequencyAverager creates an empty averager.
func NewFrequencyAverager() *FrequencyAverager {
	return &FrequencyAverager{
		entries: make(map[string][]freqSample),
	}
}

// Average updates the history for the given call and returns the average
// frequency (in kHz) across all reports within the provided window that
// sit within the supplied tolerance of the current report.
func (fa *FrequencyAverager) Average(call string, freq float64, now time.Time, window time.Duration, tolerance float64) (float64, int) {
	if call == "" {
		return freq, 1
	}

	fa.mu.Lock()
	defer fa.mu.Unlock()

	list := fa.entries[call]
	pruned := list[:0]
	sum := freq
	count := 1
	cutoff := now.Add(-window)

	for _, sample := range list {
		if sample.at.Before(cutoff) {
			continue
		}
		pruned = append(pruned, sample)
		if math.Abs(sample.freq-freq) <= tolerance {
			sum += sample.freq
			count++
		}
	}

	pruned = append(pruned, freqSample{freq: freq, at: now})
	fa.entries[call] = pruned

	return sum / float64(count), count
}
