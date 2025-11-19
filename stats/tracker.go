package stats

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Tracker tracks spot statistics by source
type Tracker struct {
	// counters live in sync.Map + atomic.Uint64 so per-spot increments don't fight over a mutex
	modeCounts           sync.Map // string -> *atomic.Uint64
	sourceCounts         sync.Map // string -> *atomic.Uint64
	sourceModeCounts     sync.Map // "source|mode" -> *atomic.Uint64
	start                atomic.Int64
	callCorrections      atomic.Uint64
	frequencyCorrections atomic.Uint64
	harmonicSuppressions atomic.Uint64
}

// NewTracker creates a new stats tracker
func NewTracker() *Tracker {
	t := &Tracker{}
	t.start.Store(time.Now().UnixNano())
	return t
}

// IncrementMode increases the count for a mode (FT8, FT4, MANUAL, etc.)
func (t *Tracker) IncrementMode(mode string) {
	incrementCounter(&t.modeCounts, mode)
}

// IncrementSource increases the count for a source node (RBN, RBN-DIGITAL, PSKREPORTER)
func (t *Tracker) IncrementSource(source string) {
	incrementCounter(&t.sourceCounts, source)
}

// IncrementSourceMode increases the count for a specific source/mode combination.
func (t *Tracker) IncrementSourceMode(source, mode string) {
	source = strings.ToUpper(strings.TrimSpace(source))
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if source == "" || mode == "" {
		return
	}
	key := source + "|" + mode
	incrementCounter(&t.sourceModeCounts, key)
}

// GetCounts returns a copy of all counts
// GetModeCounts returns a copy of mode counts
func (t *Tracker) GetModeCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	t.modeCounts.Range(func(key, value any) bool {
		counts[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return counts
}

// GetSourceCounts returns a copy of source node counts
func (t *Tracker) GetSourceCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	t.sourceCounts.Range(func(key, value any) bool {
		counts[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return counts
}

// GetSourceModeCounts returns a copy of source/mode combination counts.
func (t *Tracker) GetSourceModeCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	t.sourceModeCounts.Range(func(key, value any) bool {
		counts[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return counts
}

// GetTotal returns the total count across all sources
// GetTotal returns the total count across all sources (sum of sourceCounts)
func (t *Tracker) GetTotal() uint64 {
	var total uint64
	t.sourceCounts.Range(func(_, value any) bool {
		total += value.(*atomic.Uint64).Load()
		return true
	})
	return total
}

// GetUptime returns how long the tracker has been running
func (t *Tracker) GetUptime() time.Duration {
	start := t.start.Load()
	return time.Since(time.Unix(0, start))
}

// Reset resets all counters
func (t *Tracker) Reset() {
	t.modeCounts.Range(func(key, _ any) bool {
		t.modeCounts.Delete(key)
		return true
	})
	t.sourceCounts.Range(func(key, _ any) bool {
		t.sourceCounts.Delete(key)
		return true
	})
	t.sourceModeCounts.Range(func(key, _ any) bool {
		t.sourceModeCounts.Delete(key)
		return true
	})
	t.start.Store(time.Now().UnixNano())
}

// SnapshotLines returns human-readable stats ready for console display.
func (t *Tracker) SnapshotLines() []string {
	lines := make([]string, 0, 2)
	lines = append(lines, formatMapCounts("Spots by source", &t.sourceCounts))
	lines = append(lines, formatMapCounts("Spots by mode", &t.modeCounts))
	return lines
}

// IncrementCallCorrections increments the number of applied call corrections.
func (t *Tracker) IncrementCallCorrections() {
	t.callCorrections.Add(1)
}

// IncrementFrequencyCorrections increments the number of frequency corrections.
func (t *Tracker) IncrementFrequencyCorrections() {
	t.frequencyCorrections.Add(1)
}

// IncrementHarmonicSuppressions increments the number of spots dropped as harmonics.
func (t *Tracker) IncrementHarmonicSuppressions() {
	t.harmonicSuppressions.Add(1)
}

// CallCorrections returns the cumulative number of applied call corrections.
func (t *Tracker) CallCorrections() uint64 {
	return t.callCorrections.Load()
}

// FrequencyCorrections returns the cumulative number of frequency corrections.
func (t *Tracker) FrequencyCorrections() uint64 {
	return t.frequencyCorrections.Load()
}

// HarmonicSuppressions returns the cumulative number of dropped harmonics.
func (t *Tracker) HarmonicSuppressions() uint64 {
	return t.harmonicSuppressions.Load()
}

func formatMapCounts(label string, counts *sync.Map) string {
	var builder strings.Builder
	builder.WriteString(label)
	builder.WriteString(": ")
	first := true
	counts.Range(func(key, value any) bool {
		if !first {
			builder.WriteString(", ")
		}
		fmt.Fprintf(&builder, "%s=%d", key.(string), value.(*atomic.Uint64).Load())
		first = false
		return true
	})
	if first {
		builder.WriteString("(none)")
	}
	return builder.String()
}

func incrementCounter(m *sync.Map, key string) {
	if strings.TrimSpace(key) == "" {
		return
	}
	if value, ok := m.Load(key); ok {
		value.(*atomic.Uint64).Add(1)
		return
	}
	counter := &atomic.Uint64{}
	actual, loaded := m.LoadOrStore(key, counter)
	if loaded {
		actual.(*atomic.Uint64).Add(1)
		return
	}
	counter.Add(1)
}
