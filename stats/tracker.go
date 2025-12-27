// Package stats tracks per-source and per-mode counters plus correction metrics
// for display in the dashboard and periodic console output.
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
	unlicensedDrops      atomic.Uint64
}

// Purpose: Create a new stats tracker with a start timestamp.
// Key aspects: Initializes atomic start time; counters are lazily created.
// Upstream: main.go stats initialization.
// Downstream: Tracker methods.
func NewTracker() *Tracker {
	t := &Tracker{}
	t.start.Store(time.Now().UnixNano())
	return t
}

// Purpose: Increment the counter for a given mode.
// Key aspects: Uses sync.Map + atomic to avoid global locks.
// Upstream: Spot ingest pipeline.
// Downstream: incrementCounter.
func (t *Tracker) IncrementMode(mode string) {
	incrementCounter(&t.modeCounts, mode)
}

// Purpose: Increment the counter for a given source node.
// Key aspects: Normalizes through incrementCounter.
// Upstream: Spot ingest pipeline.
// Downstream: incrementCounter.
func (t *Tracker) IncrementSource(source string) {
	incrementCounter(&t.sourceCounts, source)
}

// Purpose: Increment the counter for a source/mode pair.
// Key aspects: Normalizes strings and combines into a composite key.
// Upstream: Spot ingest pipeline.
// Downstream: incrementCounter.
func (t *Tracker) IncrementSourceMode(source, mode string) {
	source = strings.ToUpper(strings.TrimSpace(source))
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if source == "" || mode == "" {
		return
	}
	key := source + "|" + mode
	incrementCounter(&t.sourceModeCounts, key)
}

// Purpose: Return a copy of per-mode counts.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: Dashboard/metrics rendering.
// Downstream: atomic loads.
func (t *Tracker) GetModeCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	t.modeCounts.Range(func(key, value any) bool {
		counts[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return counts
}

// Purpose: Return a copy of per-source counts.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: Dashboard/metrics rendering.
// Downstream: atomic loads.
func (t *Tracker) GetSourceCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	t.sourceCounts.Range(func(key, value any) bool {
		counts[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return counts
}

// Purpose: Return a copy of source/mode counts.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: Dashboard/metrics rendering.
// Downstream: atomic loads.
func (t *Tracker) GetSourceModeCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	t.sourceModeCounts.Range(func(key, value any) bool {
		counts[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return counts
}

// Purpose: Return the total count across all sources.
// Key aspects: Sums atomic counters from sourceCounts.
// Upstream: Dashboard/metrics rendering.
// Downstream: atomic loads.
func (t *Tracker) GetTotal() uint64 {
	var total uint64
	t.sourceCounts.Range(func(_, value any) bool {
		total += value.(*atomic.Uint64).Load()
		return true
	})
	return total
}

// Purpose: Return tracker uptime since creation/reset.
// Key aspects: Uses stored UnixNano start.
// Upstream: Dashboard/metrics rendering.
// Downstream: time.Since.
func (t *Tracker) GetUptime() time.Duration {
	start := t.start.Load()
	return time.Since(time.Unix(0, start))
}

// Purpose: Reset all counters and start time.
// Key aspects: Deletes all sync.Map entries and resets start.
// Upstream: Admin reset flows.
// Downstream: sync.Map delete, atomic store.
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

// Purpose: Format summary lines for console output.
// Key aspects: Renders source and mode counts via formatMapCounts.
// Upstream: Dashboard/console ticker.
// Downstream: formatMapCounts.
func (t *Tracker) SnapshotLines() []string {
	lines := make([]string, 0, 2)
	lines = append(lines, formatMapCounts("Spots by source", &t.sourceCounts))
	lines = append(lines, formatMapCounts("Spots by mode", &t.modeCounts))
	return lines
}

// Purpose: Increment applied call correction counter.
// Key aspects: Atomic increment.
// Upstream: Correction pipeline.
// Downstream: atomic counter.
func (t *Tracker) IncrementCallCorrections() {
	t.callCorrections.Add(1)
}

// Purpose: Increment applied frequency correction counter.
// Key aspects: Atomic increment.
// Upstream: Correction pipeline.
// Downstream: atomic counter.
func (t *Tracker) IncrementFrequencyCorrections() {
	t.frequencyCorrections.Add(1)
}

// Purpose: Increment harmonic suppression counter.
// Key aspects: Atomic increment.
// Upstream: Harmonic detector drop path.
// Downstream: atomic counter.
func (t *Tracker) IncrementHarmonicSuppressions() {
	t.harmonicSuppressions.Add(1)
}

// Purpose: Increment unlicensed drop counter.
// Key aspects: Atomic increment.
// Upstream: License enforcement in pipeline.
// Downstream: atomic counter.
func (t *Tracker) IncrementUnlicensedDrops() {
	t.unlicensedDrops.Add(1)
}

// Purpose: Return total call correction count.
// Key aspects: Atomic load.
// Upstream: Dashboard/metrics.
// Downstream: atomic counter.
func (t *Tracker) CallCorrections() uint64 {
	return t.callCorrections.Load()
}

// Purpose: Return total frequency correction count.
// Key aspects: Atomic load.
// Upstream: Dashboard/metrics.
// Downstream: atomic counter.
func (t *Tracker) FrequencyCorrections() uint64 {
	return t.frequencyCorrections.Load()
}

// Purpose: Return total harmonic suppression count.
// Key aspects: Atomic load.
// Upstream: Dashboard/metrics.
// Downstream: atomic counter.
func (t *Tracker) HarmonicSuppressions() uint64 {
	return t.harmonicSuppressions.Load()
}

// Purpose: Return total unlicensed drop count.
// Key aspects: Atomic load.
// Upstream: Dashboard/metrics.
// Downstream: atomic counter.
func (t *Tracker) UnlicensedDrops() uint64 {
	return t.unlicensedDrops.Load()
}

// Purpose: Format a sync.Map of counters into a "label: key=value" string.
// Key aspects: Stable order is not guaranteed; emits "(none)" when empty.
// Upstream: Tracker.SnapshotLines.
// Downstream: atomic loads, fmt.Fprintf.
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

// Purpose: Increment a per-key atomic counter stored in a sync.Map.
// Key aspects: Uses LoadOrStore to avoid races on first insert.
// Upstream: Tracker increment methods.
// Downstream: atomic.Uint64.
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
