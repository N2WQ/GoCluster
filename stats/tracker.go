// Package stats tracks per-source and per-mode counters plus correction metrics
// for display in the dashboard and periodic console output.
package stats

import (
	"strconv"
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
	reputationDrops      atomic.Uint64
	reputationReasons    sync.Map // string -> *atomic.Uint64
	corrDecisionTotal    atomic.Uint64
	corrDecisionApplied  atomic.Uint64
	corrDecisionRejected atomic.Uint64
	corrFallbackApplied  atomic.Uint64
	corrPriorBonusUsed   atomic.Uint64
	corrDecisionReasons  sync.Map // string -> *atomic.Uint64
	corrDecisionPaths    sync.Map // string -> *atomic.Uint64
	corrDecisionRanks    sync.Map // string(rank) -> *atomic.Uint64
}

// NewTracker creates a new stats tracker with a start timestamp.
// Key aspects: Initializes atomic start time; counters are lazily created.
// Upstream: main.go stats initialization.
// Downstream: Tracker methods.
func NewTracker() *Tracker {
	t := &Tracker{}
	t.start.Store(time.Now().UTC().UnixNano())
	return t
}

// IncrementMode increments the counter for a given mode.
// Key aspects: Uses sync.Map + atomic to avoid global locks.
// Upstream: Spot ingest pipeline.
// Downstream: incrementCounter.
func (t *Tracker) IncrementMode(mode string) {
	incrementCounter(&t.modeCounts, mode)
}

// IncrementSource increments the counter for a given source node.
// Key aspects: Normalizes through incrementCounter.
// Upstream: Spot ingest pipeline.
// Downstream: incrementCounter.
func (t *Tracker) IncrementSource(source string) {
	incrementCounter(&t.sourceCounts, source)
}

// IncrementSourceMode increments the counter for a source/mode pair.
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

// GetModeCounts returns a copy of per-mode counts.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: Dashboard/metrics rendering.
// Downstream: atomic loads.
func (t *Tracker) GetModeCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	t.modeCounts.Range(func(key, value any) bool {
		counterKey, keyOK := mapCounterKey(key)
		if !keyOK {
			return true
		}
		counter, valueOK := mapCounterValue(value)
		if !valueOK {
			return true
		}
		counts[counterKey] = counter.Load()
		return true
	})
	return counts
}

// GetSourceCounts returns a copy of per-source counts.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: Dashboard/metrics rendering.
// Downstream: atomic loads.
func (t *Tracker) GetSourceCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	t.sourceCounts.Range(func(key, value any) bool {
		counterKey, keyOK := mapCounterKey(key)
		if !keyOK {
			return true
		}
		counter, valueOK := mapCounterValue(value)
		if !valueOK {
			return true
		}
		counts[counterKey] = counter.Load()
		return true
	})
	return counts
}

// GetSourceModeCounts returns a copy of source/mode counts.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: Dashboard/metrics rendering.
// Downstream: atomic loads.
func (t *Tracker) GetSourceModeCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	t.sourceModeCounts.Range(func(key, value any) bool {
		counterKey, keyOK := mapCounterKey(key)
		if !keyOK {
			return true
		}
		counter, valueOK := mapCounterValue(value)
		if !valueOK {
			return true
		}
		counts[counterKey] = counter.Load()
		return true
	})
	return counts
}

// SourceCardinality returns the number of distinct source keys tracked.
// Key aspects: Iterates sync.Map without allocating a map copy.
// Upstream: diagnostics/logging.
// Downstream: sync.Map Range.
func (t *Tracker) SourceCardinality() int {
	count := 0
	t.sourceCounts.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// SourceModeCardinality returns the number of distinct source|mode keys tracked.
// Key aspects: Iterates sync.Map without allocating a map copy.
// Upstream: diagnostics/logging.
// Downstream: sync.Map Range.
func (t *Tracker) SourceModeCardinality() int {
	count := 0
	t.sourceModeCounts.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// GetTotal returns the total count across all sources.
// Key aspects: Sums atomic counters from sourceCounts.
// Upstream: Dashboard/metrics rendering.
// Downstream: atomic loads.
func (t *Tracker) GetTotal() uint64 {
	var total uint64
	t.sourceCounts.Range(func(_, value any) bool {
		counter, ok := mapCounterValue(value)
		if ok {
			total += counter.Load()
		}
		return true
	})
	return total
}

// GetUptime returns tracker uptime since creation/reset.
// Key aspects: Uses stored UnixNano start.
// Upstream: Dashboard/metrics rendering.
// Downstream: time.Since.
func (t *Tracker) GetUptime() time.Duration {
	start := t.start.Load()
	return time.Since(time.Unix(0, start))
}

// Reset resets all counters and start time.
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
	t.start.Store(time.Now().UTC().UnixNano())
}

// SnapshotLines formats summary lines for console output.
// Key aspects: Renders source and mode counts via formatMapCounts.
// Upstream: Dashboard/console ticker.
// Downstream: formatMapCounts.
func (t *Tracker) SnapshotLines() []string {
	lines := make([]string, 0, 2)
	lines = append(lines, formatMapCounts("Spots by source", &t.sourceCounts))
	lines = append(lines, formatMapCounts("Spots by mode", &t.modeCounts))
	return lines
}

// IncrementCallCorrections increments applied call correction counter.
// Key aspects: Atomic increment.
// Upstream: Correction pipeline.
// Downstream: atomic counter.
func (t *Tracker) IncrementCallCorrections() {
	t.callCorrections.Add(1)
}

// Purpose: Record a correction decision outcome/reason/path for observability.
// Key aspects: Tracks attempts, rejects by reason, path distribution, and fallback depth.
// Upstream: call correction decision observer.
// Downstream: atomic counters and sync.Map counters.
func (t *Tracker) ObserveCallCorrectionDecision(path, decision, reason string, candidateRank int, priorBonusApplied bool) {
	if t == nil {
		return
	}
	t.corrDecisionTotal.Add(1)

	decision = strings.ToLower(strings.TrimSpace(decision))
	switch decision {
	case "applied":
		t.corrDecisionApplied.Add(1)
	case "rejected":
		t.corrDecisionRejected.Add(1)
	default:
		incrementCounter(&t.corrDecisionReasons, "unknown_decision")
	}

	path = strings.ToLower(strings.TrimSpace(path))
	if path == "" {
		path = "unknown"
	}
	incrementCounter(&t.corrDecisionPaths, path)

	if candidateRank > 0 {
		incrementCounter(&t.corrDecisionRanks, strconv.Itoa(candidateRank))
	}
	if decision == "applied" && strings.HasPrefix(path, "consensus") && candidateRank > 1 {
		t.corrFallbackApplied.Add(1)
	}
	if priorBonusApplied {
		t.corrPriorBonusUsed.Add(1)
	}
	if decision == "rejected" {
		reason = strings.ToLower(strings.TrimSpace(reason))
		if reason == "" {
			reason = "unknown"
		}
		incrementCounter(&t.corrDecisionReasons, reason)
	}
}

// Purpose: Increment applied frequency correction counter.
// IncrementFrequencyCorrections increments applied frequency correction counter.
// Key aspects: Atomic increment.
// Upstream: Correction pipeline.
// Downstream: atomic counter.
func (t *Tracker) IncrementFrequencyCorrections() {
	t.frequencyCorrections.Add(1)
}

// IncrementHarmonicSuppressions increments harmonic suppression counter.
// Key aspects: Atomic increment.
// Upstream: Harmonic detector drop path.
// Downstream: atomic counter.
func (t *Tracker) IncrementHarmonicSuppressions() {
	t.harmonicSuppressions.Add(1)
}

// IncrementUnlicensedDrops increments unlicensed drop counter.
// Key aspects: Atomic increment.
// Upstream: License enforcement in pipeline.
// Downstream: atomic counter.
func (t *Tracker) IncrementUnlicensedDrops() {
	t.unlicensedDrops.Add(1)
}

// CallCorrections returns total call correction count.
// Key aspects: Atomic load.
// Upstream: Dashboard/metrics.
// Downstream: atomic counter.
func (t *Tracker) CallCorrections() uint64 {
	return t.callCorrections.Load()
}

// FrequencyCorrections returns total frequency correction count.
// Key aspects: Atomic load.
// Upstream: Dashboard/metrics.
// Downstream: atomic counter.
func (t *Tracker) FrequencyCorrections() uint64 {
	return t.frequencyCorrections.Load()
}

// HarmonicSuppressions returns total harmonic suppression count.
// Key aspects: Atomic load.
// Upstream: Dashboard/metrics.
// Downstream: atomic counter.
func (t *Tracker) HarmonicSuppressions() uint64 {
	return t.harmonicSuppressions.Load()
}

// UnlicensedDrops returns total unlicensed drop count.
// Key aspects: Atomic load.
// Upstream: Dashboard/metrics.
// Downstream: atomic counter.
func (t *Tracker) UnlicensedDrops() uint64 {
	return t.unlicensedDrops.Load()
}

// Purpose: Return total correction decisions observed (applied + rejected).
// Key aspects: Atomic load.
// Upstream: dashboard/metrics output.
// Downstream: atomic counter.
func (t *Tracker) CorrectionDecisionTotal() uint64 {
	return t.corrDecisionTotal.Load()
}

// Purpose: Return total applied correction decisions observed.
// Key aspects: Atomic load.
// Upstream: dashboard/metrics output.
// Downstream: atomic counter.
func (t *Tracker) CorrectionDecisionApplied() uint64 {
	return t.corrDecisionApplied.Load()
}

// Purpose: Return total rejected correction decisions observed.
// Key aspects: Atomic load.
// Upstream: dashboard/metrics output.
// Downstream: atomic counter.
func (t *Tracker) CorrectionDecisionRejected() uint64 {
	return t.corrDecisionRejected.Load()
}

// Purpose: Return applied decisions that succeeded after top-1 consensus fallback.
// Key aspects: Atomic load.
// Upstream: dashboard/metrics output.
// Downstream: atomic counter.
func (t *Tracker) CorrectionFallbackApplied() uint64 {
	return t.corrFallbackApplied.Load()
}

// Purpose: Return decisions where prior bonus logic was used.
// Key aspects: Atomic load.
// Upstream: dashboard/metrics output.
// Downstream: atomic counter.
func (t *Tracker) CorrectionPriorBonusUsed() uint64 {
	return t.corrPriorBonusUsed.Load()
}

// Purpose: Return a copy of correction rejection reasons.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: dashboard/metrics output.
// Downstream: atomic loads.
func (t *Tracker) CorrectionDecisionReasons() map[string]uint64 {
	counts := make(map[string]uint64)
	t.corrDecisionReasons.Range(func(key, value any) bool {
		counterKey, keyOK := mapCounterKey(key)
		if !keyOK {
			return true
		}
		counter, valueOK := mapCounterValue(value)
		if !valueOK {
			return true
		}
		counts[counterKey] = counter.Load()
		return true
	})
	return counts
}

// Purpose: Return a copy of correction decision path counts.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: dashboard/metrics output.
// Downstream: atomic loads.
func (t *Tracker) CorrectionDecisionPaths() map[string]uint64 {
	counts := make(map[string]uint64)
	t.corrDecisionPaths.Range(func(key, value any) bool {
		counterKey, keyOK := mapCounterKey(key)
		if !keyOK {
			return true
		}
		counter, valueOK := mapCounterValue(value)
		if !valueOK {
			return true
		}
		counts[counterKey] = counter.Load()
		return true
	})
	return counts
}

// Purpose: Return a copy of correction candidate-rank attempt counts.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: dashboard/metrics output.
// Downstream: atomic loads.
func (t *Tracker) CorrectionDecisionRanks() map[string]uint64 {
	counts := make(map[string]uint64)
	t.corrDecisionRanks.Range(func(key, value any) bool {
		counterKey, keyOK := mapCounterKey(key)
		if !keyOK {
			return true
		}
		counter, valueOK := mapCounterValue(value)
		if !valueOK {
			return true
		}
		counts[counterKey] = counter.Load()
		return true
	})
	return counts
}

// Purpose: Increment reputation drop counters by reason.
// IncrementReputationDrop increments reputation drop counters by reason.
// Key aspects: Tracks total drops plus reason-specific buckets.
// Upstream: Telnet reputation gate.
// Downstream: atomic counters and sync.Map.
func (t *Tracker) IncrementReputationDrop(reason string) {
	t.reputationDrops.Add(1)
	incrementCounter(&t.reputationReasons, reason)
}

// ReputationDrops returns total reputation drop count.
// Key aspects: Atomic load.
// Upstream: Dashboard/metrics.
// Downstream: atomic counter.
func (t *Tracker) ReputationDrops() uint64 {
	return t.reputationDrops.Load()
}

// ReputationDropReasons returns a copy of reputation drop reasons.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: Dashboard/metrics rendering.
// Downstream: atomic loads.
func (t *Tracker) ReputationDropReasons() map[string]uint64 {
	reasons := make(map[string]uint64)
	t.reputationReasons.Range(func(key, value any) bool {
		counterKey, keyOK := mapCounterKey(key)
		if !keyOK {
			return true
		}
		counter, valueOK := mapCounterValue(value)
		if !valueOK {
			return true
		}
		reasons[counterKey] = counter.Load()
		return true
	})
	return reasons
}

// Purpose: Format a sync.Map of counters into a "label: key=value" string.
// Key aspects: Stable order is not guaranteed; emits "(none)" when empty.
// Upstream: Tracker.SnapshotLines.
// Downstream: atomic loads, strconv formatting.
func formatMapCounts(label string, counts *sync.Map) string {
	var builder strings.Builder
	builder.WriteString(label)
	builder.WriteString(": ")
	first := true
	counts.Range(func(key, value any) bool {
		counterKey, keyOK := mapCounterKey(key)
		if !keyOK {
			return true
		}
		counter, valueOK := mapCounterValue(value)
		if !valueOK {
			return true
		}
		if !first {
			builder.WriteString(", ")
		}
		builder.WriteString(counterKey)
		builder.WriteByte('=')
		builder.WriteString(strconv.FormatUint(counter.Load(), 10))
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
		if counter, valid := mapCounterValue(value); valid {
			counter.Add(1)
			return
		}
	}
	counter := &atomic.Uint64{}
	actual, loaded := m.LoadOrStore(key, counter)
	if loaded {
		if existing, valid := mapCounterValue(actual); valid {
			existing.Add(1)
			return
		}
		counter.Add(1)
		m.Store(key, counter)
		return
	}
	counter.Add(1)
}

func mapCounterKey(key any) (string, bool) {
	keyStr, ok := key.(string)
	if !ok {
		return "", false
	}
	return keyStr, true
}

func mapCounterValue(value any) (*atomic.Uint64, bool) {
	counter, ok := value.(*atomic.Uint64)
	if !ok || counter == nil {
		return nil, false
	}
	return counter, true
}
