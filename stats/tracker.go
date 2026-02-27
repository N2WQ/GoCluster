// Package stats tracks per-source and per-mode counters plus correction metrics
// for display in the dashboard and periodic console output.
package stats

import (
	"dxcluster/strutil"
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
	corrAppliedReasons   sync.Map // string -> *atomic.Uint64
	corrDecisionReasons  sync.Map // string -> *atomic.Uint64
	corrDecisionPaths    sync.Map // string -> *atomic.Uint64
	corrDecisionRanks    sync.Map // string(rank) -> *atomic.Uint64
	stabHeld             atomic.Uint64
	stabImmediate        atomic.Uint64
	stabDelayed          atomic.Uint64
	stabSuppressed       atomic.Uint64
	stabOverflowRelease  atomic.Uint64
	stabHeldByReason     sync.Map // string -> *atomic.Uint64
	stabImmediateReason  sync.Map // string -> *atomic.Uint64
	stabDelayedByReason  sync.Map // string -> *atomic.Uint64
	stabSuppressedReason sync.Map // string -> *atomic.Uint64
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
	source = strutil.NormalizeUpper(source)
	mode = strutil.NormalizeUpper(mode)
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
	return snapshotCounterMap(&t.modeCounts)
}

// GetSourceCounts returns a copy of per-source counts.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: Dashboard/metrics rendering.
// Downstream: atomic loads.
func (t *Tracker) GetSourceCounts() map[string]uint64 {
	return snapshotCounterMap(&t.sourceCounts)
}

// GetSourceModeCounts returns a copy of source/mode counts.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: Dashboard/metrics rendering.
// Downstream: atomic loads.
func (t *Tracker) GetSourceModeCounts() map[string]uint64 {
	return snapshotCounterMap(&t.sourceModeCounts)
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
	t.stabHeldByReason.Range(func(key, _ any) bool {
		t.stabHeldByReason.Delete(key)
		return true
	})
	t.stabImmediateReason.Range(func(key, _ any) bool {
		t.stabImmediateReason.Delete(key)
		return true
	})
	t.stabDelayedByReason.Range(func(key, _ any) bool {
		t.stabDelayedByReason.Delete(key)
		return true
	})
	t.stabSuppressedReason.Range(func(key, _ any) bool {
		t.stabSuppressedReason.Delete(key)
		return true
	})
	t.corrAppliedReasons.Range(func(key, _ any) bool {
		t.corrAppliedReasons.Delete(key)
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

// IncrementStabilizerHeld records that a telnet spot was delayed by the
// stabilizer because it lacked recent-on-band support.
func (t *Tracker) IncrementStabilizerHeld() {
	if t == nil {
		return
	}
	t.stabHeld.Add(1)
}

// IncrementStabilizerHeldReason records the reason bucket for delayed spots.
func (t *Tracker) IncrementStabilizerHeldReason(reason string) {
	if t == nil {
		return
	}
	incrementCounter(&t.stabHeldByReason, strings.ToLower(strings.TrimSpace(reason)))
}

// IncrementStabilizerReleasedImmediate records telnet spots delivered without a
// stabilizer delay.
func (t *Tracker) IncrementStabilizerReleasedImmediate() {
	if t == nil {
		return
	}
	t.stabImmediate.Add(1)
}

// IncrementStabilizerReleasedImmediateReason records reason buckets for spots
// delivered immediately while stabilizer was enabled.
func (t *Tracker) IncrementStabilizerReleasedImmediateReason(reason string) {
	if t == nil {
		return
	}
	incrementCounter(&t.stabImmediateReason, strings.ToLower(strings.TrimSpace(reason)))
}

// IncrementStabilizerReleasedDelayed records telnet spots delivered after the
// stabilizer delay window elapsed.
func (t *Tracker) IncrementStabilizerReleasedDelayed() {
	if t == nil {
		return
	}
	t.stabDelayed.Add(1)
}

// IncrementStabilizerReleasedDelayedReason records reason buckets for delayed
// releases that eventually reached telnet output.
func (t *Tracker) IncrementStabilizerReleasedDelayedReason(reason string) {
	if t == nil {
		return
	}
	incrementCounter(&t.stabDelayedByReason, strings.ToLower(strings.TrimSpace(reason)))
}

// IncrementStabilizerSuppressedTimeout records telnet spots dropped after delay
// when the timeout action is suppress.
func (t *Tracker) IncrementStabilizerSuppressedTimeout() {
	if t == nil {
		return
	}
	t.stabSuppressed.Add(1)
}

// IncrementStabilizerSuppressedTimeoutReason records reason buckets for delayed
// spots suppressed after timeout checks were exhausted.
func (t *Tracker) IncrementStabilizerSuppressedTimeoutReason(reason string) {
	if t == nil {
		return
	}
	incrementCounter(&t.stabSuppressedReason, strings.ToLower(strings.TrimSpace(reason)))
}

// IncrementStabilizerOverflowRelease records delayed spots released immediately
// due to stabilizer queue overflow (fail-open behavior).
func (t *Tracker) IncrementStabilizerOverflowRelease() {
	if t == nil {
		return
	}
	t.stabOverflowRelease.Add(1)
}

// Purpose: Record a correction decision outcome/reason/path for observability.
// Key aspects: Tracks attempts, rejects by reason, path distribution, and fallback depth.
// Upstream: call correction decision observer.
// Downstream: atomic counters and sync.Map counters.
func (t *Tracker) ObserveCallCorrectionDecision(path, decision, reason string, candidateRank int) {
	if t == nil {
		return
	}
	t.corrDecisionTotal.Add(1)

	decision = strings.ToLower(strings.TrimSpace(decision))
	switch decision {
	case "applied":
		t.corrDecisionApplied.Add(1)
		applyReason := strings.ToLower(strings.TrimSpace(reason))
		if applyReason == "" {
			applyReason = "unknown"
		}
		incrementCounter(&t.corrAppliedReasons, applyReason)
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

// StabilizerHeld returns the count of spots delayed by the stabilizer.
func (t *Tracker) StabilizerHeld() uint64 {
	if t == nil {
		return 0
	}
	return t.stabHeld.Load()
}

// StabilizerReleasedImmediate returns spots delivered immediately when the
// stabilizer was active.
func (t *Tracker) StabilizerReleasedImmediate() uint64 {
	if t == nil {
		return 0
	}
	return t.stabImmediate.Load()
}

// StabilizerReleasedDelayed returns spots delivered after stabilizer delay.
func (t *Tracker) StabilizerReleasedDelayed() uint64 {
	if t == nil {
		return 0
	}
	return t.stabDelayed.Load()
}

// StabilizerSuppressedTimeout returns spots suppressed after the delay window.
func (t *Tracker) StabilizerSuppressedTimeout() uint64 {
	if t == nil {
		return 0
	}
	return t.stabSuppressed.Load()
}

// StabilizerOverflowRelease returns fail-open immediate releases caused by a
// full stabilizer queue.
func (t *Tracker) StabilizerOverflowRelease() uint64 {
	if t == nil {
		return 0
	}
	return t.stabOverflowRelease.Load()
}

// StabilizerHeldByReason returns delayed-spot counts by delay reason.
func (t *Tracker) StabilizerHeldByReason() map[string]uint64 {
	if t == nil {
		return map[string]uint64{}
	}
	return snapshotCounterMap(&t.stabHeldByReason)
}

// StabilizerReleasedImmediateByReason returns immediate-release counts by reason.
func (t *Tracker) StabilizerReleasedImmediateByReason() map[string]uint64 {
	if t == nil {
		return map[string]uint64{}
	}
	return snapshotCounterMap(&t.stabImmediateReason)
}

// StabilizerReleasedDelayedByReason returns delayed-release counts by reason.
func (t *Tracker) StabilizerReleasedDelayedByReason() map[string]uint64 {
	if t == nil {
		return map[string]uint64{}
	}
	return snapshotCounterMap(&t.stabDelayedByReason)
}

// StabilizerSuppressedByReason returns suppression counts by reason.
func (t *Tracker) StabilizerSuppressedByReason() map[string]uint64 {
	if t == nil {
		return map[string]uint64{}
	}
	return snapshotCounterMap(&t.stabSuppressedReason)
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

// Purpose: Return a copy of correction rejection reasons.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: dashboard/metrics output.
// Downstream: atomic loads.
func (t *Tracker) CorrectionDecisionReasons() map[string]uint64 {
	return snapshotCounterMap(&t.corrDecisionReasons)
}

// Purpose: Return a copy of correction applied reasons.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: resolver-primary telemetry and dashboards.
// Downstream: atomic loads.
func (t *Tracker) CorrectionDecisionAppliedReasons() map[string]uint64 {
	return snapshotCounterMap(&t.corrAppliedReasons)
}

// Purpose: Return a copy of correction decision path counts.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: dashboard/metrics output.
// Downstream: atomic loads.
func (t *Tracker) CorrectionDecisionPaths() map[string]uint64 {
	return snapshotCounterMap(&t.corrDecisionPaths)
}

// Purpose: Return a copy of correction candidate-rank attempt counts.
// Key aspects: Iterates sync.Map and reads atomics.
// Upstream: dashboard/metrics output.
// Downstream: atomic loads.
func (t *Tracker) CorrectionDecisionRanks() map[string]uint64 {
	return snapshotCounterMap(&t.corrDecisionRanks)
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
	return snapshotCounterMap(&t.reputationReasons)
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

func snapshotCounterMap(m *sync.Map) map[string]uint64 {
	counts := make(map[string]uint64)
	if m == nil {
		return counts
	}
	m.Range(func(key, value any) bool {
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
