package cluster

import (
	"fmt"
	"time"

	"dxcluster/dedup"
)

// secondaryShadowReporter borrows the existing stats ticker. Its fixed prior
// snapshots are process-local; logs are disposable and no totals are restored
// after restart. Every line is cumulative so a later surviving line can cover
// an earlier dropped summary. No dedupe lock is held while emit executes.
type secondaryShadowReporter struct {
	previous [3]dedup.ShadowSnapshot
	sequence uint64
}

type secondaryPolicyEngines [3]*dedup.SecondaryDeduper

func (r *secondaryShadowReporter) report(now time.Time, engines secondaryPolicyEngines, emit func(string)) {
	r.sequence++
	for policy, engine := range engines {
		if engine == nil {
			continue
		}
		current, enabled := engine.ShadowStats()
		if !enabled {
			continue
		}
		previous := r.previous[policy]
		if !previous.StartedAt.Equal(current.StartedAt) {
			previous = dedup.ShadowSnapshot{StartedAt: current.StartedAt}
		}
		// Copy before logging: neither a slow consumer nor logger backpressure
		// may retain a policy lock or reset counters after a dropped record.
		r.previous[policy] = current
		for class, counters := range current.Cohorts {
			delta := shadowCounterDelta(counters, previous.Cohorts[class])
			emit(formatSecondaryShadowSummary(policy, class, r.sequence, now, current, counters, delta))
		}
	}
}

func shadowCounterDelta(current, previous dedup.ShadowCounters) dedup.ShadowCounters {
	return dedup.ShadowCounters{
		BaselineForwarded:    current.BaselineForwarded - previous.BaselineForwarded,
		BaselineSuppressed:   current.BaselineSuppressed - previous.BaselineSuppressed,
		AddedNewEntry:        current.AddedNewEntry - previous.AddedNewEntry,
		AddedReportException: current.AddedReportException - previous.AddedReportException,
		AddedFrequencyChange: current.AddedFrequencyChange - previous.AddedFrequencyChange,
		LostForwards:         current.LostForwards - previous.LostForwards,
		FrequencyAdmissions:  current.FrequencyAdmissions - previous.FrequencyAdmissions,
		LimitBlocked:         current.LimitBlocked - previous.LimitBlocked,
	}
}

func formatSecondaryShadowSummary(policy, class int, sequence uint64, now time.Time, snapshot dedup.ShadowSnapshot, c, delta dedup.ShadowCounters) string {
	policies := [...]string{"FAST", "MED", "SLOW"}
	return fmt.Sprintf("Dedupe shadow admissions policy=%s class=%s run=%s at=%s seq=%d cumulative[base=%d suppressed=%d added_new=%d added_report=%d added_frequency=%d lost=%d frequency_admissions=%d limit_blocked=%d net_pct=%s limit_pct=%s] interval[base=%d suppressed=%d added_new=%d added_report=%d added_frequency=%d lost=%d frequency_admissions=%d limit_blocked=%d] cache[baseline=%d candidate=%d]",
		policies[policy], dedup.ShadowClassName(class), snapshot.StartedAt.Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), sequence,
		c.BaselineForwarded, c.BaselineSuppressed, c.AddedNewEntry, c.AddedReportException, c.AddedFrequencyChange, c.LostForwards,
		c.FrequencyAdmissions, c.LimitBlocked, shadowNetPercent(c), shadowLimitPercent(c),
		delta.BaselineForwarded, delta.BaselineSuppressed, delta.AddedNewEntry, delta.AddedReportException, delta.AddedFrequencyChange, delta.LostForwards,
		delta.FrequencyAdmissions, delta.LimitBlocked, snapshot.BaselineCacheSize, snapshot.CandidateCacheSize)
}

func shadowNetPercent(c dedup.ShadowCounters) string {
	if c.BaselineForwarded == 0 {
		return "unavailable"
	}
	// Convert before subtraction: a negative net is meaningful, not uint wrap.
	added := float64(c.AddedNewEntry) + float64(c.AddedReportException) + float64(c.AddedFrequencyChange)
	return fmt.Sprintf("%.2f", (added-float64(c.LostForwards))*100/float64(c.BaselineForwarded))
}

func shadowLimitPercent(c dedup.ShadowCounters) string {
	// Floating-point addition avoids overflow in a display-only denominator.
	denominator := float64(c.FrequencyAdmissions) + float64(c.LimitBlocked)
	if denominator == 0 {
		return "unavailable"
	}
	return fmt.Sprintf("%.2f", float64(c.LimitBlocked)*100/denominator)
}
