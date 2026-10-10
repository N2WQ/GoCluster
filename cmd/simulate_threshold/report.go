package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

func thresholdPercent(n, denominator int) string {
	if denominator == 0 {
		return "N/A"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(n)/float64(denominator))
}

func reportThreshold(out io.Writer, dbPath string, decisions []decisionRecord, settings thresholdSettings) error {
	r := compareThresholds(decisions, settings)
	var b strings.Builder
	fmt.Fprintf(&b, "HYPOTHETICAL THRESHOLD COMPARISON\nDatabase: %q\nSelected distance-1..3 rows: %d\n", dbPath, len(decisions))
	fmt.Fprintln(&b, "Outcome percentages use all selected rows.")
	fmt.Fprintf(&b, "Applied: %d (%s)\nRejected: %d (%s)\nUnknown: %d (%s)\n",
		r.applied, thresholdPercent(r.applied, len(decisions)), r.rejected, thresholdPercent(r.rejected, len(decisions)),
		r.unknown, thresholdPercent(r.unknown, len(decisions)))
	writeBaselineTuples(&b, r.baselines)
	writeUnknownOutcomes(&b, r.unknownOutcomes)
	fmt.Fprintf(&b, "\nHypothetical cuts: winner reports >= %d; advantage >= %d; logged score >= %d\n",
		settings.reports, settings.advantage, settings.confidence)
	fmt.Fprintln(&b, "Distance-3 assumes fixed extras: advantage +1, logged score +5; no extra reports.")
	fmt.Fprintln(&b, "These are scalar comparisons, not loaded current settings or a complete correction-model replay.")
	fmt.Fprintln(&b, "Stored counts/scores are assumed valid. Logged confidence is an unvalidated score.")
	fmt.Fprintf(&b, "Retained applied: %d\nRemoved applied: %d\nAdded rejected: %d\n", r.retained, r.removed, r.added)
	fmt.Fprintf(&b, "Recognized rows passing: %d/%d (%s)\n", r.retained+r.added, r.applied+r.rejected,
		thresholdPercent(r.retained+r.added, r.applied+r.rejected))
	net := r.added - r.removed
	fmt.Fprintf(&b, "Net change from applied: %+d\nChange relative to applied count: %s\n", net, thresholdPercent(net, r.applied))
	fmt.Fprintf(&b, "Unknown rows passing (separate): %d/%d; excluded from recognized totals and net change.\n", r.unknownPassed, r.unknown)
	writeAddedSamples(&b, r)
	fmt.Fprintln(&b, "\nNo stability, accuracy, calibration, reliability or optimal-setting prediction is established by these observations.")
	_, err := fmt.Fprint(out, b.String())
	return err
}

func writeBaselineTuples(b *strings.Builder, counts map[thresholdSettings]int) {
	fmt.Fprintln(b, "\nStored baseline tuples across all selected rows (reports, advantage, score):")
	if len(counts) == 0 {
		fmt.Fprintln(b, "  (none; no baseline inferred)")
		return
	}
	keys := make([]thresholdSettings, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, c := keys[i], keys[j]
		if a.reports != c.reports {
			return a.reports < c.reports
		}
		if a.advantage != c.advantage {
			return a.advantage < c.advantage
		}
		return a.confidence < c.confidence
	})
	for _, key := range keys {
		fmt.Fprintf(b, "  (%d, %d, %d): %d\n", key.reports, key.advantage, key.confidence, counts[key])
	}
}

func writeUnknownOutcomes(b *strings.Builder, counts map[string]int) {
	fmt.Fprintln(b, "\nUnknown outcome values:")
	if len(counts) == 0 {
		fmt.Fprintln(b, "  (none)")
		return
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(b, "  %s: %d\n", key, counts[key])
	}
}

func writeAddedSamples(b *strings.Builder, r thresholdComparison) {
	fmt.Fprintln(b, "\nRecognized rejections passing by distance:")
	for distance := 1; distance <= 3; distance++ {
		fmt.Fprintf(b, "  Distance-%d: %d\n", distance, r.addedByDistance[distance])
	}
	fmt.Fprintln(b, "Added-row samples (at most 10; passing does not establish correctness):")
	if len(r.addedSamples) == 0 {
		fmt.Fprintln(b, "  (none)")
	}
	for i := range r.addedSamples {
		d := &r.addedSamples[i]
		fmt.Fprintf(b, "  id=%d %q -> %q distance=%d support=%d/%d advantage=%d logged score=%d reason=%q\n",
			d.id, d.subject, d.winner, d.distance, d.winnerSupport, d.totalReporters,
			d.winnerSupport-d.subjectSupport, d.winnerConfidence, d.reason)
	}
}
