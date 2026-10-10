package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

func distancePercent(n, denominator int) string {
	if denominator == 0 {
		return "N/A"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(n)/float64(denominator))
}

func reportDistanceAnalysis(out io.Writer, dbPath string, analysis *distanceAnalysis) error {
	var b strings.Builder
	fmt.Fprintf(&b, "DESCRIPTIVE DISTANCE SUMMARIES\nDatabase: %q\nAll selected rows (every distance): %d\n", dbPath, analysis.totalDecisions)
	fmt.Fprintln(&b, "Overall outcome percentages use all selected rows.")
	fmt.Fprintf(&b, "Applied: %d (%s)\nRejected: %d (%s)\nUnknown: %d (%s)\n",
		analysis.appliedCount, distancePercent(analysis.appliedCount, analysis.totalDecisions),
		analysis.rejectedCount, distancePercent(analysis.rejectedCount, analysis.totalDecisions),
		analysis.unknownCount, distancePercent(analysis.unknownCount, analysis.totalDecisions))
	fmt.Fprintln(&b, "\nSummary by stored distance (percentages use that distance's rows):")
	distances := make([]int, 0, len(analysis.byDistance))
	for distance := range analysis.byDistance {
		distances = append(distances, distance)
	}
	sort.Ints(distances)
	if len(distances) == 0 {
		fmt.Fprintln(&b, "  (none; applied mean/median N/A)")
	}
	for _, distance := range distances {
		stats := analysis.byDistance[distance]
		mean, median, available := confidenceSummary(stats.confidences)
		meanText, medianText := "N/A", "N/A"
		if available {
			meanText, medianText = fmt.Sprintf("%.1f", mean), fmt.Sprintf("%.1f", median)
		}
		fmt.Fprintf(&b, "Distance %d: rows=%d applied=%d (%s) rejected=%d (%s) unknown=%d (%s)\n",
			distance, stats.totalDecisions, stats.appliedCount, distancePercent(stats.appliedCount, stats.totalDecisions),
			stats.rejectedCount, distancePercent(stats.rejectedCount, stats.totalDecisions),
			stats.unknownCount, distancePercent(stats.unknownCount, stats.totalDecisions))
		fmt.Fprintf(&b, "  Applied logged scores: n=%d mean=%s median=%s\n", stats.appliedCount, meanText, medianText)
		writeDistanceReasons(&b, stats)
	}
	fmt.Fprintln(&b, "\nUnknown outcome values (across all selected rows):")
	keys := make([]string, 0, len(analysis.unknownOutcomes))
	for key := range analysis.unknownOutcomes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		fmt.Fprintln(&b, "  (none)")
	}
	for _, key := range keys {
		fmt.Fprintf(&b, "  %s: %d\n", key, analysis.unknownOutcomes[key])
	}
	fmt.Fprintln(&b, "\nScores are unvalidated logged integers, not measured correctness probabilities.")
	fmt.Fprintln(&b, "Means/medians are approximate one-decimal floating presentations; no applied rows means N/A.")
	fmt.Fprintln(&b, "No correlation, accuracy, calibration, reliability or optimal-setting evaluation is established by this report.")
	_, err := fmt.Fprint(out, b.String())
	return err
}

func writeDistanceReasons(b *strings.Builder, stats *distanceStats) {
	fmt.Fprintf(b, "  Recognized rejection reasons (denominator %d; NULL/empty reason uses UNKNOWN):\n", stats.rejectedCount)
	keys := make([]string, 0, len(stats.rejectionReasons))
	for key := range stats.rejectionReasons {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, c := keys[i], keys[j]
		if stats.rejectionReasons[a] != stats.rejectionReasons[c] {
			return stats.rejectionReasons[a] > stats.rejectionReasons[c]
		}
		return a < c
	})
	if len(keys) == 0 {
		fmt.Fprintln(b, "    (none)")
	}
	for _, key := range keys {
		fmt.Fprintf(b, "    %q: %d (%s)\n", key, stats.rejectionReasons[key], distancePercent(stats.rejectionReasons[key], stats.rejectedCount))
	}
}
