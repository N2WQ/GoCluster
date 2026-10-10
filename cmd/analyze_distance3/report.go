package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

type distance3Summary struct {
	applied, rejected, unknown      int
	advantageTotal, scoreTotal      float64
	minAdvantage, minScore          int
	appliedSamples, rejectedSamples []distance3Case
	reasons, unknownOutcomes        map[string]int
}

// Summary state lives for this invocation. Breakdown cardinality is bounded by
// the loaded rows; sample lists retain at most ten applied and five rejected rows.
func summarizeDistance3(cases []distance3Case) distance3Summary {
	s := distance3Summary{reasons: make(map[string]int), unknownOutcomes: make(map[string]int)}
	for _, c := range cases {
		switch classifyDistance3(c.decision) {
		case "applied":
			advantage := c.winnerSupport - c.subjectSupport
			if s.applied == 0 || advantage < s.minAdvantage {
				s.minAdvantage = advantage
			}
			if s.applied == 0 || c.winnerConfidence < s.minScore {
				s.minScore = c.winnerConfidence
			}
			s.applied++
			s.advantageTotal += float64(advantage)
			s.scoreTotal += float64(c.winnerConfidence)
			if len(s.appliedSamples) < 10 {
				s.appliedSamples = append(s.appliedSamples, c)
			}
		case "rejected":
			s.rejected++
			reason := c.reason
			if reason == "" {
				reason = "(unspecified)"
			}
			s.reasons[reason]++
			if len(s.rejectedSamples) < 5 {
				s.rejectedSamples = append(s.rejectedSamples, c)
			}
		default:
			s.unknown++
			label := "<NULL>"
			if c.decision.Valid {
				label = fmt.Sprintf("%q", c.decision.String)
			}
			s.unknownOutcomes[label]++
		}
	}
	return s
}

func distance3Percent(n, denominator int) string {
	if denominator == 0 {
		return "N/A"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(n)/float64(denominator))
}

func reportDistance3(out io.Writer, dbPath string, cases []distance3Case) error {
	s := summarizeDistance3(cases)
	var b strings.Builder
	fmt.Fprintf(&b, "DISTANCE-3 HISTORICAL OBSERVATIONS\nDatabase: %q\nSelected rows: %d\n", dbPath, len(cases))
	fmt.Fprintln(&b, "Outcome percentages use all selected distance-3 rows.")
	fmt.Fprintf(&b, "Applied: %d (%s)\nRejected: %d (%s)\nUnknown: %d (%s)\n",
		s.applied, distance3Percent(s.applied, len(cases)), s.rejected, distance3Percent(s.rejected, len(cases)),
		s.unknown, distance3Percent(s.unknown, len(cases)))
	writeDistance3Breakdown(&b, "Recognized rejection reasons", s.reasons)
	writeDistance3Breakdown(&b, "Unknown outcome values", s.unknownOutcomes)
	fmt.Fprintln(&b, "\nApplied-only statistics (logged confidence is an unvalidated score):")
	if s.applied == 0 {
		fmt.Fprintln(&b, "Mean advantage: N/A\nMinimum advantage: N/A\nMean logged score: N/A\nMinimum logged score: N/A")
	} else {
		fmt.Fprintf(&b, "Mean advantage: %.1f\nMinimum advantage: %d\nMean logged score: %.1f\nMinimum logged score: %d\n",
			s.advantageTotal/float64(s.applied), s.minAdvantage, s.scoreTotal/float64(s.applied), s.minScore)
	}
	writeDistance3Samples(&b, "Applied samples (at most 10)", s.appliedSamples)
	writeDistance3Samples(&b, "Rejected samples (at most 5)", s.rejectedSamples)
	writeDistance3Scenarios(&b, cases)
	fmt.Fprintln(&b, "\nThese observations and scalar comparisons do not establish correction accuracy, calibration, reliability or optimal settings.")
	_, err := fmt.Fprint(out, b.String())
	return err
}

func writeDistance3Breakdown(b *strings.Builder, title string, counts map[string]int) {
	fmt.Fprintf(b, "\n%s:\n", title)
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

func writeDistance3Samples(b *strings.Builder, title string, cases []distance3Case) {
	fmt.Fprintf(b, "\n%s:\n", title)
	if len(cases) == 0 {
		fmt.Fprintln(b, "  (none)")
	}
	for _, c := range cases {
		fmt.Fprintf(b, "  %q -> %q: support=%d/%d, advantage=%d, logged score=%d, reason=%q\n",
			c.subject, c.winner, c.winnerSupport, c.totalReporters,
			c.winnerSupport-c.subjectSupport, c.winnerConfidence, c.reason)
	}
}
