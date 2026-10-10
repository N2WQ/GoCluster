package main

import (
	"fmt"
	"io"
	"strings"
)

func reportInspection(out io.Writer, dbPath string, data *inspection) error {
	var b strings.Builder
	fmt.Fprintf(&b, "DESCRIPTIVE DECISION INSPECTION\nDatabase: %q\n", dbPath)
	fmt.Fprintf(&b, "All rows: %d\nApplied: %d\nRejected: %d\nUnknown: %d\n", data.total, data.applied, data.rejected, data.unknown)
	fmt.Fprintln(&b, "\nRaw outcome values (case-only known classifications):")
	if len(data.outcomes) == 0 {
		fmt.Fprintln(&b, "  (none)")
	}
	for _, row := range data.outcomes {
		label := "<NULL>"
		if row.value.Valid {
			label = fmt.Sprintf("%q", row.value.String)
		}
		fmt.Fprintf(&b, "  %s: %d (%s)\n", label, row.count, outcomeKind(row.value))
	}
	fmt.Fprintln(&b, "\nApplied samples (at most 10; lexical selected-field order):")
	if len(data.appliedSamples) == 0 {
		fmt.Fprintln(&b, "  (none)")
	}
	for i := range data.appliedSamples {
		s := &data.appliedSamples[i]
		fmt.Fprintf(&b, "  %q -> %q distance=%d logged score=%d reporters=%d outcome=%q\n", s.subject, s.winner, s.distance, s.score, s.reporters, s.decision.String)
	}
	writeRejectionSamples(&b, "Distance-1 rejection samples (at most 10; support descending, selected-field ties):", data.distance1Samples, true)
	writeRejectionSamples(&b, "High-score rejection samples (score >70; at most 10; score descending, selected-field ties):", data.highScoreSamples, false)
	fmt.Fprintln(&b, "\nNULL sample reasons retain UNKNOWN; valid empty reasons remain empty.")
	fmt.Fprintln(&b, "Four independent SQL selections; a changing database can differ between selections.")
	fmt.Fprintln(&b, "Scores are unvalidated logs. Samples establish no chronological order, correction accuracy or calibration.")
	_, err := fmt.Fprint(out, b.String())
	return err
}

func writeRejectionSamples(b *strings.Builder, title string, samples []decisionSample, withThresholds bool) {
	fmt.Fprintf(b, "\n%s\n", title)
	if len(samples) == 0 {
		fmt.Fprintln(b, "  (none)")
	}
	for i := range samples {
		s := &samples[i]
		reason := "UNKNOWN"
		if s.reason.Valid {
			reason = s.reason.String
		}
		fmt.Fprintf(b, "  %q -> %q distance=%d logged score=%d support=%d/%d outcome=%q reason=%q\n",
			s.subject, s.winner, s.distance, s.score, s.support, s.reporters, s.decision.String, reason)
		if withThresholds {
			fmt.Fprintf(b, "    Stored thresholds: reports=%d advantage=%d logged score=%d\n", s.reports, s.advantage, s.confidence)
		}
	}
}
