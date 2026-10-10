package main

import (
	"fmt"
	"io"
	"strings"
	"time"
)

func reportReappearances(out io.Writer, dbPath string, lookAheadHours int, cases []reappearanceCase) error {
	var b strings.Builder
	fmt.Fprintf(&b, "DESCRIPTIVE REAPPEARANCE OBSERVATIONS\nDatabase: %q\nLookahead hours: %d\n", dbPath, lookAheadHours)
	fmt.Fprintln(&b, "Case-only applied seeds; first 100 combined subject-field follow-ups across all outcomes in (start,end].")
	fmt.Fprintln(&b, "Each identity uses its first observed frequency; follow-up winner fields are unused.")
	fmt.Fprintf(&b, "Dual-reappearance cases: %d\n", len(cases))
	below := 0
	for i := range cases {
		if cases[i].freqSeparationKHz < 0.5 {
			below++
		}
	}
	above := len(cases) - below
	if len(cases) == 0 {
		fmt.Fprintln(&b, "First-frequency separation <0.5 kHz: 0 (N/A)\nFirst-frequency separation >=0.5 kHz: 0 (N/A)")
	} else {
		fmt.Fprintf(&b, "First-frequency separation <0.5 kHz: %d (%.1f%% of dual cases)\n", below, 100*float64(below)/float64(len(cases)))
		fmt.Fprintf(&b, "First-frequency separation >=0.5 kHz: %d (%.1f%% of dual cases)\n", above, 100*float64(above)/float64(len(cases)))
	}
	fmt.Fprintln(&b, "\nSamples (at most 20; timestamp/id then selected-field ties):")
	if len(cases) == 0 {
		fmt.Fprintln(&b, "  (none)")
	}
	for i := 0; i < min(20, len(cases)); i++ {
		c := &cases[i]
		fmt.Fprintf(&b, "  ID=%d UTC=%s %q -> %q distance=%d logged score=%d\n",
			c.correctionID, c.correctionTime.Format(time.RFC3339), c.subject, c.winner, c.distance, c.confidence)
		fmt.Fprintf(&b, "    Seed frequency=%.3f kHz; first subject=%.3f kHz; first winner=%.3f kHz; separation=%.3f kHz\n",
			c.freqKHz, c.subjectFreqKHz, c.winnerFreqKHz, c.freqSeparationKHz)
	}
	fmt.Fprintln(&b, "\nThe combined cap can omit later observations; the first-frequency method is not simultaneous coverage.")
	fmt.Fprintln(&b, "Selections do not promise one database snapshot. Historical numbers are unvalidated; separation assumes finite frequencies.")
	fmt.Fprintln(&b, "These logged observations establish no correction accuracy, calibration, station legitimacy or simultaneous operation.")
	_, err := fmt.Fprint(out, b.String())
	return err
}
