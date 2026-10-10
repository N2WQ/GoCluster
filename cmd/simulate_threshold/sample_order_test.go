package main

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func thresholdSampleSection(t *testing.T, report string) string {
	t.Helper()
	start := strings.Index(report, "Added-row samples (at most 10; passing does not establish correctness):\n")
	end := strings.Index(report, "\nNo stability, accuracy, calibration, reliability or optimal-setting prediction")
	if start < 0 || end <= start {
		t.Fatalf("missing sample section boundaries:\n%s", report)
	}
	return report[start:end]
}

// Support and reason tie-breakers cross the cap; unknown rows sort before
// recognized rejections and must never consume a sample slot.
func TestThresholdRealSQLiteSampleCapAndPermutation(t *testing.T) {
	var values []thresholdFixtureRow
	for i := range 12 {
		value := thresholdFixtureValue(1, "REJECTED_TIE", 55, "rejected")
		value.reason = fmt.Sprintf("z%02d", i)
		if i >= 9 {
			value.support, value.reporters, value.reason = 4, 5, fmt.Sprintf("a%02d", i)
		}
		values = append(values, value)
	}
	for i, outcome := range []any{nil, "", " applied ", "pending"} {
		value := thresholdFixtureValue(0, fmt.Sprintf("UNKNOWN%d", i), 99, outcome)
		values = append(values, value)
	}
	forward := thresholdFixtureDB(t, values)
	slices.Reverse(values)
	reverse := thresholdFixtureDB(t, values)
	var reports [2]string
	for i, path := range []string{forward, reverse} {
		selected := thresholdLoadFixture(t, path)
		r := compareThresholds(selected, thresholdSettings{3, 1, 55})
		if r.applied != 0 || r.rejected != 12 || r.unknown != 4 || r.added != 12 || r.unknownPassed != 4 || len(r.addedSamples) != 10 {
			t.Fatalf("sample family/cap counts: %+v", r)
		}
		for j, sample := range r.addedSamples {
			wantReason, wantSupport := fmt.Sprintf("z%02d", j), 3
			if j == 9 {
				wantReason, wantSupport = "a09", 4
			}
			if sample.subject != "REJECTED_TIE" || sample.reason != wantReason || sample.winnerSupport != wantSupport {
				t.Errorf("sample%d=%+v, want recognized rejection support%d reason%s", j, sample, wantSupport, wantReason)
			}
		}
		var output bytes.Buffer
		if err := reportThreshold(&output, "fixed historical fixture", selected, thresholdSettings{3, 1, 55}); err != nil {
			t.Fatal(err)
		}
		reports[i] = output.String()
		section := thresholdSampleSection(t, reports[i])
		if strings.Count(section, " -> ") != 10 || strings.Contains(section, "UNKNOWN") || strings.Contains(section, `reason="a10"`) || strings.Contains(section, `reason="a11"`) {
			t.Fatalf("wrong capped sample membership:\n%s", section)
		}
	}
	if reports[0] != reports[1] {
		t.Fatalf("permutation changed report:\nforward:\n%s\nreverse:\n%s", reports[0], reports[1])
	}
}

// All leading sort keys tie and the legacy column collations ignore case.
// Literal raw outcome spelling determines the binary order; the printable
// reason identifies which row crossed the ten-row sample boundary.
func TestThresholdLegacyNoCaseOutcomeOrderIsExplicitBinary(t *testing.T) {
	variants := []string{"REJEcted", "REJected", "REjEcted", "REjected", "ReJEcted", "ReJected", "RejEcted", "Rejected",
		"rEJEcted", "rEJected", "rEjEcted", "rEjected", "reJEcted", "reJected", "rejEcted", "rejected"}
	var values []thresholdFixtureRow
	for i, outcome := range variants {
		value := thresholdFixtureValue(1, "SAME", 55, outcome)
		// Reverse lexical reason order to ensure the outcome key actually wins.
		value.reason = fmt.Sprintf("row%02d", 15-i)
		values = append(values, value)
	}
	forward := thresholdFixtureDBWithCollation(t, values, " COLLATE NOCASE")
	slices.Reverse(values)
	reverse := thresholdFixtureDBWithCollation(t, values, " COLLATE NOCASE")
	var reports [2]string
	for i, path := range []string{forward, reverse} {
		selected := thresholdLoadFixture(t, path)
		var outcomes []string
		for _, row := range selected {
			outcomes = append(outcomes, row.decision.String)
		}
		if !reflect.DeepEqual(outcomes, variants) {
			t.Fatalf("literal binary outcome order lost under legacy NOCASE: %q", outcomes)
		}
		r := compareThresholds(selected, thresholdSettings{3, 1, 55})
		if r.rejected != 16 || r.added != 16 || r.unknown != 0 || len(r.addedSamples) != 10 {
			t.Fatalf("case variants/cap: %+v", r)
		}
		for j, sample := range r.addedSamples {
			if sample.decision.String != variants[j] || sample.reason != fmt.Sprintf("row%02d", 15-j) {
				t.Errorf("binary sample%d=%+v", j, sample)
			}
		}
		var output bytes.Buffer
		if err := reportThreshold(&output, "legacy collation fixture", selected, thresholdSettings{3, 1, 55}); err != nil {
			t.Fatal(err)
		}
		reports[i] = output.String()
		section := thresholdSampleSection(t, reports[i])
		if strings.Count(section, " -> ") != 10 || strings.Contains(section, `reason="row05"`) {
			t.Fatalf("binary sample cap boundary lost:\n%s", section)
		}
	}
	if reports[0] != reports[1] {
		t.Fatalf("legacy NOCASE permutation changed report:\n%s\n%s", reports[0], reports[1])
	}
}
