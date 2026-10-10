package main

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func thresholdRequireText(t *testing.T, output string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(output, want) {
			t.Errorf("missing literal %q from report:\n%s", want, output)
		}
	}
}

func thresholdRequireDescriptive(t *testing.T, output string) {
	t.Helper()
	thresholdRequireText(t, output,
		"These are scalar comparisons, not loaded current settings or a complete correction-model replay.",
		"Stored counts/scores are assumed valid. Logged confidence is an unvalidated score.",
		"No stability, accuracy, calibration, reliability or optimal-setting prediction is established by these observations.")
	for _, obsolete := range []string{"CURRENT CONFIGURATION", "PREDICTED STABILITY", "88.3%", "~85-87%", "MODERATE IMPACT", "NO IMPACT", "NaN", "+Inf", "-Inf"} {
		if strings.Contains(output, obsolete) {
			t.Errorf("unsupported or nonfinite report text %q:\n%s", obsolete, output)
		}
	}
}

func TestThresholdRealSQLiteSelectionAndLiteralComparison(t *testing.T) {
	values := thresholdMixedFixture()
	outside := thresholdFixtureValue(6, "OUTSIDE_ZERO", 99, "applied")
	outside.distance = 0
	values = append(values, outside)
	outside.id, outside.subject, outside.distance = 7, "OUTSIDE_FOUR", 4
	values = append(values, outside)
	path := thresholdFixtureDB(t, values)
	selected := thresholdLoadFixture(t, path)
	var ids []int64
	for _, value := range selected {
		ids = append(ids, value.id)
	}
	if !reflect.DeepEqual(ids, []int64{1, 5, 2, 3, 4}) {
		t.Fatalf("distance selection/order IDs=%v", ids)
	}
	if selected[0].subject != "K1ABC" || selected[0].winner != "W1XYZ" || selected[0].decision != (sql.NullString{String: "APPLIED", Valid: true}) {
		t.Fatalf("normalized calls/raw outcome changed: %+v", selected[0])
	}
	r := compareThresholds(selected, thresholdSettings{3, 1, 55})
	if r.applied != 2 || r.rejected != 2 || r.unknown != 1 || r.retained != 1 || r.removed != 1 || r.added != 1 || r.unknownPassed != 1 {
		t.Fatalf("literal A={a,b}, R={c,d}, U={u}, P={a,c,u}: %+v", r)
	}
	if !reflect.DeepEqual(r.baselines, map[thresholdSettings]int{{3, 1, 60}: 3, {2, 2, 70}: 2}) || len(r.addedSamples) != 1 || r.addedSamples[0].subject != "C" || r.addedByDistance[3] != 1 {
		t.Fatalf("baseline/sample ownership: %+v", r)
	}
	var output bytes.Buffer
	if err := runThreshold(path, thresholdSettings{3, 1, 55}, &output); err != nil {
		t.Fatal(err)
	}
	thresholdRequireText(t, output.String(), fmt.Sprintf("Database: %q\n", path),
		"Selected distance-1..3 rows: 5\n", "Applied: 2 (40.0%)\nRejected: 2 (40.0%)\nUnknown: 1 (20.0%)\n",
		"  (2, 2, 70): 2\n  (3, 1, 60): 3\n", "Retained applied: 1\nRemoved applied: 1\nAdded rejected: 1\n",
		"Recognized rows passing: 2/4 (50.0%)\nNet change from applied: +0\nChange relative to applied count: 0.0%\n",
		"Unknown rows passing (separate): 1/1; excluded from recognized totals and net change.\n",
		"  Distance-1: 0\n  Distance-2: 0\n  Distance-3: 1\n", `id=3 "C" -> "W1XYZ" distance=3 support=3/4 advantage=2 logged score=60`)
	thresholdRequireDescriptive(t, output.String())
}

func TestThresholdNullableOutcomeClassificationDoesNotTrim(t *testing.T) {
	var values []thresholdFixtureRow
	for i, outcome := range []any{"aPpLiEd", "rEjEcTeD", nil, "", "pending", " applied ", "rejected\t"} {
		value := thresholdFixtureValue(int64(i+1), fmt.Sprintf("CASE%d", i), 55, outcome)
		if i == 2 {
			value.reason = nil
		}
		values = append(values, value)
	}
	selected := thresholdLoadFixture(t, thresholdFixtureDB(t, values))
	if len(selected) != 7 || selected[2].decision.Valid || selected[2].reason != "" {
		t.Fatalf("nullable historical cells: %+v", selected)
	}
	r := compareThresholds(selected, thresholdSettings{3, 1, 55})
	if r.applied != 1 || r.rejected != 1 || r.unknown != 5 || r.retained != 1 || r.removed != 0 || r.added != 1 || r.unknownPassed != 5 {
		t.Fatalf("case-only recognition/separate unknown: %+v", r)
	}
	wantUnknown := map[string]int{"<NULL>": 1, `""`: 1, `"pending"`: 1, `" applied "`: 1, `"rejected\t"`: 1}
	if !reflect.DeepEqual(r.unknownOutcomes, wantUnknown) || len(r.addedSamples) != 1 || r.addedSamples[0].subject != "CASE1" {
		t.Fatalf("unknown labels leaked into recognized samples: %+v", r)
	}
	var output bytes.Buffer
	if err := reportThreshold(&output, "nullable fixture", selected, thresholdSettings{3, 1, 55}); err != nil {
		t.Fatal(err)
	}
	thresholdRequireText(t, output.String(), "Applied: 1 (14.3%)\nRejected: 1 (14.3%)\nUnknown: 5 (71.4%)\n",
		"Recognized rows passing: 2/2 (100.0%)\nNet change from applied: +1\n",
		"Unknown rows passing (separate): 5/5; excluded from recognized totals and net change.\n",
		"  \" applied \": 1\n  \"\": 1\n  \"pending\": 1\n  \"rejected\\t\": 1\n  <NULL>: 1\n")
}

func TestThresholdUnknownPassingCountUsesPredicate(t *testing.T) {
	values := []thresholdFixtureRow{
		thresholdFixtureValue(1, "UNKNOWN_EXACT", 55, nil),
		thresholdFixtureValue(2, "UNKNOWN_BELOW", 54, "pending"),
	}
	selected := thresholdLoadFixture(t, thresholdFixtureDB(t, values))
	r := compareThresholds(selected, thresholdSettings{3, 1, 55})
	if r.unknown != 2 || r.unknownPassed != 1 || r.applied != 0 || r.rejected != 0 || r.retained != 0 || r.removed != 0 || r.added != 0 || len(r.addedSamples) != 0 {
		t.Fatalf("unknown exact/below cuts must stay separate: %+v", r)
	}
	if !reflect.DeepEqual(r.baselines, map[thresholdSettings]int{{3, 1, 60}: 2}) {
		t.Fatalf("unknown rows lost from baseline population: %+v", r.baselines)
	}
	var output bytes.Buffer
	if err := reportThreshold(&output, "unknown cut boundary", selected, thresholdSettings{3, 1, 55}); err != nil {
		t.Fatal(err)
	}
	thresholdRequireText(t, output.String(), "Unknown: 2 (100.0%)\n",
		"Recognized rows passing: 0/0 (N/A)\nNet change from applied: +0\n",
		"Unknown rows passing (separate): 1/2;", "  (none)\n")
}

func TestThresholdNetDirectionAndEmptyDenominators(t *testing.T) {
	a := decisionRecord{id: 1, subject: "A", winner: "W", distance: 1, winnerSupport: 3, subjectSupport: 1, totalReporters: 4, winnerConfidence: 55, decision: sql.NullString{String: "applied", Valid: true}}
	r := a
	r.id, r.subject, r.decision.String = 2, "R", "rejected"
	fail := a
	fail.winnerConfidence = 54
	u := a
	u.id, u.subject, u.decision = 3, "U", sql.NullString{}
	for _, tc := range []struct {
		name  string
		rows  []decisionRecord
		wants []string
	}{
		{"positive", []decisionRecord{a, r, u}, []string{"Retained applied: 1\nRemoved applied: 0\nAdded rejected: 1\n", "Recognized rows passing: 2/2 (100.0%)\nNet change from applied: +1\nChange relative to applied count: 100.0%\n"}},
		{"negative", []decisionRecord{a, fail, u}, []string{"Retained applied: 1\nRemoved applied: 1\nAdded rejected: 0\n", "Recognized rows passing: 1/2 (50.0%)\nNet change from applied: -1\nChange relative to applied count: -50.0%\n"}},
		{"only-removal", []decisionRecord{fail, u}, []string{"Retained applied: 0\nRemoved applied: 1\nAdded rejected: 0\n", "Recognized rows passing: 0/1 (0.0%)\nNet change from applied: -1\nChange relative to applied count: -100.0%\n"}},
		{"no-applied", []decisionRecord{r, u}, []string{"Applied: 0 (0.0%)\n", "Recognized rows passing: 1/1 (100.0%)\nNet change from applied: +1\nChange relative to applied count: N/A\n"}},
		{"unknown-only", []decisionRecord{u}, []string{"Applied: 0 (0.0%)\nRejected: 0 (0.0%)\nUnknown: 1 (100.0%)\n", "Recognized rows passing: 0/0 (N/A)\nNet change from applied: +0\nChange relative to applied count: N/A\n", "  (0, 0, 0): 1\n"}},
		{"empty", nil, []string{"Applied: 0 (N/A)\nRejected: 0 (N/A)\nUnknown: 0 (N/A)\n", "Recognized rows passing: 0/0 (N/A)\nNet change from applied: +0\nChange relative to applied count: N/A\n", "  (none; no baseline inferred)\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := reportThreshold(&output, "literal set fixture", tc.rows, thresholdSettings{3, 1, 55}); err != nil {
				t.Fatal(err)
			}
			thresholdRequireText(t, output.String(), tc.wants...)
			thresholdRequireDescriptive(t, output.String())
		})
	}
}

type thresholdReportFaultWriter struct{ cause error }

func (w thresholdReportFaultWriter) Write([]byte) (int, error) { return 0, w.cause }

func TestThresholdReportFailureFollowsRealSuccessfulLoad(t *testing.T) {
	path := thresholdFixtureDB(t, thresholdMixedFixture())
	var valid bytes.Buffer
	if err := runThreshold(path, thresholdSettings{3, 1, 55}, &valid); err != nil {
		t.Fatal(err)
	}
	thresholdRequireText(t, valid.String(), "Selected distance-1..3 rows: 5\n", "Net change from applied: +0\n")
	cause := errors.New("fixture report write refused")
	err := runThreshold(path, thresholdSettings{3, 1, 55}, thresholdReportFaultWriter{cause})
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "write threshold comparison") {
		t.Fatalf("causal report failure lost: %v", err)
	}
}

func TestThresholdRealSQLiteBaselineReversalAndSignedMainNet(t *testing.T) {
	positive := thresholdMixedFixture()
	positive[3].subjectSupport, positive[3].reporters = 1, 4
	negative := thresholdMixedFixture()
	negative = append(negative, thresholdFixtureValue(6, "SECOND_REMOVAL", 54, "applied"))
	for _, tc := range []struct {
		name   string
		values []thresholdFixtureRow
		wants  []string
	}{
		{"zero", thresholdMixedFixture(), []string{"  (2, 2, 70): 2\n  (3, 1, 60): 3\n", "Retained applied: 1\nRemoved applied: 1\nAdded rejected: 1\n", "Net change from applied: +0\nChange relative to applied count: 0.0%\n"}},
		{"added2-removed1", positive, []string{"  (2, 2, 70): 2\n  (3, 1, 60): 3\n", "Retained applied: 1\nRemoved applied: 1\nAdded rejected: 2\n", "Recognized rows passing: 3/4 (75.0%)\nNet change from applied: +1\nChange relative to applied count: 50.0%\n"}},
		{"added1-removed2", negative, []string{"  (2, 2, 70): 2\n  (3, 1, 60): 4\n", "Retained applied: 1\nRemoved applied: 2\nAdded rejected: 1\n", "Recognized rows passing: 2/5 (40.0%)\nNet change from applied: -1\nChange relative to applied count: -33.3%\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forward := thresholdFixtureDB(t, tc.values)
			reversed := append([]thresholdFixtureRow(nil), tc.values...)
			slices.Reverse(reversed)
			reverse := thresholdFixtureDB(t, reversed)
			var reports [2]string
			for i, path := range []string{forward, reverse} {
				selected := thresholdLoadFixture(t, path)
				var output bytes.Buffer
				if err := reportThreshold(&output, "identical source label", selected, thresholdSettings{3, 1, 55}); err != nil {
					t.Fatal(err)
				}
				reports[i] = output.String()
				thresholdRequireText(t, reports[i], tc.wants...)
			}
			if reports[0] != reports[1] {
				t.Fatalf("stored tuple frequencies/sample report depends on insertion order:\n%s\n%s", reports[0], reports[1])
			}
			code, output, stderr := thresholdMainResult(t, forward, "run")
			if code != 0 || stderr != "" {
				t.Fatalf("signed set actual main status=%d stdout=%q stderr=%q", code, output, stderr)
			}
			thresholdRequireText(t, output, tc.wants...)
			thresholdRequireDescriptive(t, output)
		})
	}
}
