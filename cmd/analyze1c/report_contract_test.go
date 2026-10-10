package main

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func oneCRequireText(t *testing.T, output string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(output, want) {
			t.Errorf("literal report text %q absent:\n%s", want, output)
		}
	}
}

func oneCRequireDescriptive(t *testing.T, output string) {
	t.Helper()
	oneCRequireText(t, output,
		"Overall outcome percentages use all selected rows.",
		"Summary by stored distance (percentages use that distance's rows):",
		"Scores are unvalidated logged integers, not measured correctness probabilities.",
		"Means/medians are approximate one-decimal floating presentations; no applied rows means N/A.",
		"No correlation, accuracy, calibration, reliability or optimal-setting evaluation is established by this report.")
	for _, obsolete := range []string{"METHOD 1C: DISTANCE-CONFIDENCE CORRELATION ANALYSIS", "ANALYSIS & RECOMMENDATIONS", "WELL-CALIBRATED", "CONSERVATIVE:", "BALANCED:", "equally reliable", "Reducing min_", "aggressive thresholds", "You may be rejecting many valid", "NaN", "+Inf", "-Inf"} {
		if strings.Contains(output, obsolete) {
			t.Errorf("unsupported/nonfinite report text %q:\n%s", obsolete, output)
		}
	}
}

func oneCMixedReportLiterals() []string {
	return []string{
		"All selected rows (every distance): 14\n",
		"Applied: 5 (35.7%)\nRejected: 4 (28.6%)\nUnknown: 5 (35.7%)\n",
		"Distance 1: rows=10 applied=3 (30.0%) rejected=2 (20.0%) unknown=5 (50.0%)\n",
		"  Applied logged scores: n=3 mean=40.0 median=20.0\n",
		"  Recognized rejection reasons (denominator 2; NULL/empty reason uses UNKNOWN):\n    \"GATE_A\": 1 (50.0%)\n    \"GATE_B\": 1 (50.0%)\n",
		"Distance 3: rows=4 applied=2 (50.0%) rejected=2 (50.0%) unknown=0 (0.0%)\n",
		"  Applied logged scores: n=2 mean=60.0 median=60.0\n",
		"    \"UNKNOWN\": 2 (100.0%)\n",
		"Unknown outcome values (across all selected rows):\n  \" applied \": 1\n  \"\": 1\n  \"pending\": 1\n  \"rejected\\t\": 1\n  <NULL>: 1\n",
	}
}

func TestDistanceAnalysisRealSQLiteReportAndReasonOrdering(t *testing.T) {
	values := oneCMixedFixture()
	forward := oneCFixtureDB(t, values)
	slices.Reverse(values)
	reverse := oneCFixtureDB(t, values)
	var reports [2]string
	for i, path := range []string{forward, reverse} {
		a := oneCLoadFixture(t, path)
		var output bytes.Buffer
		if err := reportDistanceAnalysis(&output, "fixed historical source", a); err != nil {
			t.Fatal(err)
		}
		reports[i] = output.String()
		oneCRequireText(t, reports[i], oneCMixedReportLiterals()...)
		oneCRequireDescriptive(t, reports[i])
		for _, excluded := range []string{"APPLIED_REASON", "NULL_UNKNOWN", "EMPTY_UNKNOWN", "OTHER_UNKNOWN", "PADDED_UNKNOWN", "TAB_UNKNOWN"} {
			if strings.Contains(reports[i], excluded) {
				t.Errorf("non-rejected reason%q entered report:\n%s", excluded, reports[i])
			}
		}
	}
	if reports[0] != reports[1] {
		t.Fatalf("insertion reversal changed aggregate/reason order:\n%s\n%s", reports[0], reports[1])
	}
}

func TestDistanceAnalysisReasonOrderCountThenRawLabel(t *testing.T) {
	values := []oneCFixtureRow{{2, 80, "rejected", "a"}, {2, 80, "rejected", "B"},
		{2, 80, "rejected", "z"}, {2, 80, "Rejected", "z"}, {2, 100, "pending", "z"}}
	for range 2 {
		a := oneCLoadFixture(t, oneCFixtureDB(t, values))
		var output bytes.Buffer
		if err := reportDistanceAnalysis(&output, "reason ties", a); err != nil {
			t.Fatal(err)
		}
		oneCRequireText(t, output.String(), "Recognized rejection reasons (denominator 4; NULL/empty reason uses UNKNOWN):\n    \"z\": 2 (50.0%)\n    \"B\": 1 (25.0%)\n    \"a\": 1 (25.0%)\n")
		slices.Reverse(values)
	}
}

func TestDistanceAnalysisNumericDistanceReportOrder(t *testing.T) {
	a := oneCLoadFixture(t, oneCFixtureDB(t, []oneCFixtureRow{
		{4, 7, "applied", nil}, {0, 100, nil, nil}, {3, 60, "applied", nil},
		{-1, 80, "rejected", nil}, {1, 0, "applied", nil},
	}))
	var output bytes.Buffer
	if err := reportDistanceAnalysis(&output, "unordered distances", a); err != nil {
		t.Fatal(err)
	}
	previous := -1
	for _, head := range []string{"Distance -1:", "Distance 0:", "Distance 1:", "Distance 3:", "Distance 4:"} {
		position := strings.Index(output.String(), head)
		if position <= previous {
			t.Fatalf("literal numeric distance order lost at%q:\n%s", head, output.String())
		}
		previous = position
	}
}

func TestDistanceAnalysisNativeExtremaLiteralReport(t *testing.T) {
	m := int(^uint(0) >> 1)
	minimum := -m - 1
	path := oneCFixtureDB(t, []oneCFixtureRow{{1, m, "applied", nil}, {1, minimum, "APPLIED", nil},
		{2, m, "applied", nil}, {2, m, "Applied", nil},
		{3, minimum, "applied", nil}, {3, minimum, "Applied", nil}})
	var output bytes.Buffer
	if err := run(path, &output); err != nil {
		t.Fatal(err)
	}
	oneCRequireText(t, output.String(), "Applied logged scores: n=2 mean=-0.5 median=-0.5\n",
		fmt.Sprintf("Applied logged scores: n=2 mean=%.1f median=%.1f\n", float64(m), float64(m)),
		fmt.Sprintf("Applied logged scores: n=2 mean=%.1f median=%.1f\n", float64(minimum), float64(minimum)))
	oneCRequireDescriptive(t, output.String())
}

type oneCPartialWriter struct {
	cause     error
	calls     int
	published []byte
}

func (w *oneCPartialWriter) Write(p []byte) (int, error) {
	w.calls++
	n := min(11, len(p))
	w.published = append(w.published, p[:n]...)
	return n, w.cause
}

func TestDistanceAnalysisOutputFailureAfterRealSuccessfulLoad(t *testing.T) {
	path := oneCFixtureDB(t, oneCMixedFixture())
	var valid bytes.Buffer
	if err := run(path, &valid); err != nil {
		t.Fatal(err)
	}
	oneCRequireText(t, valid.String(), oneCMixedReportLiterals()...)
	cause := errors.New("fixture partial stdout failure")
	w := &oneCPartialWriter{cause: cause}
	err := run(path, w)
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "write distance summary") || w.calls != 1 || string(w.published) != "DESCRIPTIVE" {
		t.Fatalf("causal late output failure/prefix lost: err=%v calls%d prefix%q", err, w.calls, w.published)
	}
}
