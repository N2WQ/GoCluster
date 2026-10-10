package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func inspectionRequireText(t *testing.T, output string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(output, want) {
			t.Errorf("literal report text%q absent:\n%s", want, output)
		}
	}
}

func inspectionRequireDescriptive(t *testing.T, output string) {
	t.Helper()
	inspectionRequireText(t, output,
		"Applied samples (at most 10; lexical selected-field order):",
		"NULL sample reasons retain UNKNOWN; valid empty reasons remain empty.",
		"Four independent SQL selections; a changing database can differ between selections.",
		"Scores are unvalidated logs. Samples establish no chronological order, correction accuracy or calibration.")
	for _, obsolete := range []string{"conf=", "HIGH CONFIDENCE REJECTIONS (>70%)", "confidence=", "NaN", "+Inf", "-Inf"} {
		if strings.Contains(output, obsolete) {
			t.Errorf("unsupported/nonfinite report text%q:\n%s", obsolete, output)
		}
	}
}

func inspectionMixedReportLiterals() []string {
	return []string{
		"All rows: 14\nApplied: 3\nRejected: 4\nUnknown: 7\n",
		"  <NULL>: 1 (unknown)\n", "  \"<NULL>\": 1 (unknown)\n", "  \"\": 1 (unknown)\n",
		"  \"pending\": 1 (unknown)\n", "  \"PENDING\": 1 (unknown)\n", "  \" applied \": 1 (unknown)\n", "  \"rejected\\t\": 1 (unknown)\n",
		"  \"APPLIED\": 1 (applied)\n", "  \"Applied\": 1 (applied)\n", "  \"applied\": 1 (applied)\n",
		"  \"R_D1_71\" -> \"W1XYZ\" distance=1 logged score=71 support=4/5 outcome=\"REJECTED\" reason=\"\"\n    Stored thresholds: reports=3 advantage=1 logged score=60\n",
		"  \"R_D1_70\" -> \"W1XYZ\" distance=1 logged score=70 support=3/4 outcome=\"rEjEcTeD\" reason=\"UNKNOWN\"\n",
		"  \"A_LOW\" -> \"W1XYZ\" distance=1 logged score=10 reporters=4 outcome=\"applied\"\n",
		"  \"A_MIXED\" -> \"W1XYZ\" distance=3 logged score=90 reporters=4 outcome=\"Applied\"\n",
		"  \"A_UPPER\" -> \"W1XYZ\" distance=2 logged score=20 reporters=4 outcome=\"APPLIED\"\n",
	}
}

func inspectionSection(t *testing.T, output, start, end string) string {
	t.Helper()
	a, b := strings.Index(output, start), strings.Index(output, end)
	if a < 0 || b <= a {
		t.Fatalf("missing sample section bounds%q..%q:\n%s", start, end, output)
	}
	return output[a:b]
}

func TestInspectionRealSQLiteReportSampleMembershipAndReasons(t *testing.T) {
	path := inspectionFixtureDB(t, inspectionMixedFixture())
	var output bytes.Buffer
	if err := run(path, &output); err != nil {
		t.Fatal(err)
	}
	inspectionRequireText(t, output.String(), inspectionMixedReportLiterals()...)
	inspectionRequireDescriptive(t, output.String())
	high := inspectionSection(t, output.String(), "High-score rejection samples (score >70; at most 10; score descending, selected-field ties):", "NULL sample reasons retain UNKNOWN;")
	inspectionRequireText(t, high, "\"R_D2_95\" -> \"W1XYZ\" distance=2 logged score=95 support=3/4 outcome=\"Rejected\" reason=\"GATE\"\n",
		"\"R_D2_71\" -> \"W1XYZ\" distance=2 logged score=71 support=5/6 outcome=\"rejected\" reason=\"GATE\"\n")
	if strings.Count(high, " -> ") != 3 || strings.Contains(high, "R_D1_70") || strings.Contains(high, "UNKNOWN0") || strings.Contains(high, "Stored thresholds:") {
		t.Fatalf("high-score selected projection/membership changed:\n%s", high)
	}
}

type inspectionPartialWriter struct {
	cause     error
	calls     int
	published []byte
}

func (w *inspectionPartialWriter) Write(p []byte) (int, error) {
	w.calls++
	n := min(11, len(p))
	w.published = append(w.published, p[:n]...)
	return n, w.cause
}

func TestInspectionOutputFailureAfterRealSuccessfulLoad(t *testing.T) {
	path := inspectionFixtureDB(t, inspectionMixedFixture())
	var valid bytes.Buffer
	if err := run(path, &valid); err != nil {
		t.Fatal(err)
	}
	inspectionRequireText(t, valid.String(), inspectionMixedReportLiterals()...)
	cause := errors.New("fixture partial stdout failure")
	w := &inspectionPartialWriter{cause: cause}
	err := run(path, w)
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "write decision inspection") || w.calls != 1 || string(w.published) != "DESCRIPTIVE" {
		t.Fatalf("causal partial report failure/prefix lost: err=%v calls%d prefix%q", err, w.calls, w.published)
	}
}
