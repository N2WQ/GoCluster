package main

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

func reappearanceRequireText(t *testing.T, output string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(output, want) {
			t.Errorf("literal reappearance text%q absent:\n%s", want, output)
		}
	}
}

func reappearanceRequireDescriptive(t *testing.T, output string) {
	t.Helper()
	reappearanceRequireText(t, output,
		"Case-only applied seeds; first 100 combined subject-field follow-ups across all outcomes in (start,end].",
		"Each identity uses its first observed frequency; follow-up winner fields are unused.",
		"The combined cap can omit later observations; the first-frequency method is not simultaneous coverage.",
		"Selections do not promise one database snapshot. Historical numbers are unvalidated; separation assumes finite frequencies.",
		"These logged observations establish no correction accuracy, calibration, station legitimacy or simultaneous operation.")
	for _, old := range []string{"conf=", "% confidence", "LIKELY LEGITIMATE", "POTENTIAL ISSUE", "Recommendation:", "Same frequency (<0.5 kHz)", "Different frequency (", "NaN", "+Inf", "-Inf"} {
		if strings.Contains(output, old) {
			t.Errorf("obsolete/advice/nonfinite text%q:\n%s", old, output)
		}
	}
}

func reappearanceBoundaryFixture() []reappearanceFixtureRow {
	var values []reappearanceFixtureRow
	for i, winnerFreq := range []float64{7000.499, 7000.500, 7000.501} {
		id := int64(i + 1)
		subject, winner := fmt.Sprintf("S%02d", id), fmt.Sprintf("W%02d", id)
		values = append(values, reappearanceSeed(id, 1000, subject, winner, "Applied"),
			reappearanceObservation(100+id*2, 1001, subject, 7000, "pending"),
			reappearanceObservation(101+id*2, 1002, winner, winnerFreq, "rejected"),
			reappearanceObservation(200+id*2, 1003, subject, 8000, nil),
			reappearanceObservation(201+id*2, 1004, winner, 9000, ""))
	}
	return values
}

func reappearanceBoundaryReportLiterals() []string {
	return []string{
		"Dual-reappearance cases: 3\n",
		"First-frequency separation <0.5 kHz: 1 (33.3% of dual cases)\n",
		"First-frequency separation >=0.5 kHz: 2 (66.7% of dual cases)\n",
		`ID=1 UTC=1970-01-01T00:16:40Z "S01" -> "W01" distance=3 logged score=65`,
		`ID=2 UTC=1970-01-01T00:16:40Z "S02" -> "W02" distance=3 logged score=65`,
		`ID=3 UTC=1970-01-01T00:16:40Z "S03" -> "W03" distance=3 logged score=65`,
		"Seed frequency=14074.000 kHz; first subject=7000.000 kHz; first winner=7000.499 kHz; separation=0.499 kHz\n",
		"Seed frequency=14074.000 kHz; first subject=7000.000 kHz; first winner=7000.500 kHz; separation=0.500 kHz\n",
		"Seed frequency=14074.000 kHz; first subject=7000.000 kHz; first winner=7000.501 kHz; separation=0.501 kHz\n",
	}
}

func TestReappearanceRealSQLiteArithmeticBucketsAndFirstValues(t *testing.T) {
	path := reappearanceFixtureDB(t, reappearanceBoundaryFixture())
	cases := reappearanceLoadFixture(t, path, 24)
	if len(cases) != 3 {
		t.Fatalf("literal boundary cases: %+v", cases)
	}
	for i, want := range []float64{0.499, 0.500, 0.501} {
		if math.Abs(cases[i].freqSeparationKHz-want) > 1e-9 || cases[i].subjectFreqKHz != 7000 || cases[i].winnerFreqKHz != []float64{7000.499, 7000.500, 7000.501}[i] {
			t.Errorf("literal finite subtraction/firstfrequency at%d: %+v", i, cases[i])
		}
	}
	var output bytes.Buffer
	if err := run(path, 24, &output); err != nil {
		t.Fatal(err)
	}
	reappearanceRequireText(t, output.String(), reappearanceBoundaryReportLiterals()...)
	reappearanceRequireDescriptive(t, output.String())
}

func TestReappearanceEmptyAndNoDualHaveUnavailableRatios(t *testing.T) {
	for _, values := range [][]reappearanceFixtureRow{nil, {reappearanceSeed(1, 1000, "S", "W", "applied"), reappearanceObservation(2, 1001, "S", 7000, "pending")}} {
		var output bytes.Buffer
		if err := run(reappearanceFixtureDB(t, values), 24, &output); err != nil {
			t.Fatal(err)
		}
		reappearanceRequireText(t, output.String(), "Dual-reappearance cases: 0\n",
			"First-frequency separation <0.5 kHz: 0 (N/A)\nFirst-frequency separation >=0.5 kHz: 0 (N/A)\n",
			"Samples (at most 20; timestamp/id then selected-field ties):\n  (none)\n")
		reappearanceRequireDescriptive(t, output.String())
	}
}

func reappearanceRequireSampleCap(t *testing.T, output string) {
	t.Helper()
	if strings.Count(output, "  ID=") != 20 {
		t.Errorf("literal sample20cap changed:\n%s", output)
	}
	for i := range 20 {
		reappearanceRequireText(t, output, fmt.Sprintf(`ID=1 UTC=1970-01-01T00:16:40Z "S%02d" -> "W%02d" distance=3 logged score=65`, i, i))
	}
	if strings.Contains(output, `"S20" -> "W20"`) {
		t.Errorf("sample21 crossed20cap:\n%s", output)
	}
}

type reappearancePartialWriter struct {
	cause     error
	calls     int
	published []byte
}

func (w *reappearancePartialWriter) Write(p []byte) (int, error) {
	w.calls++
	n := min(11, len(p))
	w.published = append(w.published, p[:n]...)
	return n, w.cause
}

func TestReappearanceCheckedPartialWriterAfterRealSuccessfulLoad(t *testing.T) {
	path := reappearanceFixtureDB(t, reappearanceBoundaryFixture())
	var valid bytes.Buffer
	if err := run(path, 24, &valid); err != nil {
		t.Fatal(err)
	}
	reappearanceRequireText(t, valid.String(), reappearanceBoundaryReportLiterals()...)
	cause := errors.New("fixture partial reappearance output failure")
	w := &reappearancePartialWriter{cause: cause}
	err := run(path, 24, w)
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "write reappearance observations") || w.calls != 1 || string(w.published) != "DESCRIPTIVE" {
		t.Fatalf("partial report failure lost: err=%v calls%d prefix%q", err, w.calls, w.published)
	}
}
