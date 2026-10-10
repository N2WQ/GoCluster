package main

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

type distance3FixtureRow struct {
	subject, winner                           string
	support, subjectSupport, reporters, score int
	outcome, reason                           any
	distance                                  int
}

func distance3FixtureValue(subject string, score int, outcome, reason any, distance int) distance3FixtureRow {
	return distance3FixtureRow{subject, "W1XYZ", 3, 1, 4, score, outcome, reason, distance}
}

// This schema qualifies the historical consumer's selected columns, not a
// current production logger. The fixture is closed before a child opens it.
func distance3FixtureDB(t *testing.T, values []distance3FixtureRow, statements ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "decisions.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("fixture database close: %v", err)
		}
	})
	if _, err := db.ExecContext(t.Context(), `CREATE TABLE decisions (
		subject TEXT, winner TEXT, winner_support INTEGER, subject_support INTEGER,
		total_reporters INTEGER, winner_confidence INTEGER, decision TEXT, reason TEXT,
		distance INTEGER)`); err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if _, err := db.ExecContext(t.Context(), "INSERT INTO decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			value.subject, value.winner, value.support, value.subjectSupport, value.reporters,
			value.score, value.outcome, value.reason, value.distance); err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func distance3MixedFixture() []distance3FixtureRow {
	return []distance3FixtureRow{
		distance3FixtureValue("A10", 10, "applied", "", 3),
		distance3FixtureValue("A20", 20, "APPLIED", "", 3),
		distance3FixtureValue("A90", 90, "Applied", "", 3),
		distance3FixtureValue("R80", 80, "rejected", "zeta", 3),
		distance3FixtureValue("R70", 70, "ReJeCtEd", nil, 3),
		distance3FixtureValue("U_NULL", 100, nil, "unknown-only", 3),
		distance3FixtureValue("U_EMPTY", 100, "", "unknown-only", 3),
		distance3FixtureValue("U_PENDING", 100, "pending", "unknown-only", 3),
		distance3FixtureValue("U_PADDED", 100, " applied ", "unknown-only", 3),
		distance3FixtureValue("OTHER_DISTANCE", 1000, "applied", "", 2),
	}
}

func distance3RequireText(t *testing.T, report string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(report, want) {
			t.Errorf("missing literal %q in report:\n%s", want, report)
		}
	}
}

func distance3RequireDescriptive(t *testing.T, report string) {
	t.Helper()
	distance3RequireText(t, report,
		"These assumptions are not loaded current settings; other correction gates are not replayed.",
		"These observations and scalar comparisons do not establish correction accuracy, calibration, reliability or optimal settings.")
	for _, oldClaim := range []string{"95.3%", "87.4%", "CURRENT DISTANCE-3 SETTINGS", "CURRENT SETTINGS ARE OPTIMAL", "PERFECTLY calibrated", "DO NOT CHANGE", "Reduce min_consensus_reports", "Temporal stability:", "Would RESCUE"} {
		if strings.Contains(report, oldClaim) {
			t.Errorf("unsupported historical assertion retained: %q", oldClaim)
		}
	}
	for _, nonfinite := range []string{"NaN", "+Inf", "-Inf"} {
		if strings.Contains(report, nonfinite) {
			t.Errorf("nonfinite report value %q", nonfinite)
		}
	}
}

func TestDistance3RealSQLitePartitionAndStatistics(t *testing.T) {
	path := distance3FixtureDB(t, distance3MixedFixture())
	var out bytes.Buffer
	if err := run(path, &out); err != nil {
		t.Fatal(err)
	}
	report := out.String()
	distance3RequireText(t, report,
		"Selected rows: 9\n", "Outcome percentages use all selected distance-3 rows.\n",
		"Applied: 3 (33.3%)\nRejected: 2 (22.2%)\nUnknown: 4 (44.4%)\n",
		"Recognized rejection reasons:\n  (unspecified): 1\n  zeta: 1\n",
		"Unknown outcome values:\n  \" applied \": 1\n  \"\": 1\n  \"pending\": 1\n  <NULL>: 1\n",
		"Mean advantage: 2.0\nMinimum advantage: 2\nMean logged score: 40.0\nMinimum logged score: 10\n")
	if strings.Contains(report, "OTHER_DISTANCE") || strings.Contains(report, "unknown-only") {
		t.Fatal("out-of-population row or unknown reason entered selected report")
	}
	distance3RequireDescriptive(t, report)
}

func TestDistance3OutcomeVocabularyIsCaseOnly(t *testing.T) {
	for _, tc := range []struct {
		value sql.NullString
		want  string
	}{
		{sql.NullString{String: "applied", Valid: true}, "applied"},
		{sql.NullString{String: "APPLIED", Valid: true}, "applied"},
		{sql.NullString{String: "ReJeCtEd", Valid: true}, "rejected"},
		{sql.NullString{}, "unknown"},
		{sql.NullString{Valid: true}, "unknown"},
		{sql.NullString{String: "pending", Valid: true}, "unknown"},
		{sql.NullString{String: " applied ", Valid: true}, "unknown"},
		{sql.NullString{String: "rejected\t", Valid: true}, "unknown"},
	} {
		if got := classifyDistance3(tc.value); got != tc.want {
			t.Errorf("outcome %+v classified %q, want %q", tc.value, got, tc.want)
		}
	}
}

func distance3CaseValue(subject string, support, subjectSupport, score int, outcome string) distance3Case {
	return distance3Case{subject: subject, winner: "W1XYZ", winnerSupport: support,
		subjectSupport: subjectSupport, totalReporters: max(4, support+subjectSupport), winnerConfidence: score,
		decision: sql.NullString{String: outcome, Valid: true}}
}

func TestDistance3ScenarioMembershipAndInclusiveBoundaries(t *testing.T) {
	scenario := distance3Scenario{name: "declared fixture", extraAdvantage: 1, extraConf: 5}
	cases := []distance3Case{
		distance3CaseValue("a", 3, 1, 65, "APPLIED"),
		distance3CaseValue("b", 3, 1, 64, "applied"),
		distance3CaseValue("c", 3, 1, 66, "Rejected"),
		distance3CaseValue("d", 2, 0, 65, "rejected"),
		distance3CaseValue("u", 3, 1, 65, "pending"),
	}
	if got := compareDistance3(cases, scenario); got != (distance3Comparison{retained: 1, removed: 1, added: 1, unknownPassed: 1}) {
		t.Fatalf("set comparison %+v, want retained1 removed1 added1 unknown1", got)
	}
	var out bytes.Buffer
	if err := reportDistance3(&out, "fixture", cases); err != nil {
		t.Fatal(err)
	}
	distance3RequireText(t, out.String(),
		"Assumed baseline: extras reports=0, advantage=1, score=5\n  Retained applied: 1; removed applied: 1; added rejected: 1\n  Recognized rows passing: 2/4 (50.0%); net change from applied: +0\n  Unknown rows passing (separate): 1/1\n")
	for _, tc := range []struct {
		support, subject, score int
		pass                    bool
	}{
		{3, 1, 64, false}, {3, 1, 65, true}, {3, 1, 66, true},
		{2, 0, 65, false}, {4, 2, 65, true}, {3, 2, 65, false}, {3, 0, 65, true},
	} {
		got := compareDistance3([]distance3Case{distance3CaseValue("boundary", tc.support, tc.subject, tc.score, "rejected")}, scenario)
		want := 0
		if tc.pass {
			want = 1
		}
		if got.added != want || got.retained != 0 || got.removed != 0 || got.unknownPassed != 0 {
			t.Errorf("support=%d subject=%d score=%d: %+v, want added%d only", tc.support, tc.subject, tc.score, got, want)
		}
	}
}

func TestDistance3ReportChangesWithAppliedInput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		scores []int
		want   string
	}{
		{"low-mixed", []int{10, 20, 90}, "Mean logged score: 40.0\nMinimum logged score: 10\n"},
		{"high", []int{90, 90, 90}, "Mean logged score: 90.0\nMinimum logged score: 90\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := []distance3FixtureRow{distance3FixtureValue("EXCLUDED_R", 80, "rejected", "", 3), distance3FixtureValue("EXCLUDED_U", 100, "pending", "", 3)}
			for i, score := range tc.scores {
				values = append(values, distance3FixtureValue(fmt.Sprintf("A%d", i), score, "applied", "", 3))
			}
			var out bytes.Buffer
			if err := run(distance3FixtureDB(t, values), &out); err != nil {
				t.Fatal(err)
			}
			distance3RequireText(t, out.String(), tc.want)
			distance3RequireDescriptive(t, out.String())
		})
	}
}

func TestDistance3SamplesBoundedAndBreakdownsSorted(t *testing.T) {
	var cases []distance3Case
	for i := range 12 {
		cases = append(cases, distance3CaseValue(fmt.Sprintf("A%02d", i), 3, 1, 65, "applied"))
	}
	for i := range 7 {
		c := distance3CaseValue(fmt.Sprintf("R%02d", i), 3, 1, 65, "rejected")
		c.reason = "zeta"
		if i%2 == 1 {
			c.reason = "alpha"
		}
		cases = append(cases, c)
	}
	unknown := distance3CaseValue("U", 3, 1, 65, "pending")
	unknown.reason = "unknown-only"
	cases = append(cases, unknown)
	s := summarizeDistance3(cases)
	if s.applied != 12 || s.rejected != 7 || s.unknown != 1 || len(s.appliedSamples) != 10 || len(s.rejectedSamples) != 5 || len(s.reasons) != 2 || s.reasons["alpha"] != 3 || s.reasons["zeta"] != 4 {
		t.Fatalf("summary boundaries/counts diverged: %+v", s)
	}
	var out bytes.Buffer
	if err := reportDistance3(&out, "fixture", cases); err != nil {
		t.Fatal(err)
	}
	report := out.String()
	distance3RequireText(t, report, "Recognized rejection reasons:\n  alpha: 3\n  zeta: 4\n")
	for _, omitted := range []string{"A10", "A11", "R05", "R06", "unknown-only"} {
		if strings.Contains(report, omitted) {
			t.Errorf("bounded/recognized sample leaked %q", omitted)
		}
	}
	if got := strings.Count(report, " -> "); got != 15 {
		t.Fatalf("sample rows=%d, want10 applied +5 rejected", got)
	}
}

type distance3ErrorWriter struct{ err error }

func (w distance3ErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestDistance3ReportWriterFailureAndEmptyDenominators(t *testing.T) {
	sentinel := errors.New("distance3 fixture writer failure")
	if err := reportDistance3(distance3ErrorWriter{sentinel}, "fixture", nil); !errors.Is(err, sentinel) {
		t.Fatalf("writer failure lost: %v", err)
	}
	if err := reportDistance3(io.Discard, "fixture", nil); err != nil {
		t.Fatal(err)
	}
	for _, cases := range [][]distance3Case{nil, {distance3CaseValue("U", 3, 1, 65, "pending")}} {
		var out bytes.Buffer
		if err := reportDistance3(&out, "fixture", cases); err != nil {
			t.Fatal(err)
		}
		distance3RequireText(t, out.String(), "Mean advantage: N/A\nMinimum advantage: N/A\nMean logged score: N/A\nMinimum logged score: N/A\n", "Recognized rows passing: 0/0 (N/A)")
		distance3RequireDescriptive(t, out.String())
	}
}
