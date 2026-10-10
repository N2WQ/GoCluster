package main

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

type oneCFixtureRow struct {
	distance, score  int
	decision, reason any
}

func oneCMixedFixture() []oneCFixtureRow {
	return []oneCFixtureRow{
		{1, 10, "applied", "APPLIED_REASON"},
		{1, 20, "APPLIED", "APPLIED_REASON"},
		{1, 90, "Applied", "APPLIED_REASON"},
		{1, 80, "rEjEcTeD", "GATE_B"},
		{1, 70, "REJECTED", "GATE_A"},
		{1, 100, nil, "NULL_UNKNOWN"},
		{1, 99, "", "EMPTY_UNKNOWN"},
		{1, 98, "pending", "OTHER_UNKNOWN"},
		{1, 97, " applied ", "PADDED_UNKNOWN"},
		{1, 96, "rejected\t", "TAB_UNKNOWN"},
		{3, 50, "applied", "APPLIED_REASON"},
		{3, 70, "APPLIED", "APPLIED_REASON"},
		{3, 99, "rejected", nil},
		{3, 88, "Rejected", ""},
	}
}

// Only the original four columns exist. Neighboring readers' IDs, callsigns,
// supports or threshold columns must not become requirements for this reader.
func oneCFixtureDB(t *testing.T, values []oneCFixtureRow, statements ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "distance-history.db")
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
		distance INTEGER, decision TEXT COLLATE NOCASE, winner_confidence INTEGER, reason TEXT COLLATE NOCASE)`); err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if _, err := db.ExecContext(t.Context(), "INSERT INTO decisions VALUES (?,?,?,?)",
			value.distance, value.decision, value.score, value.reason); err != nil {
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

func oneCLoadFixture(t *testing.T, path string) *distanceAnalysis {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("loader database close: %v", err)
		}
	})
	a, err := loadDistanceAnalysis(db)
	if err != nil {
		t.Fatal(err)
	}
	if a == nil {
		t.Fatal("successful loader returned no analysis")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return a
}

func oneCRequireGroup(t *testing.T, a *distanceAnalysis, distance, total, applied, rejected, unknown int, scores []int, reasons map[string]int) {
	t.Helper()
	g := a.byDistance[distance]
	if g == nil {
		t.Fatalf("distance %d absent from groups %v", distance, a.byDistance)
	}
	if g.distance != distance || g.totalDecisions != total || g.appliedCount != applied || g.rejectedCount != rejected || g.unknownCount != unknown {
		t.Fatalf("distance%d counters=%+v, want total%d/A%d/R%d/U%d", distance, g, total, applied, rejected, unknown)
	}
	gotScores := append([]int(nil), g.confidences...)
	slices.Sort(gotScores)
	if !slices.Equal(gotScores, scores) {
		t.Errorf("distance%d applied-only scores=%v want%v", distance, gotScores, scores)
	}
	if len(g.rejectionReasons) != len(reasons) {
		t.Errorf("distance%d known-rejection reason map=%v want%v", distance, g.rejectionReasons, reasons)
	}
	for reason, count := range reasons {
		if g.rejectionReasons[reason] != count {
			t.Errorf("distance%d reason%q count%d want%d", distance, reason, g.rejectionReasons[reason], count)
		}
	}
}

func TestDistanceAnalysisFourColumnMixedOutcomeContract(t *testing.T) {
	a := oneCLoadFixture(t, oneCFixtureDB(t, oneCMixedFixture()))
	if a.totalDecisions != 14 || a.appliedCount != 5 || a.rejectedCount != 4 || a.unknownCount != 5 || len(a.byDistance) != 2 {
		t.Fatalf("literal fourteen-row partition: %+v", a)
	}
	oneCRequireGroup(t, a, 1, 10, 3, 2, 5, []int{10, 20, 90}, map[string]int{"GATE_A": 1, "GATE_B": 1})
	oneCRequireGroup(t, a, 3, 4, 2, 2, 0, []int{50, 70}, map[string]int{"UNKNOWN": 2})
	wantUnknown := map[string]int{"<NULL>": 1, `""`: 1, `"pending"`: 1, `" applied "`: 1, `"rejected\t"`: 1}
	if !reflect.DeepEqual(a.unknownOutcomes, wantUnknown) {
		t.Fatalf("raw unknown outcomes=%v want%v", a.unknownOutcomes, wantUnknown)
	}
	mean, median, available := confidenceSummary(a.byDistance[1].confidences)
	if !available || mean != 40 || median != 20 {
		t.Fatalf("rejected80/unknown100 contaminated applied10/20/90: mean%v median%v available%v", mean, median, available)
	}
	mean, median, available = confidenceSummary(a.byDistance[3].confidences)
	if !available || mean != 60 || median != 60 {
		t.Fatalf("distance3 applied50/70: mean%v median%v available%v", mean, median, available)
	}
}

func TestDistanceAnalysisPreservesAllDistanceGroupsAndZero(t *testing.T) {
	a := oneCLoadFixture(t, oneCFixtureDB(t, []oneCFixtureRow{
		{-1, 80, "rejected", "NEGATIVE_DISTANCE"},
		{0, 100, nil, "UNKNOWN_REASON"},
		{1, 0, "applied", nil},
		{3, 1, "APPLIED", nil},
		{4, 7, "Applied", nil},
	}))
	if a.totalDecisions != 5 || a.appliedCount != 3 || a.rejectedCount != 1 || a.unknownCount != 1 || len(a.byDistance) != 5 {
		t.Fatalf("all-distance population changed: %+v", a)
	}
	oneCRequireGroup(t, a, -1, 1, 0, 1, 0, nil, map[string]int{"NEGATIVE_DISTANCE": 1})
	oneCRequireGroup(t, a, 0, 1, 0, 0, 1, nil, nil)
	oneCRequireGroup(t, a, 1, 1, 1, 0, 0, []int{0}, nil)
	oneCRequireGroup(t, a, 3, 1, 1, 0, 0, []int{1}, nil)
	oneCRequireGroup(t, a, 4, 1, 1, 0, 0, []int{7}, nil)
	mean, median, available := confidenceSummary(a.byDistance[1].confidences)
	if !available || mean != 0 || median != 0 {
		t.Fatalf("observed applied zero became unavailable: %v %v %v", mean, median, available)
	}
	for _, distance := range []int{-1, 0} {
		_, _, available := confidenceSummary(a.byDistance[distance].confidences)
		if available {
			t.Errorf("distance%d without applied rows fabricated statistics", distance)
		}
	}
}

func TestDistanceAnalysisEmptyAndPreservedUnknownReasonBucket(t *testing.T) {
	empty := oneCLoadFixture(t, oneCFixtureDB(t, nil))
	if empty.totalDecisions != 0 || empty.appliedCount != 0 || empty.rejectedCount != 0 || empty.unknownCount != 0 || len(empty.byDistance) != 0 || len(empty.unknownOutcomes) != 0 {
		t.Fatalf("valid empty history: %+v", empty)
	}
	a := oneCLoadFixture(t, oneCFixtureDB(t, []oneCFixtureRow{
		{2, 10, "rejected", nil}, {2, 20, "Rejected", ""}, {2, 30, "REJECTED", "UNKNOWN"},
		{2, 40, "pending", "UNKNOWN"}, {2, 50, "applied", "UNKNOWN"},
	}))
	oneCRequireGroup(t, a, 2, 5, 1, 3, 1, []int{50}, map[string]int{"UNKNOWN": 3})
}
