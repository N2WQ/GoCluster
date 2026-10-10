package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

type inspectionFixtureRow struct {
	subject, winner                                                     string
	distance, score, support, reporters, reports, advantage, confidence int
	decision, reason                                                    any
}

func inspectionFixtureValue(subject string, distance, score int, decision any) inspectionFixtureRow {
	return inspectionFixtureRow{subject: subject, winner: "W1XYZ", distance: distance, score: score,
		support: 3, reporters: 4, reports: 3, advantage: 1, confidence: 60, decision: decision, reason: "GATE"}
}

func inspectionMixedFixture() []inspectionFixtureRow {
	values := []inspectionFixtureRow{
		inspectionFixtureValue("A_LOW", 1, 10, "applied"),
		inspectionFixtureValue("A_UPPER", 2, 20, "APPLIED"),
		inspectionFixtureValue("A_MIXED", 3, 90, "Applied"),
		inspectionFixtureValue("R_D1_70", 1, 70, "rEjEcTeD"),
		inspectionFixtureValue("R_D1_71", 1, 71, "REJECTED"),
		inspectionFixtureValue("R_D2_71", 2, 71, "rejected"),
		inspectionFixtureValue("R_D2_95", 2, 95, "Rejected"),
	}
	values[3].reason = nil
	values[4].support, values[4].reporters, values[4].reason = 4, 5, ""
	values[5].support, values[5].reporters = 5, 6
	for i, outcome := range []any{nil, "<NULL>", "", "pending", "PENDING", " applied ", "rejected\t"} {
		value := inspectionFixtureValue(fmt.Sprintf("UNKNOWN%02d", i), 1, 100, outcome)
		value.support, value.reporters = 100, 101
		values = append(values, value)
	}
	return values
}

// The retained legacy schema has no ID or unselected call-correction fields.
// NOCASE columns deliberately challenge binary raw grouping and sample ties.
func inspectionFixtureDB(t *testing.T, values []inspectionFixtureRow, statements ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "inspection-history.db")
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
		subject TEXT COLLATE NOCASE, winner TEXT COLLATE NOCASE, distance INTEGER,
		winner_confidence INTEGER, winner_support INTEGER, total_reporters INTEGER,
		min_reports INTEGER, min_advantage INTEGER, min_confidence INTEGER,
		decision TEXT COLLATE NOCASE, reason TEXT COLLATE NOCASE)`); err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if _, err := db.ExecContext(t.Context(), "INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
			value.subject, value.winner, value.distance, value.score, value.support, value.reporters,
			value.reports, value.advantage, value.confidence, value.decision, value.reason); err != nil {
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

func inspectionLoadFixture(t *testing.T, path string) *inspection {
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
	data, err := loadInspection(db)
	if err != nil || data == nil {
		t.Fatalf("valid inspection load: data=%+v err=%v", data, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return data
}

func inspectionSampleSubjects(samples []decisionSample) []string {
	var names []string
	for _, sample := range samples {
		names = append(names, sample.subject)
	}
	return names
}

func TestInspectionRealSQLiteKnownAndRawUnknownContract(t *testing.T) {
	data := inspectionLoadFixture(t, inspectionFixtureDB(t, inspectionMixedFixture()))
	if data.total != 14 || data.applied != 3 || data.rejected != 4 || data.unknown != 7 {
		t.Fatalf("literal known/raw-unknown partition: %+v", data)
	}
	wantOutcomes := map[sql.NullString]int64{{}: 1}
	for _, label := range []string{"applied", "APPLIED", "Applied", "rEjEcTeD", "REJECTED", "rejected", "Rejected", "<NULL>", "", "pending", "PENDING", " applied ", "rejected\t"} {
		wantOutcomes[sql.NullString{String: label, Valid: true}] = 1
	}
	gotOutcomes := make(map[sql.NullString]int64)
	for _, outcome := range data.outcomes {
		if _, duplicate := gotOutcomes[outcome.value]; duplicate {
			t.Fatalf("duplicate raw breakdown value: %+v", outcome)
		}
		gotOutcomes[outcome.value] = outcome.count
	}
	if !reflect.DeepEqual(gotOutcomes, wantOutcomes) {
		t.Fatalf("NOCASE collapsed raw spelling or NULL/literal<NULL>: got%v want%v", gotOutcomes, wantOutcomes)
	}
	if got := inspectionSampleSubjects(data.appliedSamples); !reflect.DeepEqual(got, []string{"A_LOW", "A_MIXED", "A_UPPER"}) {
		t.Fatalf("applied lexical membership/order: %v", got)
	}
	if got := inspectionSampleSubjects(data.distance1Samples); !reflect.DeepEqual(got, []string{"R_D1_71", "R_D1_70"}) {
		t.Fatalf("distance1 recognized rejection/support order: %v", got)
	}
	if got := inspectionSampleSubjects(data.highScoreSamples); !reflect.DeepEqual(got, []string{"R_D2_95", "R_D1_71", "R_D2_71"}) {
		t.Fatalf("strict score70/71 and descending score membership: %v", got)
	}
	if data.distance1Samples[0].reason != (sql.NullString{String: "", Valid: true}) || data.distance1Samples[1].reason.Valid {
		t.Fatalf("valid empty versus NULL reasons changed: %+v", data.distance1Samples)
	}
	first := data.distance1Samples[0]
	if first.support != 4 || first.reporters != 5 || first.reports != 3 || first.advantage != 1 || first.confidence != 60 || first.decision.String != "REJECTED" {
		t.Fatalf("distance1 selected numeric/raw fields changed: %+v", first)
	}
}

func inspectionIgnoredFieldFixture(t *testing.T) string {
	t.Helper()
	values := []inspectionFixtureRow{
		inspectionFixtureValue("A_UNUSED", 1, 50, "applied"),
		inspectionFixtureValue("UNKNOWN_UNUSED", 1, 100, "pending"),
		inspectionFixtureValue("HIGH_UNUSED", 2, 71, "rejected"),
	}
	return inspectionFixtureDB(t, values,
		"UPDATE decisions SET winner_support='bad-support',min_reports='bad-reports',min_advantage='bad-advantage',min_confidence='bad-confidence' WHERE subject='A_UNUSED'",
		"UPDATE decisions SET distance='bad-distance',winner_confidence='bad-score',winner_support='bad-support',total_reporters='bad-total',min_reports='bad-reports',min_advantage='bad-advantage',min_confidence='bad-confidence' WHERE subject='UNKNOWN_UNUSED'",
		"UPDATE decisions SET min_reports='bad-reports',min_advantage='bad-advantage',min_confidence='bad-confidence' WHERE subject='HIGH_UNUSED'")
}

func TestInspectionPreservesEachProjectionAndIgnoredMalformedFields(t *testing.T) {
	data := inspectionLoadFixture(t, inspectionIgnoredFieldFixture(t))
	if data.total != 3 || data.applied != 1 || data.rejected != 1 || data.unknown != 1 || len(data.appliedSamples) != 1 || len(data.distance1Samples) != 0 || len(data.highScoreSamples) != 1 {
		t.Fatalf("unselected malformed fields became validation inputs: %+v", data)
	}
	if data.appliedSamples[0].subject != "A_UNUSED" || data.appliedSamples[0].score != 50 || data.appliedSamples[0].reporters != 4 || data.highScoreSamples[0].subject != "HIGH_UNUSED" || data.highScoreSamples[0].support != 3 {
		t.Fatalf("distinct selected projections changed: %+v", data)
	}
}
