package main

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// Fixtures use the historical projection, not a restored production logger.
// IDs deliberately need not be unique so legacy ordering ties are reachable.
type thresholdFixtureRow struct {
	id                                        int64
	subject, winner                           string
	distance, score, support, subjectSupport  int
	reporters, reports, advantage, confidence int
	decision, reason                          any
}

func thresholdFixtureValue(id int64, subject string, score int, decision any) thresholdFixtureRow {
	return thresholdFixtureRow{id: id, subject: subject, winner: "W1XYZ", distance: 1,
		score: score, support: 3, subjectSupport: 1, reporters: 4,
		reports: 3, advantage: 1, confidence: 60, decision: decision, reason: ""}
}

func thresholdMixedFixture() []thresholdFixtureRow {
	a := thresholdFixtureValue(1, " k1abc ", 55, "APPLIED")
	a.winner = " w1xyz "
	b := thresholdFixtureValue(2, "B", 54, "Applied")
	b.distance = 2
	c := thresholdFixtureValue(3, "C", 60, "ReJeCtEd")
	c.distance = 3
	d := thresholdFixtureValue(4, "D", 60, "rejected")
	d.distance, d.subjectSupport, d.reporters = 3, 2, 5
	d.reports, d.advantage, d.confidence = 2, 2, 70
	u := thresholdFixtureValue(5, "U", 55, "pending")
	u.reports, u.advantage, u.confidence = 2, 2, 70
	return []thresholdFixtureRow{a, b, c, d, u}
}

func thresholdFixtureDB(t *testing.T, values []thresholdFixtureRow, statements ...string) string {
	t.Helper()
	return thresholdFixtureDBWithCollation(t, values, "", statements...)
}

func thresholdFixtureDBWithCollation(t *testing.T, values []thresholdFixtureRow, collation string, statements ...string) string {
	t.Helper()
	if collation != "" && collation != " COLLATE NOCASE" {
		t.Fatal("fixture permits only explicit legacy NOCASE collation")
	}
	path := filepath.Join(t.TempDir(), "threshold-history.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("fixture database close: %v", err)
		}
	})
	schema := `CREATE TABLE decisions (
		id INTEGER, subject TEXT` + collation + `, winner TEXT` + collation + `, distance INTEGER,
		winner_confidence INTEGER, winner_support INTEGER, subject_support INTEGER, total_reporters INTEGER,
		min_reports INTEGER, min_advantage INTEGER, min_confidence INTEGER,
		decision TEXT` + collation + `, reason TEXT` + collation + `)`
	if _, err := db.ExecContext(t.Context(), schema); err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if value.support < 0 || value.subjectSupport < 0 || value.reporters < value.support+value.subjectSupport {
			t.Fatalf("invalid count fixture: %+v", value)
		}
		_, err := db.ExecContext(t.Context(), `INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			value.id, value.subject, value.winner, value.distance, value.score,
			value.support, value.subjectSupport, value.reporters,
			value.reports, value.advantage, value.confidence, value.decision, value.reason)
		if err != nil {
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

func thresholdLoadFixture(t *testing.T, path string) []decisionRecord {
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
	values, err := loadAllDecisions(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return values
}
