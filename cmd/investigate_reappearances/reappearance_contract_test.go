package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type reappearanceFixtureRow struct {
	id, ts               int64
	subject, winner      string
	freq                 float64
	distance, confidence int
	decision             any
}

func reappearanceSeed(id, ts int64, subject, winner string, decision any) reappearanceFixtureRow {
	return reappearanceFixtureRow{id: id, ts: ts, subject: subject, winner: winner,
		freq: 14074, distance: 3, confidence: 65, decision: decision}
}

func reappearanceObservation(id, ts int64, subject string, freq float64, decision any) reappearanceFixtureRow {
	return reappearanceFixtureRow{id: id, ts: ts, subject: subject, winner: "UNUSED", freq: freq, decision: decision}
}

// Only the eight original columns are provided. NOCASE text and non-unique
// IDs challenge explicit binary ordering without a new schema assumption.
func reappearanceFixtureDB(t *testing.T, values []reappearanceFixtureRow, statements ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "reappearance-history.db")
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
		id INTEGER, ts INTEGER, subject TEXT COLLATE NOCASE, winner TEXT COLLATE NOCASE,
		freq_khz REAL, distance INTEGER, winner_confidence INTEGER, decision TEXT COLLATE NOCASE)`); err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if _, err := db.ExecContext(t.Context(), "INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?)",
			value.id, value.ts, value.subject, value.winner, value.freq, value.distance, value.confidence, value.decision); err != nil {
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

func reappearanceLoadFixture(t *testing.T, path string, hours int) []reappearanceCase {
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
	cases, err := analyzeBothReappearances(db, hours)
	if err != nil {
		t.Fatalf("valid reappearance load: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return cases
}

func reappearanceObserveFixture(t *testing.T, path string, start, end int64, subject, winner string) *firstAppearances {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("observation database close: %v", err)
		}
	})
	first, err := observeFirstAppearances(db, start, end, subject, winner)
	if err != nil || first == nil {
		t.Fatalf("valid follow-up load: first%+v err=%v", first, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return first
}

func TestReappearanceCaseOnlySeedsNormalizeAndIgnoreWinnerField(t *testing.T) {
	var values []reappearanceFixtureRow
	for i, label := range []string{"applied", "APPLIED", "Applied"} {
		id := int64(i + 1)
		values = append(values, reappearanceSeed(id, 1000, fmt.Sprintf(" s%02d ", id), fmt.Sprintf(" w%02d ", id), label),
			reappearanceObservation(100+id*2, 1100, fmt.Sprintf("s%02d", id), 7000+float64(id), "rejected"),
			reappearanceObservation(101+id*2, 1101, fmt.Sprintf("w%02d", id), 7000.5+float64(id), nil))
	}
	for i, label := range []any{nil, "", "pending", "PENDING", " applied ", "rejected", "applied\t"} {
		id := int64(10 + i)
		values = append(values, reappearanceSeed(id, 1000, fmt.Sprintf("U%d", id), fmt.Sprintf("V%d", id), label),
			reappearanceObservation(200+id*2, 1100, fmt.Sprintf("U%d", id), 7000, "pending"),
			reappearanceObservation(201+id*2, 1101, fmt.Sprintf("V%d", id), 7001, ""))
	}
	values = append(values, reappearanceSeed(4, 1000, "SUBJECTONLY", "WINNERONLY", "applied"),
		reappearanceObservation(400, 1100, "SUBJECTONLY", 7000, "pending"))
	decoy := reappearanceObservation(401, 1101, "DECOY", 7000, "pending")
	decoy.winner = "WINNERONLY"
	values = append(values, decoy)
	cases := reappearanceLoadFixture(t, reappearanceFixtureDB(t, values), 24)
	var ids []int64
	for i, c := range cases {
		ids = append(ids, c.correctionID)
		id := int64(i + 1)
		if c.subject != fmt.Sprintf("S%02d", id) || c.winner != fmt.Sprintf("W%02d", id) || c.correctionTime != time.Unix(1000, 0).UTC() || c.freqKHz != 14074 || c.distance != 3 || c.confidence != 65 || c.subjectFreqKHz != 7000+float64(id) || c.winnerFreqKHz != 7000.5+float64(id) || c.freqSeparationKHz != 0.5 {
			t.Errorf("literal normalized seed/projection/first frequencies: %+v", c)
		}
	}
	if !reflect.DeepEqual(ids, []int64{1, 2, 3}) {
		t.Fatalf("case-only seed memberships/winner-field decoy: %v", ids)
	}
}

func TestReappearanceCombinedRow100And101AcrossAllOutcomes(t *testing.T) {
	for _, winnerRow := range []int{100, 101} {
		t.Run(fmt.Sprintf("winner-row%d", winnerRow), func(t *testing.T) {
			values := []reappearanceFixtureRow{reappearanceSeed(1, 1000, "S", "W", "applied")}
			outcomes := []any{nil, "", "pending", "rejected", "APPLIED"}
			for i := 1; i <= winnerRow; i++ {
				name, freq := "S", float64(7000+i)
				if i == winnerRow {
					name, freq = "W", 8000
				}
				values = append(values, reappearanceObservation(int64(100+i), int64(1000+i), name, freq, outcomes[(i-1)%len(outcomes)]))
			}
			path := reappearanceFixtureDB(t, values)
			first := reappearanceObserveFixture(t, path, 1000, 87400, "S", "W")
			if !first.subjectSeen || first.subjectFreq != 7001 || first.winnerSeen != (winnerRow == 100) || (winnerRow == 100 && first.winnerFreq != 8000) {
				t.Fatalf("literal combined cap across outcomes: %+v", first)
			}
			cases := reappearanceLoadFixture(t, path, 24)
			if winnerRow == 100 {
				if len(cases) != 1 || cases[0].correctionID != 1 || cases[0].subjectFreqKHz != 7001 || cases[0].winnerFreqKHz != 8000 {
					t.Fatalf("row100 must form exactly seed1: %+v", cases)
				}
			} else if len(cases) != 0 {
				t.Fatalf("row101 crossed combined cap: %+v", cases)
			}
		})
	}
}

func TestReappearanceExactOpenStartClosedEndAndPreservedLookahead(t *testing.T) {
	for _, tc := range []struct {
		name                string
		subjectTS, winnerTS int64
		wantDual            bool
	}{
		{"subject-at-start", 1000, 4600, false}, {"subject-at-next-second", 1001, 4600, true},
		{"winner-at-end", 1001, 4600, true}, {"winner-after-end", 1001, 4601, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := reappearanceFixtureDB(t, []reappearanceFixtureRow{reappearanceSeed(1, 1000, "S", "W", "applied"),
				reappearanceObservation(2, tc.subjectTS, "S", 7000, "pending"), reappearanceObservation(3, tc.winnerTS, "W", 7001, "rejected")})
			cases := reappearanceLoadFixture(t, path, 1)
			wantCount := 0
			if tc.wantDual {
				wantCount = 1
			}
			if len(cases) != wantCount {
				t.Fatalf("literal(start,end] one-hour result: %+v", cases)
			}
		})
	}
	path := reappearanceFixtureDB(t, []reappearanceFixtureRow{reappearanceSeed(1, 1000, "S", "W", "applied"),
		reappearanceObservation(2, 1001, "S", 7000, "pending"), reappearanceObservation(3, 87400, "W", 7001, "pending")})
	if cases := reappearanceLoadFixture(t, path, 24); len(cases) != 1 {
		t.Fatalf("default24 exactend excluded: %+v", cases)
	}
	for _, hours := range []int{0, -1} {
		if cases := reappearanceLoadFixture(t, path, hours); len(cases) != 0 {
			t.Fatalf("preserved zero/negative lookahead%d: %+v", hours, cases)
		}
	}
}

func TestReappearanceFirstZeroAndEqualNormalizedIdentities(t *testing.T) {
	values := []reappearanceFixtureRow{reappearanceSeed(1, 1000, "S", "W", "applied"),
		reappearanceObservation(2, 1001, "s", 0, "pending"), reappearanceObservation(3, 1002, "S", 14074, "rejected"),
		reappearanceObservation(4, 1003, "w", 7000, ""), reappearanceObservation(5, 1004, "W", 21074, nil),
		reappearanceSeed(6, 2000, " same ", "SaMe", "Applied"), reappearanceObservation(7, 2001, "same", 0, "pending"),
		reappearanceObservation(8, 2002, "SAME", 7000, "rejected")}
	cases := reappearanceLoadFixture(t, reappearanceFixtureDB(t, values), 24)
	if len(cases) != 2 {
		t.Fatalf("literal zero/equal identity cases: %+v", cases)
	}
	if c := cases[0]; c.correctionID != 1 || c.subjectFreqKHz != 0 || c.winnerFreqKHz != 7000 || c.freqSeparationKHz != 7000 {
		t.Errorf("firstzero replaced by laterfrequency: %+v", c)
	}
	if c := cases[1]; c.correctionID != 6 || c.subject != "SAME" || c.winner != "SAME" || c.subjectFreqKHz != 0 || c.winnerFreqKHz != 0 || c.freqSeparationKHz != 0 {
		t.Errorf("independent equalidentity checks changed: %+v", c)
	}
	var output bytes.Buffer
	if err := reportReappearances(&output, "zero history", 24, cases); err != nil {
		t.Fatal(err)
	}
	reappearanceRequireText(t, output.String(),
		"first subject=0.000 kHz; first winner=7000.000 kHz; separation=7000.000 kHz\n",
		`"SAME" -> "SAME" distance=3 logged score=65`,
		"first subject=0.000 kHz; first winner=0.000 kHz; separation=0.000 kHz\n")
}
