package uls

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const isedMainFixtureHeader = "callsign;first_name;surname;address_line;city;prov_cd;postal_code;qual_a;qual_b;qual_c;qual_d;qual_e;club_name;club_name_2;club_address;club_city;club_prov_cd;club_postal_code\n"
const isedEventsFixtureHeader = "Special_call_sign;beg_date;end_date;event_en;event_fr;call_sign_request;use_by\n"

func newISEDTestDB(t *testing.T, mainRecords, eventRecords string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "ised.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.ExecContext(t.Context(), canadianSchema); err != nil {
		t.Fatal(err)
	}
	if err := importCanadianReaders(context.Background(), db, strings.NewReader(isedMainFixtureHeader+mainRecords), strings.NewReader(isedEventsFixtureHeader+eventRecords)); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestISEDLiteralParsingAndProvince(t *testing.T) {
	// The first fields contain actual leading/bare quotes. CSV semantics would
	// reject or combine these records; the following membership proves separation.
	main := `VE1AAA;"Bill;O"Connor;123 "Road;Halifax;NS;;A;;;;;;;;;;
VE2AAA;Jean;Dupont;;;QC;;;;;;;;;;;;
VE3AAA;A;B;;;ON;;;;;;;;;;;;
VE4AAA;A;B;;;MB;;;;;;;;;;;;
VE5AAA;A;B;;;SK;;;;;;;;;;;;
VE6AAA;A;B;;;AB;;;;;;;;;;;;
VE7AAA;A;B;;;BC;;;;;;;;;;;;
VE8AAA;A;B;;;NT;;;;;;;;;;;;
VE9AAA;A;B;;;NB;;;;;;;;;;;;
VO1AAA;A;B;;;NL;;;;;;;;;;;;
VY0AAA;A;B;;;NU;;;;;;;;;;;;
VY1AAA;A;B;;;YT;;;;;;;;;;;;
VY2AAA;A;B;;;PE;;;;;;;;;;;;
VE3CLB;A;B;;;ON;;;;;;;"Club;Name;Club address;City;BC;X
VE3BLK;A;B;;;ON;;;;;;;Club;;;;;
VE3BAD;A;B;;;ZZ;;;;;;;;;;;;
`
	db := newISEDTestDB(t, main, "")
	want := map[string]string{"VE1AAA": "NS", "VE2AAA": "QC", "VE3AAA": "ON", "VE4AAA": "MB", "VE5AAA": "SK", "VE6AAA": "AB", "VE7AAA": "BC", "VE8AAA": "NT", "VE9AAA": "NB", "VO1AAA": "NL", "VY0AAA": "NU", "VY1AAA": "YT", "VY2AAA": "PE", "VE3CLB": "BC", "VE3BLK": "", "VE3BAD": ""}
	for call, state := range want {
		var got string
		if err := db.QueryRowContext(t.Context(), "SELECT state FROM CA WHERE call_sign=?;", call).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != state {
			t.Errorf("%s province=%q want=%q", call, got, state)
		}
	}
}

func TestISEDDuplicateProvinceOrderIndependent(t *testing.T) {
	for _, states := range []string{"ON,BC,ON", "BC,ON,BC", "ON,,ON", ",ON,ON", "ON,ON,ON"} {
		t.Run(states, func(t *testing.T) {
			var records strings.Builder
			for _, state := range strings.Split(states, ",") {
				records.WriteString("VE3AAA;A;B;;;" + state + ";;;;;;;;;;;;\n")
			}
			db := newISEDTestDB(t, records.String(), "")
			var got string
			if err := db.QueryRowContext(t.Context(), "SELECT state FROM CA WHERE call_sign='VE3AAA';").Scan(&got); err != nil {
				t.Fatal(err)
			}
			want := ""
			if states == "ON,ON,ON" {
				want = "ON"
			}
			if got != want {
				t.Errorf("province=%q want %q", got, want)
			}
		})
	}
}

func TestISEDProjectionDiscardsMalformedFreeText(t *testing.T) {
	db := newISEDTestDB(t, isedLookupMain, "CG3;not a date;2026-10-31;event names discarded;autre texte;;uncertain eligibility prose\nCF3EVENT;2026-10-01;2026-10-31;event names discarded;autre texte;VE3AAA;eligibility prose\n")
	var start, useBy string
	var uncertain int
	if err := db.QueryRowContext(t.Context(), "SELECT start_day,use_by,uncertain FROM Events WHERE special='CG3';").Scan(&start, &useBy, &uncertain); err != nil {
		t.Fatal(err)
	}
	if start != "" || useBy != "" || uncertain != 1 {
		t.Fatalf("uncertain projection retained text: start=%q use_by=%q uncertain=%d", start, useBy, uncertain)
	}
	if err := db.QueryRowContext(t.Context(), "SELECT use_by,uncertain FROM Events WHERE special='CF3EVENT';").Scan(&useBy, &uncertain); err != nil {
		t.Fatal(err)
	}
	if useBy != "" || uncertain != 0 {
		t.Fatalf("exact projection retained irrelevant text: use_by=%q uncertain=%d", useBy, uncertain)
	}
}

func TestISEDStructuralFailureRollsBack(t *testing.T) {
	good := "VE3AAA;A;B;;;ON;;;;;;;;;;;;\n"
	for name, source := range map[string]string{
		"missing header": good,
		"width":          isedMainFixtureHeader + "VE3AAA;short\n",
		"truncation":     isedMainFixtureHeader + strings.TrimSuffix(good, "\n"),
		"encoding":       isedMainFixtureHeader + strings.Replace(good, "A;B", string([]byte{0xff})+";B", 1),
		"identity":       isedMainFixtureHeader + strings.Replace(good, "VE3AAA", "VE3/AAA", 1),
		"long line":      isedMainFixtureHeader + strings.Repeat("x", maxISEDLineBytes) + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			db := newISEDTestDB(t, good, "")
			if err := importCanadianReaders(context.Background(), db, strings.NewReader(source), strings.NewReader(isedEventsFixtureHeader)); err == nil {
				t.Fatal("accepted malformed source")
			}
			var count int
			if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM CA;").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("rollback membership=%d", count)
			}
		})
	}
}

func TestISEDRecordBounds(t *testing.T) {
	source := isedEventsFixtureHeader + "CG3;2026-10-01;2026-10-07;event;;;VE3\n"
	for _, tc := range []struct {
		name  string
		limit int64
		rows  int
	}{{"bytes", 10, 10}, {"rows", 1024, 0}, {"CRLF byte bound", int64(len(strings.ReplaceAll(source, "\n", "\r\n")) - 1), 10}} {
		t.Run(tc.name, func(t *testing.T) {
			input := source
			if tc.name == "CRLF byte bound" {
				input = strings.ReplaceAll(source, "\n", "\r\n")
			}
			if err := readISEDRecords(context.Background(), strings.NewReader(input), isedSpecialHeader, tc.limit, tc.rows, func([]string, map[string]int) error { return nil }); err == nil {
				t.Fatal("accepted bound overflow")
			}
		})
	}
}

func TestISEDFullDownloadedSample(t *testing.T) {
	evidenceDir := os.Getenv("GOCLUSTER_ISED_SAMPLE_DIR")
	if evidenceDir == "" {
		t.Skip("set GOCLUSTER_ISED_SAMPLE_DIR for the downloaded 2026-10-07 sample")
	}
	_, err := os.Stat(filepath.Join(evidenceDir, "amateur_delim.txt"))
	if os.IsNotExist(err) {
		t.Skip("downloaded ISED source evidence unavailable")
	}
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	tmp, err := buildCanadianDatabase(context.Background(), filepath.Join(evidenceDir, "amateur_delim.txt"), filepath.Join(evidenceDir, "special_callsign.txt"), filepath.Join(dir, "ised.db"), strings.Repeat("a", 64), strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(tmp) })
	db, err := sql.Open("sqlite", tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var assigned, events int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM CA;").Scan(&assigned); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM Events;").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if assigned != 92142 || events != 956 {
		t.Fatalf("sample assigned=%d events=%d", assigned, events)
	}
	var known int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM CA WHERE state != ''; ").Scan(&known); err != nil {
		t.Fatal(err)
	}
	if known != 90500 {
		t.Fatalf("sample known_province=%d want 90500 (unknown=1642)", known)
	}
	wantHistogram := map[string]int{"": 1642, "AB": 9173, "BC": 22670, "MB": 2525, "NB": 1995, "NL": 1559, "NS": 2975, "NT": 109, "NU": 41, "ON": 26323, "PE": 417, "QC": 20591, "SK": 1874, "YT": 248}
	rows, err := db.QueryContext(t.Context(), "SELECT state,COUNT(*) FROM CA GROUP BY state;")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			t.Fatal(err)
		}
		if wantHistogram[state] != count {
			t.Errorf("sample province %q=%d want %d", state, count, wantHistogram[state])
		}
		delete(wantHistogram, state)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(wantHistogram) != 0 {
		t.Errorf("sample missing provinces=%v", wantHistogram)
	}
	result, err := queryCanadianLicense(context.Background(), db, "VA1AA", time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC))
	if err != nil || !result.Found || result.State != "NS" {
		t.Fatalf("sample ordinary lookup=%+v err=%v", result, err)
	}
	result, err = queryCanadianLicense(context.Background(), db, "CG7GMT", time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC))
	if err != nil || result.Available {
		t.Fatalf("reversed sample event lookup=%+v err=%v", result, err)
	}
}

func FuzzISEDRecordParser(f *testing.F) {
	for _, seed := range []string{isedEventsFixtureHeader + "CG3;2026-10-01;2026-10-07;event;événement;;VE3\n", isedEventsFixtureHeader + "CG7GMT;2009-12-01;2009-11-01;event;;VE7GMT;\n", isedEventsFixtureHeader + "bad;record"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, source string) {
		if len(source) > 128<<10 {
			t.Skip()
		}
		rows := 0
		err := readISEDRecords(context.Background(), strings.NewReader(source), isedSpecialHeader, 128<<10, 100, func(fields []string, _ map[string]int) error {
			rows++
			if len(fields) != 7 {
				t.Fatal("invalid record width reached consumer")
			}
			return nil
		})
		if err == nil {
			if !strings.HasSuffix(source, "\n") {
				t.Fatal("accepted unterminated source")
			}
			if rows > 100 {
				t.Fatal("accepted row limit overflow")
			}
		}
	})
}
