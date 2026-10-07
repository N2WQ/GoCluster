package uls

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The hand-written EN records follow the FCC ULS Data File Formats, ENTITY
// table (page 3): https://wireless.fcc.gov/wtbfiles/pa_ddef51.pdf. They contain
// all 27 documented fields, including distinct city/state/ZIP values at fields
// 17/18/19. No EN record is generated with the importer's positional constants.
// ULS file numbers differ from system IDs so using the wrong identity field
// cannot accidentally match the active license projection.
func TestImportLiteralFCCStateColumnAndIdentity(t *testing.T) {
	en, err := os.ReadFile(filepath.Join("testdata", "state-column-en.dat"))
	if err != nil {
		t.Fatal(err)
	}
	for i, line := range strings.Split(strings.TrimSpace(string(en)), "\n") {
		if got := len(strings.Split(line, "|")); got != 27 {
			t.Fatalf("literal FCC EN row %d has %d fields, want 27", i+1, got)
		}
	}
	files := map[string]string{"EN.DAT": string(en)}
	for i, call := range []string{"K1COL", "K1POST", "K1BLANK", "K1CON", "K1ID", "K1NOEN", "K1OLD"} {
		status := "A"
		if call == "K1OLD" {
			status = "E"
		}
		files["HD.DAT"] += sourceRow("HD", i+101, call, status, "")
		files["AM.DAT"] += sourceRow("AM", i+101, call, "", "")
	}
	path := filepath.Join(t.TempDir(), "literal.db")
	if err := buildDatabase(context.Background(), writeSources(t, files), path, ""); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	want := map[string]string{"K1COL": "CA", "K1POST": "AP", "K1BLANK": "", "K1CON": "", "K1ID": "", "K1NOEN": ""}
	rows, err := db.QueryContext(context.Background(), "SELECT call_sign,state FROM AM ORDER BY call_sign;")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var call, state string
		if err := rows.Scan(&call, &state); err != nil {
			t.Fatal(err)
		}
		expected, exists := want[call]
		if !exists || state != expected {
			t.Fatalf("imported %s=%q, want state %q and active membership=%v", call, state, expected, exists)
		}
		delete(want, call)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(want) != 0 {
		t.Fatalf("active licenses missing from projection: %v", want)
	}
}
