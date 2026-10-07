package uls

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// Arbitrary extra EN evidence cannot remove an established active license or
// replace its known state with a conflicting guess. Inputs are bounded so each
// iteration uses one isolated in-memory transaction and releases its owner.
func FuzzImportStateEvidence(f *testing.F) {
	f.Add(sourceRow("EN", 1, "K1ABC", "L", "TX"))
	f.Add(sourceRow("EN", 1, "K1ABC", "L", ""))
	f.Add("EN|1|||K1ABC|L|short\n")
	f.Fuzz(func(t *testing.T, extra string) {
		if len(extra) > 512 {
			t.Skip()
		}
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		db.SetMaxOpenConns(1)
		if _, err := db.ExecContext(context.Background(), "CREATE TABLE AM(unique_system_identifier INTEGER,call_sign TEXT,state TEXT NOT NULL DEFAULT ''); CREATE INDEX idx_id ON AM(unique_system_identifier);"); err != nil {
			t.Fatal(err)
		}
		for id, call := range []string{"K1ABC", "K2ABC", "K3ABC", "K4ABC", "K5ABC", "K6ABC", "K7ABC"} {
			if _, err := db.ExecContext(context.Background(), "INSERT INTO AM(unique_system_identifier,call_sign) VALUES(?,?)", id+1, call); err != nil {
				t.Fatal(err)
			}
		}
		source := strings.NewReader(stateSources()["EN.DAT"] + "\n" + extra + "\n")
		if err := importStatesReader(context.Background(), db, source); err != nil {
			t.Fatal(err)
		}
		var state string
		if err := db.QueryRowContext(context.Background(), "SELECT state FROM AM WHERE call_sign='K1ABC'").Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state != "CA" && state != "" {
			t.Fatalf("conflicting state became authoritative: %q", state)
		}
		var count int
		if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM AM").Scan(&count); err != nil || count != 7 {
			t.Fatalf("license population=%d err=%v", count, err)
		}
	})
}
