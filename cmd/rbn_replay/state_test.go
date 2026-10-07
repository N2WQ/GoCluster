package main

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"dxcluster/config"
	"dxcluster/uls"
)

func TestReplayStateOffLocalOnlyAndReset(t *testing.T) {
	var requests atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(source.Close)
	t.Cleanup(func() { uls.SetLicenseDBPath(""); uls.SetLicenseChecksEnabled(true) })
	for i, state := range []string{"CA", "TX"} {
		path := filepath.Join(t.TempDir(), "fcc.db")
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `CREATE TABLE AM(call_sign TEXT, state TEXT); PRAGMA user_version=1;`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(t.Context(), `INSERT INTO AM VALUES('K1ABC',?)`, state); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		r := &replayRunner{cfg: &config.Config{FCCULS: config.FCCULSConfig{Enabled: false, DBPath: path, URL: source.URL}}}
		if err := r.configureExternalDependencies(); err != nil {
			t.Fatal(err)
		}
		result := uls.LookupUS("K1ABC")
		if !result.Available || !result.Found || result.State != state {
			t.Fatalf("run %d retained previous/offline metadata: %+v", i, result)
		}
	}
	missing := filepath.Join(t.TempDir(), "missing.db")
	r := &replayRunner{cfg: &config.Config{FCCULS: config.FCCULSConfig{Enabled: false, DBPath: missing, URL: source.URL}}}
	if err := r.configureExternalDependencies(); err != nil {
		t.Fatalf("optional offline database became required: %v", err)
	}
	if result := uls.LookupUS("K1ABC"); result.Available || result.State != "" {
		t.Fatalf("missing DB retained another run's metadata: %+v", result)
	}
	r.cfg.FCCULS.Enabled = true
	if err := r.configureExternalDependencies(); err == nil {
		t.Fatal("enabled replay no longer requires its database")
	}
	if requests.Load() != 0 {
		t.Fatal("offline replay downloaded reference data")
	}
}
