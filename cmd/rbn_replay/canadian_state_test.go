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

func TestReplayCanadianStateIsLocalAndClearsPreviousRun(t *testing.T) {
	var requests atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(source.Close)
	previous := uls.CanadianLicenseChecksEnabled()
	t.Cleanup(func() { uls.CloseLicenseDatabases(); uls.SetCanadianLicenseChecksEnabled(previous) })
	for _, province := range []string{"ON", "QC"} {
		path := filepath.Join(t.TempDir(), "ised.db")
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.ExecContext(t.Context(), `CREATE TABLE CA(call_sign TEXT PRIMARY KEY,state TEXT);
CREATE TABLE Events(id INTEGER PRIMARY KEY,special TEXT,start_day TEXT,end_day TEXT,trustee TEXT,use_by TEXT,uncertain INTEGER,prefix_kind INTEGER);
CREATE INDEX idx_Events_special ON Events(special);
CREATE TABLE SourceMeta(id INTEGER PRIMARY KEY,main_sha TEXT,special_sha TEXT);
INSERT INTO SourceMeta VALUES(1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');
PRAGMA user_version=1;`)
		if err == nil {
			_, err = db.ExecContext(t.Context(), "INSERT INTO CA VALUES('VE3ABC',?);", province)
		}
		closeErr := db.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("fixture=%v close=%v", err, closeErr)
		}
		r := &replayRunner{cfg: &config.Config{ISED: config.ISEDConfig{Enabled: false, DBPath: path, URL: source.URL, SpecialURL: source.URL}}}
		if err := r.configureExternalDependencies(); err != nil {
			t.Fatal(err)
		}
		if result := uls.LookupCanadian("VE3ABC"); result != (uls.LookupResult{Available: true, Found: true, State: province}) {
			t.Fatalf("local replay: %+v", result)
		}
	}
	r := &replayRunner{cfg: &config.Config{ISED: config.ISEDConfig{DBPath: filepath.Join(t.TempDir(), "missing.db"), URL: source.URL, SpecialURL: source.URL}}}
	if err := r.configureExternalDependencies(); err != nil {
		t.Fatal(err)
	}
	if result := uls.LookupCanadian("VE3ABC"); result.Available || result.State != "" {
		t.Fatalf("previous run leaked: %+v", result)
	}
	r.cfg.ISED.Enabled = true
	if err := r.configureExternalDependencies(); err == nil {
		t.Fatal("enabled Canadian replay accepted missing database")
	}
	if requests.Load() != 0 {
		t.Fatal("offline replay downloaded ISED data")
	}
}
