package uls

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/download"
	"dxcluster/spot"
)

func TestImportStatesOrderIndependent(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			files := stateSources()
			if reverse {
				lines := strings.Split(strings.TrimSpace(files["EN.DAT"]), "\n")
				for i, j := 0, len(lines)-1; i < j; i, j = i+1, j-1 {
					lines[i], lines[j] = lines[j], lines[i]
				}
				files["EN.DAT"] = strings.Join(lines, "\n") + "\n"
			}
			path := filepath.Join(t.TempDir(), "built.db")
			if err := buildDatabase(context.Background(), writeSources(t, files), path, ""); err != nil {
				t.Fatal(err)
			}
			db, _ := sql.Open("sqlite", path)
			defer db.Close()
			for _, call := range []string{"K1ABC", "K2ABC", "K3ABC", "K4ABC", "K5ABC", "K6ABC", "K7ABC"} {
				var state string
				if err := db.QueryRowContext(context.Background(), "SELECT state FROM AM WHERE call_sign=?", call).Scan(&state); err != nil {
					t.Fatal(err)
				}
				want := ""
				if call == "K1ABC" {
					want = "CA"
				}
				if state != want {
					t.Fatalf("%s state=%q want %q", call, state, want)
				}
			}
			var count, version int
			if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM AM").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 7 {
				t.Fatal(count)
			}
			if err := db.QueryRowContext(context.Background(), "PRAGMA user_version").Scan(&version); err != nil || version != 1 {
				t.Fatalf("version=%d err=%v", version, err)
			}
			if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count); err != nil || count != 2 {
				t.Fatalf("persistent tables=%d err=%v", count, err)
			}
		})
	}
}

func TestImportAllStateCodesAndBlank(t *testing.T) {
	files := map[string]string{}
	for id, state := range append(spot.FCCStateCodes(), "") {
		call := fmt.Sprintf("K1X%d", id)
		files["HD.DAT"] += sourceRow("HD", id+1, call, "A", "")
		files["AM.DAT"] += sourceRow("AM", id+1, call, "", "")
		files["EN.DAT"] += sourceRow("EN", id+1, call, "L", state)
	}
	path := filepath.Join(t.TempDir(), "built.db")
	if err := buildDatabase(context.Background(), writeSources(t, files), path, ""); err != nil {
		t.Fatal(err)
	}
	db, _ := sql.Open("sqlite", path)
	defer db.Close()
	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM AM WHERE state!=''").Scan(&count); err != nil || count != 60 {
		t.Fatalf("known=%d err=%v", count, err)
	}
}

func TestDuplicateActiveCallRequiresEveryLicenseIdentity(t *testing.T) {
	for _, evidence := range []string{"missing", "", "TX", "CA", "INVALID"} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%v", evidence, reverse), func(t *testing.T) {
				files := stateSources()
				files["HD.DAT"] += sourceRow("HD", 10, "K1ABC", "A", "")
				files["AM.DAT"] += sourceRow("AM", 10, "K1ABC", "", "")
				if evidence != "missing" {
					files["EN.DAT"] += sourceRow("EN", 10, "K1ABC", "L", evidence)
				}
				if reverse {
					lines := strings.Split(strings.TrimSpace(files["EN.DAT"]), "\n")
					for i, j := 0, len(lines)-1; i < j; i, j = i+1, j-1 {
						lines[i], lines[j] = lines[j], lines[i]
					}
					files["EN.DAT"] = strings.Join(lines, "\n") + "\n"
				}
				path := filepath.Join(t.TempDir(), "built.db")
				if err := buildDatabase(context.Background(), writeSources(t, files), path, ""); err != nil {
					t.Fatal(err)
				}
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				want := ""
				if evidence == "CA" {
					want = "CA"
				}
				var count int
				if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM AM WHERE call_sign='K1ABC' AND state=?", want).Scan(&count); err != nil || count != 2 {
					t.Fatalf("state=%q retained licenses=%d err=%v", want, count, err)
				}
			})
		}
	}
}

func TestImportFailureKeepsLastGood(t *testing.T) {
	for _, en := range []string{"missing", "garbage", "unrelated"} {
		t.Run(en, func(t *testing.T) {
			files := stateSources()
			switch en {
			case "missing":
				delete(files, "EN.DAT")
			case "garbage":
				files["EN.DAT"] = "broken\n"
			case "unrelated":
				files["EN.DAT"] = sourceRow("EN", 500, "K9ABC", "L", "CA")
			}
			path := fixtureDB(t, true)
			before, _ := os.ReadFile(path)
			if err := buildDatabase(context.Background(), writeSources(t, files), path, ""); err == nil {
				t.Fatal("expected rejected build")
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(before, after) {
				t.Fatal("last-good changed")
			}
		})
	}
}

func TestLookupFactualAndLegacy(t *testing.T) {
	defer SetLicenseDBPath("")
	defer SetLicenseChecksEnabled(true)
	for _, legacy := range []bool{false, true} {
		SetLicenseDBPath(fixtureDB(t, legacy))
		SetLicenseChecksEnabled(false)
		wantState := "CA"
		if legacy {
			wantState = ""
		}
		if got := LookupUS("VE3/K1ABC-#"); got != (LookupResult{Available: true, Found: true, State: wantState}) {
			t.Fatal(got)
		}
		if !IsLicensedUS("K9MISSING") {
			t.Fatal("disabled enforcement")
		}
		if got := LookupUS("K9MISSING"); !got.Available || got.Found {
			t.Fatal(got)
		}
		SetLicenseChecksEnabled(true)
		if IsLicensedUS("K9MISSING") {
			t.Fatal("missing membership allowed")
		}
		SetRefreshInProgress(true)
		if got := LookupUS("K1ABC"); got.Available || got.State != "" {
			t.Fatal(got)
		}
		if !IsLicensedUS("K9MISSING") {
			t.Fatal("refresh did not fail open")
		}
		SetRefreshInProgress(false)
	}
	SetLicenseDBPath(filepath.Join(t.TempDir(), "absent.db"))
	if got := LookupUS("K1ABC"); got.Available {
		t.Fatal(got)
	}
	if LookupStats().Entries != 0 {
		t.Fatal("cached outage")
	}
}

func TestLookupGenerationBarrierAndChurn(t *testing.T) {
	defer SetLicenseDBPath("")
	SetLicenseDBPath(fixtureDB(t, false))
	old := licenseCache.Load()
	ResetLicenseDB()
	if result := finishLookup(old, "K1ABC", LookupResult{Available: true, Found: true, State: "NY"}, time.Now()); result.Available {
		t.Fatal("old result published")
	}
	if LookupStats().Entries != 0 {
		t.Fatal("old cache entry in new owner")
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				LookupUS("K1ABC")
				LookupStats()
			}
		}()
	}
	for n := 0; n < 20; n++ {
		ResetLicenseDB()
	}
	wg.Wait()
	cache := newLicenseCache(time.Minute, 7)
	for n := 0; n < 1000; n++ {
		cache.set(fmt.Sprint(n), LookupResult{Available: true}, time.Now())
	}
	if len(cache.entries) > 7 || len(cache.slots) != 7 {
		t.Fatal("unbounded cache")
	}
	for key, entry := range cache.entries {
		if cache.slots[entry.slot].key != key {
			t.Fatal("orphan slot")
		}
	}
}

func TestRefreshMigrationFailureRetryAndFirstBoot(t *testing.T) {
	defer SetLicenseDBPath("")
	files := stateSources()
	valid := zippedSources(t, files)
	delete(files, "EN.DAT")
	bad := zippedSources(t, files)
	var mode atomic.Int32
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("ETag", "unchanged")
		if mode.Load() == 0 {
			w.Write(bad)
		} else {
			w.Write(valid)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	cfg := config.FCCULSConfig{URL: server.URL, Archive: filepath.Join(dir, "a.zip"), DBPath: fixtureDB(t, true)}
	SetLicenseDBPath(cfg.DBPath)
	if _, err := Refresh(context.Background(), cfg, false); err == nil {
		t.Fatal("missing EN accepted")
	}
	if got := LookupUS("K1ABC"); !got.Available || !got.Found || got.State != "" {
		t.Fatal(got)
	}
	meta, _ := download.ReadMetadata(download.MetadataPath(cfg.Archive))
	if meta == nil || meta.ProcessedOK {
		t.Fatal("failure status missing")
	}
	mode.Store(1)
	if updated, err := Refresh(context.Background(), cfg, false); err != nil || !updated {
		t.Fatalf("retry updated=%v err=%v", updated, err)
	}
	if got := LookupUS("K1ABC"); got.State != "CA" {
		t.Fatal(got)
	}
	if requests.Load() != 2 {
		t.Fatal(requests.Load())
	}
	cfg.DBPath = filepath.Join(dir, "first.db")
	SetLicenseDBPath(cfg.DBPath)
	if _, err := Refresh(context.Background(), cfg, false); err != nil {
		t.Fatal(err)
	}
	if got := LookupUS("K1ABC"); got.State != "CA" {
		t.Fatal("first build not armed", got)
	}
}

func TestRefreshRetriesUnchangedFailedPublication(t *testing.T) {
	defer SetLicenseDBPath("")
	payload := zippedSources(t, stateSources())
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("If-None-Match") != "" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", "same-content")
		w.Write(payload)
	}))
	defer server.Close()
	dir := t.TempDir()
	cfg := config.FCCULSConfig{URL: server.URL, Archive: filepath.Join(dir, "a.zip"), DBPath: fixtureDB(t, false)}
	original := replaceDBOnceFn
	defer func() { replaceDBOnceFn = original }()
	replaceDBOnceFn = func(string, string) error { return errors.New("injected disk full") }
	if _, err := Refresh(context.Background(), cfg, true); err == nil {
		t.Fatal("publication failure missing")
	}
	replaceDBOnceFn = original
	if updated, err := Refresh(context.Background(), cfg, false); err != nil || !updated {
		t.Fatalf("unchanged retry updated=%v err=%v", updated, err)
	}
	if requests.Load() != 2 {
		t.Fatal(requests.Load())
	}
	meta, _ := download.ReadMetadata(download.MetadataPath(cfg.Archive))
	if meta == nil || !meta.ProcessedOK {
		t.Fatal("success status missing")
	}
	if updated, err := Refresh(context.Background(), cfg, false); err != nil || updated {
		t.Fatalf("ready unchanged updated=%v err=%v", updated, err)
	}
}

func TestBackgroundBuildsWhenEnforcementDisabled(t *testing.T) {
	defer SetLicenseDBPath("")
	payload := zippedSources(t, stateSources())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(payload) }))
	defer server.Close()
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "first.db")
			if legacy {
				path = fixtureDB(t, true)
			}
			cfg := config.FCCULSConfig{Enabled: false, URL: server.URL, Archive: filepath.Join(dir, "a.zip"), DBPath: path}
			SetLicenseDBPath(path)
			t.Cleanup(func() { SetLicenseDBPath("") })
			ctx, cancel := context.WithCancel(context.Background())
			done := StartBackground(ctx, cfg)
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("background worker did not exit")
				}
			}()
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			poll := time.NewTicker(10 * time.Millisecond)
			defer poll.Stop()
			for {
				select {
				case <-poll.C:
					if got := LookupUS("K1ABC"); got.State == "CA" {
						cancel()
						return
					}
				case <-deadline.C:
					t.Fatal("background schema rebuild did not complete")
				}
			}
		})
	}
}

func TestCancellationAndLastGoodRename(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Refresh(ctx, config.FCCULSConfig{}, false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "old.db")
	os.WriteFile(path, []byte("last good"), 0600)
	if err := replaceDBOnce(path, filepath.Join(dir, "missing")); err == nil {
		t.Fatal("expected failed rename")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "last good" {
		t.Fatal("old DB removed")
	}
	original := replaceDBOnceFn
	defer func() { replaceDBOnceFn = original }()
	replaceDBOnceFn = func(string, string) error { return errors.New("sharing violation") }
	ctx, cancel = context.WithCancel(context.Background())
	timer := time.AfterFunc(10*time.Millisecond, cancel)
	defer timer.Stop()
	if err := replaceDBWithRetryContext(ctx, path, "tmp"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestLookupStatsDoesNotWaitForDBOwner(t *testing.T) {
	licenseMu.Lock()
	result := make(chan LookupStatsSnapshot, 1)
	go func() { result <- LookupStats() }()
	select {
	case stats := <-result:
		licenseMu.Unlock()
		if stats.Capacity != defaultLicenseCacheMaxEntries {
			t.Fatal(stats)
		}
	case <-time.After(time.Second):
		licenseMu.Unlock()
		t.Fatal("stats blocked behind database owner")
	}
}

func TestLookupProbeFailureLogsOnceWithoutCaching(t *testing.T) {
	defer SetLicenseDBPath("")
	path := filepath.Join(t.TempDir(), "corrupt.db")
	if err := os.WriteFile(path, []byte("not sqlite"), 0600); err != nil {
		t.Fatal(err)
	}
	SetLicenseDBPath(path)
	previous := log.Writer()
	var output bytes.Buffer
	log.SetOutput(&output)
	defer log.SetOutput(previous)
	for n := 0; n < 3; n++ {
		if got := LookupUS("K1ABC"); got.Available {
			t.Fatal("corrupt lookup became factual", got)
		}
	}
	if count := strings.Count(output.String(), "FCC ULS lookup unavailable:"); count != 1 {
		t.Fatalf("diagnostics=%d output=%q", count, output.String())
	}
	if LookupStats().Entries != 0 {
		t.Fatal("probe outage retained as cache result")
	}
}

func TestBackgroundCancellationJoinsActiveDownload(t *testing.T) {
	select {
	//lint:ignore SA1012 Negative test verifies that an absent worker context returns a joined owner.
	case <-StartBackground(nil, config.FCCULSConfig{}): //nolint:staticcheck // The integrated analyzer also requires its directive for this nil-context contract test.
	default:
		t.Fatal("nil-context completion remains open")
	}
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done() }))
	defer server.Close()
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := StartBackground(ctx, config.FCCULSConfig{URL: server.URL, Archive: filepath.Join(dir, "a.zip"), DBPath: filepath.Join(dir, "fcc.db")})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("download did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled worker did not join")
	}
	if RefreshInProgress() {
		t.Fatal("refresh flag survived worker completion")
	}
}

func TestExtractionFailureCleansDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "duplicate.zip")
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for i := 0; i < 2; i++ {
		f, err := w.Create("EN.DAT")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.Write([]byte("bad")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := extractArchiveContext(context.Background(), path); err == nil {
		t.Fatal("duplicate member accepted")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "duplicate.zip" {
		t.Fatalf("partial extraction retained: %v", entries)
	}
}
