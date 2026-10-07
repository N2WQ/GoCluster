package uls

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/download"
)

func isedZipFixture(t *testing.T, member, body string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	w := zip.NewWriter(&buffer)
	f, err := w.Create(member)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

type isedHTTPFixture struct {
	mu                    sync.Mutex
	main, special         []byte
	mainETag, specialETag string
	failSpecial           bool
	ignoreValidators      bool
	statuses              []int
}

func newISEDHTTPFixture(t *testing.T) (*isedHTTPFixture, config.ISEDConfig) {
	t.Helper()
	f := &isedHTTPFixture{main: isedZipFixture(t, "amateur_delim.txt", isedMainFixtureHeader+"VE3AAA;A;B;;;ON;;;;;;;;;;;;\n"), special: isedZipFixture(t, "special_callsign.txt", isedEventsFixtureHeader+"CG3;2000-01-01;2099-12-31;event;;;VE3\n"), mainETag: "main-v1", specialETag: "special-v1"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		body, etag := f.main, f.mainETag
		if r.URL.Path == "/special" {
			if f.failSpecial {
				f.statuses = append(f.statuses, 500)
				w.WriteHeader(500)
				return
			}
			body, etag = f.special, f.specialETag
		}
		w.Header().Set("ETag", etag)
		if !f.ignoreValidators && r.Header.Get("If-None-Match") == etag {
			f.statuses = append(f.statuses, 304)
			w.WriteHeader(304)
			return
		}
		f.statuses = append(f.statuses, 200)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	dir := t.TempDir()
	cfg := config.ISEDConfig{Enabled: true, URL: server.URL + "/main", SpecialURL: server.URL + "/special", Archive: filepath.Join(dir, "main.zip"), SpecialArchive: filepath.Join(dir, "special.zip"), DBPath: filepath.Join(dir, "ised.db"), TempDir: dir, RefreshUTC: "02:20"}
	SetCanadianLicenseDBPath(cfg.DBPath)
	t.Cleanup(func() { SetCanadianLicenseDBPath(""); SetCanadianRefreshInProgress(false) })
	return f, cfg
}

func requireISEDRefresh(t *testing.T, cfg config.ISEDConfig, force, wantUpdated bool) {
	t.Helper()
	updated, err := RefreshCanadian(context.Background(), cfg, force)
	if err != nil || updated != wantUpdated {
		t.Fatalf("refresh updated=%t err=%v want=%t", updated, err, wantUpdated)
	}
}

func requireISEDAssignedState(t *testing.T, call, state string) {
	t.Helper()
	result := LookupCanadian(call)
	if !result.Available || !result.Found || result.State != state {
		t.Fatalf("lookup %s=%+v want province %q", call, result, state)
	}
}

func requireISEDNoScratch(t *testing.T, cfg config.ISEDConfig) {
	t.Helper()
	for _, dir := range []string{filepath.Dir(cfg.DBPath), cfg.TempDir} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "ised-") || strings.HasPrefix(entry.Name(), "license-extract-") || strings.HasPrefix(entry.Name(), "download-") {
				t.Errorf("retained scratch path %s", filepath.Join(dir, entry.Name()))
			}
		}
	}
}

func TestISEDRefreshDoesNotRedirectSQLiteTempDirectory(t *testing.T) {
	_, cfg := newISEDHTTPFixture(t)
	cfg.TempDir = filepath.Join(t.TempDir(), "separate", "scratch")
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var before, after sql.NullString
	// SQLite returns no row when the process-wide directory is unset.
	if err := db.QueryRowContext(t.Context(), "PRAGMA temp_store_directory;").Scan(&before); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	requireISEDRefresh(t, cfg, true, true)
	if err := db.QueryRowContext(t.Context(), "PRAGMA temp_store_directory;").Scan(&after); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("Canadian refresh redirected SQLite temp files: before=%+v after=%+v", before, after)
	}
	requireISEDAssignedState(t, "VE3AAA", "ON")
	requireISEDNoScratch(t, cfg)
}

func TestISEDRefreshPartialDownloadThenBoth304AndRestart(t *testing.T) {
	f, cfg := newISEDHTTPFixture(t)
	requireISEDRefresh(t, cfg, true, true)
	requireISEDAssignedState(t, "VE3AAA", "ON")
	old, err := os.ReadFile(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.main = isedZipFixture(t, "amateur_delim.txt", isedMainFixtureHeader+"VE3AAA;A;B;;;ON;;;;;;;;;;;;\nVE3BBB;A;B;;;BC;;;;;;;;;;;;\n")
	f.mainETag = "main-v2"
	f.failSpecial = true
	f.mu.Unlock()
	if updated, err := RefreshCanadian(context.Background(), cfg, false); err == nil || updated {
		t.Fatalf("partial refresh updated=%t err=%v", updated, err)
	}
	got, err := os.ReadFile(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(old, got) {
		t.Fatal("partial download replaced last-good database")
	}
	if result := LookupCanadian("VE3BBB"); !result.Available || result.Found {
		t.Fatalf("partial pair leaked membership: %+v", result)
	}
	// A process restart loses all in-memory ownership. Both archives now return
	// 304; their actual pair differs from the database manifest and must rebuild.
	SetCanadianLicenseDBPath("")
	SetCanadianLicenseDBPath(cfg.DBPath)
	f.mu.Lock()
	f.failSpecial = false
	f.statuses = nil
	f.mu.Unlock()
	requireISEDRefresh(t, cfg, false, true)
	requireISEDAssignedState(t, "VE3BBB", "BC")
	f.mu.Lock()
	statuses := append([]int(nil), f.statuses...)
	f.mu.Unlock()
	if len(statuses) != 2 || statuses[0] != 304 || statuses[1] != 304 {
		t.Fatalf("retry statuses=%v want [304 304]", statuses)
	}
	requireISEDRefresh(t, cfg, false, false)
	ready, mainSHA, specialSHA := canadianPublishedPair(context.Background(), cfg.DBPath)
	wantMain, err := canadianArchiveSHA(context.Background(), cfg.Archive, maxISEDMainBytes)
	if err != nil {
		t.Fatal(err)
	}
	wantSpecial, err := canadianArchiveSHA(context.Background(), cfg.SpecialArchive, maxISEDSpecialBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !ready || mainSHA != wantMain || specialSHA != wantSpecial {
		t.Fatalf("manifest ready=%t main=%s special=%s", ready, mainSHA, specialSHA)
	}
	requireISEDNoScratch(t, cfg)
}

func TestISEDRefreshFailedBuildRetries304PreservesLastGood(t *testing.T) {
	f, cfg := newISEDHTTPFixture(t)
	requireISEDRefresh(t, cfg, true, true)
	old, err := os.ReadFile(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.special = isedZipFixture(t, "special_callsign.txt", isedEventsFixtureHeader+"CG3;truncated")
	f.specialETag = "special-bad"
	f.mu.Unlock()
	for i := 0; i < 2; i++ {
		if updated, err := RefreshCanadian(context.Background(), cfg, false); err == nil || updated {
			t.Fatalf("bad build %d updated=%t err=%v", i, updated, err)
		}
	}
	got, err := os.ReadFile(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(old, got) {
		t.Fatal("failed structural import changed last-good database")
	}
	requireISEDAssignedState(t, "VE3AAA", "ON")
	requireISEDNoScratch(t, cfg)
}

func TestISEDRefreshSwapFailureRetriesBoth304(t *testing.T) {
	f, cfg := newISEDHTTPFixture(t)
	requireISEDRefresh(t, cfg, true, true)
	old, err := os.ReadFile(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.main = isedZipFixture(t, "amateur_delim.txt", isedMainFixtureHeader+"VE3AAA;A;B;;;BC;;;;;;;;;;;;\n")
	f.mainETag = "main-v2"
	f.mu.Unlock()
	original := replaceDBOnceFn
	t.Cleanup(func() { replaceDBOnceFn = original })
	replaceDBOnceFn = func(string, string) error { return errors.New("injected publication failure") }
	if updated, err := RefreshCanadian(context.Background(), cfg, false); err == nil || updated {
		t.Fatalf("swap failure updated=%t err=%v", updated, err)
	}
	got, err := os.ReadFile(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(old, got) {
		t.Fatal("failed publication lost last-good file")
	}
	requireISEDAssignedState(t, "VE3AAA", "ON")
	replaceDBOnceFn = original
	f.mu.Lock()
	f.statuses = nil
	f.mu.Unlock()
	requireISEDRefresh(t, cfg, false, true)
	requireISEDAssignedState(t, "VE3AAA", "BC")
	f.mu.Lock()
	statuses := append([]int(nil), f.statuses...)
	f.mu.Unlock()
	if len(statuses) != 2 || statuses[0] != 304 || statuses[1] != 304 {
		t.Fatalf("swap retry statuses=%v", statuses)
	}
	requireISEDNoScratch(t, cfg)
}

func TestISEDRefreshMetadataFailureDoesNotLieAboutPublication(t *testing.T) {
	f, cfg := newISEDHTTPFixture(t)
	requireISEDRefresh(t, cfg, true, true)
	metaPath := download.MetadataPath(cfg.Archive)
	if err := os.Remove(metaPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(metaPath, 0o755); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.main = isedZipFixture(t, "amateur_delim.txt", isedMainFixtureHeader+"VE3AAA;A;B;;;BC;;;;;;;;;;;;\n")
	f.mainETag = "main-v2"
	f.mu.Unlock()
	requireISEDRefresh(t, cfg, false, true)
	requireISEDAssignedState(t, "VE3AAA", "BC")
	requireISEDRefresh(t, cfg, false, false)
}

func TestISEDRefreshStaleSidecarAllowsUpstreamRollback(t *testing.T) {
	for _, mode := range []string{"304 validators", "200 same-content"} {
		t.Run(mode, func(t *testing.T) {
			f, cfg := newISEDHTTPFixture(t)
			requireISEDRefresh(t, cfg, true, true)
			metaPath := download.MetadataPath(cfg.Archive)
			metaA, err := os.ReadFile(metaPath)
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			archiveA := append([]byte(nil), f.main...)
			f.main = isedZipFixture(t, "amateur_delim.txt", isedMainFixtureHeader+"VE3AAA;A;B;;;BC;;;;;;;;;;;;\n")
			f.mainETag = "main-v2"
			f.mu.Unlock()
			// A directory makes sidecar persistence fail. Restoring the saved A sidecar
			// then reproduces an old metadata file surviving the publication of B.
			if err := os.Remove(metaPath); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(metaPath, 0o755); err != nil {
				t.Fatal(err)
			}
			requireISEDRefresh(t, cfg, false, true)
			requireISEDAssignedState(t, "VE3AAA", "BC")
			if err := os.Remove(metaPath); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(metaPath, metaA, 0o644); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.main = archiveA
			f.mainETag = "main-v1"
			f.ignoreValidators = mode == "200 same-content"
			f.statuses = nil
			f.mu.Unlock()
			requireISEDRefresh(t, cfg, false, true)
			requireISEDAssignedState(t, "VE3AAA", "ON")
			f.mu.Lock()
			statuses := append([]int(nil), f.statuses...)
			f.mu.Unlock()
			if len(statuses) != 2 || statuses[0] != 200 {
				t.Fatalf("stale metadata rollback statuses=%v want main 200", statuses)
			}
			retained, err := os.ReadFile(cfg.Archive)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(retained, archiveA) {
				t.Fatal("stale same-content shortcut kept newer B after fetching rollback A")
			}
		})
	}
}

func TestISEDBackgroundCancellationJoinsNetworkAndScheduler(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done() }))
	defer server.Close()
	dir := t.TempDir()
	cfg := config.ISEDConfig{URL: server.URL, SpecialURL: server.URL, Archive: filepath.Join(dir, "main.zip"), SpecialArchive: filepath.Join(dir, "special.zip"), DBPath: filepath.Join(dir, "ised.db"), TempDir: dir, RefreshUTC: "02:20"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := StartCanadianBackground(ctx, cfg)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("startup did not start HTTP")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("startup worker did not join after cancellation")
	}
	if CanadianRefreshInProgress() {
		t.Fatal("canceled worker retained publication flag")
	}
	requireISEDNoScratch(t, cfg)
	//lint:ignore SA1012 Testing the explicit nil-context startup contract.
	nilDone := StartCanadianBackground(nil, cfg) //nolint:staticcheck // Tests the explicit nil-context startup contract.
	select {
	case <-nilDone:
	default:
		t.Fatal("nil-context startup did not complete")
	}
}

func TestISEDRefreshOwnerWaitCancellation(t *testing.T) {
	canadianRefreshOwner <- struct{}{}
	defer func() { <-canadianRefreshOwner }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := RefreshCanadian(ctx, config.ISEDConfig{}, false); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked owner cancellation=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled owner waiter leaked")
	}
}

func TestISEDExtractionBoundsAndMembers(t *testing.T) {
	for _, tc := range []struct {
		name, member, body string
		limit              int64
	}{{"missing", "other.txt", "x", 100}, {"oversize", "amateur_delim.txt", "123456789", 8}, {"duplicate", "amateur_delim.txt", "x", 100}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "main.zip")
			body := isedZipFixture(t, tc.member, tc.body)
			if tc.name == "duplicate" {
				var buffer bytes.Buffer
				w := zip.NewWriter(&buffer)
				for i := 0; i < 2; i++ {
					f, err := w.Create(tc.member)
					if err != nil {
						t.Fatal(err)
					}
					_, _ = f.Write([]byte("x"))
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				body = buffer.Bytes()
			}
			if err := os.WriteFile(path, body, 0o644); err != nil {
				t.Fatal(err)
			}
			if extract, err := extractNamedArchiveContext(context.Background(), path, "amateur_delim.txt", dir, tc.limit); err == nil || extract != "" {
				t.Fatalf("bad archive extract=%s err=%v", extract, err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 {
				t.Fatalf("failed extraction retained scratch: %v", entries)
			}
		})
	}
}

func TestISEDExtractionUsesConfiguredSharedDirectory(t *testing.T) {
	archiveDir := t.TempDir()
	path := filepath.Join(archiveDir, "main.zip")
	if err := os.WriteFile(path, isedZipFixture(t, "amateur_delim.txt", "literal record\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	scratch := filepath.Join(t.TempDir(), "shared", "scratch")
	first, err := extractNamedArchiveContext(t.Context(), path, "amateur_delim.txt", scratch, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(first)
	second, err := extractNamedArchiveContext(t.Context(), path, "amateur_delim.txt", scratch, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(second)
	if filepath.Dir(first) != scratch || filepath.Dir(second) != scratch || first == second {
		t.Fatalf("shared scratch ownership: first=%q second=%q configured=%q", first, second, scratch)
	}
	for _, dir := range []string{first, second} {
		body, err := os.ReadFile(filepath.Join(dir, "amateur_delim.txt"))
		if err != nil || string(body) != "literal record\n" {
			t.Fatalf("extracted contents=%q err=%v", body, err)
		}
	}
	if _, err := extractNamedArchiveContext(t.Context(), path, "missing.txt", scratch, 100); err == nil {
		t.Fatal("accepted missing member")
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 2 {
		t.Fatalf("failed extraction affected shared scratch: entries=%v err=%v", entries, err)
	}
}

func TestISEDManifestProbeRejectsMissingPair(t *testing.T) {
	db := newISEDTestDB(t, isedLookupMain, "")
	if _, err := db.ExecContext(t.Context(), "PRAGMA user_version=1;"); err != nil {
		t.Fatal(err)
	}
	if err := probeCanadianDatabase(context.Background(), db); err == nil {
		t.Fatal("accepted missing pair")
	}
	if _, err := db.ExecContext(t.Context(), "INSERT INTO SourceMeta VALUES(1,?,?);", strings.Repeat("a", 64), strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if err := probeCanadianDatabase(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), "UPDATE SourceMeta SET main_sha='bad';"); err != nil {
		t.Fatal(err)
	}
	if err := probeCanadianDatabase(context.Background(), db); err == nil {
		t.Fatal("accepted malformed hash")
	}
}

func TestISEDRefreshDirectPathCollisionsPreserveFiles(t *testing.T) {
	for _, name := range []string{"db metadata", "archive metadata", "case alias", "ancestor", "hard link", "symlink ancestor"} {
		t.Run(name, func(t *testing.T) {
			fixture, cfg := newISEDHTTPFixture(t)
			dir := filepath.Dir(cfg.DBPath)
			var protected string
			switch name {
			case "db metadata":
				cfg.DBPath = download.MetadataPath(cfg.Archive)
				protected = cfg.DBPath
			case "archive metadata":
				cfg.SpecialArchive = download.MetadataPath(cfg.Archive)
				protected = cfg.DBPath
			case "case alias":
				cfg.SpecialArchive = strings.Replace(cfg.Archive, "main.zip", "MAIN.ZIP", 1)
				protected = cfg.DBPath
			case "ancestor":
				cfg.Archive = filepath.Join(dir, "source")
				cfg.SpecialArchive = filepath.Join(cfg.Archive, "special.zip")
				protected = cfg.DBPath
			case "hard link":
				protected = cfg.DBPath
			case "symlink ancestor":
				protected = cfg.DBPath
				realDir := filepath.Join(dir, "real")
				aliasDir := filepath.Join(dir, "alias")
				if err := os.Mkdir(realDir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(realDir, aliasDir); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
				cfg.Archive = filepath.Join(realDir, "source.zip")
				cfg.SpecialArchive = filepath.Join(aliasDir, "source.zip")
			}
			if err := os.WriteFile(protected, []byte("last-good snapshot"), 0o644); err != nil {
				t.Fatal(err)
			}
			if name == "hard link" {
				if err := os.Link(cfg.DBPath, cfg.Archive); err != nil {
					t.Skipf("hard links unavailable: %v", err)
				}
			}
			if updated, err := RefreshCanadian(context.Background(), cfg, false); err == nil || updated {
				t.Fatalf("colliding refresh updated=%t err=%v", updated, err)
			}
			got, err := os.ReadFile(protected)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "last-good snapshot" {
				t.Fatalf("colliding refresh changed protected file: %q", got)
			}
			fixture.mu.Lock()
			requests := len(fixture.statuses)
			fixture.mu.Unlock()
			if requests != 0 {
				t.Fatalf("invalid paths made %d HTTP requests", requests)
			}
		})
	}
}

func TestISEDRefreshRejectsDanglingManagedSymlinks(t *testing.T) {
	for _, mode := range []string{"metadata aliases future DB", "broken ancestor"} {
		t.Run(mode, func(t *testing.T) {
			fixture, cfg := newISEDHTTPFixture(t)
			link, target := download.MetadataPath(cfg.Archive), cfg.DBPath
			if mode == "broken ancestor" {
				link = filepath.Join(filepath.Dir(cfg.DBPath), "alias")
				target = filepath.Join(filepath.Dir(cfg.DBPath), "future-directory")
				cfg.SpecialArchive = filepath.Join(link, "special.zip")
			}
			if err := os.Symlink(target, link); err != nil {
				t.Skipf("symlinks unavailable: %v", err)
			}
			if updated, err := RefreshCanadian(t.Context(), cfg, false); err == nil || updated {
				t.Fatalf("dangling symlink refresh updated=%t err=%v", updated, err)
			}
			if _, err := os.Stat(cfg.DBPath); !os.IsNotExist(err) {
				t.Fatalf("invalid path created future database: %v", err)
			}
			if got, err := os.Readlink(link); err != nil || got != target {
				t.Fatalf("managed symlink changed: target=%q err=%v", got, err)
			}
			if _, err := os.Stat(target); !os.IsNotExist(err) {
				t.Fatalf("invalid path created symlink target: %v", err)
			}
			fixture.mu.Lock()
			requests := len(fixture.statuses)
			fixture.mu.Unlock()
			if requests != 0 {
				t.Fatalf("dangling symlink made %d HTTP requests", requests)
			}
		})
	}
}
