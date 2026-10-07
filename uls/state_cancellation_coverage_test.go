package uls

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/download"
)

// The existing context boundary observes real output progress. Cancellation is
// triggered by written bytes, rather than a timer racing a small extraction.
func TestRefreshCancellationAfterSubstantialExtraction(t *testing.T) {
	path := fixtureDB(t, false)
	SetLicenseDBPath(path)
	t.Cleanup(func() { SetLicenseDBPath("") })
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	files := stateSources()
	files["EN.DAT"] = strings.Repeat(sourceRow("EN", 1, "K1ABC", "L", "TX"), 4096)
	payload := zippedSources(t, files)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(payload) }))
	defer server.Close()
	dir := t.TempDir()
	cfg := config.FCCULSConfig{URL: server.URL, Archive: filepath.Join(dir, "cancel.zip"), DBPath: path}
	base, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const threshold = 256 << 10
	var extracted atomic.Int64
	ctx := &stateProgressContext{Context: base, progress: func() {
		paths, _ := filepath.Glob(filepath.Join(dir, "fcc-uls-extract-*", "EN.DAT"))
		for _, path := range paths {
			info, err := os.Stat(path)
			if err == nil && info.Size() >= threshold {
				extracted.Store(info.Size())
				cancel()
			}
		}
	}}
	if updated, err := Refresh(ctx, cfg, true); !errors.Is(err, context.Canceled) || updated {
		t.Fatalf("canceled extraction updated=%v err=%v", updated, err)
	}
	if got := extracted.Load(); got < threshold || got >= int64(len(files["EN.DAT"])) {
		t.Fatalf("cancellation progress=%d, want a partial extraction of at least %d bytes", got, threshold)
	}
	assertNoULSBuildTemps(t, dir)
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("last-good database changed: err=%v", err)
	}
	if got := LookupUS("K1ABC"); got != (LookupResult{Available: true, Found: true, State: "CA"}) {
		t.Fatalf("last-good state after extraction cancellation: %+v", got)
	}
	if RefreshInProgress() {
		t.Fatal("refresh flag survived extraction cancellation")
	}
	meta, _ := download.ReadMetadata(download.MetadataPath(cfg.Archive))
	if meta == nil || meta.ProcessedOK || meta.ProcessedAt.IsZero() {
		t.Fatalf("canceled build status missing: metadata=%+v", meta)
	}
	t.Logf("canceled after %d extracted EN bytes; extraction directory removed and last-good CA retained", extracted.Load())
}

// The next source read occurs only after Scanner has consumed every complete
// row in the prefix. Canceling the real context before returning valid tail
// data exercises the import loop's cancellation check in an active transaction
// after thousands of matching records, with more valid input still unread.
func TestStateImportCancellationAfterSubstantialRows(t *testing.T) {
	const rows = 4096
	path := filepath.Join(t.TempDir(), "last-good.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(context.Background(), `
		CREATE TABLE AM(unique_system_identifier INTEGER,call_sign TEXT,state TEXT NOT NULL);
		CREATE INDEX idx_AM_id ON AM(unique_system_identifier);
		CREATE INDEX idx_AM_call_sign ON AM(call_sign);
		WITH RECURSIVE ids(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM ids WHERE id<4096)
		INSERT INTO AM SELECT id, 'K'||id||'CANCEL', 'CA' FROM ids;
		PRAGMA user_version=1;`); err != nil {
		t.Fatal(err)
	}
	var prefix strings.Builder
	for id := 1; id <= rows; id++ {
		prefix.WriteString(sourceRow("EN", id, fmt.Sprintf("K%dCANCEL", id), "L", "TX"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tail := strings.Repeat(sourceRow("EN", 1, "K1CANCEL", "L", "TX"), rows)
	source := &stateCancelBoundaryReader{
		prefix: strings.NewReader(prefix.String()), tail: strings.NewReader(tail), cancel: cancel, db: db,
	}
	if err := importStatesReader(ctx, db, source); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-import cancellation err=%v", err)
	}
	if !source.reached || source.progressBytes != int64(prefix.Len()) || source.inUse != 1 || source.tail.Len() == 0 || source.bytes <= source.progressBytes {
		t.Fatalf("cancellation boundary not reached with valid data remaining: reached=%v progress=%d/%d in_use=%d bytes=%d unread=%d", source.reached, source.progressBytes, prefix.Len(), source.inUse, source.bytes, source.tail.Len())
	}
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer verifyCancel()
	var count, scratch int
	if err := db.QueryRowContext(verifyCtx, "SELECT COUNT(*) FROM AM WHERE state='CA';").Scan(&count); err != nil || count != rows {
		t.Fatalf("last-good states after rollback=%d, want %d; err=%v", count, rows, err)
	}
	if err := db.QueryRowContext(verifyCtx, "SELECT COUNT(*) FROM sqlite_temp_master WHERE name IN ('state_candidates','state_coverage');").Scan(&scratch); err != nil || scratch != 0 {
		t.Fatalf("retained import scratch tables=%d err=%v", scratch, err)
	}
	var check string
	if err := db.QueryRowContext(verifyCtx, "PRAGMA quick_check;").Scan(&check); err != nil || check != "ok" {
		t.Fatalf("last-good integrity=%q err=%v", check, err)
	}
	tx, err := db.BeginTx(verifyCtx, nil)
	if err != nil {
		t.Fatalf("database connection not reusable after cancellation: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats.InUse != 0 {
		t.Fatalf("import retained an in-use connection: %+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Renaming the closed file also exercises native Windows handle ownership.
	moved := path + ".closed"
	if err := os.Rename(path, moved); err != nil {
		t.Fatalf("closed import database could not be renamed: %v", err)
	}
	if err := os.Rename(moved, path); err != nil {
		t.Fatal(err)
	}
	SetLicenseDBPath(path)
	t.Cleanup(func() { SetLicenseDBPath("") })
	if got := LookupUS("K1CANCEL"); got != (LookupResult{Available: true, Found: true, State: "CA"}) {
		t.Fatalf("last-good factual lookup after import rollback: %+v", got)
	}
	assertNoULSBuildTemps(t, filepath.Dir(path))
	t.Logf("canceled after %d complete EN records (%d bytes), with %d tail bytes unread; transaction rolled back and database reusable", rows, source.progressBytes, source.tail.Len())
}

type stateCancelBoundaryReader struct {
	prefix, tail  *strings.Reader
	cancel        context.CancelFunc
	db            *sql.DB
	bytes         int64
	progressBytes int64
	reached       bool
	inUse         int
}

func (r *stateCancelBoundaryReader) Read(p []byte) (int, error) {
	if r.prefix.Len() != 0 {
		n, err := r.prefix.Read(p)
		r.bytes += int64(n)
		return n, err
	}
	if !r.reached {
		r.reached = true
		r.progressBytes = r.bytes
		r.inUse = r.db.Stats().InUse
		r.cancel()
	}
	n, err := r.tail.Read(p)
	r.bytes += int64(n)
	return n, err
}

// This wrapper observes existing cancellation checkpoints. The progress
// callback is owned by the test; production receives an ordinary Context.
type stateProgressContext struct {
	context.Context
	progress func()
}

func (c *stateProgressContext) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	c.progress()
	return c.Context.Err()
}

func assertNoULSBuildTemps(t *testing.T, dir string) {
	t.Helper()
	for _, pattern := range []string{"fcc-uls-extract-*", "fcc-uls-*.dbtmp*"} {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil || len(matches) != 0 {
			t.Fatalf("retained ULS build resources: %v err=%v", matches, err)
		}
	}
}
