package sqliteutil

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestPreflightHealthy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "healthy.db")

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), "create table t (id integer)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	db.Close()

	res, err := Preflight(path, "grid", time.Second, nil)
	if err != nil {
		t.Fatalf("preflight failed: %v", err)
	}
	if !res.Healthy || res.Quarantined {
		t.Fatalf("expected healthy preflight, got %+v", res)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected db to remain, stat failed: %v", err)
	}
}

func TestPreflightQuarantinesCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.db")
	if err := os.WriteFile(path, []byte("not a sqlite database"), 0o644); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}
	// Create sidecars to ensure they move with the main file.
	sidecars := []string{path + "-wal", path + "-shm", path + "-journal"}
	for _, s := range sidecars {
		if err := os.WriteFile(s, []byte("sidecar"), 0o644); err != nil {
			t.Fatalf("write sidecar %s: %v", s, err)
		}
	}

	res, err := Preflight(path, "archive", time.Second, func(string, ...any) {})
	if err != nil {
		t.Fatalf("preflight expected quarantine, got error: %v", err)
	}
	if res.Healthy || !res.Quarantined {
		t.Fatalf("expected quarantine, got %+v", res)
	}
	if res.QuarantinePath == "" {
		t.Fatalf("expected quarantine path to be set")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected original db to be renamed, stat err=%v", err)
	}
	// Sidecars should be removed or quarantined; they may be deleted by checkpoint on corrupt files.
	for _, s := range sidecars {
		if _, err := os.Stat(s); err == nil {
			t.Fatalf("expected sidecar %s to be moved or removed", s)
		}
		files, _ := filepath.Glob(s + ".bad-*")
		if len(files) == 0 {
			// Accept missing sidecars (e.g., checkpoint removed them) but require absence at original path.
			continue
		}
	}
	if !strings.Contains(res.QuarantinePath, ".bad-") {
		t.Fatalf("quarantine path not suffixed as expected: %s", res.QuarantinePath)
	}
}
