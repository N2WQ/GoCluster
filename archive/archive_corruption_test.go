package archive

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/config"
)

// Purpose: Ensure non-directory archive paths are deleted and recreated when enabled.
// Key aspects: Writes invalid bytes, enables auto-delete, and verifies directory exists.
// Upstream: go test.
// Downstream: NewWriter.
func TestAutoDeleteCorruptDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "archive-pebble")
	if err := os.WriteFile(dbPath, []byte("not a sqlite db"), 0o644); err != nil {
		t.Fatalf("write corrupt db: %v", err)
	}

	cfg := config.ArchiveConfig{
		Enabled:                true,
		DBPath:                 dbPath,
		QueueSize:              10,
		BatchSize:              10,
		BatchIntervalMS:        1,
		CleanupIntervalSeconds: 60,
		RetentionSeconds:       1,
		Synchronous:            "off",
		AutoDeleteCorruptDB:    true,
	}
	writer, err := NewWriter(cfg)
	if err != nil {
		t.Fatalf("NewWriter() error: %v", err)
	}
	defer writer.Stop()

	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("stat db path failed: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected archive path to be directory, got file")
	}
}

func TestOpenArchiveDBRejectsNonDirectoryWithoutAutoDelete(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "archive-pebble")
	if err := os.WriteFile(dbPath, []byte("not a pebble db"), 0o644); err != nil {
		t.Fatalf("write db path file: %v", err)
	}

	cfg := config.ArchiveConfig{
		DBPath:              dbPath,
		AutoDeleteCorruptDB: false,
		Synchronous:         "off",
	}
	db, err := openArchiveDB(cfg)
	if err == nil {
		_ = db.Close()
		t.Fatalf("openArchiveDB() succeeded, want non-directory error")
	}
	if !strings.Contains(err.Error(), "exists and is not a directory") {
		t.Fatalf("openArchiveDB() error = %v, want non-directory error", err)
	}
	info, statErr := os.Stat(dbPath)
	if statErr != nil {
		t.Fatalf("stat db path after failed open: %v", statErr)
	}
	if info.IsDir() {
		t.Fatalf("db path was converted to directory with auto-delete disabled")
	}
}

func TestOpenArchiveDBCreatesMissingDirectory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "nested", "archive-pebble")
	cfg := config.ArchiveConfig{
		DBPath:              " " + dbPath + " ",
		AutoDeleteCorruptDB: false,
		Synchronous:         "off",
	}
	db, err := openArchiveDB(cfg)
	if err != nil {
		t.Fatalf("openArchiveDB() error: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("stat db path: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected archive path to be directory")
	}
}
