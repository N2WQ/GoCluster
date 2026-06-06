package archive

import (
	"os"
	"path/filepath"
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
