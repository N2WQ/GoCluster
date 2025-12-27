package config

import (
	"os"
	"path/filepath"
	"testing"
)

// Purpose: Verify archive cleanup batch defaults are applied when omitted.
// Key aspects: Loads minimal YAML and inspects normalized config values.
// Upstream: go test.
// Downstream: Load.
func TestArchiveCleanupBatchDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	cfgText := "archive:\n  enabled: true\n"
	if err := os.WriteFile(path, []byte(cfgText), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Archive.CleanupBatchSize != 2000 {
		t.Fatalf("expected cleanup_batch_size=2000, got %d", cfg.Archive.CleanupBatchSize)
	}
	if cfg.Archive.CleanupBatchYieldMS != 5 {
		t.Fatalf("expected cleanup_batch_yield_ms=5, got %d", cfg.Archive.CleanupBatchYieldMS)
	}
}

// Purpose: Verify archive cleanup batch overrides are honored.
// Key aspects: Ensures explicit zero yield is preserved.
// Upstream: go test.
// Downstream: Load.
func TestArchiveCleanupBatchOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	cfgText := "archive:\n  cleanup_batch_size: 500\n  cleanup_batch_yield_ms: 0\n"
	if err := os.WriteFile(path, []byte(cfgText), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Archive.CleanupBatchSize != 500 {
		t.Fatalf("expected cleanup_batch_size=500, got %d", cfg.Archive.CleanupBatchSize)
	}
	if cfg.Archive.CleanupBatchYieldMS != 0 {
		t.Fatalf("expected cleanup_batch_yield_ms=0, got %d", cfg.Archive.CleanupBatchYieldMS)
	}
}
