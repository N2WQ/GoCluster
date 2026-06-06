package config

import (
	"strings"
	"testing"
)

// Purpose: Verify archive cleanup batch defaults are applied when omitted.
// Key aspects: Loads minimal YAML and inspects normalized config values.
// Upstream: go test.
// Downstream: Load.
func TestArchiveCleanupBatchDefaults(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := "archive:\n  enabled: true\n"
	writeTestConfigOverlay(t, dir, "archive.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Archive.CleanupBatchSize != 2000 {
		t.Fatalf("expected cleanup_batch_size=2000, got %d", cfg.Archive.CleanupBatchSize)
	}
	if cfg.Archive.CleanupBatchYieldMS != 50 {
		t.Fatalf("expected cleanup_batch_yield_ms=50 from shipped YAML, got %d", cfg.Archive.CleanupBatchYieldMS)
	}
	if cfg.Archive.RetentionSeconds != DefaultArchiveRetentionSeconds {
		t.Fatalf("expected retention_seconds=%d from shipped YAML, got %d", DefaultArchiveRetentionSeconds, cfg.Archive.RetentionSeconds)
	}
}

// Purpose: Verify archive cleanup batch overrides are honored.
// Key aspects: Ensures explicit zero yield is preserved.
// Upstream: go test.
// Downstream: Load.
func TestArchiveCleanupBatchOverrides(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := "archive:\n  cleanup_batch_size: 500\n  cleanup_batch_yield_ms: 0\n"
	writeTestConfigOverlay(t, dir, "archive.yaml", cfgText)

	cfg, err := Load(dir)
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

// Purpose: Verify archive synchronous defaults to off when omitted.
// Key aspects: Ensures config normalization applies durability default.
// Upstream: go test.
// Downstream: Load.
func TestArchiveSynchronousDefault(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := "archive:\n  enabled: true\n"
	writeTestConfigOverlay(t, dir, "archive.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Archive.Synchronous != "off" {
		t.Fatalf("expected archive.synchronous=off, got %q", cfg.Archive.Synchronous)
	}
}

// Purpose: Verify invalid archive synchronous mode fails validation.
// Key aspects: Confirms config rejects unknown durability strings.
// Upstream: go test.
// Downstream: Load.
func TestArchiveSynchronousInvalid(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := "archive:\n  synchronous: \"fast\"\n"
	writeTestConfigOverlay(t, dir, "archive.yaml", cfgText)

	if _, err := Load(dir); err == nil {
		t.Fatalf("expected Load() to fail for invalid archive.synchronous")
	}
}

// Purpose: Verify removed split archive retention keys fail with a migration hint.
// Key aspects: Prevents silently preserving mode-specific archive retention.
// Upstream: go test.
// Downstream: Load, validateRemovedRuntimeKeys.
func TestArchiveSplitRetentionKeysRejected(t *testing.T) {
	for _, key := range []string{"retention_ft_seconds", "retention_default_seconds"} {
		t.Run(key, func(t *testing.T) {
			dir := testConfigDir(t)
			writeRequiredFloodControlFile(t, dir)
			writeTestConfigOverlay(t, dir, "archive.yaml", "archive:\n  "+key+": 3600\n")

			if _, err := Load(dir); err == nil {
				t.Fatalf("expected Load() to reject removed archive retention key")
			} else if got := err.Error(); got == "" || !strings.Contains(got, "archive.retention_seconds") {
				t.Fatalf("expected archive.retention_seconds migration hint, got %v", err)
			}
		})
	}
}
