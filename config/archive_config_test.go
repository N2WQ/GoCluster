package config

import (
	"strings"
	"testing"
)

// Purpose: Verify archive retention defaults when omitted from YAML.
// Key aspects: Loads minimal YAML and inspects the normalized single retention window.
// Upstream: go test.
// Downstream: Load.
func TestArchiveRetentionDefault(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := "archive:\n  enabled: true\n"
	writeTestConfigOverlay(t, dir, "archive.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Archive.RetentionSeconds != DefaultArchiveRetentionSeconds {
		t.Fatalf("expected retention_seconds=%d from shipped YAML, got %d", DefaultArchiveRetentionSeconds, cfg.Archive.RetentionSeconds)
	}
}

// Purpose: Verify removed archive cleanup batch keys fail with a migration hint.
// Key aspects: Prevents obsolete cleanup knobs from appearing to control range deletion.
// Upstream: go test.
// Downstream: Load, validateRemovedRuntimeKeys.
func TestArchiveCleanupBatchKeysRejected(t *testing.T) {
	for _, key := range []string{"cleanup_batch_size", "cleanup_batch_yield_ms"} {
		t.Run(key, func(t *testing.T) {
			dir := testConfigDir(t)
			writeRequiredFloodControlFile(t, dir)
			writeTestConfigOverlay(t, dir, "archive.yaml", "archive:\n  "+key+": 1\n")

			if _, err := Load(dir); err == nil {
				t.Fatalf("expected Load() to reject removed archive cleanup key")
			} else if got := err.Error(); got == "" || !strings.Contains(got, "range deletion") {
				t.Fatalf("expected range deletion migration hint, got %v", err)
			}
		})
	}
}

// Purpose: Verify removed ignored archive compatibility keys are no longer schema fields.
// Key aspects: Relies on strict YAML decoding rather than a compatibility presence check.
// Upstream: go test.
// Downstream: Load, yaml decoder KnownFields.
func TestArchiveIgnoredCompatibilityKeysRejected(t *testing.T) {
	for _, key := range []string{"busy_timeout_ms", "preflight_timeout_ms"} {
		t.Run(key, func(t *testing.T) {
			dir := testConfigDir(t)
			writeRequiredFloodControlFile(t, dir)
			writeTestConfigOverlay(t, dir, "archive.yaml", "archive:\n  "+key+": 1\n")

			if _, err := Load(dir); err == nil {
				t.Fatalf("expected Load() to reject removed archive compatibility key")
			} else if got := err.Error(); got == "" || !strings.Contains(got, key) {
				t.Fatalf("expected strict schema error for %s, got %v", key, err)
			}
		})
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
