package config

import (
	"strings"
	"testing"
)

func TestDedupSecondaryPreferStrongDefaultsFromPrimaryWhenOmitted(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `dedup:
  prefer_stronger_snr: true
`
	writeTestConfigOverlay(t, dir, "dedupe.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Dedup.SecondaryFastPreferStrong {
		t.Fatalf("expected secondary_fast_prefer_stronger_snr=false from shipped YAML")
	}
	if cfg.Dedup.SecondaryMedPreferStrong {
		t.Fatalf("expected secondary_med_prefer_stronger_snr=false from shipped YAML")
	}
	if cfg.Dedup.SecondarySlowPreferStrong {
		t.Fatalf("expected secondary_slow_prefer_stronger_snr=false from shipped YAML")
	}
}

func TestDedupLegacySecondaryKeysRemainIgnored(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `dedup:
  secondary_fast_window_seconds: 120
  secondary_med_window_seconds: 300
  secondary_slow_window_seconds: 480
  secondary_window_seconds: 999
  secondary_prefer_stronger_snr: true
`
	writeTestConfigOverlay(t, dir, "dedupe.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Dedup.SecondaryFastWindowSeconds != 120 {
		t.Fatalf("expected legacy secondary_window_seconds to be ignored; got secondary_fast_window_seconds=%d", cfg.Dedup.SecondaryFastWindowSeconds)
	}
	if cfg.Dedup.SecondaryMedWindowSeconds != 300 {
		t.Fatalf("expected legacy secondary_window_seconds to be ignored; got secondary_med_window_seconds=%d", cfg.Dedup.SecondaryMedWindowSeconds)
	}
	if cfg.Dedup.SecondarySlowWindowSeconds != 480 {
		t.Fatalf("expected legacy secondary_window_seconds to be ignored; got secondary_slow_window_seconds=%d", cfg.Dedup.SecondarySlowWindowSeconds)
	}
	if cfg.Dedup.SecondaryFastPreferStrong {
		t.Fatalf("expected legacy secondary_prefer_stronger_snr to be ignored for secondary_fast_prefer_stronger_snr")
	}
	if cfg.Dedup.SecondaryMedPreferStrong {
		t.Fatalf("expected legacy secondary_prefer_stronger_snr to be ignored for secondary_med_prefer_stronger_snr")
	}
	if cfg.Dedup.SecondarySlowPreferStrong {
		t.Fatalf("expected legacy secondary_prefer_stronger_snr to be ignored for secondary_slow_prefer_stronger_snr")
	}
}

func TestDedupDefaultPolicyLoadsFromYAML(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	writeTestConfigOverlay(t, dir, "dedupe.yaml", "dedup:\n  default_policy: FAST\n")

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Dedup.DefaultPolicy != "FAST" {
		t.Fatalf("expected dedup.default_policy FAST, got %q", cfg.Dedup.DefaultPolicy)
	}
}

func TestDedupDefaultPolicyRejectsInvalidValue(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	writeTestConfigOverlay(t, dir, "dedupe.yaml", "dedup:\n  default_policy: TURBO\n")

	_, err := Load(dir)
	if err == nil || !strings.Contains(err.Error(), `invalid dedup.default_policy "TURBO"`) {
		t.Fatalf("expected invalid dedup.default_policy error, got %v", err)
	}
}

func TestDedupDefaultPolicyIsRequired(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	removeTestConfigKey(t, dir, "dedupe.yaml", "dedup", "default_policy")

	_, err := Load(dir)
	if err == nil || !strings.Contains(err.Error(), `required YAML setting "dedup.default_policy" is missing`) {
		t.Fatalf("expected missing dedup.default_policy error, got %v", err)
	}
}
