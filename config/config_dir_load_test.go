package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDirectoryMergesFiles(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)

	app := `server:
  name: "Alpha"
cty:
  enabled: false
`
	dedupe := `server:
  node_id: "NODE-1"
dedup:
  secondary_fast_prefer_stronger_snr: true
`
	writeTestConfigOverlay(t, dir, "app.yaml", app)
	writeTestConfigOverlay(t, dir, "data.yaml", "cty:\n  enabled: false\n")
	writeTestConfigOverlay(t, dir, "dedupe.yaml", dedupe)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if got := filepath.Clean(cfg.LoadedFrom); got != filepath.Clean(dir) {
		t.Fatalf("expected LoadedFrom=%s, got %s", dir, got)
	}
	if cfg.Server.Name != "Alpha" {
		t.Fatalf("expected server.name to merge from app.yaml, got %q", cfg.Server.Name)
	}
	if cfg.Server.NodeID != "NODE-1" {
		t.Fatalf("expected server.node_id to merge from dedupe.yaml, got %q", cfg.Server.NodeID)
	}
	if !cfg.Dedup.SecondaryFastPreferStrong {
		t.Fatalf("expected dedup.secondary_fast_prefer_stronger_snr=true from dedupe.yaml")
	}
	if cfg.CTY.Enabled {
		t.Fatalf("expected cty.enabled=false from app.yaml, got true")
	}
}

func TestLoadRejectsSingleFilePath(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	path := filepath.Join(dir, "runtime.yaml")

	if _, err := Load(path); err == nil {
		t.Fatalf("expected Load() to reject non-directory config path")
	}
}

func TestLoadRejectsUnknownYAMLFile(t *testing.T) {
	dir := testConfigDir(t)
	if err := os.WriteFile(filepath.Join(dir, "mode_allocations.yaml"), []byte("bands: []\n"), 0o644); err != nil {
		t.Fatalf("write unknown config file: %v", err)
	}
	_, err := Load(dir)
	if err == nil || !strings.Contains(err.Error(), "unrecognized config file") {
		t.Fatalf("expected unrecognized config file error, got %v", err)
	}
}

func TestLoadWarnsUnknownRuntimeKey(t *testing.T) {
	dir := testConfigDir(t)
	writeTestConfigOverlay(t, dir, "runtime.yaml", "telnet:\n  mystery_knob: 1\n")
	_, diagnostics, err := LoadWithDiagnostics(dir)
	if err != nil {
		t.Fatalf("LoadWithDiagnostics() error: %v", err)
	}
	if !containsDiagnostic(diagnostics.Warnings, "telnet.mystery_knob") {
		t.Fatalf("expected warning for telnet.mystery_knob, got %#v", diagnostics.Warnings)
	}
	if containsDiagnostic(diagnostics.Errors, "telnet.mystery_knob") {
		t.Fatalf("did not expect fatal error for telnet.mystery_knob, got %#v", diagnostics.Errors)
	}
}

func TestLoadReportsAllMissingStartupConfigInputs(t *testing.T) {
	dir := testConfigDir(t)
	removeTestConfigKey(t, dir, "runtime.yaml", "telnet", "port")
	writeTestConfigOverlay(t, dir, "archive.yaml", "archive:\n  cleanup_batch_size: 10\n")
	if err := os.Remove(filepath.Join(dir, "path_reliability.yaml")); err != nil {
		t.Fatalf("remove path_reliability.yaml: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "solarweather.yaml")); err != nil {
		t.Fatalf("remove solarweather.yaml: %v", err)
	}

	_, diagnostics, err := LoadWithDiagnostics(dir)
	if err == nil {
		t.Fatalf("expected missing startup config inputs to fail")
	}
	for _, want := range []string{
		`required config file "path_reliability.yaml"`,
		`required config file "solarweather.yaml"`,
		`required YAML setting "telnet.port" is missing`,
		`removed YAML setting "archive.cleanup_batch_size"`,
	} {
		if !containsDiagnostic(diagnostics.Errors, want) {
			t.Fatalf("expected diagnostic containing %q, got %#v", want, diagnostics.Errors)
		}
	}
}

func TestLoadRejectsMissingRequiredRuntimeSetting(t *testing.T) {
	dir := testConfigDir(t)
	removeTestConfigKey(t, dir, "runtime.yaml", "telnet", "port")
	_, err := Load(dir)
	if err == nil || !strings.Contains(err.Error(), `required YAML setting "telnet.port" is missing`) {
		t.Fatalf("expected missing telnet.port error, got %v", err)
	}
}

func containsDiagnostic(messages []string, needle string) bool {
	for _, message := range messages {
		if strings.Contains(message, needle) {
			return true
		}
	}
	return false
}

func TestLoadPreservesDocumentedZeroSentinels(t *testing.T) {
	dir := testConfigDir(t)
	writeTestConfigOverlay(t, dir, "app.yaml", `
ui:
  refresh_ms: 0
`)
	writeTestConfigOverlay(t, dir, "runtime.yaml", `
telnet:
  broadcast_batch_interval_ms: 0
  keepalive_seconds: 0
`)
	writeTestConfigOverlay(t, dir, "ingest.yaml", `
rbn:
  keepalive_seconds: 0
rbn_digital:
  keepalive_seconds: 0
human_telnet:
  keepalive_seconds: 0
`)
	writeTestConfigOverlay(t, dir, "peering.yaml", `
peering:
  keepalive_seconds: 0
  config_seconds: 0
`)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.UI.RefreshMS != 0 {
		t.Fatalf("ui.refresh_ms = %d, want 0", cfg.UI.RefreshMS)
	}
	if cfg.Telnet.BroadcastBatchIntervalMS != 0 {
		t.Fatalf("telnet.broadcast_batch_interval_ms = %d, want 0", cfg.Telnet.BroadcastBatchIntervalMS)
	}
	if cfg.Telnet.KeepaliveSeconds != 0 {
		t.Fatalf("telnet.keepalive_seconds = %d, want 0", cfg.Telnet.KeepaliveSeconds)
	}
	if cfg.RBN.KeepaliveSec != 0 || cfg.RBNDigital.KeepaliveSec != 0 || cfg.HumanTelnet.KeepaliveSec != 0 {
		t.Fatalf("feed keepalives = rbn:%d rbn_digital:%d human:%d, want all 0", cfg.RBN.KeepaliveSec, cfg.RBNDigital.KeepaliveSec, cfg.HumanTelnet.KeepaliveSec)
	}
	if cfg.Peering.KeepaliveSeconds != 0 || cfg.Peering.ConfigSeconds != 0 {
		t.Fatalf("peering timers = keepalive:%d config:%d, want both 0", cfg.Peering.KeepaliveSeconds, cfg.Peering.ConfigSeconds)
	}
}

func TestLoadRejectsNullYAMLOwnedPointerSetting(t *testing.T) {
	dir := testConfigDir(t)
	writeTestConfigOverlay(t, dir, "data.yaml", "grid_db_check_on_miss:\n")

	_, err := Load(dir)
	if err == nil || !strings.Contains(err.Error(), `required YAML setting "grid_db_check_on_miss" must not be null`) {
		t.Fatalf("expected null grid_db_check_on_miss error, got %v", err)
	}
}
