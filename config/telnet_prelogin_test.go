package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppliesTelnetPreloginDefaults(t *testing.T) {
	dir := t.TempDir()
	config := `telnet:
  max_prelogin_sessions: 0
  prelogin_timeout_seconds: 0
  accept_rate_per_ip: 0
  accept_burst_per_ip: 0
  prelogin_concurrency_per_ip: 0
`
	if err := os.WriteFile(filepath.Join(dir, "runtime.yaml"), []byte(config), 0o644); err != nil {
		t.Fatalf("write runtime.yaml: %v", err)
	}
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Telnet.MaxPreloginSessions; got != 256 {
		t.Fatalf("expected max_prelogin_sessions default 256, got %d", got)
	}
	if got := cfg.Telnet.PreloginTimeoutSeconds; got != 15 {
		t.Fatalf("expected prelogin_timeout_seconds default 15, got %d", got)
	}
	if got := cfg.Telnet.AcceptRatePerIP; got != 3 {
		t.Fatalf("expected accept_rate_per_ip default 3, got %v", got)
	}
	if got := cfg.Telnet.AcceptBurstPerIP; got != 6 {
		t.Fatalf("expected accept_burst_per_ip default 6, got %d", got)
	}
	if got := cfg.Telnet.PreloginConcurrencyPerIP; got != 3 {
		t.Fatalf("expected prelogin_concurrency_per_ip default 3, got %d", got)
	}
}

func TestLoadClampsPreloginConcurrencyToGlobalCap(t *testing.T) {
	dir := t.TempDir()
	config := `telnet:
  max_prelogin_sessions: 2
  prelogin_concurrency_per_ip: 10
`
	if err := os.WriteFile(filepath.Join(dir, "runtime.yaml"), []byte(config), 0o644); err != nil {
		t.Fatalf("write runtime.yaml: %v", err)
	}
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.Telnet.PreloginConcurrencyPerIP; got != 2 {
		t.Fatalf("expected prelogin_concurrency_per_ip clamped to 2, got %d", got)
	}
}
