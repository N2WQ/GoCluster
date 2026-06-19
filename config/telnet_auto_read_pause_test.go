package config

import (
	"strings"
	"testing"
)

func TestLoadAppliesTelnetAutoReadPauseSettings(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if got := cfg.Telnet.AutoReadPauseMinRows; got != 10 {
		t.Fatalf("auto_read_pause_min_rows = %d, want 10", got)
	}
	if got := cfg.Telnet.AutoReadPauseSeconds; got != 30 {
		t.Fatalf("auto_read_pause_seconds = %d, want 30", got)
	}
}

func TestLoadAllowsDisabledTelnetAutoReadPause(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	writeTestConfigOverlay(t, dir, "runtime.yaml", `telnet:
  auto_read_pause_min_rows: 0
  auto_read_pause_seconds: 0
`)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Telnet.AutoReadPauseMinRows != 0 || cfg.Telnet.AutoReadPauseSeconds != 0 {
		t.Fatalf("auto read pause = rows:%d seconds:%d, want disabled", cfg.Telnet.AutoReadPauseMinRows, cfg.Telnet.AutoReadPauseSeconds)
	}
}

func TestLoadRejectsInvalidTelnetAutoReadPause(t *testing.T) {
	tests := []struct {
		name    string
		overlay string
		want    string
	}{
		{
			name: "negative rows",
			overlay: `telnet:
  auto_read_pause_min_rows: -1
`,
			want: "telnet.auto_read_pause_min_rows",
		},
		{
			name: "excessive rows",
			overlay: `telnet:
  auto_read_pause_min_rows: 501
`,
			want: "telnet.auto_read_pause_min_rows",
		},
		{
			name: "negative seconds",
			overlay: `telnet:
  auto_read_pause_seconds: -1
`,
			want: "telnet.auto_read_pause_seconds",
		},
		{
			name: "excessive seconds",
			overlay: `telnet:
  auto_read_pause_seconds: 301
`,
			want: "telnet.auto_read_pause_seconds",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeRequiredFloodControlFile(t, dir)
			writeTestConfigOverlay(t, dir, "runtime.yaml", tt.overlay)

			_, err := Load(dir)
			if err == nil {
				t.Fatalf("expected error for %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}
