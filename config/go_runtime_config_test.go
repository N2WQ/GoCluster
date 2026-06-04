package config

import (
	"strings"
	"testing"
)

func TestLoadGoRuntimeConfigFromShippedRuntimeYAML(t *testing.T) {
	dir := testConfigDir(t)
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.GoRuntime.MemoryLimitMiB != 750 {
		t.Fatalf("expected go_runtime.memory_limit_mib=750, got %d", cfg.GoRuntime.MemoryLimitMiB)
	}
	if cfg.GoRuntime.GCPercent != 50 {
		t.Fatalf("expected go_runtime.gc_percent=50, got %d", cfg.GoRuntime.GCPercent)
	}
	if cfg.GoRuntime.MaxProcs != 0 {
		t.Fatalf("expected go_runtime.max_procs=0, got %d", cfg.GoRuntime.MaxProcs)
	}
}

func TestLoadGoRuntimeZeroSentinels(t *testing.T) {
	dir := testConfigDir(t)
	writeTestConfigOverlay(t, dir, "runtime.yaml", `
go_runtime:
  memory_limit_mib: 0
  gc_percent: 0
  max_procs: 0
`)
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.GoRuntime.MemoryLimitMiB != 0 || cfg.GoRuntime.GCPercent != 0 || cfg.GoRuntime.MaxProcs != 0 {
		t.Fatalf("expected zero sentinels to survive load, got %+v", cfg.GoRuntime)
	}
}

func TestLoadRejectsInvalidGoRuntimeConfig(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "negative memory",
			body: "go_runtime:\n  memory_limit_mib: -1\n",
			want: "go_runtime.memory_limit_mib",
		},
		{
			name: "tiny positive memory",
			body: "go_runtime:\n  memory_limit_mib: 63\n",
			want: "go_runtime.memory_limit_mib",
		},
		{
			name: "negative gc",
			body: "go_runtime:\n  gc_percent: -1\n",
			want: "go_runtime.gc_percent",
		},
		{
			name: "negative max procs",
			body: "go_runtime:\n  max_procs: -1\n",
			want: "go_runtime.max_procs",
		},
		{
			name: "missing section",
			body: "",
			want: "go_runtime",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := testConfigDir(t)
			if tt.name == "missing section" {
				removeTestConfigKey(t, dir, "runtime.yaml", "go_runtime")
			} else {
				writeTestConfigOverlay(t, dir, "runtime.yaml", tt.body)
			}
			_, err := Load(dir)
			if err == nil {
				t.Fatal("expected Load() error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
		})
	}
}
