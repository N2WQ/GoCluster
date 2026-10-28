package cluster

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/config"
)

func TestTelnetCommandRateRuntimeWiring(t *testing.T) {
	source := filepath.Join("..", "..", "data", "config")
	for _, tc := range []struct {
		name  string
		rate  int
		burst int
	}{
		{name: "shipped", rate: 5, burst: 30},
		{name: "disabled", rate: 0, burst: 1},
		{name: "custom", rate: 3, burst: 7},
		{name: "maximum", rate: 1_000_000_000, burst: 1_000_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			entries, err := os.ReadDir(source)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if !entry.Type().IsRegular() || !strings.EqualFold(filepath.Ext(entry.Name()), ".yaml") {
					continue
				}
				data, err := os.ReadFile(filepath.Join(source, entry.Name()))
				if err != nil {
					t.Fatal(err)
				}
				if entry.Name() == "runtime.yaml" {
					for old, replacement := range map[string]string{
						"command_rate_per_second: 5": fmt.Sprintf("command_rate_per_second: %d", tc.rate),
						"command_burst: 30":          fmt.Sprintf("command_burst: %d", tc.burst),
					} {
						if bytes.Count(data, []byte(old)) != 1 {
							t.Fatalf("shipped runtime config does not contain exactly one %q", old)
						}
						data = bytes.Replace(data, []byte(old), []byte(replacement), 1)
					}
				}
				if err := os.WriteFile(filepath.Join(dir, entry.Name()), data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := config.Load(dir)
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			opts := (&clusterRuntime{cfg: cfg}).buildTelnetServerOptions()
			if opts.CommandRatePerSecond != tc.rate || opts.CommandBurst != tc.burst {
				t.Fatalf("Telnet command rate/burst = %d/%d, want %d/%d", opts.CommandRatePerSecond, opts.CommandBurst, tc.rate, tc.burst)
			}
		})
	}
}
