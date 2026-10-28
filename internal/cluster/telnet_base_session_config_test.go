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

func TestTelnetBaseSessionLimitRuntimeWiring(t *testing.T) {
	source := filepath.Join("..", "..", "data", "config")
	for _, limit := range []int{10, 0, 3} {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
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
					old := []byte("max_sessions_per_base_call: 10")
					if bytes.Count(data, old) != 1 {
						t.Fatal("shipped runtime config does not contain exactly one expected base-session setting")
					}
					data = bytes.Replace(data, old, []byte(fmt.Sprintf("max_sessions_per_base_call: %d", limit)), 1)
				}
				if err := os.WriteFile(filepath.Join(dir, entry.Name()), data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := config.Load(dir)
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			runtime := &clusterRuntime{cfg: cfg}
			opts := runtime.buildTelnetServerOptions()
			if opts.MaxSessionsPerBaseCall != limit {
				t.Fatalf("Telnet server option MaxSessionsPerBaseCall = %d, want %d", opts.MaxSessionsPerBaseCall, limit)
			}
			if opts.MaxConnections != cfg.Telnet.MaxConnections {
				t.Fatalf("global limit option = %d, want %d", opts.MaxConnections, cfg.Telnet.MaxConnections)
			}
		})
	}
}
