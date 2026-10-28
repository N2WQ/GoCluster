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

func TestTelnetIPSessionLimitRuntimeWiring(t *testing.T) {
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
					old := []byte("max_sessions_per_ip: 10")
					if bytes.Count(data, old) != 1 {
						t.Fatal("shipped runtime config does not contain exactly one expected IP-session setting")
					}
					data = bytes.Replace(data, old, []byte(fmt.Sprintf("max_sessions_per_ip: %d", limit)), 1)
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
			if opts.MaxSessionsPerIP != limit {
				t.Fatalf("Telnet server option MaxSessionsPerIP = %d, want %d", opts.MaxSessionsPerIP, limit)
			}
			if opts.MaxConnections != cfg.Telnet.MaxConnections || opts.MaxSessionsPerBaseCall != cfg.Telnet.MaxSessionsPerBaseCall {
				t.Fatalf("other session limits changed: global=%d station=%d", opts.MaxConnections, opts.MaxSessionsPerBaseCall)
			}
		})
	}
}
