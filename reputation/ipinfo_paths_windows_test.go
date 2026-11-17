package reputation

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIPInfoManagedPathWindowsAliasesBeforeCreation(t *testing.T) {
	cfg := ipinfoTestConfig(t, "http://unused.invalid")
	pointer := filepath.Join(cfg.IPInfoPebblePath, ipinfoPebbleCurrentFile)
	if err := os.Mkdir(cfg.IPInfoPebblePath, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, path, message string }{
		{"extended", `\\?\` + pointer, "IPinfo path collision"},
		{"trailing dot", pointer + ".", "IPinfo path collision"},
		{"unnamed stream", pointer + "::$DATA", "alternate data stream"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := cfg
			cfg.IPInfoSnapshotPath = tc.path
			g := openTestGate(t, cfg)
			if err := validateIPInfoPaths(g.cfg, nil); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("expected %s before creating the pointer, got %v", tc.message, err)
			}
		})
	}
}
