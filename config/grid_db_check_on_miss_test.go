package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGridDBCheckOnMissDefaultsTrue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.GridDBCheckOnMiss == nil {
		t.Fatalf("expected GridDBCheckOnMiss to be set")
	}
	if !*cfg.GridDBCheckOnMiss {
		t.Fatalf("expected GridDBCheckOnMiss=true by default")
	}
}

func TestGridDBCheckOnMissAllowsFalse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("grid_db_check_on_miss: false\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.GridDBCheckOnMiss == nil {
		t.Fatalf("expected GridDBCheckOnMiss to be set")
	}
	if *cfg.GridDBCheckOnMiss {
		t.Fatalf("expected GridDBCheckOnMiss=false when configured")
	}
}
