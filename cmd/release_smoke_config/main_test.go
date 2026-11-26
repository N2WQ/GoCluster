package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"dxcluster/config"
)

func TestSmokeConfigUsesPackagedAssetsAndLocalEndpoints(t *testing.T) {
	src := filepath.Join("..", "..", "data", "config")
	dir := t.TempDir()
	if err := os.CopyFS(dir, os.DirFS(src)); err != nil {
		t.Fatal(err)
	}
	before, err := config.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = configure(dir, 54001, 54002, "owned-smoke> "); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Telnet.Port != 54001 || cfg.Telnet.LoginPrompt != "owned-smoke> " {
		t.Fatalf("unexpected Telnet probe endpoint or prompt: %d %q", cfg.Telnet.Port, cfg.Telnet.LoginPrompt)
	}
	for _, url := range []string{cfg.CTY.URL, cfg.FCCULS.URL, cfg.ISED.URL, cfg.ISED.SpecialURL} {
		if url != "http://127.0.0.1:54002/release-smoke" {
			t.Fatalf("refresh can escape the owned sink: %s", url)
		}
	}
	if cfg.RBN.Enabled || cfg.RBNDigital.Enabled || cfg.PSKReporter.Enabled || cfg.DXSummit.Enabled ||
		cfg.Peering.Enabled || cfg.Reputation.Enabled || cfg.PropReport.Enabled || cfg.Skew.Enabled {
		t.Fatal("smoke config enabled an external feed or background fetch")
	}
	for _, feed := range cfg.HumanTelnet {
		if feed.Enabled {
			t.Fatal("smoke config enabled a human feed")
		}
	}
	if !reflect.DeepEqual(cfg.CallCorrection, before.CallCorrection) ||
		cfg.H3TablePath != before.H3TablePath || cfg.FCCULS.AllowlistPath != before.FCCULS.AllowlistPath {
		t.Fatal("smoke config replaced packaged startup inputs")
	}
}
