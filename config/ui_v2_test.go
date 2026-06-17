package config

import (
	"strings"
	"testing"
)

func TestLoadUIV2Defaults(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `ui:
  mode: tview-v2
`
	writeTestConfigOverlay(t, dir, "app.yaml", cfgText)
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.UI.Mode != "tview-v2" {
		t.Fatalf("expected ui.mode=tview-v2, got %q", cfg.UI.Mode)
	}
	if cfg.UI.V2.TargetFPS != 15 {
		t.Fatalf("expected ui.v2.target_fps=15 from shipped YAML, got %d", cfg.UI.V2.TargetFPS)
	}
	if len(cfg.UI.V2.Pages) != 4 {
		t.Fatalf("expected 4 default pages, got %d", len(cfg.UI.V2.Pages))
	}
	if cfg.UI.V2.Pages[3] != "events" {
		t.Fatalf("expected default events page in position 4, got %q", cfg.UI.V2.Pages[3])
	}
	if cfg.UI.V2.EventBuffer.MaxEvents != 1000 || cfg.UI.V2.EventBuffer.MaxBytesMB != 1 {
		t.Fatalf("unexpected event buffer defaults: %+v", cfg.UI.V2.EventBuffer)
	}
	if cfg.UI.V2.DebugBuffer.MaxEvents != 5000 || cfg.UI.V2.DebugBuffer.MaxBytesMB != 2 {
		t.Fatalf("unexpected debug buffer defaults: %+v", cfg.UI.V2.DebugBuffer)
	}
	if !cfg.UI.V2.Keybindings.UseAlternatives {
		t.Fatalf("expected ui.v2.keybindings.use_alternatives default true")
	}
}

func TestLoadUIV2InvalidPage(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `ui:
  mode: tview-v2
  v2:
    pages: ["overview", "badpage"]
`
	writeTestConfigOverlay(t, dir, "app.yaml", cfgText)
	if _, err := Load(dir); err == nil {
		t.Fatalf("expected invalid page error")
	}
}

func TestLoadUIV2DuplicatePages(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `ui:
  mode: tview-v2
  v2:
    pages: ["overview", "overview"]
`
	writeTestConfigOverlay(t, dir, "app.yaml", cfgText)
	if _, err := Load(dir); err == nil {
		t.Fatalf("expected duplicate page error")
	}
}

func TestLoadUIV2EventsPageAllowed(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `ui:
  mode: tview-v2
  v2:
    pages: ["overview", "events"]
`
	writeTestConfigOverlay(t, dir, "app.yaml", cfgText)
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if len(cfg.UI.V2.Pages) != 2 {
		t.Fatalf("expected 2 pages, got %d", len(cfg.UI.V2.Pages))
	}
	if cfg.UI.V2.Pages[1] != "events" {
		t.Fatalf("expected events page, got %q", cfg.UI.V2.Pages[1])
	}
}

func TestLoadUIHeadlessModeAllowed(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	writeTestConfigOverlay(t, dir, "app.yaml", `
ui:
  mode: headless
`)
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.UI.Mode != "headless" {
		t.Fatalf("expected ui.mode=headless, got %q", cfg.UI.Mode)
	}
}

func TestLoadRejectsLegacyUIModes(t *testing.T) {
	for _, mode := range []string{"ansi", "tview", "auto", "ansi_poc", "none"} {
		t.Run(mode, func(t *testing.T) {
			dir := testConfigDir(t)
			writeRequiredFloodControlFile(t, dir)
			writeTestConfigOverlay(t, dir, "app.yaml", `
ui:
  mode: `+mode+`
`)
			_, err := Load(dir)
			if err == nil {
				t.Fatalf("expected ui.mode=%s to fail", mode)
			}
			if !strings.Contains(err.Error(), "invalid ui.mode") || !strings.Contains(err.Error(), "headless or tview-v2") {
				t.Fatalf("expected legacy ui.mode migration error, got %v", err)
			}
		})
	}
}

func TestLoadWarnsAndIgnoresLegacyUIKeys(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	writeTestConfigOverlay(t, dir, "app.yaml", `
ui:
  mode: headless
  refresh_ms: 300
  color: true
  clear_screen: true
  pane_lines:
    stats: 10
`)
	cfg, diagnostics, err := LoadWithDiagnostics(dir)
	if err != nil {
		t.Fatalf("LoadWithDiagnostics() error: %v", err)
	}
	if cfg.UI.Mode != "headless" {
		t.Fatalf("expected ui.mode=headless, got %q", cfg.UI.Mode)
	}
	for _, key := range []string{"ui.refresh_ms", "ui.color", "ui.clear_screen", "ui.pane_lines"} {
		if !containsDiagnostic(diagnostics.Warnings, key) {
			t.Fatalf("expected warning for %s, got %#v", key, diagnostics.Warnings)
		}
	}
}
