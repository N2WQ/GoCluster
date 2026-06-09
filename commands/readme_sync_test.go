package commands

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"dxcluster/config"
)

const (
	readmeHelpBeginMarker = "<!-- BEGIN DEFAULT_GO_HELP -->"
	readmeHelpEndMarker   = "<!-- END DEFAULT_GO_HELP -->"
)

func TestReadmeDefaultGoHelpBlockMatchesProcessor(t *testing.T) {
	repoRoot := repoRootFromTestFile(t)

	cfg, err := config.Load(filepath.Join(repoRoot, "data", "config"))
	if err != nil {
		t.Fatalf("load shipped config: %v", err)
	}
	pathCfg := cfg.PathReliability

	p := NewProcessor(nil, nil, nil, nil, nil, nil,
		WithDedupeHelp(DedupeHelpConfig{
			Configured:        true,
			FastWindowSeconds: cfg.Dedup.SecondaryFastWindowSeconds,
			MedWindowSeconds:  cfg.Dedup.SecondaryMedWindowSeconds,
			SlowWindowSeconds: cfg.Dedup.SecondarySlowWindowSeconds,
		}),
		WithWhoSpotsMeHelp(WhoSpotsMeHelpConfig{
			Configured:    true,
			WindowMinutes: cfg.WhoSpotsMe.WindowMinutes,
		}),
		WithPathGlyphHelp(PathGlyphHelpConfig{
			Enabled:      pathCfg.DisplayEnabled,
			High:         pathCfg.GlyphSymbols.High,
			Medium:       pathCfg.GlyphSymbols.Medium,
			Low:          pathCfg.GlyphSymbols.Low,
			Unlikely:     pathCfg.GlyphSymbols.Unlikely,
			Insufficient: pathCfg.GlyphSymbols.Insufficient,
			Closed:       pathCfg.GlyphSymbols.Closed,
		}),
	)

	want := normalizeReadmeHelpText(p.ProcessCommandForClient("HELP", "N0CALL", "", nil, "go"))

	readmePath := filepath.Join(repoRoot, "README.md")
	readmeBytes, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("read %s: %v", readmePath, err)
	}
	got, err := extractMarkedCodeBlock(string(readmeBytes), readmeHelpBeginMarker, readmeHelpEndMarker)
	if err != nil {
		t.Fatalf("extract README HELP block: %v", err)
	}

	if got != want {
		t.Fatalf("README HELP block drifted.\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
}

func repoRootFromTestFile(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(filepath.Dir(file))
}

func extractMarkedCodeBlock(doc, beginMarker, endMarker string) (string, error) {
	doc = strings.ReplaceAll(doc, "\r\n", "\n")

	begin := strings.Index(doc, beginMarker)
	if begin < 0 {
		return "", errors.New("begin marker not found")
	}
	begin += len(beginMarker)

	endRel := strings.Index(doc[begin:], endMarker)
	if endRel < 0 {
		return "", errors.New("end marker not found")
	}

	section := strings.TrimSpace(doc[begin : begin+endRel])
	const prefix = "```text\n"
	const suffix = "\n```"
	if !strings.HasPrefix(section, prefix) || !strings.HasSuffix(section, suffix) {
		return "", errors.New("marked README section is not a text code block")
	}

	return normalizeReadmeHelpText(strings.TrimSuffix(strings.TrimPrefix(section, prefix), suffix)), nil
}

func normalizeReadmeHelpText(text string) string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	if !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	return text
}
