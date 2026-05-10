package propreport

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/pathreliability"
)

func TestParsePredictionTotalsWithAndWithoutStale(t *testing.T) {
	withStale := "2026/04/20 12:00:00 Path predictions (5m): total=1,200 derived=5 combined=700 insufficient=500 no_sample=300 low_count=75 low_weight=75 stale=50 cap_limited=25 cap_would_block=10 override_r=0 override_g=0"
	got, ok := parsePredictionTotals(withStale)
	if !ok {
		t.Fatalf("expected prediction totals to parse")
	}
	if got.Total != 1200 || got.Combined != 700 || got.Insufficient != 500 || got.NoSample != 300 || got.LowCount != 75 || got.LowWeight != 75 || got.Stale != 50 || got.CapLimited != 25 || got.CapWouldBlock != 10 {
		t.Fatalf("unexpected parsed totals with stale: %+v", got)
	}

	withoutStale := "2026/04/20 12:00:00 Path predictions (5m): total=100 derived=2 combined=60 insufficient=40 no_sample=30 low_weight=10 override_r=0 override_g=0"
	got, ok = parsePredictionTotals(withoutStale)
	if !ok {
		t.Fatalf("expected legacy prediction totals to parse")
	}
	if got.Total != 100 || got.Combined != 60 || got.Insufficient != 40 || got.NoSample != 30 || got.LowWeight != 10 || got.Stale != 0 {
		t.Fatalf("unexpected parsed legacy totals: %+v", got)
	}
	if got.LowCount != 0 {
		t.Fatalf("expected legacy low_count=0, got %d", got.LowCount)
	}
}

func TestPredictionActivitySummarySeparatesCountAndWeightLimits(t *testing.T) {
	got := predictionActivitySummary([]predictionHour{
		{Hour: "01:00", AvgTotal: 100, AvgCombined: 20, AvgInsufficient: 80, AvgLowCount: 60, AvgLowWeight: 10},
		{Hour: "02:00", AvgTotal: 200, AvgCombined: 150, AvgInsufficient: 50, AvgLowCount: 5, AvgLowWeight: 40},
	})
	if !strings.Contains(got, "Hours mostly count-limited: 01:00") {
		t.Fatalf("expected count-limited summary, got %q", got)
	}
	if !strings.Contains(got, "Hours mostly weight-limited: 02:00") {
		t.Fatalf("expected weight-limited summary, got %q", got)
	}
}

func TestBuildModelContextIncludesPredictionAgeGate(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	cfg.DefaultHalfLifeSec = 240
	cfg.BandHalfLifeSec = map[string]int{"20m": 360, "10m": 240}
	cfg.MaxPredictionAgeHalfLifeMultiplier = 1.25
	cfg.ReceiverContributionMode = pathreliability.ReceiverContributionShadow

	ctx := buildModelContext(cfg, []string{"20m", "10m"})
	if ctx.MaxPredictionAgeHalfLifeMultiplier != 1.25 {
		t.Fatalf("expected max prediction age multiplier 1.25, got %v", ctx.MaxPredictionAgeHalfLifeMultiplier)
	}
	if got := ctx.MaxPredictionAgeByBand["20m"]; got != 450 {
		t.Fatalf("expected 20m max age 450, got %d", got)
	}
	if got := ctx.MaxPredictionAgeByBand["10m"]; got != 300 {
		t.Fatalf("expected 10m max age 300, got %d", got)
	}
	if ctx.MinObservationCount != cfg.MinObservationCount || ctx.ReceiverContributionMode != pathreliability.ReceiverContributionShadow {
		t.Fatalf("expected receiver contribution context, got %+v", ctx)
	}
	if got := ctx.NoiseOffsets["URBAN"]; got != 17 {
		t.Fatalf("expected urban noise offset 17, got %v", got)
	}

	cfg.MaxPredictionAgeHalfLifeMultiplier = 0
	ctx = buildModelContext(cfg, []string{"20m"})
	if got := ctx.MaxPredictionAgeByBand["20m"]; got != 0 {
		t.Fatalf("expected disabled max age 0, got %d", got)
	}
}

func TestFormatNoiseOffsets(t *testing.T) {
	got := formatNoiseOffsets(map[string]float64{
		"URBAN": 17,
		"QUIET": 0,
	})
	if got != "QUIET=0; URBAN=17" {
		t.Fatalf("unexpected noise offset summary: %q", got)
	}
}

func writeOpenAIConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "openai.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write openai config: %v", err)
	}
	return path
}

func validOpenAIConfigBody() string {
	return `
api_key: ""
model: "gpt-5-nano"
endpoint: "https://api.openai.com/v1/chat/completions"
max_tokens: 400
temperature: 0
system_prompt: "Summarize without inventing facts."
`
}

func TestResolveConfigDirSupportsPrimaryAndLegacyInputs(t *testing.T) {
	if got := resolveConfigDir(filepath.Join("data", "config"), ""); got != filepath.Join("data", "config") {
		t.Fatalf("primary config dir = %s", got)
	}
	legacyFile := filepath.Join("data", "config", "path_reliability.yaml")
	if got := resolveConfigDir("", legacyFile); got != filepath.Join("data", "config") {
		t.Fatalf("legacy path config file resolved to %s", got)
	}
	if got := resolveConfigDir("", filepath.Join("custom", "config")); got != filepath.Join("custom", "config") {
		t.Fatalf("legacy config dir resolved to %s", got)
	}
}

func TestLoadOpenAIConfigValidatesKnownFieldsAndRequiredValues(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key")
	cfg, err := loadOpenAIConfig(writeOpenAIConfig(t, validOpenAIConfigBody()))
	if err != nil {
		t.Fatalf("loadOpenAIConfig() error: %v", err)
	}
	if cfg.Model != "gpt-5-nano" || cfg.MaxTokens != 400 || cfg.Temperature != 0 {
		t.Fatalf("unexpected OpenAI config: %+v", cfg)
	}

	_, err = loadOpenAIConfig(writeOpenAIConfig(t, strings.Replace(validOpenAIConfigBody(), `model: "gpt-5-nano"`+"\n", "", 1)))
	if err == nil || !strings.Contains(err.Error(), "model") {
		t.Fatalf("expected missing model error, got %v", err)
	}

	_, err = loadOpenAIConfig(writeOpenAIConfig(t, validOpenAIConfigBody()+"unexpected: true\n"))
	if err == nil || !strings.Contains(err.Error(), "field unexpected not found") {
		t.Fatalf("expected unknown field error, got %v", err)
	}

	t.Setenv("OPENAI_API_KEY", "")
	_, err = loadOpenAIConfig(writeOpenAIConfig(t, validOpenAIConfigBody()))
	if err == nil || !strings.Contains(err.Error(), "OpenAI API key missing") {
		t.Fatalf("expected missing API key error, got %v", err)
	}
}

func TestGenerateNoLLMDoesNotRequireOpenAIConfig(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "empty.log")
	if err := os.WriteFile(logPath, nil, 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}
	jsonOut := filepath.Join(dir, "prop.json")
	reportOut := filepath.Join(dir, "prop.md")

	_, err := Generate(context.Background(), Options{
		Date:             time.Date(2026, 4, 22, 0, 0, 0, 0, time.UTC),
		LogPath:          logPath,
		JSONOut:          jsonOut,
		ReportOut:        reportOut,
		ConfigDir:        filepath.Join("..", "..", "data", "config"),
		OpenAIConfigPath: filepath.Join(dir, "missing-openai.yaml"),
		NoLLM:            true,
	})
	if err != nil {
		t.Fatalf("Generate() with NoLLM error: %v", err)
	}
	if _, err := os.Stat(jsonOut); err != nil {
		t.Fatalf("expected JSON output: %v", err)
	}
	if _, err := os.Stat(reportOut); err != nil {
		t.Fatalf("expected report output: %v", err)
	}
}

func TestGenerateLLMRequiresValidOpenAIConfigBeforeWritingOutputs(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "empty.log")
	if err := os.WriteFile(logPath, nil, 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}
	jsonOut := filepath.Join(dir, "prop.json")
	reportOut := filepath.Join(dir, "prop.md")

	_, err := Generate(context.Background(), Options{
		Date:             time.Date(2026, 4, 22, 0, 0, 0, 0, time.UTC),
		LogPath:          logPath,
		JSONOut:          jsonOut,
		ReportOut:        reportOut,
		ConfigDir:        filepath.Join("..", "..", "data", "config"),
		OpenAIConfigPath: filepath.Join(dir, "missing-openai.yaml"),
		NoLLM:            false,
	})
	if err == nil || !strings.Contains(err.Error(), "load OpenAI config") {
		t.Fatalf("expected hard OpenAI config load error, got %v", err)
	}
	if _, statErr := os.Stat(jsonOut); !os.IsNotExist(statErr) {
		t.Fatalf("expected JSON output not to be written, stat err=%v", statErr)
	}
	if _, statErr := os.Stat(reportOut); !os.IsNotExist(statErr) {
		t.Fatalf("expected report output not to be written, stat err=%v", statErr)
	}
}
