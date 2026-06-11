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
	withStale := "2026/04/20 12:00:00 Path predictions (5m): total=1,200 derived=5 combined=700 voacap_closed=20 voacap_aligned=12 voacap_sparse_upgrade=7 voacap_open=9 insufficient=500 no_sample=300 low_count=75 low_receiver=25 low_weight=50 stale=50 cap_limited=25 cap_would_block=10 override_r=0 override_g=0"
	got, ok := parsePredictionTotals(withStale)
	if !ok {
		t.Fatalf("expected prediction totals to parse")
	}
	if got.Total != 1200 || got.Combined != 700 || got.VOACAPClosed != 20 || got.VOACAPAligned != 12 || got.VOACAPSparseUpgrade != 7 || got.VOACAPOpen != 9 || got.Insufficient != 500 || got.NoSample != 300 || got.LowCount != 75 || got.LowReceiver != 25 || got.LowWeight != 50 || got.Stale != 50 || got.CapLimited != 25 || got.CapWouldBlock != 10 {
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
	if got.LowReceiver != 0 {
		t.Fatalf("expected legacy low_receiver=0, got %d", got.LowReceiver)
	}
	if got.VOACAPClosed != 0 {
		t.Fatalf("expected legacy voacap_closed=0, got %d", got.VOACAPClosed)
	}
	if got.VOACAPAligned != 0 {
		t.Fatalf("expected legacy voacap_aligned=0, got %d", got.VOACAPAligned)
	}
	if got.VOACAPSparseUpgrade != 0 || got.VOACAPOpen != 0 {
		t.Fatalf("expected legacy REL-gated VOACAP counters=0, got %+v", got)
	}
}

func TestParseCapShadowTotals(t *testing.T) {
	line := "2026/04/20 12:00:00 Path cap shadow (5m): total=1,200 cap5_pass=10 cap5_low_count=20 cap5_low_receiver=25 cap5_low_weight=30 cap5_block=40 cap6_pass=50 cap6_low_count=60 cap6_low_receiver=65 cap6_low_weight=70 cap6_block=80 cap8_pass=90 cap8_low_count=100 cap8_low_receiver=105 cap8_low_weight=110 cap8_block=120"
	got, ok := parseCapShadowTotals(line)
	if !ok {
		t.Fatalf("expected cap-shadow totals to parse")
	}
	if got.Total != 1200 || len(got.Candidates) != 3 {
		t.Fatalf("unexpected cap-shadow totals: %+v", got)
	}
	if got.Candidates[0].MaxEffectiveCount != 5 || got.Candidates[0].Pass != 10 || got.Candidates[0].LowCount != 20 || got.Candidates[0].LowReceiver != 25 || got.Candidates[0].LowWeight != 30 || got.Candidates[0].Block != 40 {
		t.Fatalf("unexpected cap5 totals: %+v", got.Candidates[0])
	}
	if got.Candidates[2].MaxEffectiveCount != 8 || got.Candidates[2].Pass != 90 || got.Candidates[2].Block != 120 {
		t.Fatalf("unexpected cap8 totals: %+v", got.Candidates[2])
	}
}

func TestParseCapP50ShadowTotals(t *testing.T) {
	line := "2026/04/20 12:00:00 Path cap p50 shadow (5m): total=1,200 cap5_p50_pass_unlikely=1 cap5_p50_pass_low=2 cap5_p50_pass_medium=3 cap5_p50_pass_high=4 cap5_p50_same=5 cap5_p50_stronger=6 cap5_p50_weaker=7 cap5_p50_to_insufficient=8 cap6_p50_pass_unlikely=9 cap6_p50_pass_low=10 cap6_p50_pass_medium=11 cap6_p50_pass_high=12 cap6_p50_same=13 cap6_p50_stronger=14 cap6_p50_weaker=15 cap6_p50_to_insufficient=16"
	got, ok := parseCapP50ShadowTotals(line)
	if !ok {
		t.Fatalf("expected cap-p50-shadow totals to parse")
	}
	if got.Total != 1200 || len(got.Candidates) != 2 {
		t.Fatalf("unexpected cap-p50-shadow totals: %+v", got)
	}
	if got.Candidates[0].MaxEffectiveCount != 5 || got.Candidates[0].PassUnlikely != 1 || got.Candidates[0].PassHigh != 4 || got.Candidates[0].Same != 5 || got.Candidates[0].ToInsufficient != 8 {
		t.Fatalf("unexpected cap5 p50 totals: %+v", got.Candidates[0])
	}
	if got.Candidates[1].MaxEffectiveCount != 6 || got.Candidates[1].PassLow != 10 || got.Candidates[1].Stronger != 14 || got.Candidates[1].Weaker != 15 {
		t.Fatalf("unexpected cap6 p50 totals: %+v", got.Candidates[1])
	}
}

func FuzzParsePathPredictionLogTotals(f *testing.F) {
	f.Add("2026/04/20 12:00:00 Path predictions (5m): total=1,200 derived=5 combined=700 voacap_closed=20 voacap_aligned=12 voacap_sparse_upgrade=7 voacap_open=9 insufficient=500 no_sample=300 low_count=75 low_receiver=25 low_weight=50 stale=50 cap_limited=25 cap_would_block=10 override_r=0 override_g=0")
	f.Add("2026/04/20 12:00:00 Path cap shadow (5m): total=1,200 cap5_pass=10 cap5_low_count=20 cap5_low_receiver=25 cap5_low_weight=30 cap5_block=40 cap6_pass=50 cap6_low_count=60 cap6_low_receiver=65 cap6_low_weight=70 cap6_block=80 cap8_pass=90 cap8_low_count=100 cap8_low_receiver=105 cap8_low_weight=110 cap8_block=120")
	f.Add("2026/04/20 12:00:00 Path cap p50 shadow (5m): total=1,200 cap5_p50_pass_unlikely=1 cap5_p50_pass_low=2 cap5_p50_pass_medium=3 cap5_p50_pass_high=4 cap5_p50_same=5 cap5_p50_stronger=6 cap5_p50_weaker=7 cap5_p50_to_insufficient=8")
	f.Add("")
	f.Fuzz(func(t *testing.T, line string) {
		if len(line) > 4096 {
			t.Skip()
		}
		_, _ = parsePredictionTotals(line)
		_, _ = parseCapShadowTotals(line)
		_, _ = parseCapP50ShadowTotals(line)
	})
}

func TestPredictionActivitySummarySeparatesCountAndWeightLimits(t *testing.T) {
	got := predictionActivitySummary([]predictionHour{
		{Hour: "01:00", AvgTotal: 100, AvgCombined: 20, AvgInsufficient: 80, AvgLowCount: 60, AvgLowReceiver: 5, AvgLowWeight: 10},
		{Hour: "02:00", AvgTotal: 200, AvgCombined: 150, AvgVOACAPClosed: 4, AvgVOACAPAligned: 3, AvgVOACAPSparseUpgrade: 2, AvgVOACAPOpen: 1, AvgInsufficient: 50, AvgLowCount: 5, AvgLowReceiver: 10, AvgLowWeight: 40},
	})
	if !strings.Contains(got, "Hours mostly count-limited: 01:00") {
		t.Fatalf("expected count-limited summary, got %q", got)
	}
	if !strings.Contains(got, "Hours mostly weight-limited: 02:00") {
		t.Fatalf("expected weight-limited summary, got %q", got)
	}
	if !strings.Contains(got, "Hours with VOACAP closed fallback predictions: 02:00") {
		t.Fatalf("expected VOACAP closed summary, got %q", got)
	}
	if !strings.Contains(got, "Hours with VOACAP-aligned sparse p50 predictions: 02:00") {
		t.Fatalf("expected VOACAP-aligned summary, got %q", got)
	}
	if !strings.Contains(got, "Hours with REL-gated VOACAP sparse p50 upgrades: 02:00") {
		t.Fatalf("expected REL-gated sparse-upgrade summary, got %q", got)
	}
	if !strings.Contains(got, "Hours with REL-gated VOACAP no-p50 open predictions: 02:00") {
		t.Fatalf("expected REL-gated no-p50 open summary, got %q", got)
	}
}

func TestCapShadowActivitySummary(t *testing.T) {
	got := capShadowActivitySummary([]capShadowHour{
		{
			Hour:    "01:00",
			Samples: 2,
			Candidates: []capShadowCandidateHour{
				{MaxEffectiveCount: 5, AvgPass: 10, AvgLowCount: 20, AvgLowReceiver: 25, AvgLowWeight: 30, AvgBlock: 40},
				{MaxEffectiveCount: 6, AvgPass: 50, AvgLowCount: 60, AvgLowReceiver: 65, AvgLowWeight: 70, AvgBlock: 80},
			},
		},
	})
	if !strings.Contains(got, "cap5 avg pass 10.0, low_count 20.0, low_receiver 25.0, low_weight 30.0, would_block 40.0") {
		t.Fatalf("expected cap5 summary, got %q", got)
	}
	if !strings.Contains(got, "cap6 avg pass 50.0") {
		t.Fatalf("expected cap6 summary, got %q", got)
	}
}

func TestCapP50ShadowActivitySummary(t *testing.T) {
	got := capP50ShadowActivitySummary([]capP50ShadowHour{
		{
			Hour:    "01:00",
			Samples: 2,
			Candidates: []capP50ShadowCandidateHour{
				{MaxEffectiveCount: 5, AvgPassUnlikely: 1, AvgPassLow: 2, AvgPassMedium: 3, AvgPassHigh: 4, AvgSame: 5, AvgStronger: 6, AvgWeaker: 7, AvgToInsufficient: 8},
			},
		},
	})
	if !strings.Contains(got, "cap5 p50 avg pass unlikely 1.0, low 2.0, medium 3.0, high 4.0") {
		t.Fatalf("expected cap5 p50 pass summary, got %q", got)
	}
	if !strings.Contains(got, "same 5.0, stronger 6.0, weaker 7.0, to_insufficient 8.0") {
		t.Fatalf("expected cap5 p50 movement summary, got %q", got)
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
