package pathreliability

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func writeTempConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "path_reliability.yaml")
	if err := os.WriteFile(path, []byte(contents), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func writeTempConfigOverlay(t *testing.T, contents string) string {
	t.Helper()
	base, err := os.ReadFile(filepath.Join("..", "data", "config", "path_reliability.yaml"))
	if err != nil {
		t.Fatalf("read shipped path reliability config: %v", err)
	}
	var merged map[string]any
	if err := yaml.Unmarshal(base, &merged); err != nil {
		t.Fatalf("parse shipped path reliability config: %v", err)
	}
	var override map[string]any
	if err := yaml.Unmarshal([]byte(contents), &override); err != nil {
		t.Fatalf("parse override path reliability config: %v", err)
	}
	merged = mergeTestYAMLMaps(merged, override)
	data, err := yaml.Marshal(merged)
	if err != nil {
		t.Fatalf("marshal override path reliability config: %v", err)
	}
	return writeTempConfig(t, string(data))
}

func writeTempConfigWithoutKey(t *testing.T, path ...string) string {
	t.Helper()
	base, err := os.ReadFile(filepath.Join("..", "data", "config", "path_reliability.yaml"))
	if err != nil {
		t.Fatalf("read shipped path reliability config: %v", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(base, &doc); err != nil {
		t.Fatalf("parse shipped path reliability config: %v", err)
	}
	current := doc
	for _, key := range path[:len(path)-1] {
		next, ok := current[key].(map[string]any)
		if !ok {
			t.Fatalf("test path %s missing before final key", strings.Join(path, "."))
		}
		current = next
	}
	delete(current, path[len(path)-1])
	data, err := yaml.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal config without %s: %v", strings.Join(path, "."), err)
	}
	return writeTempConfig(t, string(data))
}

func mergeTestYAMLMaps(dst, src map[string]any) map[string]any {
	if dst == nil {
		dst = make(map[string]any)
	}
	for key, val := range src {
		if existing, ok := dst[key]; ok {
			existingMap, okExisting := existing.(map[string]any)
			incomingMap, okIncoming := val.(map[string]any)
			if okExisting && okIncoming {
				dst[key] = mergeTestYAMLMaps(existingMap, incomingMap)
				continue
			}
		}
		dst[key] = val
	}
	return dst
}

func TestLoadFileRejectsLegacyThresholdKeys(t *testing.T) {
	path := writeTempConfigOverlay(t, `
glyph_thresholds:
  excellent: -13
  good: -17
  marginal: -21
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected legacy threshold keys to fail")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "unsupported glyph threshold key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadFileRejectsInvalidGlyphSymbols(t *testing.T) {
	path := writeTempConfigOverlay(t, `
glyph_symbols:
  high: "++"
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected invalid glyph symbol to fail")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "glyph_symbols.high") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDefaultNoiseOffsets(t *testing.T) {
	cfg := DefaultConfig()
	model := cfg.NoiseModel()
	cases := []struct {
		class   string
		penalty float64
	}{
		{"QUIET", 0},
		{"RURAL", 4},
		{"SUBURBAN", 12},
		{"URBAN", 17},
		{"INDUSTRIAL", 20},
	}
	for _, tc := range cases {
		if got := model.Penalty(tc.class); got != tc.penalty {
			t.Fatalf("Penalty(%s) = %v, want %v", tc.class, got, tc.penalty)
		}
	}
}

func TestDefaultMaxPredictionAgeMultiplier(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxPredictionAgeHalfLifeMultiplier != 1.25 {
		t.Fatalf("default max prediction age multiplier = %v, want 1.25", cfg.MaxPredictionAgeHalfLifeMultiplier)
	}
}

func TestDefaultMinObservationCount(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MinObservationCount != 19 {
		t.Fatalf("default min observation count = %v, want 19", cfg.MinObservationCount)
	}
}

func TestDefaultReceiverContributionCaps(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ReceiverContributionMode != ReceiverContributionShadow {
		t.Fatalf("default receiver contribution mode = %q, want %q", cfg.ReceiverContributionMode, ReceiverContributionShadow)
	}
	if cfg.ReceiverFineSlots != 4 || cfg.ReceiverCoarseSlots != 8 {
		t.Fatalf("default receiver slots fine=%d coarse=%d, want fine=4 coarse=8", cfg.ReceiverFineSlots, cfg.ReceiverCoarseSlots)
	}
	if cfg.ReceiverMaxEffectiveCount != 5 {
		t.Fatalf("default receiver max effective count = %d, want 5", cfg.ReceiverMaxEffectiveCount)
	}
	if cfg.ReceiverMaxEffectiveWeight != 5 {
		t.Fatalf("default receiver max effective weight = %v, want 5", cfg.ReceiverMaxEffectiveWeight)
	}
}

func TestLoadFileRejectsNegativeMaxPredictionAgeMultiplier(t *testing.T) {
	path := writeTempConfigOverlay(t, `
max_prediction_age_half_life_multiplier: -1
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected negative max prediction age multiplier to fail")
	}
	if !strings.Contains(err.Error(), "max_prediction_age_half_life_multiplier") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadFileRejectsNonPositiveMinObservationCount(t *testing.T) {
	path := writeTempConfigOverlay(t, `
min_observation_count: 0
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected non-positive min observation count to fail")
	}
	if !strings.Contains(err.Error(), "min_observation_count") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadFileRejectsInvalidReceiverContributionMode(t *testing.T) {
	path := writeTempConfigOverlay(t, `
receiver_contribution_mode: maybe
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected invalid receiver contribution mode to fail")
	}
	if !strings.Contains(err.Error(), "receiver_contribution_mode") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadFileRejectsInvalidReceiverContributionCaps(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{name: "fine slots zero", body: "receiver_fine_slots: 0\n", want: "receiver_fine_slots"},
		{name: "fine slots too large", body: "receiver_fine_slots: 5\n", want: "receiver_fine_slots"},
		{name: "coarse slots zero", body: "receiver_coarse_slots: 0\n", want: "receiver_coarse_slots"},
		{name: "coarse slots too large", body: "receiver_coarse_slots: 9\n", want: "receiver_coarse_slots"},
		{name: "max count zero", body: "receiver_max_effective_count: 0\n", want: "receiver_max_effective_count"},
		{name: "max weight zero", body: "receiver_max_effective_weight: 0\n", want: "receiver_max_effective_weight"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadFile(writeTempConfigOverlay(t, tc.body))
			if err == nil {
				t.Fatalf("expected invalid %s to fail", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error to mention %s, got %v", tc.want, err)
			}
		})
	}
}

func TestLoadFileRejectsNegativeNoisePenalty(t *testing.T) {
	path := writeTempConfigOverlay(t, `
noise_offsets:
  rural: -3
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected negative noise penalty to fail")
	}
	if !strings.Contains(err.Error(), "noise_offsets.RURAL") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadFilePreservesExplicitFT4Zero(t *testing.T) {
	cfg, err := LoadFile(filepath.Join("..", "data", "config", "path_reliability.yaml"))
	if err != nil {
		t.Fatalf("load shipped config: %v", err)
	}
	if cfg.ModeOffsets.FT4 != 0 {
		t.Fatalf("expected explicit mode_offsets.ft4=0 to survive load, got %v", cfg.ModeOffsets.FT4)
	}
}

func TestLoadFileRejectsMissingRequiredYAMLSettings(t *testing.T) {
	cases := []struct {
		name string
		path []string
		want string
	}{
		{name: "enabled", path: []string{"enabled"}, want: "enabled"},
		{name: "display enabled", path: []string{"display_enabled"}, want: "display_enabled"},
		{name: "min observation count", path: []string{"min_observation_count"}, want: "min_observation_count"},
		{name: "receiver contribution mode", path: []string{"receiver_contribution_mode"}, want: "receiver_contribution_mode"},
		{name: "receiver fine slots", path: []string{"receiver_fine_slots"}, want: "receiver_fine_slots"},
		{name: "receiver coarse slots", path: []string{"receiver_coarse_slots"}, want: "receiver_coarse_slots"},
		{name: "receiver max effective count", path: []string{"receiver_max_effective_count"}, want: "receiver_max_effective_count"},
		{name: "receiver max effective weight", path: []string{"receiver_max_effective_weight"}, want: "receiver_max_effective_weight"},
		{name: "ft4 offset", path: []string{"mode_offsets", "ft4"}, want: "mode_offsets.ft4"},
		{name: "noise offsets", path: []string{"noise_offsets"}, want: "noise_offsets"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadFile(writeTempConfigWithoutKey(t, tc.path...))
			if err == nil {
				t.Fatalf("expected missing %s to fail", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error to mention %s, got %v", tc.want, err)
			}
		})
	}
}

func TestLoadFileRejectsRemovedClampKeys(t *testing.T) {
	path := writeTempConfigOverlay(t, `
clamp_min: -25
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected removed clamp_min key to fail")
	}
	if !strings.Contains(err.Error(), "field clamp_min not found") {
		t.Fatalf("expected strict YAML error for clamp_min, got %v", err)
	}
}

func TestLoadFileRejectsNullRequiredYAMLSetting(t *testing.T) {
	path := writeTempConfigOverlay(t, `
mode_offsets:
  ft4:
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected null mode_offsets.ft4 to fail")
	}
	if !strings.Contains(err.Error(), "mode_offsets.ft4") {
		t.Fatalf("expected error to mention mode_offsets.ft4, got %v", err)
	}
}

func TestLoadFileRejectsObsoleteNoiseOffsetsByBand(t *testing.T) {
	path := writeTempConfigOverlay(t, `
noise_offsets_by_band:
  quiet:
    20m: 0
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected obsolete noise_offsets_by_band to fail")
	}
	if !strings.Contains(err.Error(), "noise_offsets_by_band is no longer supported") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadFileRejectsMissingNoiseClass(t *testing.T) {
	path := writeTempConfig(t, `
noise_offsets:
  quiet: 0
  rural: 4
  suburban: 12
  urban: 17
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected missing industrial class to fail")
	}
	if !strings.Contains(err.Error(), "missing required class INDUSTRIAL") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadFileRejectsUnsupportedNoiseClass(t *testing.T) {
	path := writeTempConfigOverlay(t, `
noise_offsets:
  mobile: 9
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected unsupported noise class to fail")
	}
	if !strings.Contains(err.Error(), `unsupported noise class "mobile"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadFileRejectsDuplicateNoiseClass(t *testing.T) {
	path := writeTempConfig(t, `
noise_offsets:
  quiet: 0
  QUIET: 1
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected duplicate noise class to fail")
	}
	if !strings.Contains(err.Error(), `duplicate noise class "QUIET"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadFileRejectsMalformedNoiseOffsets(t *testing.T) {
	path := writeTempConfigOverlay(t, `
noise_offsets:
  quiet:
    20m: 0
`)
	_, err := LoadFile(path)
	if err == nil {
		t.Fatalf("expected malformed noise_offsets to fail")
	}
	if !strings.Contains(err.Error(), "noise_offsets.quiet must be a scalar penalty") {
		t.Fatalf("unexpected error: %v", err)
	}
}
