package voacap

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadExperimentConfigFromShippedYAML(t *testing.T) {
	cfg, err := LoadExperimentConfig(filepath.Join("..", "..", "data", "config", "voacap_experiment.yaml"))
	if err != nil {
		t.Fatalf("LoadExperimentConfig() error: %v", err)
	}
	if cfg.SSNFetchIntervalSeconds != 1800 {
		t.Fatalf("SSNFetchIntervalSeconds = %d, want 1800", cfg.SSNFetchIntervalSeconds)
	}
	if cfg.EWMAHalfLifeSeconds != 28800 {
		t.Fatalf("EWMAHalfLifeSeconds = %d, want 28800", cfg.EWMAHalfLifeSeconds)
	}
	if cfg.RecomputeDeltaPercent != 12 {
		t.Fatalf("RecomputeDeltaPercent = %v, want 12", cfg.RecomputeDeltaPercent)
	}
	if len(cfg.CenterFrequenciesMHz) != 10 {
		t.Fatalf("CenterFrequenciesMHz len = %d, want 10", len(cfg.CenterFrequenciesMHz))
	}
}

func TestLoadExperimentConfigRequiresEveryKey(t *testing.T) {
	path := writeExperimentConfig(t, strings.Replace(validExperimentConfigYAML(), "state_path: \".tmp/voacap-ssn-forecast-state.json\"\n", "", 1))
	_, err := LoadExperimentConfig(path)
	if err == nil || !strings.Contains(err.Error(), `required YAML setting "state_path" is missing`) {
		t.Fatalf("expected missing state_path error, got %v", err)
	}
}

func TestLoadExperimentConfigRejectsUnknownKey(t *testing.T) {
	path := writeExperimentConfig(t, validExperimentConfigYAML()+"mystery: true\n")
	_, err := LoadExperimentConfig(path)
	if err == nil || !strings.Contains(err.Error(), "field mystery not found") {
		t.Fatalf("expected unknown key error, got %v", err)
	}
}

func TestExperimentConfigValidateRejectsInvalidValues(t *testing.T) {
	cfg := validExperimentConfig()
	cfg.CenterFrequenciesMHz = nil
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "center_frequencies_mhz") {
		t.Fatalf("expected empty frequency error, got %v", err)
	}

	cfg = validExperimentConfig()
	cfg.RecomputeDeltaPercent = 0
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "recompute_delta_percent") {
		t.Fatalf("expected zero delta error, got %v", err)
	}

	cfg = validExperimentConfig()
	cfg.OutputNamePrefix = "../bad"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "output_name_prefix") {
		t.Fatalf("expected unsafe prefix error, got %v", err)
	}
}

func writeExperimentConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "voacap_experiment.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write experiment config: %v", err)
	}
	return path
}

func validExperimentConfig() ExperimentConfig {
	return ExperimentConfig{
		SSNFetchIntervalSeconds:  1800,
		SSNRequestTimeoutSeconds: 30,
		EWMAHalfLifeSeconds:      28800,
		RecomputeDeltaPercent:    12,
		VOACAPHome:               `C:\itshfbc`,
		VOACAPTimeoutSeconds:     30,
		ForecastHours:            8,
		CenterFrequenciesMHz:     []float64{3.57, 7.04, 10.14, 14.07, 18.10, 21.07, 24.91, 28.07},
		StatePath:                filepath.Join(".tmp", "voacap-ssn-forecast-state.json"),
		OutputNamePrefix:         "gocluster_voacap_forecast",
	}
}

func validExperimentConfigYAML() string {
	return `ssn_fetch_interval_seconds: 1800
ssn_request_timeout_seconds: 30
ewma_half_life_seconds: 28800
recompute_delta_percent: 12
voacap_home: "C:\\itshfbc"
voacap_timeout_seconds: 30
forecast_hours: 8
center_frequencies_mhz: [3.57, 7.04, 10.14, 14.07, 18.10, 21.07, 24.91, 28.07]
state_path: ".tmp/voacap-ssn-forecast-state.json"
output_name_prefix: "gocluster_voacap_forecast"
`
}
