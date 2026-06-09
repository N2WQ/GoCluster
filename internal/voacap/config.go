package voacap

import (
	"fmt"
	"math"
	"strings"
	"time"

	"dxcluster/internal/yamlconfig"
)

const maxExperimentCenterFrequencies = 10

type ExperimentConfig struct {
	SSNFetchIntervalSeconds  int       `yaml:"ssn_fetch_interval_seconds"`
	SSNRequestTimeoutSeconds int       `yaml:"ssn_request_timeout_seconds"`
	EWMAHalfLifeSeconds      int       `yaml:"ewma_half_life_seconds"`
	RecomputeDeltaPercent    float64   `yaml:"recompute_delta_percent"`
	VOACAPHome               string    `yaml:"voacap_home"`
	VOACAPTimeoutSeconds     int       `yaml:"voacap_timeout_seconds"`
	ForecastHours            int       `yaml:"forecast_hours"`
	CenterFrequenciesMHz     []float64 `yaml:"center_frequencies_mhz"`
	StatePath                string    `yaml:"state_path"`
	OutputNamePrefix         string    `yaml:"output_name_prefix"`
}

var requiredExperimentConfigPaths = []yamlconfig.Path{
	{"ssn_fetch_interval_seconds"},
	{"ssn_request_timeout_seconds"},
	{"ewma_half_life_seconds"},
	{"recompute_delta_percent"},
	{"voacap_home"},
	{"voacap_timeout_seconds"},
	{"forecast_hours"},
	{"center_frequencies_mhz"},
	{"state_path"},
	{"output_name_prefix"},
}

func LoadExperimentConfig(path string) (ExperimentConfig, error) {
	var cfg ExperimentConfig
	if err := yamlconfig.DecodeFile(path, &cfg, requiredExperimentConfigPaths); err != nil {
		return ExperimentConfig{}, err
	}
	if err := cfg.Validate(); err != nil {
		return ExperimentConfig{}, err
	}
	return cfg, nil
}

func (cfg ExperimentConfig) Validate() error {
	if cfg.SSNFetchIntervalSeconds <= 0 {
		return invalidExperimentSetting("ssn_fetch_interval_seconds", "must be > 0")
	}
	if cfg.SSNRequestTimeoutSeconds <= 0 {
		return invalidExperimentSetting("ssn_request_timeout_seconds", "must be > 0")
	}
	if cfg.EWMAHalfLifeSeconds <= 0 {
		return invalidExperimentSetting("ewma_half_life_seconds", "must be > 0")
	}
	if !finitePositive(cfg.RecomputeDeltaPercent) {
		return invalidExperimentSetting("recompute_delta_percent", "must be finite and > 0")
	}
	if strings.TrimSpace(cfg.VOACAPHome) == "" {
		return invalidExperimentSetting("voacap_home", "must not be empty")
	}
	if cfg.VOACAPTimeoutSeconds <= 0 {
		return invalidExperimentSetting("voacap_timeout_seconds", "must be > 0")
	}
	if cfg.ForecastHours <= 0 || cfg.ForecastHours > 24 {
		return invalidExperimentSetting("forecast_hours", "must be between 1 and 24")
	}
	if len(cfg.CenterFrequenciesMHz) == 0 {
		return invalidExperimentSetting("center_frequencies_mhz", "must contain at least one frequency")
	}
	if len(cfg.CenterFrequenciesMHz) > maxExperimentCenterFrequencies {
		return invalidExperimentSetting("center_frequencies_mhz", "must contain at most ten frequencies")
	}
	for i, freq := range cfg.CenterFrequenciesMHz {
		if !finitePositive(freq) {
			return invalidExperimentSetting(fmt.Sprintf("center_frequencies_mhz[%d]", i), "must be finite and > 0")
		}
	}
	if strings.TrimSpace(cfg.StatePath) == "" {
		return invalidExperimentSetting("state_path", "must not be empty")
	}
	if strings.TrimSpace(cfg.OutputNamePrefix) == "" {
		return invalidExperimentSetting("output_name_prefix", "must not be empty")
	}
	if _, err := cleanVOACAPFileName(cfg.OutputNamePrefix + ".out"); err != nil {
		return invalidExperimentSetting("output_name_prefix", err.Error())
	}
	return nil
}

func (cfg ExperimentConfig) SSNFetchInterval() time.Duration {
	return time.Duration(cfg.SSNFetchIntervalSeconds) * time.Second
}

func (cfg ExperimentConfig) SSNRequestTimeout() time.Duration {
	return time.Duration(cfg.SSNRequestTimeoutSeconds) * time.Second
}

func (cfg ExperimentConfig) EWMAHalfLife() time.Duration {
	return time.Duration(cfg.EWMAHalfLifeSeconds) * time.Second
}

func (cfg ExperimentConfig) RecomputeThreshold() float64 {
	return cfg.RecomputeDeltaPercent / 100
}

func (cfg ExperimentConfig) VOACAPTimeout() time.Duration {
	return time.Duration(cfg.VOACAPTimeoutSeconds) * time.Second
}

func invalidExperimentSetting(path string, reason string) error {
	return fmt.Errorf("invalid YAML setting %q: %s", path, reason)
}

func finitePositive(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value > 0
}
