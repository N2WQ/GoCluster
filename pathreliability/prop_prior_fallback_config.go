// File role: Optional-when-omitted YAML configuration, validation, and active
// fallback routing helpers for the pure-Go internal/propprior HF fallback.
// Crawler notes: Start here for prop_prior_fallback YAML keys, HF band gating,
// mutual exclusivity with voacap_fallback, and active-fallback config helpers.
package pathreliability

import (
	"fmt"
	"math"
	"strings"

	"dxcluster/internal/yamlconfig"

	"gopkg.in/yaml.v3"
)

const (
	propPriorMinCacheBucketSeconds = 60
	propPriorMaxCacheBucketSeconds = 3600
	propPriorMinCacheCapacity      = 256
	propPriorMaxCacheCapacity      = 262144
	propPriorMinReqSNRDB           = -40.0
	propPriorMaxReqSNRDB           = 20.0
	propPriorMinSNROffsetDB        = -20.0
	propPriorMaxSNROffsetDB        = 20.0
)

// PropPriorFallbackConfig owns the pure-Go internal/propprior sparse-path HF
// skywave fallback and SHOW PROP outlook settings. Omitting the section in YAML
// keeps it disabled for backward compatibility with existing private configs.
type PropPriorFallbackConfig struct {
	Enabled                                bool      `yaml:"enabled"`
	CacheBucketSeconds                     int       `yaml:"cache_bucket_seconds"`
	CacheCapacity                          int       `yaml:"cache_capacity"`
	ReqSNRDB                               float64   `yaml:"req_snr_db"`
	SNROffsetDB                            float64   `yaml:"snr_offset_db"`
	ForecastHours                          int       `yaml:"forecast_hours"`
	CenterFrequenciesMHz                   []float64 `yaml:"center_frequencies_mhz"`
	ReliabilityGatedOpenEnabled            bool      `yaml:"reliability_gated_open_enabled"`
	ReliabilitySparseUpgradeEnabled        bool      `yaml:"reliability_sparse_upgrade_enabled"`
	ReliabilityMinHigh                     float64   `yaml:"reliability_min_high"`
	ReliabilityMinMedium                   float64   `yaml:"reliability_min_medium"`
	ReliabilityMinLow                      float64   `yaml:"reliability_min_low"`
	ReliabilityMinUnlikely                 float64   `yaml:"reliability_min_unlikely"`
	SparseP50DiagnosticMaxObservationCount int       `yaml:"sparse_p50_diagnostic_max_observation_count"`
}

// DefaultPropPriorFallbackConfig returns the shipped Phase 2B defaults.
func DefaultPropPriorFallbackConfig() PropPriorFallbackConfig {
	return PropPriorFallbackConfig{
		Enabled:                                true,
		CacheBucketSeconds:                     300,
		CacheCapacity:                          16384,
		ReqSNRDB:                               -24.0,
		SNROffsetDB:                            0.0,
		ForecastHours:                          24,
		CenterFrequenciesMHz:                   []float64{3.75, 5.37, 7.15, 10.13, 14.18, 18.12, 21.23, 24.94, 28.85},
		ReliabilityGatedOpenEnabled:            true,
		ReliabilitySparseUpgradeEnabled:        true,
		ReliabilityMinHigh:                     0.70,
		ReliabilityMinMedium:                   0.50,
		ReliabilityMinLow:                      0.35,
		ReliabilityMinUnlikely:                 0.25,
		SparseP50DiagnosticMaxObservationCount: 5,
	}
}

var propPriorFallbackRequiredPaths = []yamlconfig.Path{
	{"prop_prior_fallback", "enabled"},
	{"prop_prior_fallback", "cache_bucket_seconds"},
	{"prop_prior_fallback", "cache_capacity"},
	{"prop_prior_fallback", "req_snr_db"},
	{"prop_prior_fallback", "snr_offset_db"},
	{"prop_prior_fallback", "forecast_hours"},
	{"prop_prior_fallback", "center_frequencies_mhz"},
	{"prop_prior_fallback", "reliability_gated_open_enabled"},
	{"prop_prior_fallback", "reliability_sparse_upgrade_enabled"},
	{"prop_prior_fallback", "reliability_min_high"},
	{"prop_prior_fallback", "reliability_min_medium"},
	{"prop_prior_fallback", "reliability_min_low"},
	{"prop_prior_fallback", "reliability_min_unlikely"},
	{"prop_prior_fallback", "sparse_p50_diagnostic_max_observation_count"},
}

func propPriorFallbackPresenceDiagnostics(source string, bs []byte) ([]yamlconfig.Diagnostic, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(bs, &root); err != nil {
		return nil, err
	}
	if len(root.Content) == 0 || root.Content[0].Kind != yaml.MappingNode {
		return nil, nil
	}
	doc := root.Content[0]
	var section *yaml.Node
	for i := 0; i+1 < len(doc.Content); i += 2 {
		if strings.TrimSpace(doc.Content[i].Value) == "prop_prior_fallback" {
			section = doc.Content[i+1]
			break
		}
	}
	if section == nil {
		// Section omitted: backward-compatible disabled state.
		return nil, nil
	}
	if section.Kind == yaml.ScalarNode && section.Tag == "!!null" {
		return []yamlconfig.Diagnostic{{
			Severity: yamlconfig.DiagnosticError,
			Source:   source,
			Path:     "prop_prior_fallback",
			Message:  `required YAML setting "prop_prior_fallback" must not be null`,
		}}, nil
	}
	if section.Kind != yaml.MappingNode {
		return []yamlconfig.Diagnostic{{
			Severity: yamlconfig.DiagnosticError,
			Source:   source,
			Path:     "prop_prior_fallback",
			Message:  `invalid YAML setting "prop_prior_fallback": must be a mapping`,
		}}, nil
	}
	return yamlconfig.RequiredPathDiagnostics(source, bs, propPriorFallbackRequiredPaths)
}

func (c *PropPriorFallbackConfig) finalize() error {
	if c == nil {
		return nil
	}
	if c.CacheBucketSeconds < propPriorMinCacheBucketSeconds || c.CacheBucketSeconds > propPriorMaxCacheBucketSeconds {
		return fmt.Errorf("prop_prior_fallback.cache_bucket_seconds must be between %d and %d", propPriorMinCacheBucketSeconds, propPriorMaxCacheBucketSeconds)
	}
	if c.CacheCapacity < propPriorMinCacheCapacity || c.CacheCapacity > propPriorMaxCacheCapacity || c.CacheCapacity&(c.CacheCapacity-1) != 0 {
		return fmt.Errorf("prop_prior_fallback.cache_capacity must be a power of two between %d and %d", propPriorMinCacheCapacity, propPriorMaxCacheCapacity)
	}
	if math.IsNaN(c.ReqSNRDB) || math.IsInf(c.ReqSNRDB, 0) || c.ReqSNRDB < propPriorMinReqSNRDB || c.ReqSNRDB > propPriorMaxReqSNRDB {
		return fmt.Errorf("prop_prior_fallback.req_snr_db must be between %.0f and %.0f", propPriorMinReqSNRDB, propPriorMaxReqSNRDB)
	}
	if math.IsNaN(c.SNROffsetDB) || math.IsInf(c.SNROffsetDB, 0) || c.SNROffsetDB < propPriorMinSNROffsetDB || c.SNROffsetDB > propPriorMaxSNROffsetDB {
		return fmt.Errorf("prop_prior_fallback.snr_offset_db must be between %.0f and %.0f", propPriorMinSNROffsetDB, propPriorMaxSNROffsetDB)
	}
	if c.ForecastHours <= 0 || c.ForecastHours > 24 {
		return fmt.Errorf("prop_prior_fallback.forecast_hours must be between 1 and 24")
	}
	if len(c.CenterFrequenciesMHz) == 0 {
		return fmt.Errorf("prop_prior_fallback.center_frequencies_mhz must contain at least one HF band frequency")
	}
	if len(c.CenterFrequenciesMHz) > propPriorBandCount {
		return fmt.Errorf("prop_prior_fallback.center_frequencies_mhz must contain at most %d HF band frequencies", propPriorBandCount)
	}
	var seen [propPriorBandCount]bool
	for i, freq := range c.CenterFrequenciesMHz {
		if math.IsNaN(freq) || math.IsInf(freq, 0) || freq <= 0 {
			return fmt.Errorf("prop_prior_fallback.center_frequencies_mhz[%d] must be finite and > 0", i)
		}
		band := bandForMHz(freq)
		idx, ok := propPriorBandIndex(band)
		if !ok {
			return fmt.Errorf("prop_prior_fallback.center_frequencies_mhz[%d]=%.3f maps to unsupported band %q (allowed: 80m-10m HF skywave)", i, freq, band)
		}
		if seen[idx] {
			return fmt.Errorf("prop_prior_fallback.center_frequencies_mhz[%d]=%.3f duplicates band %s", i, freq, band)
		}
		seen[idx] = true
	}
	if err := validateReliabilityThreshold("prop_prior_fallback.reliability_min_high", c.ReliabilityMinHigh); err != nil {
		return err
	}
	if err := validateReliabilityThreshold("prop_prior_fallback.reliability_min_medium", c.ReliabilityMinMedium); err != nil {
		return err
	}
	if err := validateReliabilityThreshold("prop_prior_fallback.reliability_min_low", c.ReliabilityMinLow); err != nil {
		return err
	}
	if err := validateReliabilityThreshold("prop_prior_fallback.reliability_min_unlikely", c.ReliabilityMinUnlikely); err != nil {
		return err
	}
	if !(c.ReliabilityMinHigh >= c.ReliabilityMinMedium &&
		c.ReliabilityMinMedium >= c.ReliabilityMinLow &&
		c.ReliabilityMinLow >= c.ReliabilityMinUnlikely) {
		return fmt.Errorf("prop_prior_fallback reliability thresholds must satisfy high >= medium >= low >= unlikely")
	}
	if c.SparseP50DiagnosticMaxObservationCount < 0 {
		return fmt.Errorf("prop_prior_fallback.sparse_p50_diagnostic_max_observation_count must be >= 0")
	}
	return nil
}

// PropPriorFallbackActive reports whether prop_prior_fallback is configured and enabled.
func (c Config) PropPriorFallbackActive() bool {
	return c.PropPriorFallback != nil && c.PropPriorFallback.Enabled
}

// FallbackReliabilityGatedOpenEnabled returns the active fallback engine's
// reliability-gated open toggle.
func (c Config) FallbackReliabilityGatedOpenEnabled() bool {
	if c.PropPriorFallbackActive() {
		return c.PropPriorFallback.ReliabilityGatedOpenEnabled
	}
	return c.VOACAPFallback.ReliabilityGatedOpenEnabled
}

// FallbackReliabilitySparseUpgradeEnabled returns the active fallback engine's
// one-tier sparse-upgrade toggle.
func (c Config) FallbackReliabilitySparseUpgradeEnabled() bool {
	if c.PropPriorFallbackActive() {
		return c.PropPriorFallback.ReliabilitySparseUpgradeEnabled
	}
	return c.VOACAPFallback.ReliabilitySparseUpgradeEnabled
}

// FallbackReliabilityThresholdForClass returns the active fallback engine's
// minimum REL gate for an ordinary open class.
func (c Config) FallbackReliabilityThresholdForClass(class string) (float64, bool) {
	if c.PropPriorFallbackActive() {
		switch class {
		case classHigh:
			return c.PropPriorFallback.ReliabilityMinHigh, true
		case classMedium:
			return c.PropPriorFallback.ReliabilityMinMedium, true
		case classLow:
			return c.PropPriorFallback.ReliabilityMinLow, true
		case classUnlikely:
			return c.PropPriorFallback.ReliabilityMinUnlikely, true
		default:
			return 0, false
		}
	}
	switch class {
	case classHigh:
		return c.VOACAPFallback.ReliabilityMinHigh, true
	case classMedium:
		return c.VOACAPFallback.ReliabilityMinMedium, true
	case classLow:
		return c.VOACAPFallback.ReliabilityMinLow, true
	case classUnlikely:
		return c.VOACAPFallback.ReliabilityMinUnlikely, true
	default:
		return 0, false
	}
}

// FallbackSparseP50DiagnosticMaxObservationCount returns the active fallback
// engine's sparse-p50 trace threshold.
func (c Config) FallbackSparseP50DiagnosticMaxObservationCount() int {
	if c.PropPriorFallbackActive() {
		return c.PropPriorFallback.SparseP50DiagnosticMaxObservationCount
	}
	return c.VOACAPFallback.SparseP50DiagnosticMaxObservationCount
}

// FallbackForecastHours returns the active fallback engine's SHOW PROP horizon.
func (c Config) FallbackForecastHours() int {
	if c.PropPriorFallbackActive() {
		return c.PropPriorFallback.ForecastHours
	}
	return c.VOACAPFallback.ForecastHours
}

// FallbackBands returns the ordered band list for the active fallback engine.
func (c Config) FallbackBands() []string {
	if c.PropPriorFallbackActive() {
		return PropPriorFallbackBands(*c.PropPriorFallback)
	}
	return VOACAPFallbackBands(c.VOACAPFallback)
}

// FallbackCenterFrequencyMHz returns the representative center frequency for
// band in the active fallback engine.
func (c Config) FallbackCenterFrequencyMHz(band string) (float64, bool) {
	if c.PropPriorFallbackActive() {
		return PropPriorFallbackCenterFrequencyMHz(*c.PropPriorFallback, band)
	}
	return VOACAPFallbackCenterFrequencyMHz(c.VOACAPFallback, band)
}

// PropPriorFallbackBands returns the ordered supported bands from cfg.
func PropPriorFallbackBands(cfg PropPriorFallbackConfig) []string {
	var seen [propPriorBandCount]bool
	bands := make([]string, 0, len(cfg.CenterFrequenciesMHz))
	for _, freq := range cfg.CenterFrequenciesMHz {
		band := bandForMHz(freq)
		idx, ok := propPriorBandIndex(band)
		if !ok || seen[idx] {
			continue
		}
		seen[idx] = true
		bands = append(bands, band)
	}
	return bands
}

// PropPriorFallbackCenterFrequencyMHz resolves band to its configured MHz.
func PropPriorFallbackCenterFrequencyMHz(cfg PropPriorFallbackConfig, band string) (float64, bool) {
	idx, ok := propPriorBandIndex(band)
	if !ok {
		return 0, false
	}
	for _, freq := range cfg.CenterFrequenciesMHz {
		if bIdx, valid := propPriorBandIndex(bandForMHz(freq)); valid && bIdx == idx {
			return freq, true
		}
	}
	return 0, false
}
