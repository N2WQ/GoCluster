// File role: Owns path reliability configuration loading, defaults, and validation.
// Crawler notes: Start here for YAML keys, operator tuning boundaries, required
// config contracts, receiver-cap mode, and derived noise models used by the
// predictor.
// Related docs: pathreliability/README.md, data/config/path_reliability.yaml.
// Related tests: pathreliability/config_test.go.
package pathreliability

import (
	"dxcluster/internal/yamlconfig"
	"dxcluster/strutil"
	"fmt"
	"math"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config holds tuning knobs for path reliability aggregation and display.
type Config struct {
	Enabled                            bool                       `yaml:"enabled"`
	AllowedBands                       []string                   `yaml:"allowed_bands"`
	DefaultHalfLifeSec                 int                        `yaml:"default_half_life_seconds"`               // fallback half-life when band not listed
	BandHalfLifeSec                    map[string]int             `yaml:"band_half_life_seconds"`                  // per-band overrides
	StaleAfterSeconds                  int                        `yaml:"stale_after_seconds"`                     // fallback purge when older than this
	StaleAfterHalfLifeMultiplier       float64                    `yaml:"stale_after_half_life_multiplier"`        // stale = k * half-life (per band)
	MaxPredictionAgeHalfLifeMultiplier float64                    `yaml:"max_prediction_age_half_life_multiplier"` // prediction age gate = k * half-life; 0 disables
	MinEffectiveWeight                 float64                    `yaml:"min_effective_weight"`                    // minimum decayed weight to report
	MinObservationCount                int                        `yaml:"min_observation_count"`                   // minimum raw selected observations to report
	ReceiverContributionMode           string                     `yaml:"receiver_contribution_mode"`              // off, shadow, or enforce receiver contribution caps
	ReceiverFineSlots                  int                        `yaml:"receiver_fine_slots"`                     // tracked receiver slots in fine buckets
	ReceiverCoarseSlots                int                        `yaml:"receiver_coarse_slots"`                   // tracked receiver slots in coarse buckets
	ReceiverMaxEffectiveCount          uint32                     `yaml:"receiver_max_effective_count"`            // max accepted reports per receiver per bucket
	ReceiverMaxEffectiveWeight         float64                    `yaml:"receiver_max_effective_weight"`           // max accepted effective weight per receiver per bucket
	MinFineWeight                      float64                    `yaml:"min_fine_weight"`                         // minimum fine weight to blend with coarse
	FineOnlyWeight                     float64                    `yaml:"fine_only_weight"`                        // minimum fine weight to use fine only
	ReverseHintDiscount                float64                    `yaml:"reverse_hint_discount"`                   // multiplier when using reverse direction
	MergeReceiveWeight                 float64                    `yaml:"merge_receive_weight"`                    // merge weight for DX->user
	MergeTransmitWeight                float64                    `yaml:"merge_transmit_weight"`                   // merge weight for user->DX
	BeaconWeightCap                    float64                    `yaml:"beacon_weight_cap"`                       // cap per-beacon contribution
	DisplayEnabled                     bool                       `yaml:"display_enabled"`                         // toggle glyph rendering
	ModeOffsets                        ModeOffsets                `yaml:"mode_offsets"`                            // per-mode FT8-equiv offsets
	ModeThresholds                     map[string]GlyphThresholds `yaml:"mode_thresholds"`                         // per-mode glyph thresholds in FT8-equiv dB
	GlyphThresholds                    GlyphThresholds            `yaml:"glyph_thresholds"`                        // fallback glyph thresholds in FT8-equiv dB
	GlyphSymbols                       GlyphSymbols               `yaml:"glyph_symbols"`                           // glyph mapping for high/medium/low/unlikely/insufficient/closed
	VOACAPFallback                     VOACAPFallbackConfig       `yaml:"voacap_fallback"`                         // optional VOACAP sparse-data fallback
	NoiseOffsets                       map[string]float64         `yaml:"noise_offsets"`                           // noise class -> dB penalty

	noiseModel NoiseModel
}

var requiredConfigPaths = []yamlconfig.Path{
	{"enabled"},
	{"allowed_bands"},
	{"glyph_symbols", "high"},
	{"glyph_symbols", "medium"},
	{"glyph_symbols", "low"},
	{"glyph_symbols", "unlikely"},
	{"glyph_symbols", "insufficient"},
	{"glyph_symbols", "closed"},
	{"voacap_fallback", "enabled"},
	{"voacap_fallback", "ssn_fetch_interval_seconds"},
	{"voacap_fallback", "ssn_request_timeout_seconds"},
	{"voacap_fallback", "ssn_ewma_half_life_seconds"},
	{"voacap_fallback", "recompute_delta_percent"},
	{"voacap_fallback", "voacap_home"},
	{"voacap_fallback", "voacap_timeout_seconds"},
	{"voacap_fallback", "forecast_hours"},
	{"voacap_fallback", "show_prop_wait_milliseconds"},
	{"voacap_fallback", "center_frequencies_mhz"},
	{"voacap_fallback", "delay_seconds"},
	{"voacap_fallback", "cache_ttl_seconds"},
	{"voacap_fallback", "max_cache_entries"},
	{"voacap_fallback", "max_delay_entries"},
	{"voacap_fallback", "max_queue_depth"},
	{"voacap_fallback", "worker_count"},
	{"voacap_fallback", "output_name_prefix"},
	{"default_half_life_seconds"},
	{"band_half_life_seconds"},
	{"stale_after_seconds"},
	{"stale_after_half_life_multiplier"},
	{"max_prediction_age_half_life_multiplier"},
	{"min_effective_weight"},
	{"min_observation_count"},
	{"receiver_contribution_mode"},
	{"receiver_fine_slots"},
	{"receiver_coarse_slots"},
	{"receiver_max_effective_count"},
	{"receiver_max_effective_weight"},
	{"min_fine_weight"},
	{"fine_only_weight"},
	{"reverse_hint_discount"},
	{"merge_receive_weight"},
	{"merge_transmit_weight"},
	{"mode_thresholds"},
	{"glyph_thresholds", "high"},
	{"glyph_thresholds", "medium"},
	{"glyph_thresholds", "low"},
	{"glyph_thresholds", "unlikely"},
	{"glyph_thresholds", "closed"},
	{"beacon_weight_cap"},
	{"display_enabled"},
	{"mode_offsets", "ft4"},
	{"mode_offsets", "cw"},
	{"mode_offsets", "rtty"},
	{"mode_offsets", "psk"},
	{"mode_offsets", "wspr"},
	{"noise_offsets"},
}

// ModeOffsets normalizes non-FT8 modes to FT8-equivalent dB.
type ModeOffsets struct {
	FT4  float64 `yaml:"ft4"`
	CW   float64 `yaml:"cw"`
	RTTY float64 `yaml:"rtty"`
	PSK  float64 `yaml:"psk"`
	WSPR float64 `yaml:"wspr"`
}

const (
	ReceiverContributionOff     = "off"
	ReceiverContributionShadow  = "shadow"
	ReceiverContributionEnforce = "enforce"

	maxFineReceiverSlots   = 6
	maxCoarseReceiverSlots = 12
)

// GlyphThresholds defines FT8-equiv dB cutoffs for glyphs.
type GlyphThresholds struct {
	High     float64 `yaml:"high"`     // >= High
	Medium   float64 `yaml:"medium"`   // >= Medium
	Low      float64 `yaml:"low"`      // >= Low
	Unlikely float64 `yaml:"unlikely"` // >= Unlikely (still "unlikely" below)
	Closed   float64 `yaml:"closed"`   // <= Closed for closed-band fallback

	hasHigh     bool
	hasMedium   bool
	hasLow      bool
	hasUnlikely bool
	hasClosed   bool
}

// UnmarshalYAML accepts the current high/medium/low/unlikely/closed keys. Removed
// legacy threshold names are rejected before permissive decode so ordinary
// extra keys can still be reported as warning-only startup diagnostics.
func (t *GlyphThresholds) UnmarshalYAML(value *yaml.Node) error {
	if t == nil {
		return nil
	}
	if value.Kind == yaml.ScalarNode && value.Tag == "!!null" {
		return nil
	}
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("glyph thresholds must be a mapping")
	}
	*t = GlyphThresholds{}
	raw := make(map[string]float64, len(value.Content)/2)
	if err := value.Decode(&raw); err != nil {
		return err
	}
	for k, v := range raw {
		key := strings.ToLower(strings.TrimSpace(k))
		switch key {
		case "high":
			t.High = v
			t.hasHigh = true
		case "medium":
			t.Medium = v
			t.hasMedium = true
		case "low":
			t.Low = v
			t.hasLow = true
		case "unlikely":
			t.Unlikely = v
			t.hasUnlikely = true
		case "closed":
			t.Closed = v
			t.hasClosed = true
		default:
			continue
		}
	}
	return nil
}

// GlyphSymbols defines the glyph characters for each reliability class.
type GlyphSymbols struct {
	High         string `yaml:"high"`
	Medium       string `yaml:"medium"`
	Low          string `yaml:"low"`
	Unlikely     string `yaml:"unlikely"`
	Insufficient string `yaml:"insufficient"`
	Closed       string `yaml:"closed"`
}

// UnmarshalYAML enforces single printable ASCII glyphs. Unknown keys are
// ignored here after the loader has reported warning-only startup diagnostics.
func (s *GlyphSymbols) UnmarshalYAML(value *yaml.Node) error {
	if s == nil {
		return nil
	}
	if value.Kind == yaml.ScalarNode && value.Tag == "!!null" {
		return nil
	}
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("glyph_symbols must be a mapping")
	}
	*s = GlyphSymbols{}
	raw := make(map[string]string, len(value.Content)/2)
	if err := value.Decode(&raw); err != nil {
		return err
	}
	for k, v := range raw {
		key := strings.ToLower(strings.TrimSpace(k))
		if err := validateGlyphSymbol(v); err != nil {
			return fmt.Errorf("glyph_symbols.%s: %w", key, err)
		}
		switch key {
		case "high":
			s.High = v
		case "medium":
			s.Medium = v
		case "low":
			s.Low = v
		case "unlikely":
			s.Unlikely = v
		case "insufficient":
			s.Insufficient = v
		case "closed":
			s.Closed = v
		default:
			continue
		}
	}
	return nil
}

// VOACAPFallbackConfig owns the optional fallback from insufficient bucket
// evidence to cached VOACAP FT8-equivalent SNR forecasts.
// Bounds are required even when disabled so enabling the fallback cannot create
// process-lifetime unbounded queues or cache maps.
type VOACAPFallbackConfig struct {
	Enabled                  bool      `yaml:"enabled"`
	SSNFetchIntervalSeconds  int       `yaml:"ssn_fetch_interval_seconds"`
	SSNRequestTimeoutSeconds int       `yaml:"ssn_request_timeout_seconds"`
	SSNEWMAHalfLifeSeconds   int       `yaml:"ssn_ewma_half_life_seconds"`
	RecomputeDeltaPercent    float64   `yaml:"recompute_delta_percent"`
	VOACAPHome               string    `yaml:"voacap_home"`
	VOACAPTimeoutSeconds     int       `yaml:"voacap_timeout_seconds"`
	ForecastHours            int       `yaml:"forecast_hours"`
	ShowPropWaitMilliseconds int       `yaml:"show_prop_wait_milliseconds"`
	CenterFrequenciesMHz     []float64 `yaml:"center_frequencies_mhz"`
	DelaySeconds             int       `yaml:"delay_seconds"`
	CacheTTLSeconds          int       `yaml:"cache_ttl_seconds"`
	MaxCacheEntries          int       `yaml:"max_cache_entries"`
	MaxDelayEntries          int       `yaml:"max_delay_entries"`
	MaxQueueDepth            int       `yaml:"max_queue_depth"`
	WorkerCount              int       `yaml:"worker_count"`
	OutputNamePrefix         string    `yaml:"output_name_prefix"`
}

// DefaultConfig returns a safe, enabled configuration.
func DefaultConfig() Config {
	cfg := Config{
		Enabled:                            true,
		AllowedBands:                       []string{"160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m", "6m"},
		DefaultHalfLifeSec:                 300,
		BandHalfLifeSec:                    map[string]int{},
		StaleAfterSeconds:                  1800,
		StaleAfterHalfLifeMultiplier:       5,
		MaxPredictionAgeHalfLifeMultiplier: 1.25,
		MinEffectiveWeight:                 1.0,
		MinObservationCount:                19,
		ReceiverContributionMode:           ReceiverContributionShadow,
		ReceiverFineSlots:                  6,
		ReceiverCoarseSlots:                12,
		ReceiverMaxEffectiveCount:          5,
		ReceiverMaxEffectiveWeight:         5.0,
		MinFineWeight:                      5.0,
		FineOnlyWeight:                     20.0,
		ReverseHintDiscount:                0.5,
		MergeReceiveWeight:                 0.6,
		MergeTransmitWeight:                0.4,
		BeaconWeightCap:                    1.0,
		DisplayEnabled:                     true,
		ModeOffsets: ModeOffsets{
			FT4:  -3,
			CW:   -7,
			RTTY: -7,
			PSK:  -7,
			WSPR: 26,
		},
		ModeThresholds: map[string]GlyphThresholds{
			"FT8":  {High: -13, Medium: -17, Low: -21, Unlikely: -24, Closed: -29, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true, hasClosed: true},
			"FT4":  {High: -5, Medium: -10, Low: -14, Unlikely: -17, Closed: -22, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true, hasClosed: true},
			"CW":   {High: -1, Medium: -6, Low: -10, Unlikely: -14, Closed: -19, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true, hasClosed: true},
			"RTTY": {High: -1, Medium: -6, Low: -10, Unlikely: -14, Closed: -19, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true, hasClosed: true},
			"PSK":  {High: 5, Medium: 0, Low: -4, Unlikely: -7, Closed: -12, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true, hasClosed: true},
			"USB":  {High: 10, Medium: 5, Low: 0, Unlikely: -5, Closed: -10, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true, hasClosed: true},
			"LSB":  {High: 10, Medium: 5, Low: 0, Unlikely: -5, Closed: -10, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true, hasClosed: true},
		},
		GlyphThresholds: GlyphThresholds{
			High:        -13,
			Medium:      -17,
			Low:         -21,
			Unlikely:    -24,
			Closed:      -29,
			hasHigh:     true,
			hasMedium:   true,
			hasLow:      true,
			hasUnlikely: true,
			hasClosed:   true,
		},
		GlyphSymbols: GlyphSymbols{
			High:         "+",
			Medium:       "=",
			Low:          "-",
			Unlikely:     "!",
			Insufficient: "?",
			Closed:       "!",
		},
		VOACAPFallback: defaultVOACAPFallbackConfig(),
		NoiseOffsets:   defaultNoiseOffsets(),
	}
	cfg.buildCaches()
	return cfg
}

// normalize validates relationship constraints and rebuilds derived caches.
func (c *Config) normalize() {
	if err := c.finalize(); err != nil {
		return
	}
}

func (c *Config) finalize() error {
	if c == nil {
		return nil
	}
	if len(c.AllowedBands) == 0 {
		return fmt.Errorf("allowed_bands must not be empty")
	}
	if c.DefaultHalfLifeSec <= 0 {
		return fmt.Errorf("default_half_life_seconds must be > 0")
	}
	if c.StaleAfterSeconds <= 0 {
		return fmt.Errorf("stale_after_seconds must be > 0")
	}
	if c.StaleAfterHalfLifeMultiplier <= 0 {
		return fmt.Errorf("stale_after_half_life_multiplier must be > 0")
	}
	if c.MaxPredictionAgeHalfLifeMultiplier < 0 {
		return fmt.Errorf("max_prediction_age_half_life_multiplier must be >= 0")
	}
	if c.MinEffectiveWeight <= 0 {
		return fmt.Errorf("min_effective_weight must be > 0")
	}
	if c.MinObservationCount <= 0 {
		return fmt.Errorf("min_observation_count must be > 0")
	}
	mode := strings.ToLower(strings.TrimSpace(c.ReceiverContributionMode))
	switch mode {
	case ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce:
		c.ReceiverContributionMode = mode
	default:
		return fmt.Errorf("receiver_contribution_mode must be one of off, shadow, or enforce")
	}
	if c.ReceiverFineSlots <= 0 || c.ReceiverFineSlots > maxFineReceiverSlots {
		return fmt.Errorf("receiver_fine_slots must be > 0 and <= %d", maxFineReceiverSlots)
	}
	if c.ReceiverCoarseSlots <= 0 || c.ReceiverCoarseSlots > maxCoarseReceiverSlots {
		return fmt.Errorf("receiver_coarse_slots must be > 0 and <= %d", maxCoarseReceiverSlots)
	}
	if c.ReceiverMaxEffectiveCount == 0 {
		return fmt.Errorf("receiver_max_effective_count must be > 0")
	}
	if c.ReceiverMaxEffectiveWeight <= 0 {
		return fmt.Errorf("receiver_max_effective_weight must be > 0")
	}
	if c.MinFineWeight <= 0 {
		return fmt.Errorf("min_fine_weight must be > 0")
	}
	if c.FineOnlyWeight <= 0 {
		return fmt.Errorf("fine_only_weight must be > 0")
	}
	if c.FineOnlyWeight < c.MinFineWeight {
		c.FineOnlyWeight = c.MinFineWeight
	}
	if c.ReverseHintDiscount <= 0 || c.ReverseHintDiscount > 1 {
		return fmt.Errorf("reverse_hint_discount must be > 0 and <= 1")
	}
	if c.MergeReceiveWeight <= 0 {
		return fmt.Errorf("merge_receive_weight must be > 0")
	}
	if c.MergeTransmitWeight <= 0 {
		return fmt.Errorf("merge_transmit_weight must be > 0")
	}
	sum := c.MergeReceiveWeight + c.MergeTransmitWeight
	if sum <= 0 {
		return fmt.Errorf("merge weights must sum above 0")
	}
	if sum != 1 {
		c.MergeReceiveWeight /= sum
		c.MergeTransmitWeight /= sum
	}
	if c.BeaconWeightCap <= 0 {
		return fmt.Errorf("beacon_weight_cap must be > 0")
	}
	if len(c.BandHalfLifeSec) > 0 {
		normalizedHalfLife := make(map[string]int, len(c.BandHalfLifeSec))
		for band, halfLife := range c.BandHalfLifeSec {
			key := normalizeBand(band)
			if key == "" {
				return fmt.Errorf("band_half_life_seconds contains empty band")
			}
			if halfLife < 0 {
				return fmt.Errorf("band_half_life_seconds.%s must be >= 0", band)
			}
			normalizedHalfLife[key] = halfLife
		}
		c.BandHalfLifeSec = normalizedHalfLife
	}
	if len(c.ModeThresholds) == 0 {
		return fmt.Errorf("mode_thresholds must not be empty")
	}
	normalizedThresholds := make(map[string]GlyphThresholds, len(c.ModeThresholds))
	for k, v := range c.ModeThresholds {
		key := strutil.NormalizeUpper(k)
		if key == "" {
			return fmt.Errorf("mode_thresholds contains empty mode")
		}
		if !completeGlyphThresholds(v) {
			return fmt.Errorf("mode_thresholds.%s must define valid high/medium/low/unlikely/closed thresholds", k)
		}
		normalizedThresholds[key] = v
	}
	c.ModeThresholds = normalizedThresholds
	for _, mode := range []string{"FT8", "FT4", "CW", "RTTY", "PSK", "USB", "LSB"} {
		if _, ok := c.ModeThresholds[mode]; !ok {
			return fmt.Errorf("mode_thresholds missing required mode %s", mode)
		}
	}
	if !completeGlyphThresholds(c.GlyphThresholds) {
		return fmt.Errorf("glyph_thresholds must define valid high/medium/low/unlikely/closed thresholds")
	}
	if c.GlyphSymbols.High == "" ||
		c.GlyphSymbols.Medium == "" ||
		c.GlyphSymbols.Low == "" ||
		c.GlyphSymbols.Unlikely == "" ||
		c.GlyphSymbols.Insufficient == "" ||
		c.GlyphSymbols.Closed == "" {
		return fmt.Errorf("glyph_symbols must define high, medium, low, unlikely, insufficient, and closed")
	}
	if err := c.VOACAPFallback.finalize(); err != nil {
		return err
	}
	if c.VOACAPFallback.Enabled && !c.Enabled {
		return fmt.Errorf("voacap_fallback.enabled requires path reliability enabled")
	}
	if err := validateNoiseOffsets(c.NoiseOffsets); err != nil {
		return err
	}
	c.NoiseOffsets = normalizeNoiseOffsets(c.NoiseOffsets, nil)
	c.buildCaches()
	return nil
}

func defaultVOACAPFallbackConfig() VOACAPFallbackConfig {
	return VOACAPFallbackConfig{
		Enabled:                  false,
		SSNFetchIntervalSeconds:  1800,
		SSNRequestTimeoutSeconds: 30,
		SSNEWMAHalfLifeSeconds:   28800,
		RecomputeDeltaPercent:    12,
		VOACAPHome:               `C:\itshfbc`,
		VOACAPTimeoutSeconds:     30,
		ForecastHours:            8,
		ShowPropWaitMilliseconds: 750,
		CenterFrequenciesMHz:     []float64{1.9, 3.6, 5.36, 7.1, 10.14, 14.1, 18.1, 21.15, 24.91, 28.1},
		DelaySeconds:             900,
		CacheTTLSeconds:          28800,
		MaxCacheEntries:          50000,
		MaxDelayEntries:          50000,
		MaxQueueDepth:            1000,
		WorkerCount:              1,
		OutputNamePrefix:         "gocluster_voacap_path",
	}
}

func (c *VOACAPFallbackConfig) finalize() error {
	if c == nil {
		return nil
	}
	if c.SSNFetchIntervalSeconds <= 0 {
		return fmt.Errorf("voacap_fallback.ssn_fetch_interval_seconds must be > 0")
	}
	if c.SSNRequestTimeoutSeconds <= 0 {
		return fmt.Errorf("voacap_fallback.ssn_request_timeout_seconds must be > 0")
	}
	if c.SSNEWMAHalfLifeSeconds <= 0 {
		return fmt.Errorf("voacap_fallback.ssn_ewma_half_life_seconds must be > 0")
	}
	if math.IsNaN(c.RecomputeDeltaPercent) || math.IsInf(c.RecomputeDeltaPercent, 0) || c.RecomputeDeltaPercent <= 0 {
		return fmt.Errorf("voacap_fallback.recompute_delta_percent must be > 0")
	}
	if c.Enabled && strings.TrimSpace(c.VOACAPHome) == "" {
		return fmt.Errorf("voacap_fallback.voacap_home must not be empty when enabled")
	}
	if c.VOACAPTimeoutSeconds <= 0 {
		return fmt.Errorf("voacap_fallback.voacap_timeout_seconds must be > 0")
	}
	if c.ForecastHours <= 0 || c.ForecastHours > 24 {
		return fmt.Errorf("voacap_fallback.forecast_hours must be between 1 and 24")
	}
	if c.ShowPropWaitMilliseconds < 0 || c.ShowPropWaitMilliseconds > 2000 {
		return fmt.Errorf("voacap_fallback.show_prop_wait_milliseconds must be between 0 and 2000")
	}
	if len(c.CenterFrequenciesMHz) == 0 {
		return fmt.Errorf("voacap_fallback.center_frequencies_mhz must contain at least one frequency")
	}
	if len(c.CenterFrequenciesMHz) > 10 {
		return fmt.Errorf("voacap_fallback.center_frequencies_mhz must contain at most ten frequencies")
	}
	for i, freq := range c.CenterFrequenciesMHz {
		if math.IsNaN(freq) || math.IsInf(freq, 0) || freq <= 0 {
			return fmt.Errorf("voacap_fallback.center_frequencies_mhz[%d] must be > 0", i)
		}
	}
	if c.DelaySeconds < 0 {
		return fmt.Errorf("voacap_fallback.delay_seconds must be >= 0")
	}
	if c.CacheTTLSeconds <= 0 {
		return fmt.Errorf("voacap_fallback.cache_ttl_seconds must be > 0")
	}
	if c.MaxCacheEntries <= 0 {
		return fmt.Errorf("voacap_fallback.max_cache_entries must be > 0")
	}
	if c.MaxDelayEntries <= 0 {
		return fmt.Errorf("voacap_fallback.max_delay_entries must be > 0")
	}
	if c.MaxQueueDepth <= 0 {
		return fmt.Errorf("voacap_fallback.max_queue_depth must be > 0")
	}
	if c.WorkerCount != 1 {
		return fmt.Errorf("voacap_fallback.worker_count must be 1 for the shared VOACAP run directory")
	}
	if c.Enabled && strings.TrimSpace(c.OutputNamePrefix) == "" {
		return fmt.Errorf("voacap_fallback.output_name_prefix must not be empty when enabled")
	}
	if len(strings.TrimSpace(c.OutputNamePrefix)) > 24 {
		return fmt.Errorf("voacap_fallback.output_name_prefix must be at most 24 characters")
	}
	return nil
}

func (c *Config) buildCaches() {
	if c == nil {
		return
	}
	c.noiseModel = newNoiseModel(c.NoiseOffsets)
}

func validGlyphThresholds(t GlyphThresholds) bool {
	return t.High > t.Medium && t.Medium > t.Low && t.Low >= t.Unlikely && t.Unlikely > t.Closed
}

func completeGlyphThresholds(t GlyphThresholds) bool {
	if t.hasHigh || t.hasMedium || t.hasLow || t.hasUnlikely || t.hasClosed {
		return t.hasHigh && t.hasMedium && t.hasLow && t.hasUnlikely && t.hasClosed && validGlyphThresholds(t)
	}
	return validGlyphThresholds(t)
}

func validateGlyphSymbol(symbol string) error {
	if len(symbol) != 1 {
		return fmt.Errorf("must be a single printable ASCII character")
	}
	b := symbol[0]
	if b < 0x20 || b > 0x7e {
		return fmt.Errorf("must be a single printable ASCII character")
	}
	return nil
}

// NoiseModel returns the normalized receive-side noise penalty model.
func (c Config) NoiseModel() NoiseModel {
	if !c.noiseModel.empty() {
		return c.noiseModel
	}
	return newNoiseModel(c.NoiseOffsets)
}

// LoadFile loads YAML config, validates required YAML-owned settings, and builds derived caches.
func LoadFile(path string) (Config, error) {
	cfg, _, errors, err := LoadFileWithDiagnostics(path)
	if err != nil {
		return cfg, err
	}
	if len(errors) > 0 {
		return cfg, fmt.Errorf("%s", strings.Join(errors, "; "))
	}
	return cfg, nil
}

func LoadFileWithWarnings(path string) (Config, []string, error) {
	cfg, warnings, errors, err := LoadFileWithDiagnostics(path)
	if err != nil {
		return cfg, warnings, err
	}
	if len(errors) > 0 {
		return cfg, warnings, fmt.Errorf("%s", strings.Join(errors, "; "))
	}
	return cfg, warnings, nil
}

func LoadFileWithDiagnostics(path string) (Config, []string, []string, error) {
	var cfg Config
	if strings.TrimSpace(path) == "" {
		return cfg, nil, nil, fmt.Errorf("path reliability config path is required")
	}
	bs, err := os.ReadFile(path)
	if err != nil {
		return cfg, nil, nil, err
	}
	if err := rejectRemovedPathReliabilityKeys(bs); err != nil {
		return cfg, nil, nil, err
	}
	unknown, err := yamlconfig.UnknownFieldDiagnostics(path, bs, Config{})
	if err != nil {
		return cfg, nil, nil, err
	}
	_, _, err = decodeNoiseOffsets(bs)
	if err != nil {
		return cfg, nil, nil, err
	}
	required, err := yamlconfig.RequiredPathDiagnostics(path, bs, requiredConfigPaths)
	if err != nil {
		return cfg, nil, nil, err
	}
	warnings := make([]string, 0, len(unknown))
	errors := make([]string, 0, len(required))
	for _, diag := range unknown {
		warnings = append(warnings, diag.Error())
	}
	for _, diag := range required {
		errors = append(errors, diag.Error())
	}
	if len(errors) > 0 {
		return cfg, warnings, errors, nil
	}
	if err := yamlconfig.DecodeBytesPermissive(path, bs, &cfg, requiredConfigPaths); err != nil {
		return cfg, warnings, nil, err
	}
	if err := cfg.finalize(); err != nil {
		return cfg, warnings, nil, err
	}
	return cfg, warnings, nil, nil
}

func rejectRemovedPathReliabilityKeys(bs []byte) error {
	var root yaml.Node
	if err := yaml.Unmarshal(bs, &root); err != nil {
		return err
	}
	if len(root.Content) == 0 {
		return nil
	}
	doc := root.Content[0]
	if doc.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(doc.Content); i += 2 {
		key := strings.ToLower(strings.TrimSpace(doc.Content[i].Value))
		value := doc.Content[i+1]
		switch key {
		case "clamp_min", "clamp_max":
			return fmt.Errorf("field %s not found in type pathreliability.Config", key)
		case "voacap_fallback":
			if err := rejectRemovedVOACAPFallbackKeys(value); err != nil {
				return err
			}
		case "glyph_thresholds":
			if err := rejectLegacyGlyphThresholdKeys("glyph_thresholds", value); err != nil {
				return err
			}
		case "mode_thresholds":
			if value.Kind != yaml.MappingNode {
				continue
			}
			for j := 0; j+1 < len(value.Content); j += 2 {
				mode := strings.TrimSpace(value.Content[j].Value)
				if err := rejectLegacyGlyphThresholdKeys("mode_thresholds."+mode, value.Content[j+1]); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func rejectLegacyGlyphThresholdKeys(path string, node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.ToLower(strings.TrimSpace(node.Content[i].Value))
		switch key {
		case "excellent", "good", "marginal":
			return fmt.Errorf("unsupported glyph threshold key %q at %s; use high, medium, low, unlikely, or closed", key, path)
		}
	}
	return nil
}

func rejectRemovedVOACAPFallbackKeys(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.ToLower(strings.TrimSpace(node.Content[i].Value))
		switch key {
		case "closed_base_ft8_snr_db", "closed_safety_margin_db":
			return fmt.Errorf("voacap_fallback.%s is no longer supported; configure mode_thresholds.<mode>.closed instead", key)
		}
	}
	return nil
}
