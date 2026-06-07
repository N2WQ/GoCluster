package solarweather

import (
	"dxcluster/internal/yamlconfig"
	"dxcluster/strutil"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

// Config holds tunables for solar/geomagnetic override gating.
type Config struct {
	Enabled            bool            `yaml:"enabled"`
	FetchIntervalSec   int             `yaml:"fetch_interval_seconds"`
	RequestTimeoutSec  int             `yaml:"request_timeout_seconds"`
	Sun                SunConfig       `yaml:"sun"`
	Daylight           DaylightConfig  `yaml:"daylight"`
	HighLat            HighLatConfig   `yaml:"high_lat"`
	GateCache          GateCacheConfig `yaml:"gate_cache"`
	GOES               GOESConfig      `yaml:"goes"`
	Kp                 KpConfig        `yaml:"kp"`
	RLevels            []RLevel        `yaml:"r_levels"`
	GLevels            []GLevel        `yaml:"g_levels"`
	Glyphs             GlyphsConfig    `yaml:"glyphs"`
	PathKeyIncludeBand *bool           `yaml:"path_key_include_band"`

	rLevels  []rLevel
	gLevels  []gLevel
	rMaxHold time.Duration
	levelErr error
}

var requiredConfigPaths = []yamlconfig.Path{
	{"enabled"},
	{"fetch_interval_seconds"},
	{"request_timeout_seconds"},
	{"sun", "cache_seconds"},
	{"sun", "twilight_degrees"},
	{"sun", "daylight_enter"},
	{"sun", "daylight_exit"},
	{"sun", "near_terminator_hold"},
	{"daylight", "cross_norm_tiny"},
	{"daylight", "d_small_rad"},
	{"daylight", "d_antipodal_rad"},
	{"daylight", "eps_base_rad"},
	{"daylight", "eps_scale"},
	{"high_lat", "use_kp_boundary"},
	{"high_lat", "fixed_l_edge_deg"},
	{"high_lat", "l_edge_min_deg"},
	{"high_lat", "l_edge_max_deg"},
	{"high_lat", "l_edge_slope_deg_per_kp"},
	{"high_lat", "enter_max_abs_offset_deg"},
	{"high_lat", "exit_max_abs_offset_deg"},
	{"high_lat", "enter_frac"},
	{"high_lat", "exit_frac"},
	{"high_lat", "m_tiny"},
	{"gate_cache", "max_entries"},
	{"gate_cache", "ttl_seconds"},
	{"goes", "url"},
	{"goes", "energy_band"},
	{"goes", "max_age_seconds"},
	{"kp", "url"},
	{"kp", "max_age_seconds"},
	{"r_levels"},
	{"r_levels", "*", "name"},
	{"r_levels", "*", "min_flux_wm2"},
	{"r_levels", "*", "hold_minutes"},
	{"r_levels", "*", "bands"},
	{"g_levels"},
	{"g_levels", "*", "name"},
	{"g_levels", "*", "min_kp"},
	{"g_levels", "*", "bands"},
	{"glyphs", "r"},
	{"glyphs", "g"},
	{"path_key_include_band"},
}

// SunConfig controls solar-position caching and twilight thresholds.
type SunConfig struct {
	CacheSeconds       int     `yaml:"cache_seconds"`
	TwilightDegrees    float64 `yaml:"twilight_degrees"`
	DaylightEnter      float64 `yaml:"daylight_enter"`
	DaylightExit       float64 `yaml:"daylight_exit"`
	NearTerminatorHold bool    `yaml:"near_terminator_hold"`
}

// DaylightConfig holds numerical tolerances for the daylight fraction solver.
type DaylightConfig struct {
	CrossNormTiny float64 `yaml:"cross_norm_tiny"`
	DSmallRad     float64 `yaml:"d_small_rad"`
	DAntipodalRad float64 `yaml:"d_antipodal_rad"`
	EpsBaseRad    float64 `yaml:"eps_base_rad"`
	EpsScale      float64 `yaml:"eps_scale"`
}

// HighLatConfig holds dipole-gating parameters.
type HighLatConfig struct {
	UseKpBoundary     bool    `yaml:"use_kp_boundary"`
	FixedLEdgeDeg     float64 `yaml:"fixed_l_edge_deg"`
	LEdgeMinDeg       float64 `yaml:"l_edge_min_deg"`
	LEdgeMaxDeg       float64 `yaml:"l_edge_max_deg"`
	LEdgeSlopeDegPerK float64 `yaml:"l_edge_slope_deg_per_kp"`
	EnterMaxAbsOffset float64 `yaml:"enter_max_abs_offset_deg"`
	ExitMaxAbsOffset  float64 `yaml:"exit_max_abs_offset_deg"`
	EnterFrac         float64 `yaml:"enter_frac"`
	ExitFrac          float64 `yaml:"exit_frac"`
	MTiny             float64 `yaml:"m_tiny"`
}

// GateCacheConfig bounds per-path hysteresis state.
type GateCacheConfig struct {
	MaxEntries int `yaml:"max_entries"`
	TTLSeconds int `yaml:"ttl_seconds"`
}

// GOESConfig defines the GOES X-ray feed settings.
type GOESConfig struct {
	URL        string `yaml:"url"`
	EnergyBand string `yaml:"energy_band"`
	MaxAgeSec  int    `yaml:"max_age_seconds"`
}

// KpConfig defines the observed Kp feed settings.
type KpConfig struct {
	URL       string `yaml:"url"`
	MaxAgeSec int    `yaml:"max_age_seconds"`
}

// RLevel defines a flux threshold, hold-down, and band list for R overrides.
type RLevel struct {
	Name        string   `yaml:"name"`
	MinFluxWM2  float64  `yaml:"min_flux_wm2"`
	HoldMinutes int      `yaml:"hold_minutes"`
	Bands       []string `yaml:"bands"`
}

// GLevel defines a Kp threshold and band list for G overrides.
type GLevel struct {
	Name  string   `yaml:"name"`
	MinKp float64  `yaml:"min_kp"`
	Bands []string `yaml:"bands"`
}

// GlyphsConfig maps override glyphs.
type GlyphsConfig struct {
	R string `yaml:"r"`
	G string `yaml:"g"`
}

type rLevel struct {
	Name     string
	MinFlux  float64
	Hold     time.Duration
	BandMask bandMask
}

type gLevel struct {
	Name     string
	MinKp    float64
	BandMask bandMask
}

func boolPtr(v bool) *bool {
	return &v
}

// DefaultConfig returns a pinned default configuration.
func DefaultConfig() Config {
	cfg := Config{
		Enabled:            false,
		FetchIntervalSec:   60,
		RequestTimeoutSec:  10,
		PathKeyIncludeBand: boolPtr(true),
		Sun: SunConfig{
			CacheSeconds:       60,
			TwilightDegrees:    0,
			DaylightEnter:      0.55,
			DaylightExit:       0.45,
			NearTerminatorHold: true,
		},
		Daylight: DaylightConfig{
			CrossNormTiny: 1e-12,
			DSmallRad:     1e-8,
			DAntipodalRad: math.Pi - 1e-8,
			EpsBaseRad:    1e-6,
			EpsScale:      1e-6,
		},
		HighLat: HighLatConfig{
			UseKpBoundary:     true,
			FixedLEdgeDeg:     55,
			LEdgeMinDeg:       48,
			LEdgeMaxDeg:       66,
			LEdgeSlopeDegPerK: 2,
			EnterMaxAbsOffset: 3,
			ExitMaxAbsOffset:  1,
			EnterFrac:         0.15,
			ExitFrac:          0.10,
			MTiny:             1e-12,
		},
		GateCache: GateCacheConfig{
			MaxEntries: 100000,
			TTLSeconds: 10800,
		},
		GOES: GOESConfig{
			URL:        "https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json",
			EnergyBand: "0.1-0.8nm",
			MaxAgeSec:  600,
		},
		Kp: KpConfig{
			URL:       "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json",
			MaxAgeSec: 21600,
		},
		RLevels: []RLevel{
			{
				Name:        "R2",
				MinFluxWM2:  5e-5,
				HoldMinutes: 60,
				Bands:       []string{"80m", "60m", "40m", "30m", "20m"},
			},
			{
				Name:        "R3",
				MinFluxWM2:  1e-4,
				HoldMinutes: 90,
				Bands:       []string{"80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m"},
			},
			{
				Name:        "R4",
				MinFluxWM2:  1e-3,
				HoldMinutes: 120,
				Bands:       []string{"80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m"},
			},
		},
		GLevels: []GLevel{
			{
				Name:  "G2",
				MinKp: 6,
				Bands: []string{"20m", "17m", "15m", "12m", "10m"},
			},
			{
				Name:  "G3",
				MinKp: 7,
				Bands: []string{"40m", "30m", "20m", "17m", "15m", "12m", "10m"},
			},
			{
				Name:  "G4",
				MinKp: 8,
				Bands: []string{"160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m"},
			},
		},
		Glyphs: GlyphsConfig{
			R: "R",
			G: "G",
		},
	}
	return cfg
}

// normalize validates relationship constraints and rebuilds derived level caches.
func (c *Config) normalize() {
	if err := c.finalize(); err != nil {
		return
	}
}

func (c *Config) finalize() error {
	if c == nil {
		return nil
	}
	if c.FetchIntervalSec <= 0 {
		return fmt.Errorf("fetch_interval_seconds must be > 0")
	}
	if c.RequestTimeoutSec <= 0 {
		return fmt.Errorf("request_timeout_seconds must be > 0")
	}
	if c.Sun.CacheSeconds <= 0 {
		return fmt.Errorf("sun.cache_seconds must be > 0")
	}
	if c.Sun.DaylightEnter < c.Sun.DaylightExit {
		return fmt.Errorf("sun.daylight_enter must be >= sun.daylight_exit")
	}
	if c.Daylight.CrossNormTiny <= 0 {
		return fmt.Errorf("daylight.cross_norm_tiny must be > 0")
	}
	if c.Daylight.DSmallRad <= 0 {
		return fmt.Errorf("daylight.d_small_rad must be > 0")
	}
	if c.Daylight.DAntipodalRad <= 0 {
		return fmt.Errorf("daylight.d_antipodal_rad must be > 0")
	}
	if c.Daylight.EpsBaseRad <= 0 {
		return fmt.Errorf("daylight.eps_base_rad must be > 0")
	}
	if c.Daylight.EpsScale <= 0 {
		return fmt.Errorf("daylight.eps_scale must be > 0")
	}
	if c.HighLat.FixedLEdgeDeg <= 0 {
		return fmt.Errorf("high_lat.fixed_l_edge_deg must be > 0")
	}
	if c.HighLat.LEdgeMinDeg <= 0 {
		return fmt.Errorf("high_lat.l_edge_min_deg must be > 0")
	}
	if c.HighLat.LEdgeMaxDeg <= 0 {
		return fmt.Errorf("high_lat.l_edge_max_deg must be > 0")
	}
	if c.HighLat.LEdgeMaxDeg < c.HighLat.LEdgeMinDeg {
		return fmt.Errorf("high_lat.l_edge_max_deg must be >= high_lat.l_edge_min_deg")
	}
	if c.HighLat.LEdgeSlopeDegPerK <= 0 {
		return fmt.Errorf("high_lat.l_edge_slope_deg_per_kp must be > 0")
	}
	if c.HighLat.EnterMaxAbsOffset <= 0 {
		return fmt.Errorf("high_lat.enter_max_abs_offset_deg must be > 0")
	}
	if c.HighLat.ExitMaxAbsOffset <= 0 {
		return fmt.Errorf("high_lat.exit_max_abs_offset_deg must be > 0")
	}
	if c.HighLat.EnterFrac <= 0 {
		return fmt.Errorf("high_lat.enter_frac must be > 0")
	}
	if c.HighLat.ExitFrac <= 0 {
		return fmt.Errorf("high_lat.exit_frac must be > 0")
	}
	if c.HighLat.MTiny <= 0 {
		return fmt.Errorf("high_lat.m_tiny must be > 0")
	}
	if c.GateCache.MaxEntries <= 0 {
		return fmt.Errorf("gate_cache.max_entries must be > 0")
	}
	if c.GateCache.TTLSeconds <= 0 {
		return fmt.Errorf("gate_cache.ttl_seconds must be > 0")
	}
	c.GOES.URL = strings.TrimSpace(c.GOES.URL)
	if c.GOES.URL == "" {
		return fmt.Errorf("goes.url must not be empty")
	}
	if strings.TrimSpace(c.GOES.EnergyBand) == "" {
		return fmt.Errorf("goes.energy_band must not be empty")
	}
	if c.GOES.MaxAgeSec <= 0 {
		return fmt.Errorf("goes.max_age_seconds must be > 0")
	}
	c.Kp.URL = strings.TrimSpace(c.Kp.URL)
	if c.Kp.URL == "" {
		return fmt.Errorf("kp.url must not be empty")
	}
	if c.Kp.MaxAgeSec <= 0 {
		return fmt.Errorf("kp.max_age_seconds must be > 0")
	}
	if strings.TrimSpace(c.Glyphs.R) == "" {
		return fmt.Errorf("glyphs.r must not be empty")
	}
	if strings.TrimSpace(c.Glyphs.G) == "" {
		return fmt.Errorf("glyphs.g must not be empty")
	}
	if c.PathKeyIncludeBand == nil {
		return fmt.Errorf("path_key_include_band must be set")
	}

	rLevels, rMaxHold, rErr := normalizeRLevels(c.RLevels)
	if rErr != nil {
		return rErr
	}
	c.rLevels = rLevels
	c.rMaxHold = rMaxHold

	gLevels, gErr := normalizeGLevels(c.GLevels)
	if gErr != nil {
		return gErr
	}
	c.gLevels = gLevels
	return nil
}

func normalizeRLevels(input []RLevel) ([]rLevel, time.Duration, error) {
	if len(input) == 0 {
		return nil, 0, fmt.Errorf("r_levels cannot be empty")
	}
	levels := make([]rLevel, 0, len(input))
	var maxHold time.Duration
	for i, lvl := range input {
		name := strutil.NormalizeUpper(lvl.Name)
		if name == "" {
			return nil, 0, fmt.Errorf("r_levels[%d] name must not be empty", i)
		}
		if lvl.MinFluxWM2 <= 0 {
			return nil, 0, fmt.Errorf("r_levels[%d] min_flux_wm2 must be > 0", i)
		}
		if lvl.HoldMinutes <= 0 {
			return nil, 0, fmt.Errorf("r_levels[%d] hold_minutes must be > 0", i)
		}
		mask, err := bandsToMask(lvl.Bands)
		if err != nil {
			return nil, 0, fmt.Errorf("r_levels[%d] bands: %w", i, err)
		}
		hold := time.Duration(lvl.HoldMinutes) * time.Minute
		if hold > maxHold {
			maxHold = hold
		}
		levels = append(levels, rLevel{
			Name:     name,
			MinFlux:  lvl.MinFluxWM2,
			Hold:     hold,
			BandMask: mask,
		})
	}
	sort.Slice(levels, func(i, j int) bool { return levels[i].MinFlux < levels[j].MinFlux })
	for i := 1; i < len(levels); i++ {
		if levels[i].MinFlux <= levels[i-1].MinFlux {
			return nil, 0, fmt.Errorf("r_levels thresholds must be strictly increasing")
		}
	}
	return levels, maxHold, nil
}

func normalizeGLevels(input []GLevel) ([]gLevel, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("g_levels cannot be empty")
	}
	levels := make([]gLevel, 0, len(input))
	for i, lvl := range input {
		name := strutil.NormalizeUpper(lvl.Name)
		if name == "" {
			return nil, fmt.Errorf("g_levels[%d] name must not be empty", i)
		}
		if lvl.MinKp <= 0 {
			return nil, fmt.Errorf("g_levels[%d] min_kp must be > 0", i)
		}
		mask, err := bandsToMask(lvl.Bands)
		if err != nil {
			return nil, fmt.Errorf("g_levels[%d] bands: %w", i, err)
		}
		levels = append(levels, gLevel{
			Name:     name,
			MinKp:    lvl.MinKp,
			BandMask: mask,
		})
	}
	sort.Slice(levels, func(i, j int) bool { return levels[i].MinKp < levels[j].MinKp })
	for i := 1; i < len(levels); i++ {
		if levels[i].MinKp <= levels[i-1].MinKp {
			return nil, fmt.Errorf("g_levels thresholds must be strictly increasing")
		}
	}
	return levels, nil
}

// LoadFile loads YAML config, validates required YAML-owned settings, and builds derived level caches.
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
		return cfg, nil, nil, fmt.Errorf("solar weather config path is required")
	}
	bs, err := os.ReadFile(path)
	if err != nil {
		return cfg, nil, nil, err
	}
	unknown, err := yamlconfig.UnknownFieldDiagnostics(path, bs, Config{})
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

// Validate performs sanity checks on the configuration.
func (c Config) Validate() error {
	if err := c.finalize(); err != nil {
		return err
	}
	if c.levelErr != nil {
		return c.levelErr
	}
	if c.Sun.DaylightEnter < c.Sun.DaylightExit {
		return fmt.Errorf("sun.daylight_enter must be >= sun.daylight_exit")
	}
	if c.GateCache.MaxEntries <= 0 {
		return fmt.Errorf("gate_cache.max_entries must be > 0")
	}
	if c.GateCache.TTLSeconds <= 0 {
		return fmt.Errorf("gate_cache.ttl_seconds must be > 0")
	}
	if len(c.rLevels) == 0 {
		return fmt.Errorf("r_levels cannot be empty")
	}
	if len(c.gLevels) == 0 {
		return fmt.Errorf("g_levels cannot be empty")
	}
	return nil
}
