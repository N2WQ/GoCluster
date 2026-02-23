package solarweather

import (
	"dxcluster/strutil"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
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

// DefaultConfig returns a pinned default configuration.
func DefaultConfig() Config {
	cfg := Config{
		Enabled:           false,
		FetchIntervalSec:  60,
		RequestTimeoutSec: 10,
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

// normalize fills defaults and clamps invalid values.
func (c *Config) normalize() {
	if c == nil {
		return
	}
	def := DefaultConfig()
	if c.FetchIntervalSec <= 0 {
		c.FetchIntervalSec = def.FetchIntervalSec
	}
	if c.RequestTimeoutSec <= 0 {
		c.RequestTimeoutSec = def.RequestTimeoutSec
	}
	if c.Sun.CacheSeconds <= 0 {
		c.Sun.CacheSeconds = def.Sun.CacheSeconds
	}
	if c.Sun.DaylightEnter <= 0 {
		c.Sun.DaylightEnter = def.Sun.DaylightEnter
	}
	if c.Sun.DaylightExit <= 0 {
		c.Sun.DaylightExit = def.Sun.DaylightExit
	}
	if c.Daylight.CrossNormTiny <= 0 {
		c.Daylight.CrossNormTiny = def.Daylight.CrossNormTiny
	}
	if c.Daylight.DSmallRad <= 0 {
		c.Daylight.DSmallRad = def.Daylight.DSmallRad
	}
	if c.Daylight.DAntipodalRad <= 0 {
		c.Daylight.DAntipodalRad = def.Daylight.DAntipodalRad
	}
	if c.Daylight.EpsBaseRad <= 0 {
		c.Daylight.EpsBaseRad = def.Daylight.EpsBaseRad
	}
	if c.Daylight.EpsScale <= 0 {
		c.Daylight.EpsScale = def.Daylight.EpsScale
	}
	if c.HighLat.FixedLEdgeDeg <= 0 {
		c.HighLat.FixedLEdgeDeg = def.HighLat.FixedLEdgeDeg
	}
	if c.HighLat.LEdgeMinDeg <= 0 {
		c.HighLat.LEdgeMinDeg = def.HighLat.LEdgeMinDeg
	}
	if c.HighLat.LEdgeMaxDeg <= 0 {
		c.HighLat.LEdgeMaxDeg = def.HighLat.LEdgeMaxDeg
	}
	if c.HighLat.LEdgeSlopeDegPerK <= 0 {
		c.HighLat.LEdgeSlopeDegPerK = def.HighLat.LEdgeSlopeDegPerK
	}
	if c.HighLat.EnterMaxAbsOffset <= 0 {
		c.HighLat.EnterMaxAbsOffset = def.HighLat.EnterMaxAbsOffset
	}
	if c.HighLat.ExitMaxAbsOffset <= 0 {
		c.HighLat.ExitMaxAbsOffset = def.HighLat.ExitMaxAbsOffset
	}
	if c.HighLat.EnterFrac <= 0 {
		c.HighLat.EnterFrac = def.HighLat.EnterFrac
	}
	if c.HighLat.ExitFrac <= 0 {
		c.HighLat.ExitFrac = def.HighLat.ExitFrac
	}
	if c.HighLat.MTiny <= 0 {
		c.HighLat.MTiny = def.HighLat.MTiny
	}
	if c.GateCache.MaxEntries <= 0 {
		c.GateCache.MaxEntries = def.GateCache.MaxEntries
	}
	if c.GateCache.TTLSeconds <= 0 {
		c.GateCache.TTLSeconds = def.GateCache.TTLSeconds
	}
	c.GOES.URL = strings.TrimSpace(c.GOES.URL)
	if c.GOES.URL == "" {
		c.GOES.URL = def.GOES.URL
	}
	if strings.TrimSpace(c.GOES.EnergyBand) == "" {
		c.GOES.EnergyBand = def.GOES.EnergyBand
	}
	if c.GOES.MaxAgeSec <= 0 {
		c.GOES.MaxAgeSec = def.GOES.MaxAgeSec
	}
	c.Kp.URL = strings.TrimSpace(c.Kp.URL)
	if c.Kp.URL == "" {
		c.Kp.URL = def.Kp.URL
	}
	if c.Kp.MaxAgeSec <= 0 {
		c.Kp.MaxAgeSec = def.Kp.MaxAgeSec
	}
	if strings.TrimSpace(c.Glyphs.R) == "" {
		c.Glyphs.R = def.Glyphs.R
	}
	if strings.TrimSpace(c.Glyphs.G) == "" {
		c.Glyphs.G = def.Glyphs.G
	}
	if c.PathKeyIncludeBand == nil {
		v := true
		c.PathKeyIncludeBand = &v
	}

	rInput := c.RLevels
	if len(rInput) == 0 {
		rInput = def.RLevels
	}
	rLevels, rMaxHold, rErr := normalizeRLevels(rInput)
	if rErr != nil {
		c.levelErr = rErr
		fallbackLevels, fallbackMaxHold, fallbackErr := normalizeRLevels(def.RLevels)
		if fallbackErr == nil {
			rLevels, rMaxHold = fallbackLevels, fallbackMaxHold
		}
	}
	c.rLevels = rLevels
	c.rMaxHold = rMaxHold

	gInput := c.GLevels
	if len(gInput) == 0 {
		gInput = def.GLevels
	}
	gLevels, gErr := normalizeGLevels(gInput)
	if gErr != nil {
		c.levelErr = gErr
		fallbackLevels, fallbackErr := normalizeGLevels(def.GLevels)
		if fallbackErr == nil {
			gLevels = fallbackLevels
		}
	}
	c.gLevels = gLevels
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
			name = fmt.Sprintf("R%d", i+2)
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
			name = fmt.Sprintf("G%d", i+2)
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

// LoadFile loads YAML config and applies defaults.
func LoadFile(path string) (Config, error) {
	cfg := DefaultConfig()
	if strings.TrimSpace(path) == "" {
		return cfg, nil
	}
	bs, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(bs, &cfg); err != nil {
		return cfg, err
	}
	cfg.normalize()
	return cfg, nil
}

// Validate performs sanity checks on the configuration.
func (c Config) Validate() error {
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
