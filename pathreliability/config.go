package pathreliability

import (
	"dxcluster/strutil"
	"fmt"
	"math"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config holds tuning knobs for path reliability aggregation and display.
type Config struct {
	Enabled                      bool                       `yaml:"enabled"`
	AllowedBands                 []string                   `yaml:"allowed_bands"`
	ClampMin                     float64                    `yaml:"clamp_min"`                        // FT8-equiv floor (dB)
	ClampMax                     float64                    `yaml:"clamp_max"`                        // FT8-equiv ceiling (dB)
	DefaultHalfLifeSec           int                        `yaml:"default_half_life_seconds"`        // fallback half-life when band not listed
	BandHalfLifeSec              map[string]int             `yaml:"band_half_life_seconds"`           // per-band overrides
	StaleAfterSeconds            int                        `yaml:"stale_after_seconds"`              // fallback purge when older than this
	StaleAfterHalfLifeMultiplier float64                    `yaml:"stale_after_half_life_multiplier"` // stale = k * half-life (per band)
	MinEffectiveWeight           float64                    `yaml:"min_effective_weight"`             // minimum decayed weight to report
	MinFineWeight                float64                    `yaml:"min_fine_weight"`                  // minimum fine weight to blend with coarse
	FineOnlyWeight               float64                    `yaml:"fine_only_weight"`                 // minimum fine weight to use fine only
	ReverseHintDiscount          float64                    `yaml:"reverse_hint_discount"`            // multiplier when using reverse direction
	MergeReceiveWeight           float64                    `yaml:"merge_receive_weight"`             // merge weight for DX->user
	MergeTransmitWeight          float64                    `yaml:"merge_transmit_weight"`            // merge weight for user->DX
	BeaconWeightCap              float64                    `yaml:"beacon_weight_cap"`                // cap per-beacon contribution
	DisplayEnabled               bool                       `yaml:"display_enabled"`                  // toggle glyph rendering
	ModeOffsets                  ModeOffsets                `yaml:"mode_offsets"`                     // per-mode FT8-equiv offsets
	ModeThresholds               map[string]GlyphThresholds `yaml:"mode_thresholds"`                  // per-mode glyph thresholds in FT8-equiv dB
	GlyphThresholds              GlyphThresholds            `yaml:"glyph_thresholds"`                 // fallback glyph thresholds in FT8-equiv dB
	GlyphSymbols                 GlyphSymbols               `yaml:"glyph_symbols"`                    // glyph mapping for high/medium/low/unlikely/insufficient
	NoiseOffsets                 map[string]float64         `yaml:"noise_offsets"`                    // noise class -> dB penalty

	modeThresholdsPower  map[string]GlyphThresholdsPower
	glyphThresholdsPower GlyphThresholdsPower
	noisePenaltyDivisors map[float64]float64
	powerLUT             []float64
	powerLUTMinDB        float64
	powerLUTStepDB       float64
}

// ModeOffsets normalizes non-FT8 modes to FT8-equivalent dB.
type ModeOffsets struct {
	FT4  float64 `yaml:"ft4"`
	CW   float64 `yaml:"cw"`
	RTTY float64 `yaml:"rtty"`
	PSK  float64 `yaml:"psk"`
	WSPR float64 `yaml:"wspr"`
}

// GlyphThresholds defines FT8-equiv dB cutoffs for glyphs.
type GlyphThresholds struct {
	High     float64 `yaml:"high"`     // >= High
	Medium   float64 `yaml:"medium"`   // >= Medium
	Low      float64 `yaml:"low"`      // >= Low
	Unlikely float64 `yaml:"unlikely"` // >= Unlikely (still "unlikely" below)

	hasHigh     bool
	hasMedium   bool
	hasLow      bool
	hasUnlikely bool
}

// GlyphThresholdsPower defines power-domain cutoffs for glyphs.
type GlyphThresholdsPower struct {
	High     float64
	Medium   float64
	Low      float64
	Unlikely float64
}

// UnmarshalYAML enforces the new high/medium/low/unlikely keys and rejects legacy names.
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
		case "excellent", "good", "marginal":
			return fmt.Errorf("unsupported glyph threshold key %q; use high/medium/low/unlikely", k)
		default:
			return fmt.Errorf("unsupported glyph threshold key %q; use high/medium/low/unlikely", k)
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
}

// UnmarshalYAML enforces single printable ASCII glyphs and rejects unknown keys.
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
		default:
			return fmt.Errorf("unsupported glyph_symbols key %q; use high/medium/low/unlikely/insufficient", k)
		}
	}
	return nil
}

// DefaultConfig returns a safe, enabled configuration.
func DefaultConfig() Config {
	cfg := Config{
		Enabled:                      true,
		AllowedBands:                 []string{"160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m", "6m"},
		ClampMin:                     -25,
		ClampMax:                     15,
		DefaultHalfLifeSec:           300,
		BandHalfLifeSec:              map[string]int{},
		StaleAfterSeconds:            1800,
		StaleAfterHalfLifeMultiplier: 5,
		MinEffectiveWeight:           1.0,
		MinFineWeight:                5.0,
		FineOnlyWeight:               20.0,
		ReverseHintDiscount:          0.5,
		MergeReceiveWeight:           0.6,
		MergeTransmitWeight:          0.4,
		BeaconWeightCap:              1.0,
		DisplayEnabled:               true,
		ModeOffsets: ModeOffsets{
			FT4:  -3,
			CW:   -7,
			RTTY: -7,
			PSK:  -7,
			WSPR: 26,
		},
		ModeThresholds: map[string]GlyphThresholds{
			"FT8":  {High: -13, Medium: -17, Low: -21, Unlikely: -21, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true},
			"FT4":  {High: -13, Medium: -17, Low: -21, Unlikely: -21, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true},
			"CW":   {High: 5, Medium: -1, Low: -5, Unlikely: -5, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true},
			"RTTY": {High: 5, Medium: -1, Low: -5, Unlikely: -5, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true},
			"PSK":  {High: 5, Medium: -1, Low: -5, Unlikely: -5, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true},
			"USB":  {High: 5, Medium: -1, Low: -5, Unlikely: -5, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true},
			"LSB":  {High: 5, Medium: -1, Low: -5, Unlikely: -5, hasHigh: true, hasMedium: true, hasLow: true, hasUnlikely: true},
		},
		GlyphThresholds: GlyphThresholds{
			High:        -13,
			Medium:      -17,
			Low:         -21,
			Unlikely:    -21,
			hasHigh:     true,
			hasMedium:   true,
			hasLow:      true,
			hasUnlikely: true,
		},
		GlyphSymbols: GlyphSymbols{
			High:         "+",
			Medium:       "=",
			Low:          "-",
			Unlikely:     "!",
			Insufficient: "?",
		},
		NoiseOffsets: map[string]float64{
			"QUIET":      0,
			"RURAL":      6,
			"SUBURBAN":   9,
			"URBAN":      12,
			"INDUSTRIAL": 21,
		},
	}
	cfg.buildCaches()
	return cfg
}

// normalize fills defaults and clamps obvious invalids.
func (c *Config) normalize() {
	if c == nil {
		return
	}
	def := DefaultConfig()
	if len(c.AllowedBands) == 0 {
		c.AllowedBands = append([]string(nil), def.AllowedBands...)
	}
	if c.ClampMax <= c.ClampMin {
		c.ClampMin = def.ClampMin
		c.ClampMax = def.ClampMax
	}
	if c.DefaultHalfLifeSec <= 0 {
		c.DefaultHalfLifeSec = def.DefaultHalfLifeSec
	}
	if c.StaleAfterSeconds <= 0 {
		c.StaleAfterSeconds = def.StaleAfterSeconds
	}
	if c.StaleAfterHalfLifeMultiplier <= 0 {
		c.StaleAfterHalfLifeMultiplier = def.StaleAfterHalfLifeMultiplier
	}
	if c.MinEffectiveWeight <= 0 {
		c.MinEffectiveWeight = def.MinEffectiveWeight
	}
	if c.MinFineWeight <= 0 {
		c.MinFineWeight = def.MinFineWeight
	}
	if c.FineOnlyWeight <= 0 {
		c.FineOnlyWeight = def.FineOnlyWeight
	}
	if c.FineOnlyWeight < c.MinFineWeight {
		c.FineOnlyWeight = c.MinFineWeight
	}
	if c.ReverseHintDiscount <= 0 || c.ReverseHintDiscount > 1 {
		c.ReverseHintDiscount = def.ReverseHintDiscount
	}
	if c.MergeReceiveWeight <= 0 {
		c.MergeReceiveWeight = def.MergeReceiveWeight
	}
	if c.MergeTransmitWeight <= 0 {
		c.MergeTransmitWeight = def.MergeTransmitWeight
	}
	sum := c.MergeReceiveWeight + c.MergeTransmitWeight
	if sum <= 0 {
		c.MergeReceiveWeight = def.MergeReceiveWeight
		c.MergeTransmitWeight = def.MergeTransmitWeight
		sum = c.MergeReceiveWeight + c.MergeTransmitWeight
	}
	if sum != 1 {
		c.MergeReceiveWeight /= sum
		c.MergeTransmitWeight /= sum
	}
	if c.BeaconWeightCap <= 0 {
		c.BeaconWeightCap = def.BeaconWeightCap
	}
	// Mode offsets: leave caller-provided values unless zeroed.
	if c.ModeOffsets.FT4 == 0 {
		c.ModeOffsets.FT4 = def.ModeOffsets.FT4
	}
	if c.ModeOffsets.CW == 0 {
		c.ModeOffsets.CW = def.ModeOffsets.CW
	}
	if c.ModeOffsets.RTTY == 0 {
		c.ModeOffsets.RTTY = def.ModeOffsets.RTTY
	}
	if c.ModeOffsets.PSK == 0 {
		c.ModeOffsets.PSK = def.ModeOffsets.PSK
	}
	if c.ModeOffsets.WSPR == 0 {
		c.ModeOffsets.WSPR = def.ModeOffsets.WSPR
	}
	if c.ModeThresholds == nil {
		c.ModeThresholds = map[string]GlyphThresholds{}
	}
	normalizedThresholds := make(map[string]GlyphThresholds, len(c.ModeThresholds))
	for k, v := range c.ModeThresholds {
		key := strutil.NormalizeUpper(k)
		if key == "" {
			continue
		}
		normalizedThresholds[key] = v
	}
	c.ModeThresholds = normalizedThresholds
	for mode, defThresholds := range def.ModeThresholds {
		if t, ok := c.ModeThresholds[mode]; ok {
			merged := mergeGlyphThresholds(defThresholds, t)
			if validGlyphThresholds(merged) {
				c.ModeThresholds[mode] = merged
				continue
			}
		}
		c.ModeThresholds[mode] = defThresholds
	}
	mergedFallback := mergeGlyphThresholds(def.GlyphThresholds, c.GlyphThresholds)
	if validGlyphThresholds(mergedFallback) {
		c.GlyphThresholds = mergedFallback
	} else {
		c.GlyphThresholds = def.GlyphThresholds
	}
	if c.GlyphSymbols.High == "" {
		c.GlyphSymbols.High = def.GlyphSymbols.High
	}
	if c.GlyphSymbols.Medium == "" {
		c.GlyphSymbols.Medium = def.GlyphSymbols.Medium
	}
	if c.GlyphSymbols.Low == "" {
		c.GlyphSymbols.Low = def.GlyphSymbols.Low
	}
	if c.GlyphSymbols.Unlikely == "" {
		c.GlyphSymbols.Unlikely = def.GlyphSymbols.Unlikely
	}
	if c.GlyphSymbols.Insufficient == "" {
		c.GlyphSymbols.Insufficient = def.GlyphSymbols.Insufficient
	}
	if c.NoiseOffsets == nil {
		c.NoiseOffsets = map[string]float64{}
	}
	// Ensure canonical noise keys exist.
	for k, v := range def.NoiseOffsets {
		if _, ok := c.NoiseOffsets[k]; !ok {
			c.NoiseOffsets[k] = v
		}
	}
	// Normalize noise keys to uppercase for lookup stability.
	normalized := make(map[string]float64, len(c.NoiseOffsets))
	for k, v := range c.NoiseOffsets {
		normalized[strutil.NormalizeUpper(k)] = v
	}
	c.NoiseOffsets = normalized
	c.buildCaches()
}

func (c *Config) buildCaches() {
	if c == nil {
		return
	}
	if c.powerLUTStepDB <= 0 {
		c.powerLUTStepDB = 0.1
	}
	minDB := math.Floor(c.ClampMin/c.powerLUTStepDB) * c.powerLUTStepDB
	maxDB := math.Ceil(c.ClampMax/c.powerLUTStepDB) * c.powerLUTStepDB
	if maxDB < minDB {
		minDB = c.ClampMin
		maxDB = c.ClampMax
	}
	size := int(math.Round((maxDB-minDB)/c.powerLUTStepDB)) + 1
	if size < 2 {
		size = 2
	}
	lut := make([]float64, size)
	for i := 0; i < size; i++ {
		db := minDB + float64(i)*c.powerLUTStepDB
		lut[i] = dbToPower(db)
	}
	c.powerLUT = lut
	c.powerLUTMinDB = minDB

	c.noisePenaltyDivisors = make(map[float64]float64, len(c.NoiseOffsets))
	for _, penalty := range c.NoiseOffsets {
		if penalty <= 0 {
			continue
		}
		c.noisePenaltyDivisors[penalty] = dbToPower(penalty)
	}

	c.glyphThresholdsPower = thresholdsPower(c.GlyphThresholds)
	c.modeThresholdsPower = make(map[string]GlyphThresholdsPower, len(c.ModeThresholds))
	for mode, thresholds := range c.ModeThresholds {
		c.modeThresholdsPower[mode] = thresholdsPower(thresholds)
	}
}

func thresholdsPower(t GlyphThresholds) GlyphThresholdsPower {
	return GlyphThresholdsPower{
		High:     dbToPower(t.High),
		Medium:   dbToPower(t.Medium),
		Low:      dbToPower(t.Low),
		Unlikely: dbToPower(t.Unlikely),
	}
}

func dbToPower(db float64) float64 {
	return math.Pow(10, db/10)
}

func validGlyphThresholds(t GlyphThresholds) bool {
	return t.High > t.Medium && t.Medium > t.Low && t.Low >= t.Unlikely
}

func mergeGlyphThresholds(def, override GlyphThresholds) GlyphThresholds {
	if !override.hasHigh && !override.hasMedium && !override.hasLow && !override.hasUnlikely {
		if validGlyphThresholds(override) {
			return override
		}
		return def
	}
	out := def
	if override.hasHigh {
		out.High = override.High
	}
	if override.hasMedium {
		out.Medium = override.Medium
	}
	if override.hasLow {
		out.Low = override.Low
	}
	if override.hasUnlikely {
		out.Unlikely = override.Unlikely
	}
	return out
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
