package pathreliability

import (
	"dxcluster/strutil"
	"math"
)

// FT8Equivalent returns the FT8-equivalent dB for a mode/SNR pair.
// Returns ok=false when mode is unsupported for path reliability.
func FT8Equivalent(mode string, snr int, cfg Config) (float64, bool) {
	switch normalizeMode(mode) {
	case "FT8":
		return float64(snr), true
	case "FT4":
		return float64(snr) + cfg.ModeOffsets.FT4, true
	case "CW":
		return float64(snr) + cfg.ModeOffsets.CW, true
	case "RTTY":
		return float64(snr) + cfg.ModeOffsets.RTTY, true
	case "PSK":
		return float64(snr) + cfg.ModeOffsets.PSK, true
	case "WSPR":
		return float64(snr) + cfg.ModeOffsets.WSPR, true
	default:
		return 0, false
	}
}

func normalizeMode(mode string) string {
	up := strutil.NormalizeUpper(mode)
	return up
}

// ApplyNoisePower applies a noise penalty (dB) in the power domain.
func ApplyNoisePower(power float64, penaltyDB float64, cfg Config) float64 {
	if power <= 0 || penaltyDB <= 0 {
		return power
	}
	divisor := cfg.noiseDivisorForPenalty(penaltyDB)
	if divisor <= 0 {
		return power
	}
	return power / divisor
}

// GlyphForPower maps power to the ASCII glyph scale for a given mode.
func GlyphForPower(power float64, mode string, cfg Config) string {
	thresholds := thresholdsForModePower(mode, cfg)
	switch {
	case power >= thresholds.High:
		return cfg.GlyphSymbols.High
	case power >= thresholds.Medium:
		return cfg.GlyphSymbols.Medium
	case power >= thresholds.Low:
		return cfg.GlyphSymbols.Low
	case power >= thresholds.Unlikely:
		return cfg.GlyphSymbols.Unlikely
	default:
		return cfg.GlyphSymbols.Unlikely
	}
}

const (
	classHigh     = "HIGH"
	classMedium   = "MEDIUM"
	classLow      = "LOW"
	classUnlikely = "UNLIKELY"
)

// ClassForPower maps power to the threshold class name for filtering.
func ClassForPower(power float64, mode string, cfg Config) string {
	thresholds := thresholdsForModePower(mode, cfg)
	switch {
	case power >= thresholds.High:
		return classHigh
	case power >= thresholds.Medium:
		return classMedium
	case power >= thresholds.Low:
		return classLow
	default:
		return classUnlikely
	}
}

func thresholdsForModePower(mode string, cfg Config) GlyphThresholdsPower {
	key := normalizeMode(mode)
	if cfg.modeThresholdsPower != nil {
		if t, ok := cfg.modeThresholdsPower[key]; ok {
			return t
		}
	}
	return cfg.glyphThresholdsPower
}

// SelectSample chooses or blends fine/coarse samples by confidence weight.
// If fine is below minFineWeight, coarse wins to prevent weak fine data
// from overriding stronger regional evidence. If fine meets fineOnlyWeight,
// fine wins outright to preserve local detail.
func SelectSample(fine Sample, coarse Sample, minFineWeight float64, fineOnlyWeight float64) Sample {
	coarseCandidate := coarse
	hasFine := fine.Weight > 0
	hasCoarse := coarseCandidate.Weight > 0
	switch {
	case hasFine && !hasCoarse:
		return fine
	case !hasFine && hasCoarse:
		return coarseCandidate
	case !hasFine && !hasCoarse:
		return Sample{}
	}
	if fineOnlyWeight > 0 && fine.Weight >= fineOnlyWeight {
		return fine
	}
	if minFineWeight > 0 && fine.Weight < minFineWeight {
		return coarseCandidate
	}
	sum := fine.Weight + coarseCandidate.Weight
	if sum <= 0 {
		return Sample{}
	}
	age := fine.AgeSec
	if coarseCandidate.AgeSec < age {
		age = coarseCandidate.AgeSec
	}
	return Sample{
		Value:  (fine.Value*fine.Weight + coarseCandidate.Value*coarseCandidate.Weight) / sum,
		Weight: sum,
		AgeSec: age,
	}
}

func (c Config) powerFromDB(db float64) float64 {
	if c.powerLUTStepDB <= 0 || len(c.powerLUT) == 0 {
		return dbToPower(clamp(db, c.ClampMin, c.ClampMax))
	}
	clamped := clamp(db, c.ClampMin, c.ClampMax)
	// LUT uses a fixed dB step; round to the nearest index.
	idx := int(math.Round((clamped - c.powerLUTMinDB) / c.powerLUTStepDB))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(c.powerLUT) {
		idx = len(c.powerLUT) - 1
	}
	return c.powerLUT[idx]
}

func (c Config) noiseDivisorForPenalty(penalty float64) float64 {
	if penalty <= 0 {
		return 1
	}
	if c.noisePenaltyDivisors != nil {
		if v, ok := c.noisePenaltyDivisors[penalty]; ok && v > 0 {
			return v
		}
	}
	return dbToPower(penalty)
}

func powerToDB(power float64) float64 {
	const minPower = 1e-10
	if power < minPower {
		power = minPower
	}
	return 10 * math.Log10(power)
}
