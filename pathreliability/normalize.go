// File role: Owns path reliability signal normalization and sample blending.
// Crawler notes: Start here for mode-to-FT8-equivalent SNR conversion, glyph
// class mapping, noise penalties, and fine/coarse sample selection used by
// prediction.
// Related docs: pathreliability/README.md, data/config/path_reliability.yaml.
// Related tests: pathreliability/*normalize*_test.go, pathreliability/*p50*_test.go.
package pathreliability

import (
	"dxcluster/strutil"
	"math"
)

// FT8Equivalent returns the FT8-equivalent dB for a mode/SNR pair.
// Returns ok=false when mode is unsupported for path reliability.
func FT8Equivalent(mode string, snr int, cfg Config) (float64, bool) {
	policy, ok := modePolicy(mode)
	if !ok || !policy.Ingest {
		return 0, false
	}
	switch policy.OffsetMode {
	case "FT8", "":
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

// GlyphForDB maps an FT8-equivalent SNR quantile to the ASCII glyph scale.
func GlyphForDB(db float64, mode string, cfg Config) string {
	return glyphForClass(ClassForDB(db, mode, cfg), cfg)
}

const (
	classHigh     = "HIGH"
	classMedium   = "MEDIUM"
	classLow      = "LOW"
	classUnlikely = "UNLIKELY"
	classClosed   = "CLOSED"
)

// ClassForDB maps an FT8-equivalent SNR quantile to the threshold class name.
func ClassForDB(db float64, mode string, cfg Config) string {
	thresholds := thresholdsForModeDB(mode, cfg)
	switch {
	case db >= thresholds.High:
		return classHigh
	case db >= thresholds.Medium:
		return classMedium
	case db >= thresholds.Low:
		return classLow
	default:
		return classUnlikely
	}
}

// ClassStrengthRank returns an ordered rank for ordinary open path classes.
// Higher ranks are stronger; unknown classes return -1.
func ClassStrengthRank(class string) int {
	switch class {
	case classHigh:
		return 3
	case classMedium:
		return 2
	case classLow:
		return 1
	case classUnlikely:
		return 0
	default:
		return -1
	}
}

// ClassUpgradeSteps reports how many ordinary path tiers separate two classes.
// Positive values mean toClass is stronger than fromClass.
func ClassUpgradeSteps(fromClass, toClass string) int {
	from := ClassStrengthRank(fromClass)
	to := ClassStrengthRank(toClass)
	if from < 0 || to < 0 {
		return 0
	}
	return to - from
}

// ClosedForDB reports whether an FT8-equivalent SNR is at or below the
// mode-owned closed threshold.
func ClosedForDB(db float64, mode string, cfg Config) bool {
	return db <= thresholdsForModeDB(mode, cfg).Closed
}

func glyphForClass(class string, cfg Config) string {
	switch class {
	case classHigh:
		return cfg.GlyphSymbols.High
	case classMedium:
		return cfg.GlyphSymbols.Medium
	case classLow:
		return cfg.GlyphSymbols.Low
	case classClosed:
		return cfg.GlyphSymbols.Closed
	default:
		return cfg.GlyphSymbols.Unlikely
	}
}

func thresholdsForModeDB(mode string, cfg Config) GlyphThresholds {
	key := normalizeMode(mode)
	if cfg.ModeThresholds != nil {
		if t, ok := cfg.ModeThresholds[key]; ok {
			return t
		}
	}
	return cfg.GlyphThresholds
}

// SelectSample chooses or blends fine/coarse samples by confidence weight.
// If fine is below minFineWeight, coarse wins to prevent weak fine data
// from overriding stronger regional evidence. If fine meets fineOnlyWeight,
// fine wins outright to preserve local detail. In the blend band, scalar
// evidence mass uses union semantics because fine and coarse are overlapping
// views in one direction; p50 bins keep the existing local-emphasis shape.
func SelectSample(fine Sample, coarse Sample, minFineWeight float64, fineOnlyWeight float64) Sample {
	coarseCandidate := coarse
	hasFine := sampleHasEvidence(fine)
	hasCoarse := sampleHasEvidence(coarseCandidate)
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
	weight := maxFloat64(fine.Weight, coarseCandidate.Weight)
	if weight <= 0 {
		return Sample{}
	}
	return Sample{
		Weight:                 weight,
		AgeSec:                 weightedFineCoarseSampleAge(fine, coarseCandidate),
		Count:                  maxCount(fine.Count, coarseCandidate.Count),
		ObservationCount:       maxCount(sampleObservationCount(fine), sampleObservationCount(coarseCandidate)),
		RawCount:               maxCount(fine.RawCount, coarseCandidate.RawCount),
		RawWeight:              maxFloat64(fine.RawWeight, coarseCandidate.RawWeight),
		CappedCount:            maxCount(fine.CappedCount, coarseCandidate.CappedCount),
		CappedWeight:           maxFloat64(fine.CappedWeight, coarseCandidate.CappedWeight),
		CappedReceiverCount:    maxCount(fine.CappedReceiverCount, coarseCandidate.CappedReceiverCount),
		CappedReceiverCapacity: maxCount(fine.CappedReceiverCapacity, coarseCandidate.CappedReceiverCapacity),
		CapLimited:             fine.CapLimited || coarseCandidate.CapLimited,
	}
}

func selectSampleWithDistribution(fine sampleWithBins, coarse sampleWithBins, minFineWeight float64, fineOnlyWeight float64) sampleWithBins {
	coarseCandidate := coarse
	hasFine := sampleHasEvidence(fine.Sample)
	hasCoarse := sampleHasEvidence(coarseCandidate.Sample)
	switch {
	case hasFine && !hasCoarse:
		return fine
	case !hasFine && hasCoarse:
		return coarseCandidate
	case !hasFine && !hasCoarse:
		return sampleWithBins{}
	}
	if fineOnlyWeight > 0 && fine.Weight >= fineOnlyWeight {
		return fine
	}
	if minFineWeight > 0 && fine.Weight < minFineWeight {
		return coarseCandidate
	}
	sample := SelectSample(fine.Sample, coarseCandidate.Sample, minFineWeight, fineOnlyWeight)
	if !sampleHasEvidence(sample) {
		return sampleWithBins{}
	}
	var bins snrHistogram
	p50DB, hasP50 := 0.0, false
	if fine.HasP50 || coarseCandidate.HasP50 {
		bins.addScaled(fine.snrBins, 1)
		bins.addScaled(coarseCandidate.snrBins, 1)
		p50DB, hasP50 = bins.p50DB()
	}
	return sampleWithBins{
		Sample:  sample,
		P50DB:   p50DB,
		HasP50:  hasP50,
		snrBins: bins,
	}
}

func maxCount(left uint32, right uint32) uint32 {
	if left >= right {
		return left
	}
	return right
}

func maxFloat64(left float64, right float64) float64 {
	if left >= right {
		return left
	}
	return right
}

func saturatingAddCounts(left uint32, right uint32) uint32 {
	if maxBucketObservationCount-left < right {
		return maxBucketObservationCount
	}
	return left + right
}

func weightedFineCoarseSampleAge(fine Sample, coarse Sample) int64 {
	fineWeight := fine.Weight
	complementWeight := coarse.Weight - fine.Weight
	if complementWeight < 0 {
		complementWeight = 0
	}
	total := fineWeight + complementWeight
	if total <= 0 {
		return 0
	}
	fineAge := fine.AgeSec
	if fineAge < 0 {
		fineAge = 0
	}
	coarseAge := coarse.AgeSec
	if coarseAge < 0 {
		coarseAge = 0
	}
	age := (float64(fineAge)*fineWeight + float64(coarseAge)*complementWeight) / total
	if age <= 0 {
		return 0
	}
	return int64(math.Ceil(age))
}

func weightedSampleAge(left Sample, right Sample) int64 {
	total := left.Weight + right.Weight
	if total <= 0 {
		return 0
	}
	leftAge := left.AgeSec
	if leftAge < 0 {
		leftAge = 0
	}
	rightAge := right.AgeSec
	if rightAge < 0 {
		rightAge = 0
	}
	age := (float64(leftAge)*left.Weight + float64(rightAge)*right.Weight) / total
	if age <= 0 {
		return 0
	}
	return int64(math.Ceil(age))
}
