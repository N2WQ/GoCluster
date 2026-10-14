package pathreliability

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// This is a declared synthetic corpus, not an estimate of field frequencies.
// Every shape enters through report admission. Classification below inspects
// the final float32 array after selection, decay, and directional merging.
type medianSpeedReport struct {
	Bin       int     `json:"bin"`
	Weight    float64 `json:"weight"`
	HalfLives int     `json:"half_lives"`
}

type medianSpeedShape struct {
	name, category string
	reports        []medianSpeedReport
}

type medianSpeedCase struct {
	path     accountingRuntimePath
	shape    medianSpeedShape
	expected Result
	bits     [50]uint32
}

type medianSpeedCorpus struct {
	predictor *Predictor
	cases     []medianSpeedCase
	base      time.Time
	beacon    bool
}

type medianSpeedManifestCase struct {
	Mode            string              `json:"mode"`
	Beacon          bool                `json:"beacon"`
	FallbackPercent int                 `json:"fallback_percent"`
	Index           int                 `json:"index"`
	Shape           string              `json:"shape"`
	Category        string              `json:"category"`
	PositiveBins    int                 `json:"positive_bins"`
	QuantumSpan     int                 `json:"quantum_span"`
	CeilingLog2     int                 `json:"ceiling_log2"`
	Reports         []medianSpeedReport `json:"reports"`
	FinalBinBits    [50]uint32          `json:"final_bin_bits"`
	Expected        Result              `json:"expected"`
}

func medianSpeedShapes() []medianSpeedShape {
	shapes := []medianSpeedShape{
		{"one_underflow", "one", []medianSpeedReport{{0, 1, 0}}},
		{"one_threshold", "one", []medianSpeedReport{{12, 1, 0}}},
		{"one_overflow", "one", []medianSpeedReport{{49, 1, 0}}},
		{"two_equal", "two", []medianSpeedReport{{5, 1, 0}, {11, 1, 0}}},
		{"two_unequal", "two", []medianSpeedReport{{5, 2, 0}, {35, 1, 0}}},
		{"two_aged", "two", []medianSpeedReport{{5, 1, 56}, {35, 1, 0}}},
		{"certified_three", "certified", []medianSpeedReport{{2, 1, 0}, {19, 1, 0}, {35, 1, 0}}},
		{"certified_dense_equal", "certified", nil},
		{"certified_dense_varying", "certified", nil},
		{"failed_strict", "fallback", []medianSpeedReport{{2, 2, 0}, {19, 1, 56}, {35, 1, 0}}},
		{"failed_true_tie", "fallback", []medianSpeedReport{{5, 1, 0}, {30, 1, 56}, {35, 1, 0}, {45, 1, 56}}},
		{"failed_near_tie", "fallback", []medianSpeedReport{{2, 2, 0}, {19, 1, 56}, {35, 2, 0}}},
	}
	for i := 0; i < 50; i++ {
		shapes[7].reports = append(shapes[7].reports, medianSpeedReport{i, 1, 0})
		shapes[8].reports = append(shapes[8].reports, medianSpeedReport{i, .5 + float64(i%3)*.125, 0})
	}
	return shapes
}

func medianSpeedSelectedShapes(fallbackPercent int) []medianSpeedShape {
	shapes := medianSpeedShapes()
	if fallbackPercent == -1 {
		return shapes // Twelve calls: exactly three in each category.
	}
	selected := make([]medianSpeedShape, 30)
	fallback := fallbackPercent * 30 / 100
	fast := 30 - fallback
	for i := 0; i < fast; i++ {
		// Cycle categories and their variants instead of grouping the timed mix.
		category, variant := i%3, (i/3)%3
		selected[i] = shapes[3*category+variant]
	}
	for i := 0; i < fallback; i++ {
		selected[fast+i] = shapes[9+i%3]
	}
	// A fixed permutation spreads fallback calls throughout the 30-call cycle.
	var mixed [30]medianSpeedShape
	for i := range selected {
		mixed[(i*7)%30] = selected[i]
	}
	return mixed[:]
}

func newMedianSpeedCorpus(tb testing.TB, mode string, beacon bool, fallbackPercent int) *medianSpeedCorpus {
	tb.Helper()
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = mode
	cfg.MinObservationCount, cfg.BeaconMinObservationCount = 1, 1
	cfg.MinEffectiveWeight = .01
	cfg.BandHalfLifeSec = map[string]int{"20m": 240}
	corpus := &medianSpeedCorpus{predictor: NewPredictor(cfg, []string{"20m"}), base: time.Unix(1_700_000_000, 0).UTC(), beacon: beacon}
	for index, shape := range medianSpeedSelectedShapes(fallbackPercent) {
		entry := medianSpeedCase{path: accountingPath(index), shape: shape}
		var literal snrHistogram
		fresh := uint32(0)
		for reportIndex, report := range shape.reports {
			observed := corpus.base.Add(-time.Duration(report.HalfLives) * 240 * time.Second)
			literal[report.Bin] = float32(float64(literal[report.Bin]) + math.Ldexp(report.Weight, -report.HalfLives))
			if report.HalfLives == 0 {
				fresh++
			}
			db := float64(report.Bin) - 25
			if report.Bin == 49 {
				db = 24
			}
			for _, path := range []accountingRuntimePath{entry.path, entry.path.reversed()} {
				admission := corpus.predictor.UpdateWithReceiverHash(BucketCombined, path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", db, report.Weight, ObservationTimes{ObservedAt: observed, ArrivedAt: corpus.base}, false, uint64(reportIndex%12)+1)
				if !admission.Accepted() {
					tb.Fatalf("%s %s admission failed: %+v", mode, shape.name, admission)
				}
			}
		}
		entry.bits = exactP50Snapshot(&literal)
		selected := corpus.sample(entry.path)
		if selected.distributionUnavailable || exactP50Snapshot(&selected.snrBins) != entry.bits {
			tb.Fatalf("%s beacon=%v %s final materialization differs from literal reports: sample=%+v got=%v want=%v", mode, beacon, shape.name, selected.Sample, exactP50Snapshot(&selected.snrBins), entry.bits)
		}
		category, _, _, _ := medianSpeedCategory(selected.snrBins)
		if category != shape.category {
			tb.Fatalf("%s final category=%s want=%s", shape.name, category, shape.category)
		}
		wantDB, available := exactP50Reference(selected.snrBins)
		if !available {
			tb.Fatalf("%s lacks rational quantile", shape.name)
		}
		result := corpus.predict(entry.path)
		multiplier := uint32(2)
		if beacon {
			multiplier = 1
		}
		wantRaw := uint32(len(shape.reports)) * multiplier
		wantCount := wantRaw
		if mode == ReceiverContributionEnforce {
			wantCount = fresh * multiplier
		}
		wantCapped := wantRaw
		if mode != ReceiverContributionOff {
			wantCapped = fresh * multiplier
		}
		wantClass, wantGlyph := medianSpeedFT8Class(wantDB, corpus.predictor.cfg)
		if !result.HasP50 || result.P50DB != wantDB || result.Class != wantClass || result.Glyph != wantGlyph || result.P50Glyph != wantGlyph || result.Source != SourceCombined || result.InsufficientReason != InsufficientNone || !result.ObservedDistributionAvailable || !result.HasObservedSamplingTime || result.ObservedSamplingUnix != corpus.base.Unix() || result.AgeSec != 0 || result.RawCount != wantRaw || result.ObservationCount != wantRaw || result.Count != wantCount || result.CappedCount != wantCapped || result.BeaconRX != beacon || result.Weight < cfg.MinEffectiveWeight || (mode == ReceiverContributionEnforce && result.ReceiverCount < result.ReceiverRequired) {
			tb.Fatalf("%s beacon=%v %s: %+v want db/class/glyph=%g/%s/%s counts=%d/%d/%d", mode, beacon, shape.name, result, wantDB, wantClass, wantGlyph, wantRaw, wantCount, wantCapped)
		}
		entry.expected = result
		corpus.cases = append(corpus.cases, entry)
	}
	return corpus
}

func (corpus *medianSpeedCorpus) predict(path accountingRuntimePath) Result {
	if corpus.beacon {
		return corpus.predictor.PredictBeaconReceiveOnlyWithMinObservationCount(path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", "FT8", 0, 1, corpus.base)
	}
	return corpus.predictor.Predict(path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", "FT8", 0, corpus.base)
}

func (corpus *medianSpeedCorpus) sample(path accountingRuntimePath) sampleWithBins {
	var sample sampleWithBins
	if corpus.beacon {
		sample, _, _ = corpus.predictor.receiveFromStoreWithDistribution(corpus.predictor.combined, path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", 0, corpus.base)
	} else {
		sample, _, _ = corpus.predictor.mergeFromStoreWithDistribution(corpus.predictor.combined, path.receiver, path.sender, path.receiverCoarse, path.senderCoarse, "20m", 0, corpus.base)
	}
	return sample
}

// The fixture certificate uses an explicit loop for the integer ceiling. It
// does not depend on any candidate-only median helpers or their dispatch.
func medianSpeedCategory(histogram snrHistogram) (category string, n, span, ceiling int) {
	minimum, maximum := 255, 0
	for _, mass := range histogram {
		if mass <= 0 {
			continue
		}
		n++
		shift := int((math.Float32bits(mass)>>23)&255) - 1
		if shift < 0 {
			shift = 0
		}
		minimum, maximum = min(minimum, shift), max(maximum, shift)
	}
	span = maximum - minimum
	for power := 1; power < n; power *= 2 {
		ceiling++
	}
	switch {
	case n == 1:
		category = "one"
	case n == 2:
		category = "two"
	case n >= 3 && 24+span+ceiling <= 53:
		category = "certified"
	default:
		category = "fallback"
	}
	return category, n, span, ceiling
}

func medianSpeedFT8Class(db float64, cfg Config) (class, glyph string) {
	thresholds := cfg.ModeThresholds["FT8"]
	switch {
	case db >= thresholds.High:
		return "HIGH", cfg.GlyphSymbols.High
	case db >= thresholds.Medium:
		return "MEDIUM", cfg.GlyphSymbols.Medium
	case db >= thresholds.Low:
		return "LOW", cfg.GlyphSymbols.Low
	default:
		return "UNLIKELY", cfg.GlyphSymbols.Unlikely
	}
}

func TestMedianSpeedCorpus(t *testing.T) {
	var manifest []medianSpeedManifestCase
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		for _, beacon := range []bool{false, true} {
			for _, percent := range []int{-1, 0, 10, 50, 100} {
				t.Run(fmt.Sprintf("%s/beacon=%v/fallback=%d", mode, beacon, percent), func(t *testing.T) {
					corpus := newMedianSpeedCorpus(t, mode, beacon, percent)
					counts := make(map[string]int)
					for index, entry := range corpus.cases {
						before := corpus.sample(entry.path)
						if got := corpus.predict(entry.path); got != entry.expected {
							t.Fatalf("frozen prediction drifted: got=%+v want=%+v", got, entry.expected)
						}
						after := corpus.sample(entry.path)
						if exactP50Snapshot(&before.snrBins) != entry.bits || exactP50Snapshot(&after.snrBins) != entry.bits {
							t.Fatal("frozen query changed final histogram")
						}
						category, n, span, ceiling := medianSpeedCategory(before.snrBins)
						counts[category]++
						manifest = append(manifest, medianSpeedManifestCase{mode, beacon, percent, index, entry.shape.name, category, n, span, ceiling, entry.shape.reports, entry.bits, entry.expected})
						if allocations := testing.AllocsPerRun(100, func() { corpus.predict(entry.path) }); allocations != 0 {
							t.Fatalf("%s %s allocates %g per prediction", mode, entry.shape.name, allocations)
						}
					}
					if percent == -1 {
						for _, category := range []string{"one", "two", "certified", "fallback"} {
							if counts[category] != 3 {
								t.Fatalf("primary category %s has %d calls, want3", category, counts[category])
							}
						}
					} else if counts["fallback"]*100 != percent*len(corpus.cases) {
						t.Fatalf("fallback mix counts=%v expected=%d%%", counts, percent)
					}
					if allocations := testing.AllocsPerRun(100, func() {
						for _, entry := range corpus.cases {
							corpus.predict(entry.path)
						}
					}); allocations != 0 {
						t.Fatalf("complete corpus allocates %g", allocations)
					}
				})
			}
		}
	}
	if target := os.Getenv("GOCLUSTER_MEDIAN_SPEED_MANIFEST"); target != "" {
		absolute, err := filepath.Abs(target)
		if err != nil || filepath.Base(filepath.Dir(absolute)) != "path-median-speed-v6" || filepath.Base(filepath.Dir(filepath.Dir(absolute))) != ".tmp" {
			t.Fatal("manifest must have a task-owned ignored .tmp/path-median-speed-v6 parent")
		}
		data, err := json.MarshalIndent(manifest, "", "  ")
		if err == nil {
			err = os.WriteFile(absolute, data, 0600)
		}
		if err != nil {
			t.Fatalf("write corpus manifest: %v", err)
		}
	}
}
