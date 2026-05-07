package spot

import (
	"sync"
	"testing"
)

func resetDistanceWeightsForTest() {
	ConfigureMorseWeights(defaultDistanceInsertCost, defaultDistanceDeleteCost, defaultDistanceSubCost, defaultDistanceScale)
	ConfigureBaudotWeights(defaultDistanceInsertCost, defaultDistanceDeleteCost, defaultDistanceSubCost, defaultDistanceScale)
}

func TestConfigureDistanceWeightsPublishesCompleteSnapshots(t *testing.T) {
	resetDistanceWeightsForTest()
	t.Cleanup(resetDistanceWeightsForTest)

	defaultWeights := normalizeDistanceWeightSet(defaultDistanceInsertCost, defaultDistanceDeleteCost, defaultDistanceSubCost, defaultDistanceScale)
	if got, want := CallDistance("E", "T", "CW", "morse", "baudot"), weightedPatternCost(morseCodes['E'], morseCodes['T'], defaultWeights); got != want {
		t.Fatalf("expected default Morse distance %d, got %d", want, got)
	}
	if got, want := CallDistance("E", "T", "RTTY", "morse", "baudot"), weightedPatternCost(baudotCodes['E'], baudotCodes['T'], defaultWeights); got != want {
		t.Fatalf("expected default Baudot distance %d, got %d", want, got)
	}

	morseWeights := normalizeDistanceWeightSet(1, 1, 4, 2)
	ConfigureMorseWeights(morseWeights.ins, morseWeights.del, morseWeights.sub, morseWeights.scale)
	morseSnapshot := loadMorseDistanceSnapshot()
	if got := morseSnapshot.weights; got != morseWeights {
		t.Fatalf("expected custom Morse weights %+v, got %+v", morseWeights, got)
	}
	if got, want := weightedRuneDist('E', 'T', morseSnapshot.runeIndex, morseSnapshot.costTable), weightedPatternCost(morseCodes['E'], morseCodes['T'], morseWeights); got != want {
		t.Fatalf("expected custom Morse rune cost %d, got %d", want, got)
	}

	baudotWeights := normalizeDistanceWeightSet(2, 3, 5, 4)
	ConfigureBaudotWeights(baudotWeights.ins, baudotWeights.del, baudotWeights.sub, baudotWeights.scale)
	baudotSnapshot := loadBaudotDistanceSnapshot()
	if got := baudotSnapshot.weights; got != baudotWeights {
		t.Fatalf("expected custom Baudot weights %+v, got %+v", baudotWeights, got)
	}
	if got, want := weightedRuneDist('E', 'T', baudotSnapshot.runeIndex, baudotSnapshot.costTable), weightedPatternCost(baudotCodes['E'], baudotCodes['T'], baudotWeights); got != want {
		t.Fatalf("expected custom Baudot rune cost %d, got %d", want, got)
	}
}

func TestConfigureDistanceWeightsConcurrentReaders(t *testing.T) {
	resetDistanceWeightsForTest()
	t.Cleanup(resetDistanceWeightsForTest)

	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					_ = CallDistance("DL6LD", "DL6LN", "CW", "morse", "baudot")
					_ = CallDistance("RY1ABC", "RY1ABD", "RTTY", "morse", "baudot")
				}
			}
		}()
	}

	for i := 0; i < 250; i++ {
		ConfigureMorseWeights(1+i%3, 1+(i+1)%3, 2+i%4, 2+i%2)
		ConfigureBaudotWeights(1+(i+2)%3, 1+i%3, 2+(i+1)%4, 2+i%2)
	}
	close(done)
	wg.Wait()
}

func TestWeightedPatternCostEmptyPatternsUseFullInsertDeleteCost(t *testing.T) {
	weights := distanceWeightSet{ins: 2, del: 3, sub: 5, scale: 4}
	if got := weightedPatternCost("", "", weights); got != 0 {
		t.Fatalf("expected empty-to-empty cost 0, got %d", got)
	}
	if got := weightedPatternCost("", "abc", weights); got != 6 {
		t.Fatalf("expected three inserts to cost 6 after scaling, got %d", got)
	}
	if got := weightedPatternCost("abc", "", weights); got != 9 {
		t.Fatalf("expected three deletes to cost 9 after scaling, got %d", got)
	}
}

func TestNormalizeCorrectionBayesBonusPolicyConfiguredPreservesNeutralZeros(t *testing.T) {
	got := normalizeCorrectionBayesBonusPolicy(CorrectionBayesBonusPolicy{
		Configured: true,
		Enabled:    true,

		WeightedSmoothingMilli: 1000,
		RecentSmoothing:        2,
		ObsLogCapMilli:         350,
		PriorLogMaxMilli:       600,

		ReportThresholdDistance1Milli:    450,
		ReportThresholdDistance2Milli:    650,
		AdvantageThresholdDistance1Milli: 700,
		AdvantageThresholdDistance2Milli: 950,
	})

	if got.WeightDistance1Milli != 0 || got.WeightDistance2Milli != 0 {
		t.Fatalf("expected configured distance weights to preserve zero, got %d/%d", got.WeightDistance1Milli, got.WeightDistance2Milli)
	}
	if got.PriorLogMinMilli != 0 {
		t.Fatalf("expected configured prior min to preserve zero, got %d", got.PriorLogMinMilli)
	}
	if got.AdvantageMinWeightedDeltaDistance1Milli != 0 || got.AdvantageMinWeightedDeltaDistance2Milli != 0 {
		t.Fatalf("expected configured weighted deltas to preserve zero, got %d/%d",
			got.AdvantageMinWeightedDeltaDistance1Milli,
			got.AdvantageMinWeightedDeltaDistance2Milli)
	}
	if got.AdvantageExtraConfidenceDistance1 != 0 || got.AdvantageExtraConfidenceDistance2 != 0 {
		t.Fatalf("expected configured extra confidence to preserve zero, got %d/%d",
			got.AdvantageExtraConfidenceDistance1,
			got.AdvantageExtraConfidenceDistance2)
	}
}

func TestNormalizeCorrectionBayesBonusPolicyUnconfiguredDefaultsZeros(t *testing.T) {
	got := normalizeCorrectionBayesBonusPolicy(CorrectionBayesBonusPolicy{})
	if got.WeightDistance1Milli != 350 || got.WeightDistance2Milli != 200 {
		t.Fatalf("expected unconfigured distance defaults 350/200, got %d/%d", got.WeightDistance1Milli, got.WeightDistance2Milli)
	}
	if got.PriorLogMinMilli != -200 {
		t.Fatalf("expected unconfigured prior min default -200, got %d", got.PriorLogMinMilli)
	}
	if got.AdvantageMinWeightedDeltaDistance1Milli != 200 || got.AdvantageMinWeightedDeltaDistance2Milli != 300 {
		t.Fatalf("expected unconfigured weighted delta defaults 200/300, got %d/%d",
			got.AdvantageMinWeightedDeltaDistance1Milli,
			got.AdvantageMinWeightedDeltaDistance2Milli)
	}
	if got.AdvantageExtraConfidenceDistance1 != 3 || got.AdvantageExtraConfidenceDistance2 != 5 {
		t.Fatalf("expected unconfigured extra confidence defaults 3/5, got %d/%d",
			got.AdvantageExtraConfidenceDistance1,
			got.AdvantageExtraConfidenceDistance2)
	}
}
