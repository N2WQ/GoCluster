package pathreliability

import (
	"math"
	"testing"
	"time"
)

func accountingSelectionSample(db, mass float64, count uint32) sampleWithBins {
	var bins snrHistogram
	if mass > 0 {
		bins.add(snrHistogramBinIndex(db), mass)
	}
	p50, hasP50 := bins.p50DB()
	return sampleWithBins{
		Sample: Sample{Weight: mass, RawWeight: mass, Count: count, RawCount: count,
			ObservationCount: count, SamplingUnix: 20, HasSamplingTime: true},
		P50DB: p50, HasP50: hasP50, snrBins: bins,
	}
}

func TestAccountingSelectionRequiredDistributionFailurePreservesDiagnostics(t *testing.T) {
	for _, failure := range []string{"empty", "nonfinite bins", "flagged nonfinite mass"} {
		t.Run(failure, func(t *testing.T) {
			fine := accountingSelectionSample(-20, 6, 6)
			coarse := accountingSelectionSample(10, 10, 10)
			switch failure {
			case "empty":
				coarse.snrBins = snrHistogram{}
				coarse.HasP50 = false
			case "nonfinite bins":
				coarse.snrBins[snrHistogramBinIndex(10)] = float32(math.Inf(1))
			case "flagged nonfinite mass":
				coarse.Weight = math.NaN()
				coarse.distributionUnavailable = true
			}
			got := selectSampleWithDistribution(fine, coarse, 5, 20)
			if got.HasP50 || !got.distributionUnavailable || got.snrBins != (snrHistogram{}) {
				t.Fatalf("required failure exposed a partial median: %+v", got)
			}
			if got.RawCount != 10 || got.ObservationCount != 10 || !got.HasSamplingTime || got.SamplingUnix != 20 {
				t.Fatalf("failure lost selected diagnostics: %+v", got.Sample)
			}
		})
	}
}

func TestAccountingSelectionExcludedDistributionDoesNotPoison(t *testing.T) {
	badFine := accountingSelectionSample(-20, 1, 1)
	badFine.distributionUnavailable = true
	coarse := accountingSelectionSample(10, 10, 10)
	regional := selectSampleWithDistribution(badFine, coarse, 5, 20)
	if !regional.HasP50 || regional.distributionUnavailable || regional.P50DB != 10.5 {
		t.Fatalf("excluded fine distribution poisoned coarse selection: %+v", regional)
	}

	fine := accountingSelectionSample(-20, 25, 25)
	badCoarse := accountingSelectionSample(10, 100, 100)
	badCoarse.distributionUnavailable = true
	local := selectSampleWithDistribution(fine, badCoarse, 5, 25)
	if !local.HasP50 || local.distributionUnavailable || local.P50DB != -19.5 {
		t.Fatalf("excluded coarse distribution poisoned fine-only selection: %+v", local)
	}
}

func TestAccountingSelectionCountOnlyRetainsCountsAgeAndEpoch(t *testing.T) {
	fine := Sample{Count: 4, RawCount: 4, ObservationCount: 4, AgeSec: 20, HasSamplingTime: true}
	coarse := Sample{Count: 9, RawCount: 9, ObservationCount: 9, AgeSec: 30, HasSamplingTime: true}
	got := selectSampleWithDistribution(sampleWithBins{Sample: fine}, sampleWithBins{Sample: coarse}, 0, 20)
	if got.Weight != 0 || got.HasP50 || got.distributionUnavailable || got.RawCount != 9 || got.ObservationCount != 9 || got.AgeSec != 30 {
		t.Fatalf("count-only selection changed diagnostic semantics: %+v", got)
	}
	if !got.HasSamplingTime || got.SamplingUnix != 0 {
		t.Fatalf("Unix epoch was treated as absent sampling time: %+v", got.Sample)
	}
	merged, ok := mergeSamplesWithDistribution(got, sampleWithBins{Sample: fine}, DefaultConfig(), 0)
	if !ok || merged.RawCount != 13 || merged.ObservationCount != 13 || merged.HasP50 || merged.AgeSec != 30 || !merged.HasSamplingTime {
		t.Fatalf("count-only directions lost diagnostics: ok=%v result=%+v", ok, merged)
	}
}

func TestAccountingSelectionPositiveOppositeFailureCannotBecomeHint(t *testing.T) {
	cfg := DefaultConfig()
	valid := accountingSelectionSample(10, 4, 4)
	for _, failure := range []string{"empty", "flagged", "nonfinite mass", "scaled underflow"} {
		t.Run(failure, func(t *testing.T) {
			bad := accountingSelectionSample(-20, 2, 2)
			switch failure {
			case "empty":
				bad.snrBins = snrHistogram{}
				bad.HasP50 = false
			case "flagged":
				bad.distributionUnavailable = true
			case "nonfinite mass":
				bad.Weight = math.NaN()
				bad.distributionUnavailable = true
			case "scaled underflow":
				bad = accountingSelectionSample(-20, float64(math.SmallestNonzeroFloat32), 2)
				cfg.MergeReceiveWeight = .25
				cfg.MergeTransmitWeight = .75
			}
			got, ok := mergeSamplesWithDistribution(bad, valid, cfg, 0)
			if !ok || got.HasP50 || !got.distributionUnavailable || got.RawCount != 6 || !got.HasSamplingTime {
				t.Fatalf("failed direction became a surviving-leg median: ok=%v result=%+v", ok, got)
			}
		})
	}

	countOnly := sampleWithBins{Sample: Sample{Count: 3, RawCount: 3, ObservationCount: 3, SamplingUnix: 20, HasSamplingTime: true}}
	hint, ok := mergeSamplesWithDistribution(countOnly, valid, DefaultConfig(), 0)
	if !ok || !hint.HasP50 || hint.distributionUnavailable || hint.Weight != 2 || hint.P50DB != 10.5 || hint.RawCount != 7 {
		t.Fatalf("genuinely empty direction prevented the fixed reverse hint: ok=%v result=%+v", ok, hint)
	}
}

func TestAccountingSelectionNoiseShiftOverflowUnavailable(t *testing.T) {
	receive := accountingSelectionSample(-20, 1, 1)
	receive.snrBins = snrHistogram{}
	receive.snrBins[1], receive.snrBins[2] = math.MaxFloat32, math.MaxFloat32
	transmit := accountingSelectionSample(10, 1, 1)
	got, ok := mergeSamplesWithDistribution(receive, transmit, DefaultConfig(), 100)
	if !ok || got.HasP50 || !got.distributionUnavailable || got.RawCount != 2 {
		t.Fatalf("unsafe shifted distribution produced a partial median: ok=%v result=%+v", ok, got)
	}
}

func TestAccountingSelectionWeightedAgesSaturateWithoutOverflow(t *testing.T) {
	for _, tc := range []struct {
		name        string
		left, right Sample
		want        int64
	}{
		{name: "finite weighted numerator overflow", left: Sample{Weight: math.MaxFloat64, AgeSec: 100}, right: Sample{Weight: math.MaxFloat64, AgeSec: 10}, want: 55},
		{name: "int64 ceiling", left: Sample{Weight: 1, AgeSec: math.MaxInt64}, right: Sample{Weight: 1, AgeSec: math.MaxInt64}, want: math.MaxInt64},
		{name: "count-only", left: Sample{AgeSec: 20}, right: Sample{AgeSec: 30}, want: 30},
		{name: "negative ages", left: Sample{Weight: 1, AgeSec: -1}, right: Sample{Weight: 1, AgeSec: 10}, want: 5},
		{name: "invalid weights diagnostic", left: Sample{Weight: math.NaN(), AgeSec: 10}, right: Sample{Weight: 1, AgeSec: 20}, want: 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := weightedSampleAge(tc.left, tc.right); got != tc.want {
				t.Fatalf("weighted age=%d, want %d", got, tc.want)
			}
		})
	}

}

func TestAccountingSelectionPredictorUsesOneReferenceAcrossDirections(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionOff
	cfg.BandHalfLifeSec = map[string]int{"20m": 10}
	cfg.StaleAfterHalfLifeMultiplier = 100
	cfg.MaxPredictionAgeHalfLifeMultiplier = 0
	cfg.MinEffectiveWeight = .01
	cfg.MinObservationCount = 1
	cfg.BeaconMinObservationCount = 1
	p := NewPredictor(cfg, []string{"20m"})
	start := time.Unix(0, 0).UTC()
	p.Update(BucketCombined, 1, 2, InvalidCell, InvalidCell, "20m", -20, 1, ObservationTimes{ObservedAt: start, ArrivedAt: start}, false)
	later := start.Add(20 * time.Second)
	p.Update(BucketCombined, 2, 1, InvalidCell, InvalidCell, "20m", 10, 1, ObservationTimes{ObservedAt: later, ArrivedAt: later}, false)
	query := start.Add(10 * time.Second)
	got := p.Predict(1, 2, InvalidCell, InvalidCell, "20m", "FT8", 0, query)
	// The receive unit has aged two half-lives at T=20; transmit is fresh.
	if math.Abs(got.Weight-.55) > 1e-12 || got.AgeSec != 4 || !got.HasObservedSamplingTime || got.ObservedSamplingUnix != 20 || !got.ObservedDistributionAvailable || !got.HasP50 {
		t.Fatalf("normal predictor mixed candidate times: %+v", got)
	}
	beacon := p.PredictBeaconReceiveOnlyWithMinObservationCount(1, 2, InvalidCell, InvalidCell, "20m", "FT8", 0, 1, query)
	if math.Abs(beacon.Weight-.5) > 1e-12 || beacon.AgeSec != 10 || beacon.ObservedSamplingUnix != 10 || !beacon.HasObservedSamplingTime || !beacon.ObservedDistributionAvailable {
		t.Fatalf("beacon included the irrelevant reverse candidate clock: %+v", beacon)
	}
}

func TestAccountingSelectionPredictorCountOnlyKeepsObservedReference(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ReceiverContributionMode = mode
			cfg.BandHalfLifeSec = map[string]int{"20m": 1}
			cfg.StaleAfterHalfLifeMultiplier = 1000
			cfg.MaxPredictionAgeHalfLifeMultiplier = 0
			cfg.MinObservationCount = 1
			p := NewPredictor(cfg, []string{"20m"})
			observed, arrived := time.Unix(0, 0), time.Unix(160, 0)
			p.UpdateWithReceiverHash(BucketCombined, 1, 2, InvalidCell, InvalidCell, "20m", -20, 1,
				ObservationTimes{ObservedAt: observed, ArrivedAt: arrived}, false, 123)
			got := p.Predict(1, 2, InvalidCell, InvalidCell, "20m", "FT8", 0, arrived)
			if got.RawCount != 1 || got.ObservationCount != 1 || got.AgeSec != 160 || got.Weight != 0 || got.HasP50 || got.ObservedDistributionAvailable || !got.HasObservedSamplingTime || got.ObservedSamplingUnix != 160 {
				t.Fatalf("unusable incoming mass lost raw diagnostics: %+v", got)
			}
		})
	}
}

func TestAccountingSelectionEmptyAndStalePredictionsKeepReference(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionOff
	cfg.BandHalfLifeSec = map[string]int{"20m": 10}
	cfg.StaleAfterHalfLifeMultiplier = 100
	cfg.MaxPredictionAgeHalfLifeMultiplier = 1
	p := NewPredictor(cfg, []string{"20m"})
	epoch := time.Unix(0, 0)
	empty := p.Predict(1, 2, InvalidCell, InvalidCell, "20m", "FT8", 0, epoch)
	if !empty.HasObservedSamplingTime || empty.ObservedSamplingUnix != 0 || empty.ObservedDistributionAvailable || empty.InsufficientReason != InsufficientNoSample {
		t.Fatalf("empty prediction lost the epoch sampling reference: %+v", empty)
	}
	p.Update(BucketCombined, 1, 2, InvalidCell, InvalidCell, "20m", 10, 1, ObservationTimes{ObservedAt: epoch, ArrivedAt: epoch}, false)
	stale := p.Predict(1, 2, InvalidCell, InvalidCell, "20m", "FT8", 0, time.Unix(20, 0))
	if !stale.HasObservedSamplingTime || stale.ObservedSamplingUnix != 20 || stale.ObservedDistributionAvailable || stale.InsufficientReason != InsufficientStale || stale.RawCount != 1 {
		t.Fatalf("freshness rejection lost reference/count diagnostics: %+v", stale)
	}
}

func TestAccountingSelectionPredictorReportingSnapshotMetadata(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionOff
	p := NewPredictor(cfg, []string{"20m", "40m"})
	initial := p.StatsSnapshot()
	if initial.Available || initial.HasSnapshot {
		t.Fatalf("unrefreshed counts presented as available: %+v", initial)
	}
	epoch := time.Unix(0, 0)
	p.Update(BucketCombined, 1, 2, 3, 4, "20m", 10, 1, ObservationTimes{ObservedAt: epoch, ArrivedAt: epoch}, false)
	p.RefreshStatsSnapshot(epoch)
	got := p.StatsSnapshot()
	if !got.Available || !got.HasSnapshot || got.AsOfUnix != 0 || got.Stats.CombinedFine != 1 || got.Stats.CombinedCoarse != 1 || got.Bands[0].Fine != 1 || got.Bands[0].Coarse != 1 {
		t.Fatalf("public counts lost their coherent publication: %+v", got)
	}
	got.Bands[0].Fine = 999
	if p.StatsSnapshot().Bands[0].Fine != 1 {
		t.Fatal("public band counts alias the retained Store snapshot")
	}
	hist := p.WeightHistogramByBand(epoch)
	if !hist.Available || hist.AsOfUnix != 0 || len(hist.Bands) != 2 || hist.Bands[0].Total != 2 {
		t.Fatalf("public weight histogram lost availability/reference: %+v", hist)
	}
}
