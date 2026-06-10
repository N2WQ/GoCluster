package pathreliability

import (
	"testing"
	"time"
)

func TestGlyphForDB(t *testing.T) {
	cfg := DefaultConfig()
	if got := GlyphForDB(-12, "FT8", cfg); got != "+" {
		t.Fatalf("expected + for -12 dB, got %q", got)
	}
	if got := GlyphForDB(-16, "FT8", cfg); got != "=" {
		t.Fatalf("expected = for -16 dB, got %q", got)
	}
	if got := GlyphForDB(-20, "FT8", cfg); got != "-" {
		t.Fatalf("expected - for -20 dB, got %q", got)
	}
	if got := GlyphForDB(-30, "FT8", cfg); got != "!" {
		t.Fatalf("expected ! for -30 dB, got %q", got)
	}
	if got := GlyphForDB(-4, "CW", cfg); got != "=" {
		t.Fatalf("expected = for -4 dB in CW thresholds, got %q", got)
	}
}

func TestClassForDB(t *testing.T) {
	cfg := DefaultConfig()
	if got := ClassForDB(-12, "FT8", cfg); got != "HIGH" {
		t.Fatalf("expected HIGH for -12 dB, got %q", got)
	}
	if got := ClassForDB(-16, "FT8", cfg); got != "MEDIUM" {
		t.Fatalf("expected MEDIUM for -16 dB, got %q", got)
	}
	if got := ClassForDB(-20, "FT8", cfg); got != "LOW" {
		t.Fatalf("expected LOW for -20 dB, got %q", got)
	}
	if got := ClassForDB(-30, "FT8", cfg); got != "UNLIKELY" {
		t.Fatalf("expected UNLIKELY for -30 dB, got %q", got)
	}
}

func TestGlyphForDBUsesCustomSymbols(t *testing.T) {
	cfg := DefaultConfig()
	cfg.GlyphSymbols = GlyphSymbols{
		High:         "H",
		Medium:       "M",
		Low:          "L",
		Unlikely:     "U",
		Insufficient: "I",
	}
	if got := GlyphForDB(-12, "FT8", cfg); got != "H" {
		t.Fatalf("expected H for -12 dB, got %q", got)
	}
	if got := GlyphForDB(-16, "FT8", cfg); got != "M" {
		t.Fatalf("expected M for -16 dB, got %q", got)
	}
	if got := GlyphForDB(-20, "FT8", cfg); got != "L" {
		t.Fatalf("expected L for -20 dB, got %q", got)
	}
	if got := GlyphForDB(-30, "FT8", cfg); got != "U" {
		t.Fatalf("expected U for -30 dB, got %q", got)
	}
}

func TestMergeSamplesWeightedMetadata(t *testing.T) {
	cfg := DefaultConfig()
	receive := Sample{Weight: 10, Count: 3}
	transmit := Sample{Weight: 4, Count: 2}
	merged, ok := mergeSamples(receive, transmit, cfg)
	if !ok {
		t.Fatalf("expected merge ok")
	}
	if merged.Weight <= 0 {
		t.Fatalf("expected positive merged weight")
	}
	if merged.AgeSec != 0 {
		t.Fatalf("expected merged age 0, got %d", merged.AgeSec)
	}
	if merged.Count != 5 {
		t.Fatalf("expected merged count 5, got %d", merged.Count)
	}
}

func TestMergeSamplesAgeUsesDirectionalWeights(t *testing.T) {
	cfg := DefaultConfig()
	receive := Sample{Weight: 10, AgeSec: 100, Count: 10}
	transmit := Sample{Weight: 10, AgeSec: 10, Count: 10}
	balanced, ok := mergeSamples(receive, transmit, cfg)
	if !ok {
		t.Fatalf("expected balanced merge")
	}
	receive.Weight = 5
	lighterReceive, ok := mergeSamples(receive, transmit, cfg)
	if !ok {
		t.Fatalf("expected lighter receive merge")
	}
	if lighterReceive.AgeSec >= balanced.AgeSec {
		t.Fatalf("expected lower receive weight to move age toward transmit, balanced=%d lighter=%d", balanced.AgeSec, lighterReceive.AgeSec)
	}
}

func TestSelectSampleMinFineWeight(t *testing.T) {
	fine := Sample{Weight: 2, AgeSec: 12, Count: 2}
	coarse := Sample{Weight: 10, AgeSec: 30, Count: 7}
	got := SelectSample(fine, coarse, 5, 20)
	if got.Weight != coarse.Weight || got.Count != coarse.Count {
		t.Fatalf("expected coarse when fine below min, got weight=%v count=%d", got.Weight, got.Count)
	}

	fine = Sample{Weight: 6, AgeSec: 10, Count: 3}
	coarse = Sample{Weight: 10, AgeSec: 20, Count: 8}
	got = SelectSample(fine, coarse, 5, 20)
	if got.Weight != coarse.Weight {
		t.Fatalf("expected blended weight to use larger fine/coarse layer %v, got %v", coarse.Weight, got.Weight)
	}
	if got.AgeSec != 14 {
		t.Fatalf("expected blended union effective age 14, got %d", got.AgeSec)
	}
	if got.Count != 8 {
		t.Fatalf("expected blended count to use larger selected layer count 8, got %d", got.Count)
	}

	fine = Sample{Weight: 2, AgeSec: 5, Count: 4}
	got = SelectSample(fine, Sample{}, 5, 20)
	if got.Weight != fine.Weight || got.Count != fine.Count {
		t.Fatalf("expected fine sample when coarse missing, got weight=%v count=%d", got.Weight, got.Count)
	}
}

func TestSelectSampleUsesUnionWeightForOverlappingFineCoarse(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionOff
	store := NewStore(cfg, []string{"20m"})
	receiverCell := CellID(1)
	senderCell := CellID(2)
	receiverCoarse := CellID(3)
	senderCoarse := CellID(4)
	now := time.Unix(1_700_000_000, 0).UTC()

	for i := 0; i < 6; i++ {
		store.UpdateWithReceiverHash(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", -12, 1, now, ReceiverIdentityHash("K1ABC"))
	}

	fine, coarse := store.Lookup(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", now)
	got := SelectSample(fine, coarse, cfg.MinFineWeight, cfg.FineOnlyWeight)
	if got.Weight != coarse.Weight {
		t.Fatalf("selected weight=%v, want coarse union weight %v", got.Weight, coarse.Weight)
	}
	if got.RawWeight != coarse.RawWeight {
		t.Fatalf("selected raw weight=%v, want coarse union raw weight %v", got.RawWeight, coarse.RawWeight)
	}
	if got.CappedWeight != coarse.CappedWeight {
		t.Fatalf("selected capped weight=%v, want coarse union capped weight %v", got.CappedWeight, coarse.CappedWeight)
	}
	if got.Count != coarse.Count {
		t.Fatalf("selected count=%d, want coarse union count %d", got.Count, coarse.Count)
	}
}

func TestSelectSampleWeightUnionPreservesP50Shape(t *testing.T) {
	var fineBins snrHistogram
	fineBins.add(snrHistogramBinIndex(18), 6)
	fine := sampleWithBins{
		Sample:  Sample{Weight: 6, RawWeight: 6, CappedWeight: 4, Count: 6},
		P50DB:   18.5,
		HasP50:  true,
		snrBins: fineBins,
	}
	var coarseBins snrHistogram
	coarseBins.add(snrHistogramBinIndex(-20), 10)
	coarse := sampleWithBins{
		Sample:  Sample{Weight: 10, RawWeight: 10, CappedWeight: 7, Count: 10},
		P50DB:   -19.5,
		HasP50:  true,
		snrBins: coarseBins,
	}

	got := selectSampleWithDistribution(fine, coarse, 5, 20)
	var wantBins snrHistogram
	wantBins.addScaled(fineBins, 1)
	wantBins.addScaled(coarseBins, 1)
	wantP50, wantHasP50 := wantBins.p50DB()
	if got.Weight != 10 || got.RawWeight != 10 || got.CappedWeight != 7 {
		t.Fatalf("selected weights = %v/%v/%v, want 10/10/7", got.Weight, got.RawWeight, got.CappedWeight)
	}
	if got.P50DB != wantP50 || got.HasP50 != wantHasP50 {
		t.Fatalf("p50=%v has=%v, want p50=%v has=%v", got.P50DB, got.HasP50, wantP50, wantHasP50)
	}
}

func TestSelectSampleCappedWeightUsesConservativeMax(t *testing.T) {
	fine := Sample{Weight: 6, CappedWeight: 6, Count: 6, CappedCount: 6, CapLimited: true}
	coarse := Sample{Weight: 10, CappedWeight: 3, Count: 10, CappedCount: 3, CapLimited: true}
	got := SelectSample(fine, coarse, 5, 20)
	if got.Weight != 10 {
		t.Fatalf("selected active weight=%v, want 10", got.Weight)
	}
	if got.CappedWeight != 6 {
		t.Fatalf("selected capped weight=%v, want conservative max 6", got.CappedWeight)
	}
	if got.CappedCount != 6 {
		t.Fatalf("selected capped count=%d, want max 6", got.CappedCount)
	}
}

func TestPredictWeightUnionCanConservativelyWithholdOutsideInvariant(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionOff
	cfg.MinEffectiveWeight = 3
	cfg.MinObservationCount = 1
	cfg.MinFineWeight = 5
	cfg.FineOnlyWeight = 20
	cfg.ReverseHintDiscount = 0.5
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Unix(1_700_000_000, 0).UTC()

	for i := 0; i < 5; i++ {
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -12, 1, now, false, ReceiverIdentityHash("K1ABC"))
	}

	res := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now)
	if res.Source != SourceInsufficient || res.InsufficientReason != InsufficientLowWeight {
		t.Fatalf("expected conservative low-weight withholding, got source=%v reason=%v weight=%v", res.Source, res.InsufficientReason, res.Weight)
	}
	if res.Weight >= cfg.MinEffectiveWeight {
		t.Fatalf("expected selected weight below tuned floor, got %v >= %v", res.Weight, cfg.MinEffectiveWeight)
	}
}

func TestFineCoarseUnionAgeCanTripFreshnessGate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BandHalfLifeSec = map[string]int{"20m": 360}
	cfg.MaxPredictionAgeHalfLifeMultiplier = 1.5
	predictor := NewPredictor(cfg, []string{"20m"})

	selected := SelectSample(
		Sample{Weight: 5, AgeSec: 650, Count: 5},
		Sample{Weight: 6, AgeSec: 60, Count: 6},
		5,
		20,
	)
	if selected.AgeSec <= 540 {
		t.Fatalf("selected age=%d, want above 540s freshness gate", selected.AgeSec)
	}
	receive, _, dropped, _ := predictor.applyFreshnessGate(predictor.combined, "20m", selected, Sample{})
	if !dropped || sampleHasEvidence(receive) {
		t.Fatalf("expected union-aged fine/coarse sample to be stale-dropped, dropped=%v receive=%+v", dropped, receive)
	}
}

func TestFineCoarseUnionAgeDirectionDropCanShiftClass(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BandHalfLifeSec = map[string]int{"20m": 360}
	cfg.MaxPredictionAgeHalfLifeMultiplier = 1.5
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 1
	predictor := NewPredictor(cfg, []string{"20m"})

	var receiveBins snrHistogram
	receiveBins.add(snrHistogramBinIndex(-22), 6)
	receive := sampleWithBins{
		Sample:  Sample{Weight: 6, AgeSec: 600, Count: 6},
		P50DB:   -21.5,
		HasP50:  true,
		snrBins: receiveBins,
	}
	var transmitBins snrHistogram
	transmitBins.add(snrHistogramBinIndex(10), 6)
	transmit := sampleWithBins{
		Sample:  Sample{Weight: 6, AgeSec: 10, Count: 6},
		P50DB:   10.5,
		HasP50:  true,
		snrBins: transmitBins,
	}

	both, ok := mergeSamplesWithDistribution(receive, transmit, cfg, 0)
	if !ok {
		t.Fatalf("expected both directions to merge")
	}
	bothClass := ClassForDB(both.P50DB, "FT8", cfg)
	receiveSample, transmitSample, dropped, _ := predictor.applyFreshnessGate(predictor.combined, "20m", receive.Sample, transmit.Sample)
	if !dropped || sampleHasEvidence(receiveSample) {
		t.Fatalf("expected stale receive direction to be dropped, dropped=%v receive=%+v", dropped, receiveSample)
	}
	transmit.Sample = transmitSample
	afterDrop, ok := mergeSamplesWithDistribution(sampleWithBins{}, transmit, cfg, 0)
	if !ok {
		t.Fatalf("expected surviving transmit direction to merge")
	}
	afterClass := ClassForDB(afterDrop.P50DB, "FT8", cfg)
	if bothClass == afterClass {
		t.Fatalf("expected class shift after direction drop, both=%q after=%q p50=%v/%v", bothClass, afterClass, both.P50DB, afterDrop.P50DB)
	}
	if afterClass != classHigh {
		t.Fatalf("expected surviving transmit direction to classify HIGH, got %q", afterClass)
	}
}

func TestSelectSampleFineOnlyThreshold(t *testing.T) {
	fine := Sample{Weight: 25, AgeSec: 10}
	coarse := Sample{Weight: 100, AgeSec: 30}
	got := SelectSample(fine, coarse, 5, 20)
	if got.Weight != fine.Weight {
		t.Fatalf("expected fine-only when fine meets threshold, got weight=%v", got.Weight)
	}
}

func TestPredictUsesInsufficientGlyph(t *testing.T) {
	requireH3Mappings(t)
	cfg := DefaultConfig()
	cfg.GlyphSymbols.Insufficient = "I"
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := EncodeCell("FN31")
	dxCell := EncodeCell("FN32")
	userCoarse := EncodeCoarseCell("FN31")
	dxCoarse := EncodeCoarseCell("FN32")
	res := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, time.Now().UTC())
	if res.Glyph != "I" {
		t.Fatalf("expected insufficient glyph I, got %q", res.Glyph)
	}
	if res.Source != SourceInsufficient {
		t.Fatalf("expected insufficient source, got %v", res.Source)
	}
	if res.InsufficientReason != InsufficientNoSample {
		t.Fatalf("expected no-sample insufficient reason, got %v", res.InsufficientReason)
	}
}

func TestPredictStaleEvidenceInsufficient(t *testing.T) {
	requireH3Mappings(t)
	cfg := DefaultConfig()
	cfg.BandHalfLifeSec = map[string]int{"20m": 10}
	cfg.StaleAfterHalfLifeMultiplier = 100
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 1
	cfg.MaxPredictionAgeHalfLifeMultiplier = 1
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := EncodeCell("FN31")
	dxCell := EncodeCell("FN32")
	userCoarse := EncodeCoarseCell("FN31")
	dxCoarse := EncodeCoarseCell("FN32")
	now := time.Now().UTC()

	predictor.Update(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", 25, 10, now, false)

	fresh := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now.Add(10*time.Second))
	if fresh.Source != SourceCombined {
		t.Fatalf("expected age at cutoff to remain combined, got source=%v reason=%v", fresh.Source, fresh.InsufficientReason)
	}
	if fresh.AgeSec != 10 {
		t.Fatalf("expected fresh result age 10, got %d", fresh.AgeSec)
	}

	stale := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now.Add(11*time.Second))
	if stale.Source != SourceInsufficient {
		t.Fatalf("expected stale prediction to become insufficient, got %v", stale.Source)
	}
	if stale.InsufficientReason != InsufficientStale {
		t.Fatalf("expected stale reason, got %v", stale.InsufficientReason)
	}
}

func TestPredictMaxAgeMultiplierZeroPreservesOldBehavior(t *testing.T) {
	requireH3Mappings(t)
	cfg := DefaultConfig()
	cfg.BandHalfLifeSec = map[string]int{"20m": 10}
	cfg.StaleAfterHalfLifeMultiplier = 100
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 1
	cfg.MaxPredictionAgeHalfLifeMultiplier = 0
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := EncodeCell("FN31")
	dxCell := EncodeCell("FN32")
	userCoarse := EncodeCoarseCell("FN31")
	dxCoarse := EncodeCoarseCell("FN32")
	now := time.Now().UTC()

	predictor.Update(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", 25, 10, now, false)

	res := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now.Add(30*time.Second))
	if res.Source != SourceCombined {
		t.Fatalf("expected disabled freshness gate to preserve combined result, got source=%v reason=%v", res.Source, res.InsufficientReason)
	}
}

func TestPredictDropsOnlyStaleDirection(t *testing.T) {
	requireH3Mappings(t)
	cfg := DefaultConfig()
	cfg.BandHalfLifeSec = map[string]int{"20m": 10}
	cfg.StaleAfterHalfLifeMultiplier = 100
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 1
	cfg.MaxPredictionAgeHalfLifeMultiplier = 1
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := EncodeCell("FN31")
	dxCell := EncodeCell("FN32")
	userCoarse := EncodeCoarseCell("FN31")
	dxCoarse := EncodeCoarseCell("FN32")
	now := time.Now().UTC()

	predictor.Update(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", 25, 10, now.Add(-20*time.Second), false)
	predictor.Update(BucketCombined, dxCell, userCell, dxCoarse, userCoarse, "20m", 10, 1, now, false)

	res := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now)
	if res.Source != SourceCombined {
		t.Fatalf("expected fresh transmit direction to classify, got source=%v reason=%v", res.Source, res.InsufficientReason)
	}
	if res.AgeSec != 0 {
		t.Fatalf("expected result age from fresh surviving direction, got %d", res.AgeSec)
	}
}

func TestPredictStaleDropLowFreshWeightReportsStale(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BandHalfLifeSec = map[string]int{"20m": 10}
	cfg.StaleAfterHalfLifeMultiplier = 100
	cfg.MinEffectiveWeight = 0.6
	cfg.MinObservationCount = 1
	cfg.MaxPredictionAgeHalfLifeMultiplier = 1
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell, dxCell, userCoarse, dxCoarse := requireDistinctPathCells(t, "EM12", "IO91")
	now := time.Now().UTC()

	predictor.Update(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", 25, 10, now.Add(-20*time.Second), false)
	predictor.Update(BucketCombined, dxCell, userCell, dxCoarse, userCoarse, "20m", 10, 0.1, now, false)

	res := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now)
	if res.Source != SourceInsufficient {
		t.Fatalf("expected insufficient result, got %v", res.Source)
	}
	if res.InsufficientReason != InsufficientStale {
		t.Fatalf("expected stale reason after dropping stale evidence, got %v", res.InsufficientReason)
	}
	if res.Weight <= 0 {
		t.Fatalf("expected surviving fresh low weight to be reported, got %v", res.Weight)
	}
}

func TestFT8EquivalentBandwidthOffsets(t *testing.T) {
	cfg := DefaultConfig()
	if got, ok := FT8Equivalent("CW", 0, cfg); !ok || got != -7 {
		t.Fatalf("expected CW 0 dB to map to -7 dB, got %v (ok=%v)", got, ok)
	}
	if got, ok := FT8Equivalent("RTTY", 0, cfg); !ok || got != -7 {
		t.Fatalf("expected RTTY 0 dB to map to -7 dB, got %v (ok=%v)", got, ok)
	}
	if got, ok := FT8Equivalent("PSK", 0, cfg); !ok || got != -7 {
		t.Fatalf("expected PSK 0 dB to map to -7 dB, got %v (ok=%v)", got, ok)
	}
	if got, ok := FT8Equivalent("WSPR", 0, cfg); !ok || got != 26 {
		t.Fatalf("expected WSPR 0 dB to map to 26 dB, got %v (ok=%v)", got, ok)
	}
	if got, ok := FT8Equivalent("FT4", 0, cfg); !ok || got != -3 {
		t.Fatalf("expected FT4 0 dB to map to -3 dB, got %v (ok=%v)", got, ok)
	}
	if got, ok := FT8Equivalent("FT8", 0, cfg); !ok || got != 0 {
		t.Fatalf("expected FT8 0 dB to map to 0 dB, got %v (ok=%v)", got, ok)
	}
}

func TestBucketForIngest(t *testing.T) {
	if got := BucketForIngest("FT8"); got != BucketCombined {
		t.Fatalf("expected FT8 to map to combined bucket")
	}
	if got := BucketForIngest("CW"); got != BucketCombined {
		t.Fatalf("expected CW to map to combined bucket")
	}
	if got := BucketForIngest("WSPR"); got != BucketCombined {
		t.Fatalf("expected WSPR to map to combined bucket")
	}
	if got := BucketForIngest("USB"); got != BucketNone {
		t.Fatalf("expected USB to skip ingest")
	}
}

func TestPredictUsesCombinedBuckets(t *testing.T) {
	requireH3Mappings(t)
	cfg := DefaultConfig()
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 1
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := EncodeCell("FN31")
	dxCell := EncodeCell("FN32")
	userCoarse := EncodeCoarseCell("FN31")
	dxCoarse := EncodeCoarseCell("FN32")
	now := time.Now().UTC()

	predictor.Update(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -5, 1.0, now, false)

	resCW := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "CW", 0, now)
	if resCW.Glyph == cfg.GlyphSymbols.Insufficient {
		t.Fatalf("expected combined glyph for CW, got insufficient")
	}
	if resCW.Glyph != GlyphForDB(-5, "CW", cfg) {
		t.Fatalf("unexpected CW glyph: %q", resCW.Glyph)
	}
	if resCW.Source != SourceCombined {
		t.Fatalf("expected combined source for CW, got %v", resCW.Source)
	}

	resFT8 := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now)
	if resFT8.Glyph == cfg.GlyphSymbols.Insufficient {
		t.Fatalf("expected combined glyph for FT8, got insufficient")
	}
	if resFT8.Glyph != GlyphForDB(-5, "FT8", cfg) {
		t.Fatalf("unexpected FT8 glyph: %q", resFT8.Glyph)
	}
	if resFT8.Source != SourceCombined {
		t.Fatalf("expected combined source for FT8, got %v", resFT8.Source)
	}
}

func TestPredictUsesCoarseWhenFineMissing(t *testing.T) {
	requireH3Mappings(t)
	now := time.Now().UTC()
	userCell := EncodeCell("FN31")
	dxCell := EncodeCell("FN32")
	userCoarse := EncodeCoarseCell("FN31")
	dxCoarse := EncodeCoarseCell("FN32")

	cfg := DefaultConfig()
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 1
	predictor := NewPredictor(cfg, []string{"20m"})
	predictor.Update(BucketCombined, InvalidCell, InvalidCell, userCoarse, dxCoarse, "20m", -5, 1.0, now, false)

	res := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now)
	if res.Glyph == cfg.GlyphSymbols.Insufficient {
		t.Fatalf("expected coarse fallback glyph when enabled, got insufficient")
	}
}

func TestPredictCarriesObservationCount(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 1
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell, dxCell, userCoarse, dxCoarse := requireDistinctPathCells(t, "FN31", "DM04")
	now := time.Now().UTC()

	predictor.Update(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -5, 1.0, now, false)
	predictor.Update(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -5, 1.0, now, false)

	res := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now)
	if res.Count != 2 {
		t.Fatalf("expected selected count 2 without fine/coarse double count, got %d", res.Count)
	}
}

func TestPredictLowObservationCountInsufficient(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 19
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell, dxCell, userCoarse, dxCoarse := requireDistinctPathCells(t, "FN31", "IO91")
	now := time.Now().UTC()

	for i := 0; i < 3; i++ {
		predictor.Update(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -5, 10, now, false)
	}

	res := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now)
	if res.Source != SourceInsufficient {
		t.Fatalf("expected low observation count to be insufficient, got %v", res.Source)
	}
	if res.InsufficientReason != InsufficientLowCount {
		t.Fatalf("expected low-count insufficient reason, got %v", res.InsufficientReason)
	}
	if res.Count != 3 {
		t.Fatalf("expected selected count 3, got %d", res.Count)
	}
	if res.Weight <= cfg.MinEffectiveWeight {
		t.Fatalf("expected enough weight so count is the failing gate, got weight=%v", res.Weight)
	}
}

func TestPredictWithMinObservationCountCannotLowerConfiguredFloor(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 5
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell, dxCell, userCoarse, dxCoarse := requireDistinctPathCells(t, "FN31", "IO91")
	now := time.Now().UTC()

	for i := 0; i < 3; i++ {
		predictor.Update(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -5, 10, now, false)
	}

	res := predictor.PredictWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, 1, now)
	if res.Source != SourceInsufficient {
		t.Fatalf("expected configured observation floor to remain enforced, got %v", res.Source)
	}
	if res.InsufficientReason != InsufficientLowCount {
		t.Fatalf("expected low-count insufficient reason, got %v", res.InsufficientReason)
	}
}
