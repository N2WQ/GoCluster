package pathreliability

import (
	"math"
	"testing"
	"time"
)

func TestReceiverIdentityHashNormalizesCaseAndSpace(t *testing.T) {
	left := ReceiverIdentityHash(" n2wq ")
	right := ReceiverIdentityHash("N2WQ")
	if left == 0 {
		t.Fatalf("expected non-zero hash for receiver")
	}
	if left != right {
		t.Fatalf("expected normalized hashes to match, got %d and %d", left, right)
	}
	if got := ReceiverIdentityHash("  "); got != 0 {
		t.Fatalf("expected blank receiver hash 0, got %d", got)
	}
}

func TestReceiverCapShadowPreservesRawPredictionAndReportsWouldBlock(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionShadow
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 19
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Now().UTC()
	receiver := ReceiverIdentityHash("N2WQ")

	for i := 0; i < 19; i++ {
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -5, 1.0, now, false, receiver)
	}

	res := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now)
	if res.Source != SourceCombined {
		t.Fatalf("shadow mode should preserve raw usable prediction, got source=%v reason=%v", res.Source, res.InsufficientReason)
	}
	if res.Count != 19 || res.RawCount != 19 {
		t.Fatalf("expected raw selected count 19, got count=%d raw=%d", res.Count, res.RawCount)
	}
	if res.CappedCount != 5 {
		t.Fatalf("expected single receiver capped count 5, got %d", res.CappedCount)
	}
	if !res.CapLimited || !res.CapWouldBlock {
		t.Fatalf("expected cap limited/would-block diagnostics, got limited=%v wouldBlock=%v", res.CapLimited, res.CapWouldBlock)
	}
}

func TestReceiverCapEnforceFailsSingleReceiverLowCount(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionEnforce
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 19
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Now().UTC()
	receiver := ReceiverIdentityHash("N2WQ")

	for i := 0; i < 19; i++ {
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -5, 1.0, now, false, receiver)
	}

	res := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now)
	if res.Source != SourceInsufficient {
		t.Fatalf("expected enforced cap to block single-receiver prediction, got source=%v", res.Source)
	}
	if res.InsufficientReason != InsufficientLowCount {
		t.Fatalf("expected low-count reason, got %v", res.InsufficientReason)
	}
	if res.Count != 5 || res.RawCount != 19 || res.CappedCount != 5 {
		t.Fatalf("unexpected counts: count=%d raw=%d capped=%d", res.Count, res.RawCount, res.CappedCount)
	}
}

func TestReceiverCapEnforcePassesReceiversAtDefaultFloor(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionEnforce
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 19
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Now().UTC()
	receivers := []uint64{
		ReceiverIdentityHash("N2WQ"),
		ReceiverIdentityHash("K1ABC"),
		ReceiverIdentityHash("W1AW"),
		ReceiverIdentityHash("VE3XYZ"),
		ReceiverIdentityHash("K3LR"),
		ReceiverIdentityHash("W3LPL"),
	}

	for i := 0; i < 20; i++ {
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -5, 1.0, now, false, receivers[i%len(receivers)])
	}

	res := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now)
	if res.Source != SourceCombined {
		t.Fatalf("expected capped receivers to pass, got source=%v reason=%v count=%d", res.Source, res.InsufficientReason, res.Count)
	}
	if res.Count != 20 || res.CappedCount != 20 {
		t.Fatalf("expected capped selected count 20, got count=%d capped=%d", res.Count, res.CappedCount)
	}
}

func TestReceiverCapEnforceUnattributedDoesNotAddCappedTrust(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionEnforce
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 1
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Now().UTC()

	predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -5, 1.0, now, false, 0)

	res := predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now)
	if res.Source != SourceInsufficient {
		t.Fatalf("expected unattributed enforce update to be insufficient, got %v", res.Source)
	}
	if res.InsufficientReason != InsufficientLowCount {
		t.Fatalf("expected low-count reason, got %v", res.InsufficientReason)
	}
	if res.RawCount != 1 || res.CappedCount != 0 {
		t.Fatalf("expected raw=1 capped=0, got raw=%d capped=%d", res.RawCount, res.CappedCount)
	}
}

func TestReceiverCapEnforceDecayedCountAdmitsNewEvidence(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionEnforce
	cfg.MinEffectiveWeight = 0.01
	cfg.MinObservationCount = 1
	cfg.DefaultHalfLifeSec = 10
	cfg.StaleAfterHalfLifeMultiplier = 100
	cfg.MaxPredictionAgeHalfLifeMultiplier = 100
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Unix(1_700_000_000, 0).UTC()
	later := now.Add(20 * time.Second)
	receiver := ReceiverIdentityHash("N2WQ")

	for i := 0; i < 5; i++ {
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -20, 1.0, now, false, receiver)
	}
	for i := 0; i < 4; i++ {
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", 20, 1.0, later, false, receiver)
	}

	res := predictor.PredictWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, 1, later)
	if res.Source != SourceCombined {
		t.Fatalf("expected decayed receiver cap to admit fresh evidence, got source=%v reason=%v count=%d capped=%d raw=%d", res.Source, res.InsufficientReason, res.Count, res.CappedCount, res.RawCount)
	}
	if !res.HasP50 || res.P50DB != 20.5 {
		t.Fatalf("expected fresh strong evidence to move capped p50 to 20.5, got p50=%v has=%v", res.P50DB, res.HasP50)
	}
	if res.Count != 5 || res.CappedCount != 5 || res.RawCount != 9 {
		t.Fatalf("unexpected counts after decayed admission: count=%d capped=%d raw=%d", res.Count, res.CappedCount, res.RawCount)
	}
}

func TestReceiverCapEnforceSingleReceiverEffectiveCountStaysBelowFloor(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionEnforce
	cfg.MinEffectiveWeight = 0.01
	cfg.MinObservationCount = 30
	cfg.DefaultHalfLifeSec = 10
	cfg.StaleAfterHalfLifeMultiplier = 100
	cfg.MaxPredictionAgeHalfLifeMultiplier = 100
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Unix(1_700_000_000, 0).UTC()
	receiver := ReceiverIdentityHash("N2WQ")

	for i := 0; i < 60; i++ {
		at := now.Add(time.Duration(i) * time.Second)
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", 20, 1.0, at, false, receiver)
	}

	res := predictor.PredictWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, 30, now.Add(59*time.Second))
	if res.Source != SourceInsufficient || res.InsufficientReason != InsufficientLowCount {
		t.Fatalf("expected one receiver to stay below floor, got source=%v reason=%v count=%d capped=%d raw=%d", res.Source, res.InsufficientReason, res.Count, res.CappedCount, res.RawCount)
	}
	if res.CappedCount > cfg.ReceiverMaxEffectiveCount {
		t.Fatalf("single receiver capped effective count=%d exceeded cap=%d", res.CappedCount, cfg.ReceiverMaxEffectiveCount)
	}
}

func TestReceiverCapEnforceMultiReceiverDecayedP50Recovers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionEnforce
	cfg.MinEffectiveWeight = 0.01
	cfg.MinObservationCount = 30
	cfg.DefaultHalfLifeSec = 10
	cfg.StaleAfterHalfLifeMultiplier = 100
	cfg.MaxPredictionAgeHalfLifeMultiplier = 100
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Unix(1_700_000_000, 0).UTC()
	later := now.Add(20 * time.Second)
	receivers := []uint64{
		ReceiverIdentityHash("N2WQ"),
		ReceiverIdentityHash("K1ABC"),
		ReceiverIdentityHash("W1AW"),
		ReceiverIdentityHash("VE3XYZ"),
		ReceiverIdentityHash("K3LR"),
		ReceiverIdentityHash("W3LPL"),
	}

	for _, receiver := range receivers {
		for i := 0; i < 5; i++ {
			predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -20, 1.0, now, false, receiver)
		}
	}
	for _, receiver := range receivers {
		for i := 0; i < 4; i++ {
			predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", 20, 1.0, later, false, receiver)
		}
	}

	res := predictor.PredictWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, 30, later)
	if res.Source != SourceCombined {
		t.Fatalf("expected receiver-diverse fresh evidence to pass, got source=%v reason=%v count=%d capped=%d raw=%d", res.Source, res.InsufficientReason, res.Count, res.CappedCount, res.RawCount)
	}
	if !res.HasP50 || res.P50DB != 20.5 {
		t.Fatalf("expected receiver-diverse capped p50 to recover to 20.5, got p50=%v has=%v", res.P50DB, res.HasP50)
	}
	if res.Count != 30 || res.CappedCount != 30 || res.RawCount != 54 {
		t.Fatalf("unexpected counts after multi-receiver recovery: count=%d capped=%d raw=%d", res.Count, res.CappedCount, res.RawCount)
	}
}

func TestReceiverCapEnforceEvenSplitUsesTypicalMiddleP50(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionEnforce
	cfg.MinEffectiveWeight = 0.01
	cfg.MinObservationCount = 1
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Unix(1_700_000_000, 0).UTC()
	weakReceiver := ReceiverIdentityHash("WEAK")
	strongReceiver := ReceiverIdentityHash("STRONG")

	for i := 0; i < 5; i++ {
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -20, 1.0, now, false, weakReceiver)
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -14, 1.0, now, false, strongReceiver)
	}

	res := predictor.PredictWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, 1, now)
	if res.Source != SourceCombined {
		t.Fatalf("expected capped even split to pass, got source=%v reason=%v count=%d capped=%d raw=%d", res.Source, res.InsufficientReason, res.Count, res.CappedCount, res.RawCount)
	}
	if !res.HasP50 || res.P50DB != -16.5 {
		t.Fatalf("expected capped even-split p50=-16.5, got p50=%v has=%v", res.P50DB, res.HasP50)
	}
	if res.Class != classMedium || res.Glyph != cfg.GlyphSymbols.Medium {
		t.Fatalf("expected MEDIUM capped even-split glyph, got class=%q glyph=%q", res.Class, res.Glyph)
	}
}

func TestReceiverCapFractionalAdmissionKeepsCountAndWeightCoherent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionEnforce
	cfg.ReceiverMaxEffectiveCount = 5
	cfg.ReceiverMaxEffectiveWeight = 2.5
	store := NewStore(cfg, []string{"20m"})
	receiverCell := CellID(1)
	senderCell := CellID(2)
	receiverCoarse := CellID(3)
	senderCoarse := CellID(4)
	now := time.Unix(1_700_000_000, 0).UTC()
	receiver := ReceiverIdentityHash("N2WQ")

	for i := 0; i < 3; i++ {
		store.UpdateWithReceiverHash(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", -5, 1.0, now, receiver)
	}

	fine, _ := store.Lookup(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", now)
	if fine.Count != 2 || fine.CappedCount != 2 {
		t.Fatalf("expected floored fractional capped count 2, got count=%d capped=%d", fine.Count, fine.CappedCount)
	}
	if math.Abs(fine.CappedWeight-2.5) > 1e-9 {
		t.Fatalf("expected capped weight 2.5, got %v", fine.CappedWeight)
	}
}

func TestReceiverCapCoarseBucketsUseExtraSlots(t *testing.T) {
	cfg := DefaultConfig()
	store := NewStore(cfg, []string{"20m"})
	now := time.Now().UTC()
	receiverCoarse := CellID(3)
	senderCoarse := CellID(4)
	for i := 0; i < maxCoarseReceiverSlots; i++ {
		store.UpdateWithReceiverHash(InvalidCell, InvalidCell, receiverCoarse, senderCoarse, "20m", -5, 1.0, now, uint64(i+1))
	}

	key := packCoarseKey(receiverCoarse, senderCoarse, 0)
	sh := &store.shards[key%uint64(len(store.shards))]
	sh.mu.RLock()
	b := sh.buckets[key]
	if b == nil {
		sh.mu.RUnlock()
		t.Fatalf("expected coarse bucket")
	}
	if b.extraSlots == nil {
		sh.mu.RUnlock()
		t.Fatalf("expected coarse bucket to allocate extra receiver slots")
	}
	for i := 0; i < maxCoarseReceiverSlots-inlineReceiverSlots; i++ {
		if b.extraSlots[i].hash == 0 {
			sh.mu.RUnlock()
			t.Fatalf("expected extra slot %d to be populated", i)
		}
	}
	sh.mu.RUnlock()
}
