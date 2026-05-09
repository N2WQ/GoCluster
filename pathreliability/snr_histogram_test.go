package pathreliability

import (
	"testing"
	"time"
	"unsafe"
)

func TestSNRHistogramBinAssignment(t *testing.T) {
	cases := []struct {
		db   float64
		want int
	}{
		{db: -30, want: 0},
		{db: -24.1, want: 0},
		{db: -24, want: 1},
		{db: -23.1, want: 1},
		{db: -23, want: 2},
		{db: -0.1, want: 24},
		{db: 0, want: 25},
		{db: 23.9, want: 48},
		{db: 24, want: 49},
		{db: 30, want: 49},
	}
	for _, tc := range cases {
		if got := snrHistogramBinIndex(tc.db); got != tc.want {
			t.Fatalf("snrHistogramBinIndex(%v)=%d, want %d", tc.db, got, tc.want)
		}
	}
}

func TestSNRHistogramWeightedP50(t *testing.T) {
	var hist snrHistogram
	hist.add(snrHistogramBinIndex(-20), 1)
	hist.add(snrHistogramBinIndex(-14), 3)
	got, ok := hist.p50DB()
	if !ok {
		t.Fatalf("expected p50")
	}
	if got != -14 {
		t.Fatalf("p50=%v, want -14", got)
	}
}

func TestSNRHistogramShiftedUsesOneDBBins(t *testing.T) {
	var hist snrHistogram
	hist.add(snrHistogramBinIndex(-20), 1)

	shifted := hist.shifted(2)
	got, ok := shifted.p50DB()
	if !ok {
		t.Fatalf("expected shifted p50")
	}
	if got != -18 {
		t.Fatalf("shifted p50=%v, want -18", got)
	}

	underflow := hist.shifted(-5)
	got, ok = underflow.p50DB()
	if !ok {
		t.Fatalf("expected underflow shifted p50")
	}
	if got != -24 {
		t.Fatalf("underflow shifted p50=%v, want -24", got)
	}
}

func TestStoreSNRHistogramTracksUnclampedUnderflowOverflowAndDecay(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinEffectiveWeight = 0.01
	cfg.MinObservationCount = 1
	cfg.DefaultHalfLifeSec = 10
	cfg.StaleAfterHalfLifeMultiplier = 100
	store := NewStore(cfg, []string{"20m"})
	now := time.Unix(1_700_000_000, 0).UTC()
	receiverCell := CellID(1)
	senderCell := CellID(2)
	receiverCoarse := CellID(3)
	senderCoarse := CellID(4)

	store.UpdateWithReceiverHashDB(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", -30, cfg.powerFromDB(-30), 1, now, ReceiverIdentityHash("K1ABC"))
	fine, _ := store.lookupWithDistribution(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", now.Add(10*time.Second))
	if !fine.HasP50 || fine.P50DB != -24 {
		t.Fatalf("underflow p50=%v has=%v, want -24", fine.P50DB, fine.HasP50)
	}
	if fine.Weight >= 1 || fine.Weight <= 0 {
		t.Fatalf("expected decayed positive weight below 1, got %v", fine.Weight)
	}

	store = NewStore(cfg, []string{"20m"})
	store.UpdateWithReceiverHashDB(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", 30, cfg.powerFromDB(30), 1, now, ReceiverIdentityHash("K1ABC"))
	fine, _ = store.lookupWithDistribution(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", now)
	if !fine.HasP50 || fine.P50DB != 24 {
		t.Fatalf("overflow p50=%v has=%v, want 24", fine.P50DB, fine.HasP50)
	}
}

func TestStoreSNRHistogramRawAndCappedDiverge(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	receiverCell := CellID(1)
	senderCell := CellID(2)
	receiverCoarse := CellID(3)
	senderCoarse := CellID(4)
	r1 := ReceiverIdentityHash("K1ABC")
	r2 := ReceiverIdentityHash("W1AW")

	update := func(store *Store, cfg Config) {
		store.UpdateWithReceiverHashDB(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", 30, cfg.powerFromDB(30), 1, now, r1)
		store.UpdateWithReceiverHashDB(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", 30, cfg.powerFromDB(30), 1, now, r1)
		store.UpdateWithReceiverHashDB(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", -30, cfg.powerFromDB(-30), 1, now, r2)
	}

	shadowCfg := DefaultConfig()
	shadowCfg.MinEffectiveWeight = 0.01
	shadowCfg.MinObservationCount = 1
	shadowCfg.ReceiverContributionMode = ReceiverContributionShadow
	shadowCfg.ReceiverMaxEffectiveCount = 1
	shadowCfg.ReceiverMaxEffectiveWeight = 1
	shadow := NewStore(shadowCfg, []string{"20m"})
	update(shadow, shadowCfg)
	shadowFine, _ := shadow.lookupWithDistribution(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", now)
	if !shadowFine.HasP50 || shadowFine.P50DB != 24 {
		t.Fatalf("shadow raw p50=%v has=%v, want 24", shadowFine.P50DB, shadowFine.HasP50)
	}

	enforceCfg := shadowCfg
	enforceCfg.ReceiverContributionMode = ReceiverContributionEnforce
	enforce := NewStore(enforceCfg, []string{"20m"})
	update(enforce, enforceCfg)
	enforceFine, _ := enforce.lookupWithDistribution(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", now)
	if !enforceFine.HasP50 || enforceFine.P50DB != -24 {
		t.Fatalf("enforced capped p50=%v has=%v, want -24", enforceFine.P50DB, enforceFine.HasP50)
	}
	if !enforceFine.CapLimited || enforceFine.Count != 2 || enforceFine.RawCount != 3 || enforceFine.CappedCount != 2 {
		t.Fatalf("expected capped divergence, got limited=%v count=%d raw=%d capped=%d", enforceFine.CapLimited, enforceFine.Count, enforceFine.RawCount, enforceFine.CappedCount)
	}
}

func TestPredictDistributionOnlyWhenRequested(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinEffectiveWeight = 0.01
	cfg.MinObservationCount = 1
	predictor := NewPredictor(cfg, []string{"20m"})
	now := time.Unix(1_700_000_000, 0).UTC()
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)

	predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -15, 1, now, false, ReceiverIdentityHash("K1ABC"))
	plain := predictor.PredictWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, 1, now)
	if plain.HasP50 || plain.P50Glyph != "" {
		t.Fatalf("plain prediction should not calculate p50")
	}
	withDistribution := predictor.PredictWithMinObservationCountAndDistribution(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, 1, now)
	if !withDistribution.HasP50 || withDistribution.P50DB != -15 {
		t.Fatalf("distribution prediction p50=%v has=%v, want -15", withDistribution.P50DB, withDistribution.HasP50)
	}
	if withDistribution.P50Glyph != withDistribution.Glyph {
		t.Fatalf("expected p50 glyph %q to match active glyph %q", withDistribution.P50Glyph, withDistribution.Glyph)
	}
}

func TestStoreSNRHistogramOffAndPurgeStale(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DistributionStatisticMode = DistributionStatisticOff
	cfg.MinEffectiveWeight = 0.01
	cfg.MinObservationCount = 1
	cfg.DefaultHalfLifeSec = 1
	cfg.StaleAfterHalfLifeMultiplier = 1
	store := NewStore(cfg, []string{"20m"})
	now := time.Unix(1_700_000_000, 0).UTC()
	receiverCell := CellID(1)
	senderCell := CellID(2)
	receiverCoarse := CellID(3)
	senderCoarse := CellID(4)

	store.UpdateWithReceiverHashDB(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", -15, cfg.powerFromDB(-15), 1, now, ReceiverIdentityHash("K1ABC"))
	fine, _ := store.lookupWithDistribution(receiverCell, senderCell, receiverCoarse, senderCoarse, "20m", now)
	if fine.HasP50 {
		t.Fatalf("distribution_statistic_mode=off should not expose p50")
	}
	if removed := store.PurgeStale(now.Add(2 * time.Second)); removed != 2 {
		t.Fatalf("removed stale buckets=%d, want 2", removed)
	}
	if got := store.TotalBuckets(); got != 0 {
		t.Fatalf("total buckets after purge=%d, want 0", got)
	}
}

type legacyBucketForSize struct {
	_ float64
	_ float64
	_ uint32

	_ int64

	_ float64
	_ float64
	_ uint32
	_ [inlineReceiverSlots]receiverSlot
	_ *[maxCoarseReceiverSlots - inlineReceiverSlots]receiverSlot
}

func TestBucketRetainedSizeEstimate(t *testing.T) {
	const estimatedBuckets = 110_000
	legacySize := unsafe.Sizeof(legacyBucketForSize{})
	currentSize := unsafe.Sizeof(bucket{})
	t.Logf("legacy_bucket_size_bytes=%d current_bucket_size_bytes=%d retained_heap_delta_bytes_for_%d_buckets=%d", legacySize, currentSize, estimatedBuckets, (currentSize-legacySize)*estimatedBuckets)
}
