package pathreliability

import (
	"testing"
	"time"
)

func TestStoreStatsByBandCounts(t *testing.T) {
	requireH3Mappings(t)
	cfg := DefaultConfig()
	cfg.StaleAfterSeconds = 600
	store := NewStore(cfg, []string{"160m", "80m"})
	now := time.Now().UTC()

	store.UpdateWithReceiverHash(EncodeCell("FN31"), EncodeCell("FN32"), InvalidCell, InvalidCell, "160m", -5, 1.0, now, 0)
	store.UpdateWithReceiverHash(InvalidCell, InvalidCell, EncodeCoarseCell("FN31"), EncodeCoarseCell("FN32"), "80m", -10, 1.0, now, 0)
	store.RefreshStatsSnapshot(now)

	stats := store.StatsByBand(now)
	if len(stats) != 2 {
		t.Fatalf("expected 2 bands, got %d", len(stats))
	}
	if stats[0].fine != 1 || stats[0].coarse != 0 {
		t.Fatalf("expected 160m fine=1 coarse=0, got fine=%d coarse=%d", stats[0].fine, stats[0].coarse)
	}
	if stats[1].fine != 0 || stats[1].coarse != 1 {
		t.Fatalf("expected 80m fine=0 coarse=1, got fine=%d coarse=%d", stats[1].fine, stats[1].coarse)
	}
}

func TestPredictorStatsByBand(t *testing.T) {
	requireH3Mappings(t)
	cfg := DefaultConfig()
	cfg.StaleAfterSeconds = 600
	predictor := NewPredictor(cfg, []string{"160m", "80m"})
	now := time.Now().UTC()

	predictor.Update(BucketCombined, EncodeCell("FN31"), EncodeCell("FN32"), InvalidCell, InvalidCell, "160m", -5, 1.0, now, false)
	predictor.Update(BucketCombined, InvalidCell, InvalidCell, EncodeCoarseCell("FN31"), EncodeCoarseCell("FN32"), "80m", -7, 1.0, now, false)
	predictor.RefreshStatsSnapshot(now)

	stats := predictor.StatsByBand(now)
	if len(stats) != 2 {
		t.Fatalf("expected 2 bands, got %d", len(stats))
	}
	if stats[0].Band != "160m" || stats[1].Band != "80m" {
		t.Fatalf("unexpected band order: %q, %q", stats[0].Band, stats[1].Band)
	}
	if stats[0].Fine != 1 || stats[0].Coarse != 0 {
		t.Fatalf("expected 160m fine=1 coarse=0, got fine=%d coarse=%d", stats[0].Fine, stats[0].Coarse)
	}
	if stats[1].Fine != 0 || stats[1].Coarse != 1 {
		t.Fatalf("expected 80m fine=0 coarse=1, got fine=%d coarse=%d", stats[1].Fine, stats[1].Coarse)
	}
}

func TestStoreStatsRequireExplicitRefresh(t *testing.T) {
	requireH3Mappings(t)
	cfg := DefaultConfig()
	cfg.StaleAfterSeconds = 600
	store := NewStore(cfg, []string{"160m"})
	now := time.Now().UTC()

	store.UpdateWithReceiverHash(EncodeCell("FN31"), EncodeCell("FN32"), InvalidCell, InvalidCell, "160m", -5, 1.0, now, 0)

	stats := store.StatsByBand(now)
	if len(stats) != 1 {
		t.Fatalf("expected 1 band, got %d", len(stats))
	}
	if stats[0].fine != 0 || stats[0].coarse != 0 {
		t.Fatalf("expected cached stats to remain zero before explicit refresh, got fine=%d coarse=%d", stats[0].fine, stats[0].coarse)
	}

	store.RefreshStatsSnapshot(now)
	stats = store.StatsByBand(now)
	if stats[0].fine != 1 || stats[0].coarse != 0 {
		t.Fatalf("expected explicit refresh to populate counts, got fine=%d coarse=%d", stats[0].fine, stats[0].coarse)
	}
}

func TestStoreStatsRemainCachedUntilExplicitRefresh(t *testing.T) {
	requireH3Mappings(t)
	cfg := DefaultConfig()
	cfg.DefaultHalfLifeSec = 10
	cfg.StaleAfterHalfLifeMultiplier = 1
	cfg.StaleAfterSeconds = 999
	store := NewStore(cfg, []string{"160m"})
	now := time.Now().UTC()

	store.UpdateWithReceiverHash(EncodeCell("FN31"), EncodeCell("FN32"), InvalidCell, InvalidCell, "160m", -5, 1.0, now, 0)
	store.RefreshStatsSnapshot(now)

	stats := store.StatsByBand(now)
	if stats[0].fine != 1 || stats[0].coarse != 0 {
		t.Fatalf("expected initial refreshed stats to show one fine bucket, got fine=%d coarse=%d", stats[0].fine, stats[0].coarse)
	}

	removed := store.PurgeStale(now.Add(11 * time.Second))
	if removed != 1 {
		t.Fatalf("expected purge to remove 1 bucket, got %d", removed)
	}

	stats = store.StatsByBand(now.Add(11 * time.Second))
	if stats[0].fine != 1 || stats[0].coarse != 0 {
		t.Fatalf("expected cached stats to remain stale until refresh, got fine=%d coarse=%d", stats[0].fine, stats[0].coarse)
	}

	store.RefreshStatsSnapshot(now.Add(11 * time.Second))
	stats = store.StatsByBand(now.Add(11 * time.Second))
	if stats[0].fine != 0 || stats[0].coarse != 0 {
		t.Fatalf("expected explicit refresh to reflect purge, got fine=%d coarse=%d", stats[0].fine, stats[0].coarse)
	}
}

func TestStoreWeightHistogramByBand(t *testing.T) {
	requireH3Mappings(t)
	cfg := DefaultConfig()
	cfg.StaleAfterSeconds = 600
	store := NewStore(cfg, []string{"160m"})
	now := time.Now().UTC()

	store.UpdateWithReceiverHash(EncodeCell("FN31"), EncodeCell("FN32"), InvalidCell, InvalidCell, "160m", -5, 0.5, now, 0)
	store.UpdateWithReceiverHash(EncodeCell("EM10"), EncodeCell("EM11"), InvalidCell, InvalidCell, "160m", -5, 2.5, now, 0)

	edges := []float64{1, 2, 3}
	hist := store.WeightHistogramByBand(now, edges)
	if len(hist) != 1 {
		t.Fatalf("expected 1 band, got %d", len(hist))
	}
	if hist[0].total != 2 {
		t.Fatalf("expected total=2, got %d", hist[0].total)
	}
	if len(hist[0].bins) != len(edges)+1 {
		t.Fatalf("expected %d bins, got %d", len(edges)+1, len(hist[0].bins))
	}
	if hist[0].bins[0] != 1 || hist[0].bins[2] != 1 {
		t.Fatalf("unexpected bins: %v", hist[0].bins)
	}
}

func TestPredictorWeightHistogramByBand(t *testing.T) {
	requireH3Mappings(t)
	cfg := DefaultConfig()
	cfg.StaleAfterSeconds = 600
	predictor := NewPredictor(cfg, []string{"160m"})
	now := time.Now().UTC()

	predictor.Update(BucketCombined, EncodeCell("FN31"), EncodeCell("FN32"), InvalidCell, InvalidCell, "160m", -5, 1.0, now, false)

	hist := predictor.WeightHistogramByBand(now)
	if len(hist.Edges) == 0 {
		t.Fatalf("expected histogram edges")
	}
	if len(hist.Bands) != 1 {
		t.Fatalf("expected 1 band, got %d", len(hist.Bands))
	}
	if hist.Bands[0].Total != 1 {
		t.Fatalf("expected total=1, got %d", hist.Bands[0].Total)
	}
	if len(hist.Bands[0].Bins) != len(hist.Edges)+1 {
		t.Fatalf("expected %d bins, got %d", len(hist.Edges)+1, len(hist.Bands[0].Bins))
	}
}
