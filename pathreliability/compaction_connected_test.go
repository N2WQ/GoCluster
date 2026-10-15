package pathreliability

import (
	"maps"
	"reflect"
	"testing"
	"time"
)

func TestPredictorCompactionAfterPurgePreservesPredictionAndReports(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionOff
	cfg.DefaultHalfLifeSec = 60
	cfg.StaleAfterSeconds = 60
	cfg.StaleAfterHalfLifeMultiplier = 1
	cfg.MaxPredictionAgeHalfLifeMultiplier = 1
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 1
	predictor := NewPredictor(cfg, []string{"20m"})
	now := time.Date(2026, time.October, 14, 20, 0, 0, 0, time.UTC)
	for offset, paths := range balancedCompactionCoarsePaths(t, predictor.combined) {
		for entry, path := range paths {
			observed := now
			if entry >= 8 {
				observed = now.Add(-61 * time.Second)
			}
			admission := predictor.UpdateWithReceiverHash(BucketCombined, InvalidCell, InvalidCell, path.receiver, path.sender, "20m", -10.5, 1,
				ObservationTimes{ObservedAt: observed, ArrivedAt: observed}, false, uint64(offset+1))
			if !admission.Accepted() {
				t.Fatalf("coarse report path=%+v was not admitted: %+v", path, admission)
			}
		}
	}
	if got := predictor.TotalBuckets(); got != 1088 {
		t.Fatalf("real coarse updates produced %d buckets, want 1088", got)
	}
	for i := range predictor.combined.shards {
		sh := &predictor.combined.shards[i]
		sh.mu.RLock()
		count, peak := len(sh.buckets), sh.peak
		sh.mu.RUnlock()
		if count != 17 || peak != 17 {
			t.Fatalf("shard %d before purge count=%d peak=%d, want real growth to 17", i, count, peak)
		}
	}
	before := predictor.Predict(InvalidCell, InvalidCell, 1, 2, "20m", "FT8", 0, now)
	if before.Source != SourceCombined || !before.HasP50 || !before.ObservedDistributionAvailable || before.P50DB != -10.5 || before.Count != 1 {
		t.Fatalf("connected fixture must have a usable independent -10.5 dB p50 before maintenance: %+v", before)
	}
	predictor.RefreshStatsSnapshot(now)
	statsBefore := predictor.StatsSnapshot()
	histBefore := predictor.WeightHistogramByBand(now)
	assertCompactionConnectedReports(t, statsBefore, histBefore, now)
	if got := predictor.PurgeStale(now); got != 576 {
		t.Fatalf("purged=%d, want 9 stale entries in each of 64 shards", got)
	}
	want := make([]map[uint64]*bucket, 64)
	for i := range predictor.combined.shards {
		sh := &predictor.combined.shards[i]
		sh.mu.RLock()
		want[i] = maps.Clone(sh.buckets)
		sh.mu.RUnlock()
		if len(want[i]) != 8 {
			t.Fatalf("shard %d retains %d entries after real purge, want 8", i, len(want[i]))
		}
	}
	if got := predictor.Compact(1000, 0.5); got != 64 {
		t.Fatalf("connected distributed compaction rebuilt %d shards, want 64", got)
	}
	assertCompactionContractStore(t, predictor.combined, want, 8)
	after := predictor.Predict(InvalidCell, InvalidCell, 1, 2, "20m", "FT8", 0, now)
	if after != before {
		t.Fatalf("maintenance changed usable prediction: before=%+v after=%+v", before, after)
	}
	predictor.RefreshStatsSnapshot(now)
	statsAfter := predictor.StatsSnapshot()
	histAfter := predictor.WeightHistogramByBand(now)
	assertCompactionConnectedReports(t, statsAfter, histAfter, now)
	if !reflect.DeepEqual(statsAfter, statsBefore) || !reflect.DeepEqual(histAfter, histBefore) {
		t.Fatalf("active report readback changed: stats=%+v/%+v hist=%+v/%+v", statsBefore, statsAfter, histBefore, histAfter)
	}
	if got := predictor.TotalBuckets(); got != 512 {
		t.Fatalf("maintenance retained %d physical buckets, want 512", got)
	}
	if got := predictor.Compact(1000, 0.5); got != 0 {
		t.Fatalf("connected fixture repeated compaction without contraction: %d", got)
	}
}

type compactionCoarsePath struct{ receiver, sender CellID }

// This bounded search sets up balanced maintenance work; selector correctness
// and useful distribution have separate literal and mapped-population oracles.
func balancedCompactionCoarsePaths(t *testing.T, store *Store) [][]compactionCoarsePath {
	t.Helper()
	requireH3Mappings(t)
	groups := make([][]compactionCoarsePath, len(store.shards))
	indices := make(map[*shard]int, len(store.shards))
	for i := range store.shards {
		groups[i] = make([]compactionCoarsePath, 0, 17)
		indices[&store.shards[i]] = i
	}
	index, ok := store.bandIndex.Lookup("20m")
	if !ok {
		t.Fatal("balanced compaction fixture requires 20m")
	}
	queryShard := indices[store.shardForKey(packCoarseKey(1, 2, index))]
	groups[queryShard] = append(groups[queryShard], compactionCoarsePath{1, 2})
	count := 1
search:
	for receiver := CellID(1); receiver <= 842; receiver++ {
		for sender := CellID(1); sender <= 842; sender++ {
			if coarseMapper.ToH3[receiver] == 0 || coarseMapper.ToH3[sender] == 0 {
				t.Fatal("balanced setup requires valid checked-in res-1 H3 proxy IDs")
			}
			if (receiver == 1 && sender == 2) || (receiver == 2 && sender == 1) {
				continue
			}
			i := indices[store.shardForKey(packCoarseKey(receiver, sender, index))]
			if len(groups[i]) == 17 {
				continue
			}
			groups[i] = append(groups[i], compactionCoarsePath{receiver, sender})
			count++
			if count == 64*17 {
				break search
			}
		}
	}
	if count != 64*17 {
		t.Fatalf("bounded coarse population filled %d entries, want 64*17", count)
	}
	return groups
}

func assertCompactionConnectedReports(t *testing.T, stats PredictorStatsSnapshot, hist WeightHistogram, now time.Time) {
	t.Helper()
	if !stats.Available || !stats.HasSnapshot || stats.AsOfUnix != now.Unix() || stats.Stats.CombinedFine != 0 || stats.Stats.CombinedCoarse != 512 || len(stats.Bands) != 1 || stats.Bands[0] != (BandBucketStats{Band: "20m", Coarse: 512}) {
		t.Fatalf("active coarse report must contain exactly 512 live buckets: %+v", stats)
	}
	if !hist.Available || hist.AsOfUnix != now.Unix() || len(hist.Bands) != 1 || hist.Bands[0].Band != "20m" || hist.Bands[0].Total != 512 {
		t.Fatalf("nonempty active weight report missing: %+v", hist)
	}
	total := 0
	for _, count := range hist.Bands[0].Bins {
		total += count
	}
	if total != 512 {
		t.Fatalf("weight bins contain %d entries, want 512", total)
	}
}
