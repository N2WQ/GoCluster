package pathreliability

import (
	"fmt"
	"maps"
	"testing"
	"time"
)

func TestStoreCompactionDistributedSmallShards(t *testing.T) {
	store := NewStore(DefaultConfig(), []string{"20m"})
	want := make([]map[uint64]*bucket, 64)
	for i := range store.shards {
		want[i] = growCompactionContractShard(&store.shards[i], 17, 8)
	}
	if store.TotalBuckets() != 512 {
		t.Fatalf("contracted store has %d entries, want 512", store.TotalBuckets())
	}
	if got := store.Compact(1000, 0.5); got != 64 {
		t.Fatalf("compacted shards = %d, want all 64 small shards", got)
	}
	assertCompactionContractStore(t, store, want, 8)
	if got := store.Compact(1000, 0.5); got != 0 {
		t.Fatalf("second compaction rebuilt %d shards without new contraction", got)
	}
	assertCompactionContractStore(t, store, want, 8)
}

func TestStoreDistributedContractionBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		peak    int
		current int
		ratio   float64
		want    bool
		rebuilt int
	}{
		{"peak_999_below_minimum", 999, 499, 0.5, false, 0},
		{"peak_1000_below_ratio", 1000, 499, 0.5, true, 13},
		{"peak_1000_equal_ratio", 1000, 500, 0.5, false, 0},
		{"peak_1000_above_ratio", 1000, 501, 0.5, false, 0},
		{"peak_1001_below_odd_ratio", 1001, 499, 0.5, true, 13},
		{"peak_1001_below_untruncated_ratio", 1001, 500, 0.5, true, 12},
		{"peak_1001_above_ratio", 1001, 501, 0.5, false, 0},
		{"quarter_below_ratio", 1000, 249, 0.25, true, 7},
		{"quarter_equal_ratio", 1000, 250, 0.25, false, 0},
		{"quarter_above_ratio", 1000, 251, 0.25, false, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := NewStore(DefaultConfig(), []string{"20m"})
			for i := range store.shards {
				peak, current := tc.peak/64, tc.current/64
				if i < tc.peak%64 {
					peak++
				}
				if i < tc.current%64 {
					current++
				}
				growCompactionContractShard(&store.shards[i], peak, current)
			}
			if got := store.distributedContraction(1000, tc.ratio); got != tc.want {
				t.Fatalf("aggregate peak=%d current=%d ratio=%v: contracted=%v, want %v", tc.peak, tc.current, tc.ratio, got, tc.want)
			}
			// Literal counts independently check both public wiring and the local
			// recheck. A globally contracted store need not contract every shard.
			if got := store.Compact(1000, tc.ratio); got != tc.rebuilt {
				t.Fatalf("aggregate peak=%d current=%d ratio=%v: rebuilt=%d, want %d", tc.peak, tc.current, tc.ratio, got, tc.rebuilt)
			}
		})
	}
}

func TestStoreDistributedContractionIncludesCurrentBeyondPeak(t *testing.T) {
	store := NewStore(DefaultConfig(), []string{"20m"})
	growCompactionContractShard(&store.shards[0], 999, 0)
	want := growCompactionContractShard(&store.shards[1], 1, 1)
	store.shards[1].mu.Lock()
	store.shards[1].peak = 0
	store.shards[1].mu.Unlock()
	if !store.distributedContraction(1000, 0.5) {
		t.Fatal("current entry must supply the thousandth aggregate peak entry")
	}
	assertCompactionContractShard(t, &store.shards[1], want, 0)
	if store.shards[1].compactIfContracted(1000, 0.5, true) {
		t.Fatal("a full shard must not rebuild merely because its old peak was understated")
	}
	assertCompactionContractShard(t, &store.shards[1], want, 1)
}

func TestShardCompactionPreservesLargeThreshold(t *testing.T) {
	for _, current := range []int{500, 499} {
		for _, distributed := range []bool{false, true} {
			t.Run(fmt.Sprintf("current_%d_distributed_%v", current, distributed), func(t *testing.T) {
				sh := shard{buckets: make(map[uint64]*bucket)}
				want := growCompactionContractShard(&sh, 1001, current)
				compacted := sh.compactIfContracted(1000, 0.5, distributed)
				if compacted != (current == 499) {
					t.Fatalf("peak=1001 current=%d distributed=%v rebuilt=%v", current, distributed, compacted)
				}
				wantPeak := 1001
				if compacted {
					wantPeak = current
				}
				assertCompactionContractShard(t, &sh, want, wantPeak)
			})
		}
	}
}

func TestStoreCompactionLargeTriggerSurvivesGlobalRejection(t *testing.T) {
	for _, current := range []int{500, 499} {
		t.Run(fmt.Sprintf("current_%d", current), func(t *testing.T) {
			store := NewStore(DefaultConfig(), []string{"20m"})
			wantContracted := growCompactionContractShard(&store.shards[0], 1001, current)
			wantFull := growCompactionContractShard(&store.shards[1], 2000, 2000)
			if store.distributedContraction(1000, 0.5) {
				t.Fatal("full unrelated shard must prevent the store-wide contraction trigger")
			}
			wantCount, wantPeak := 0, 1001
			if current == 499 {
				wantCount, wantPeak = 1, 499
			}
			if got := store.Compact(1000, 0.5); got != wantCount {
				t.Fatalf("compacted shards=%d, want %d despite rejected aggregate trigger", got, wantCount)
			}
			assertCompactionContractShard(t, &store.shards[0], wantContracted, wantPeak)
			assertCompactionContractShard(t, &store.shards[1], wantFull, 2000)
		})
	}
}

func TestShardCompactionRechecksAfterRefill(t *testing.T) {
	store := NewStore(DefaultConfig(), []string{"20m"})
	for i := range store.shards {
		growCompactionContractShard(&store.shards[i], 17, 8)
	}
	distributed := store.distributedContraction(1000, 0.5)
	if !distributed {
		t.Fatal("fixture must first qualify for distributed compaction")
	}
	sh := &store.shards[0]
	refilled := make(chan map[uint64]*bucket, 1)
	go func() {
		sh.mu.Lock()
		for key := uint64(9); key <= 17; key++ {
			sh.buckets[key] = &bucket{weight: float64(key)}
			sh.peak = max(sh.peak, len(sh.buckets))
		}
		want := maps.Clone(sh.buckets)
		sh.mu.Unlock()
		refilled <- want
	}()
	var want map[uint64]*bucket
	select {
	case want = <-refilled:
	case <-time.After(5 * time.Second):
		t.Fatal("refill did not complete before the local compaction check")
	}
	if sh.compactIfContracted(1000, 0.5, distributed) {
		t.Fatal("stale store-wide advice must not rebuild a refilled shard")
	}
	assertCompactionContractShard(t, sh, want, 17)
}

func TestStoreCompactionConcurrentRefillPreservesEntries(t *testing.T) {
	store := NewStore(DefaultConfig(), []string{"20m"})
	want := make([]map[uint64]*bucket, 64)
	for i := range store.shards {
		want[i] = growCompactionContractShard(&store.shards[i], 17, 8)
		for key := uint64(9); key <= 17; key++ {
			want[i][key] = &bucket{weight: float64(key)}
		}
	}
	start := make(chan struct{})
	refillDone, compactDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(refillDone)
		<-start
		for key := uint64(9); key <= 17; key++ {
			for i := range store.shards {
				sh := &store.shards[i]
				sh.mu.Lock()
				sh.buckets[key] = want[i][key]
				sh.peak = max(sh.peak, len(sh.buckets))
				sh.mu.Unlock()
			}
		}
	}()
	go func() {
		defer close(compactDone)
		<-start
		for range 64 {
			store.Compact(1000, 0.5)
		}
	}()
	close(start)
	for _, done := range []<-chan struct{}{refillDone, compactDone} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent refill/compaction did not finish")
		}
	}
	assertCompactionContractStore(t, store, want, 17)
	if got := store.TotalBuckets(); got != 1088 {
		t.Fatalf("retained entries=%d, want exactly 1088 after refill", got)
	}
}

func TestShardCompactionNilAndEmpty(t *testing.T) {
	for _, current := range []int{0, 8} {
		t.Run(fmt.Sprintf("live_%d", current), func(t *testing.T) {
			sh := shard{buckets: make(map[uint64]*bucket)}
			want := growCompactionContractShard(&sh, 20, current)
			if current > 0 {
				sh.mu.Lock()
				sh.buckets[100] = nil
				sh.mu.Unlock()
			}
			if !sh.compactIfContracted(10, 0.5, false) {
				t.Fatal("contracted large shard did not rebuild")
			}
			assertCompactionContractShard(t, &sh, want, current)
			if sh.buckets == nil {
				t.Fatal("compaction must retain a writable nonnil map")
			}
		})
	}
}

func TestStoreCompactionNormalizesArguments(t *testing.T) {
	for _, tc := range []struct {
		name  string
		peak  int
		ratio float64
	}{
		{"zero", 0, 0},
		{"negative", -1, -0.5},
		{"ratio_one", 1000, 1},
		{"ratio_above_one", 1000, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewStore(DefaultConfig(), []string{"20m"})
			for i := range store.shards {
				growCompactionContractShard(&store.shards[i], 17, 8)
			}
			if got := store.Compact(tc.peak, tc.ratio); got != 64 {
				t.Fatalf("normalized default compaction rebuilt %d shards, want 64", got)
			}
		})
	}
}

// Grow each map for real, then delete entries. Synthetic peak values alone
// cannot establish the retention/identity contract after map contraction.
func growCompactionContractShard(sh *shard, peak, current int) map[uint64]*bucket {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for key := uint64(1); key <= uint64(peak); key++ {
		sh.buckets[key] = &bucket{weight: float64(key), count: uint32(key), lastUpdate: int64(key)}
		sh.peak = max(sh.peak, len(sh.buckets))
	}
	for key := uint64(current + 1); key <= uint64(peak); key++ {
		delete(sh.buckets, key)
	}
	return maps.Clone(sh.buckets)
}

func assertCompactionContractShard(t *testing.T, sh *shard, want map[uint64]*bucket, wantPeak int) {
	t.Helper()
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if len(sh.buckets) != len(want) || sh.peak != wantPeak {
		t.Fatalf("shard has entries=%d peak=%d; want entries=%d peak=%d", len(sh.buckets), sh.peak, len(want), wantPeak)
	}
	for key, pointer := range want {
		if got, ok := sh.buckets[key]; !ok || got != pointer {
			t.Fatalf("key=%x pointer=%p present=%v; want identical bucket %p", key, got, ok, pointer)
		}
	}
}

func assertCompactionContractStore(t *testing.T, store *Store, want []map[uint64]*bucket, wantPeak int) {
	t.Helper()
	if len(store.shards) != len(want) {
		t.Fatalf("shards=%d, want %d", len(store.shards), len(want))
	}
	for i := range store.shards {
		assertCompactionContractShard(t, &store.shards[i], want[i], wantPeak)
	}
}
