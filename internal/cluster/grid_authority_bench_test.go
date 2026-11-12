package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"dxcluster/gridstore"
)

// Four concurrent cold workers plus one held owner match the five-worker bound.
// This exercises real authority contention while map compaction is unavoidable.
func BenchmarkGridAuthorityColdParallel(b *testing.B) {
	g := newGridCoordinator(nil)
	g.handle = newGridStoreHandle(nil)
	g.metrics = &gridMetrics{}
	anchor, _ := g.beginLookup("K1ANCHOR")
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("K1PAR%08d", i)
	}
	at := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func(offset int) {
			defer workers.Done()
			rec := authorityRecord("FN31AB", false, at)
			for i := offset; i < b.N; i += 4 {
				call := keys[i%len(keys)]
				rec.Call = call
				token, _ := g.beginLookup(call)
				result, _ := g.finishLookup(token, &rec, nil)
				g.endLookup(token)
				if result != (gridLookupResult{grid: "FN31AB", ok: true}) {
					b.Errorf("cold concurrent tuple=%+v", result)
					return
				}
			}
		}(worker)
	}
	workers.Wait()
	b.StopTimer()
	g.endLookup(anchor)
	if g.entries != nil || g.keyBytes != 0 || g.pinBytes != 0 {
		b.Fatal("cold concurrent owners retained")
	}
}

// Includes entry/key/identity/pin creation and all release/compaction costs.
// The held owner prevents empty-map reset from hiding historical backing work.
func BenchmarkGridAuthorityColdChurn(b *testing.B) {
	for _, mode := range []string{"empty_reset", "held_anchor"} {
		b.Run(mode, func(b *testing.B) {
			g := newGridCoordinator(nil)
			g.handle = newGridStoreHandle(nil)
			g.metrics = &gridMetrics{}
			var anchor gridLookupToken
			if mode == "held_anchor" {
				anchor, _ = g.beginLookup("K1ANCHOR")
			}
			keys := make([]string, 1024)
			for i := range keys {
				keys[i] = fmt.Sprintf("K1COLD%08d", i)
			}
			rec := authorityRecord("FN31AB", false, time.Now())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				call := keys[i%len(keys)]
				rec.Call = call
				token, _ := g.beginLookup(call)
				result, _ := g.finishLookup(token, &rec, nil)
				if result.grid != "FN31AB" || result.derived || !result.ok {
					b.Fatal("cold reported tuple lost")
				}
				g.endLookup(token)
			}
			b.StopTimer()
			if mode == "held_anchor" {
				g.endLookup(anchor)
			}
			if g.entries != nil || g.pending != 0 || g.keyBytes != 0 || g.pinBytes != 0 {
				b.Fatal("cold churn retained authority")
			}
		})
	}
}

// This identical harness is measured before and after the authority change.
// Timed work contains only actual warm hooks and worker-local selection.
func BenchmarkGridAuthorityParallel(b *testing.B) {
	for _, shape := range []string{"same_key", "sharded"} {
		for _, workload := range []string{"cache_hit", "unchanged_update", "mixed_90_10"} {
			b.Run(shape+"/"+workload, func(b *testing.B) {
				store, err := gridstore.Open(b.TempDir(), gridRetirementStoreOptions())
				if err != nil {
					b.Fatal(err)
				}
				cache := newCallMetaCache(256, 0)
				keys := []string{"K1ABC"}
				if shape == "sharded" {
					keys = nil
					seen := make(map[*callMetaCacheShard]bool)
					for i := 0; len(keys) < callMetaCacheShardCount; i++ {
						key := fmt.Sprintf("K1A%04d", i)
						shard := cache.shardFor(key)
						if !seen[shard] {
							seen[shard] = true
							keys = append(keys, key)
						}
					}
				}
				for _, key := range keys {
					cache.UpdateGrid(key, "FN20AB", false)
				}
				update, _, _, stop, lookup, _ := startGridWriter(newGridStoreHandle(store), time.Hour, cache, 0, true, nil)
				defer func() { stop(); _ = store.Close() }()
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := keys[i%len(keys)]
						if workload == "unchanged_update" || workload == "mixed_90_10" && i%10 == 0 {
							update(key, "FN20AB")
						} else if grid, derived, ok := lookup(key); grid != "FN20AB" || derived || !ok {
							b.Errorf("warm hook tuple = %q/%v/%v", grid, derived, ok)
						}
						i++
					}
				})
				b.StopTimer()
				if count, err := store.Count(); err != nil || count != 0 {
					b.Fatalf("warm workload wrote store: %d/%v", count, err)
				}
			})
		}
	}
}
