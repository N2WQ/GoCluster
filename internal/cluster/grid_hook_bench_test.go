package cluster

import (
	"testing"
	"time"

	"dxcluster/gridstore"
)

// The two cache-shaped production hook paths isolate admission synchronization
// without timing storage, queue saturation or an unrelated full output pipeline.
func BenchmarkGridHookAdmission(b *testing.B) {
	for _, name := range []string{"cache_hit", "unchanged_update"} {
		b.Run(name, func(b *testing.B) {
			store, err := gridstore.Open(b.TempDir(), gridRetirementStoreOptions())
			if err != nil {
				b.Fatal(err)
			}
			cache := newCallMetaCache(64, 0)
			cache.UpdateGrid("K1ABC", "FN20", false)
			update, _, _, stop, lookup, _ := startGridWriter(newGridStoreHandle(store), time.Hour, cache, 0, true, nil)
			defer func() { stop(); _ = store.Close() }()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if name == "cache_hit" {
					if grid, derived, ok := lookup("K1ABC"); grid != "FN20" || derived || !ok {
						b.Fatal("cached hook lost its locator")
					}
				} else {
					update("K1ABC", "FN20")
				}
			}
			b.StopTimer()
			if count, err := store.Count(); err != nil || count != 0 {
				b.Fatalf("cache-only workload wrote store: %d %v", count, err)
			}
		})
	}
}
