package pathreliability

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// BenchmarkVOACAPCompletePersistence includes encoding, disk mutation and all
// completion maintenance. Seed creation and hydration are outside the timer.
// Keep this fixture identical in the S1 baseline and candidate checkouts.
func BenchmarkVOACAPCompletePersistence(b *testing.B) {
	for _, size := range []int{1, 64, 256, 10000, 50000} {
		for _, operation := range []string{"refresh", "insert"} {
			b.Run(fmt.Sprintf("n%d/%s", size, operation), func(b *testing.B) {
				if operation == "insert" && b.N > 65535-size {
					b.Fatalf("fixed fixture exceeds CellID space: use <=%dx", 65535-size)
				}
				now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
				cfg := testVOACAPFallbackConfig().VOACAPFallback
				cfg.MaxCacheEntries = size
				store, err := OpenVOACAPForecastCacheStore(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := store.Close(); err != nil {
						b.Error(err)
					}
				})
				entry := testVOACAPForecastCacheEntry(now, -14, 20)
				batch := store.db.NewBatch()
				for i := 1; i <= size; i++ {
					key := testVOACAPForecastCacheKey(now, CellID(i))
					value, err := json.Marshal(diskRecordFromForecastCache(key, entry))
					if err != nil {
						b.Fatal(err)
					}
					if err := batch.Set(voacapForecastCacheKeyBytes(key), value, nil); err != nil {
						b.Fatal(err)
					}
				}
				if err := batch.Commit(pebble.NoSync); err != nil {
					b.Fatal(err)
				}
				if err := batch.Close(); err != nil {
					b.Fatal(err)
				}
				loaded, stats, err := store.loadCurrent(now, 112, cfg)
				if err != nil || len(loaded) != size || stats.Pruned != 0 {
					b.Fatalf("seed hydration: loaded=%d stats=%+v error=%v", len(loaded), stats, err)
				}
				key := testVOACAPForecastCacheKey(now, 1)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					at := now.Add(time.Duration(i+1) * time.Nanosecond)
					entry.storedAt = at
					if operation == "insert" {
						key.dxCell = CellID(size + i + 1)
					}
					if err := store.storeForecast(key, entry, cfg, at); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
			})
		}
	}
}
