package pathreliability

import (
	"encoding/json"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// TestVOACAPPersistenceRetainedHeap is an opt-in paired measurement. Database
// seeding is excluded; the payload returned by hydration dies in a separate
// frame before GC. This fixture also compiles at the S1 baseline.
func TestVOACAPPersistenceRetainedHeap(t *testing.T) {
	output := os.Getenv("GOCLUSTER_VOACAP_HEAP_OUTPUT")
	if output == "" {
		t.Skip("paired retained-heap measurement is opt-in")
	}
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries = 50000
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	seedVOACAPHeapFixture(t, store, now, 50000)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	hydrateVOACAPHeapFixture(t, store, now, cfg)
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(store)
	report := struct {
		Entries int    `json:"entries"`
		Before  uint64 `json:"before_bytes"`
		After   uint64 `json:"after_bytes"`
		Delta   int64  `json:"delta_bytes"`
	}{50000, before.HeapAlloc, after.HeapAlloc, int64(after.HeapAlloc) - int64(before.HeapAlloc)}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("50000-key hydration retained heap delta: %d bytes", report.Delta)
}

func seedVOACAPHeapFixture(t *testing.T, store *VOACAPForecastCacheStore, now time.Time, size int) {
	t.Helper()
	entry := testVOACAPForecastCacheEntry(now, -14, 20)
	batch := store.db.NewBatch()
	defer batch.Close()
	for i := 1; i <= size; i++ {
		key := testVOACAPForecastCacheKey(now, CellID(i))
		value, err := json.Marshal(diskRecordFromForecastCache(key, entry))
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Set(voacapForecastCacheKeyBytes(key), value, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	// Settle seeded memtables before measuring: asynchronous database flushes
	// would otherwise confound index retention with Pebble's own heap changes.
	if err := store.db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Compact([]byte(voacapForecastCacheEntryKeyPrefix), voacapForecastCacheEntryKeyUpperBound(), true); err != nil {
		t.Fatal(err)
	}
}

func hydrateVOACAPHeapFixture(t *testing.T, store *VOACAPForecastCacheStore, now time.Time, cfg VOACAPFallbackConfig) {
	t.Helper()
	loaded, stats, err := store.loadCurrent(now, 112, cfg)
	if err != nil || len(loaded) != 50000 || stats.Pruned != 0 {
		t.Fatalf("heap fixture hydration: loaded=%d stats=%+v error=%v", len(loaded), stats, err)
	}
}
