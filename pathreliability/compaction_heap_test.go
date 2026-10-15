package pathreliability

import (
	"encoding/json"
	"os"
	"runtime"
	"testing"
	"unsafe"
)

// TestStoreCompactionRetainedMapHeap isolates map backing storage: every key
// points to the same live bucket, and no receiver sidecar is allocated.
func TestStoreCompactionRetainedMapHeap(t *testing.T) {
	output := os.Getenv("GOCLUSTER_COMPACTION_HEAP_OUTPUT")
	if output == "" {
		t.Skip("map-only retained-heap artifact is opt-in")
	}
	store := NewStore(DefaultConfig(), []string{"20m"})
	owner := &bucket{weight: 1}
	runtime.GC()
	var base, sparse, compact runtime.MemStats
	runtime.ReadMemStats(&base)
	seedSparseCompactionMaps(store, owner)
	runtime.GC()
	runtime.ReadMemStats(&sparse)
	rebuilt := store.Compact(1000, 0.5)
	runtime.GC()
	runtime.ReadMemStats(&compact)
	runtime.KeepAlive(store)
	runtime.KeepAlive(owner)

	beforeBytes := int64(sparse.HeapAlloc) - int64(base.HeapAlloc)
	afterBytes := int64(compact.HeapAlloc) - int64(base.HeapAlloc)
	freed := int64(sparse.HeapAlloc) - int64(compact.HeapAlloc)
	report := struct {
		Shards, PeakPerShard, RetainedPerShard, Rebuilt int
		BucketLogicalBytes                              uintptr
		BeforeMapBytes, AfterMapBytes, FreedMapBytes    int64
		MinimumReleaseFraction, ReleaseFraction         float64
	}{64, 512, 8, rebuilt, unsafe.Sizeof(bucket{}), beforeBytes, afterBytes, freed, 0.5, float64(freed) / float64(beforeBytes)}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if len(store.shards) != 64 || rebuilt != 64 || store.TotalBuckets() != 512 || owner.extraSlots != nil {
		t.Fatalf("map-only fixture changed owners: shards=%d rebuilt=%d entries=%d sidecar=%t", len(store.shards), rebuilt, store.TotalBuckets(), owner.extraSlots != nil)
	}
	for index := range store.shards {
		sh := &store.shards[index]
		sh.mu.RLock()
		if len(sh.buckets) != 8 || sh.peak != 8 {
			sh.mu.RUnlock()
			t.Fatalf("shard %d failed retained count/peak contract", index)
		}
		for item := 1; item <= 8; item++ {
			key := uint64(index)<<32 | uint64(item)
			if sh.buckets[key] != owner {
				sh.mu.RUnlock()
				t.Fatalf("shard %d key %d lost its exact live bucket", index, key)
			}
		}
		sh.mu.RUnlock()
	}
	if beforeBytes <= 0 || float64(freed) < float64(beforeBytes)*0.5 {
		t.Fatalf("insufficient map-only release: before=%d after=%d freed=%d; want >=50%%", beforeBytes, afterBytes, freed)
	}
	t.Logf("map-only retained heap: before=%d after=%d freed=%d (%.2f%%)", beforeBytes, afterBytes, freed, report.ReleaseFraction*100)
}

func seedSparseCompactionMaps(store *Store, owner *bucket) {
	for index := range store.shards {
		sh := &store.shards[index]
		sh.mu.Lock()
		for item := 1; item <= 512; item++ {
			sh.buckets[uint64(index)<<32|uint64(item)] = owner
		}
		sh.peak = len(sh.buckets)
		for item := 9; item <= 512; item++ {
			delete(sh.buckets, uint64(index)<<32|uint64(item))
		}
		sh.mu.Unlock()
	}
}
