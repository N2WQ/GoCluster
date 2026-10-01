package peer

import (
	"fmt"
	"runtime"
	"testing"
	"unsafe"
)

func assertIndexModel[K comparable, V comparable](t *testing.T, index *boundedIndex[K, V], model map[K]V) {
	t.Helper()
	if index.Len() != len(model) {
		t.Fatalf("size %d, want %d", index.Len(), len(model))
	}
	seen := make(map[K]bool)
	for key, value := range index.All() {
		want, ok := model[key]
		if !ok || value != want || seen[key] {
			t.Fatal("enumeration lost exact identity, repeated an entry, or returned a stale value")
		}
		seen[key] = true
	}
	for key, value := range model {
		got, ok := index.Get(key)
		if !ok || got != value || !seen[key] {
			t.Fatal("model key missing from lookup or enumeration")
		}
	}
}

func TestBoundedIndexCollisionModel(t *testing.T) {
	index := newFixedIndex[ingressKey, int](64)
	// Force every key into one bucket without using the production hash to
	// select the oracle or the collision corpus. Exact pair equality still wins.
	index.buckets = make([]*boundedEntry[ingressKey, int], 1)
	model := make(map[ingressKey]int)
	keys := []ingressKey{{"AB", "C"}, {"A", "BC"}, {"A\x00B", "C"}, {"", "ABC"}}
	for i := len(keys); i < 64; i++ {
		keys = append(keys, ingressKey{fmt.Sprint(i), "peer"})
	}
	for i, key := range keys {
		if index.Set(key, i) == nil {
			t.Fatal("collision refused a fitting key")
		}
		model[key] = i
		assertIndexModel(t, index, model)
	}
	if index.Set(ingressKey{"overflow", "peer"}, 0) != nil {
		t.Fatal("cardinality overflow admitted")
	}
	for _, i := range []int{0, 31, 63} {
		entry := index.Entry(keys[i])
		if !index.Delete(keys[i]) || index.Delete(keys[i]) {
			t.Fatal("collision-chain delete did not remove exactly one key")
		}
		if entry.key != (ingressKey{}) || entry.value != 0 || entry.next != nil {
			t.Fatal("removed entry retained its key, value, or chain")
		}
		delete(model, keys[i])
		assertIndexModel(t, index, model)
		index.Set(keys[i], -i)
		model[keys[i]] = -i
		assertIndexModel(t, index, model)
	}
	if index.Set(keys[1], 999) == nil || index.Value(keys[1]) != 999 {
		t.Fatal("existing key could not update at capacity")
	}
}

func TestBoundedIndexResizeDeleteTraversal(t *testing.T) {
	index := newBoundedIndex[int, int](513)
	model := make(map[int]int)
	for cycle := range 4 {
		for key := range 513 {
			if index.Set(key, key+cycle) == nil {
				t.Fatal("fitting key refused during growth")
			}
			model[key] = key + cycle
			if len(index.buckets) != indexBucketCount(index.Len()) {
				t.Fatal("bucket storage grew independently of cardinality")
			}
		}
		assertIndexModel(t, index, model)
		visited := make(map[int]bool)
		for key := range index.All() {
			if visited[key] {
				t.Fatal("entry repeated during deletion traversal")
			}
			visited[key] = true
			if key >= 128 {
				index.Delete(key)
				delete(model, key)
			}
		}
		if len(visited) != 513 || len(index.buckets) != 1024 {
			t.Fatal("deletion skipped an entry or silently compacted live traversal")
		}
		index.Compact()
		if len(index.buckets) != 128 {
			t.Fatal("explicit compaction did not reclaim bucket backing")
		}
		assertIndexModel(t, index, model)
		for key := range index.All() {
			index.Delete(key)
			delete(model, key)
		}
		index.Compact()
		if index.buckets != nil {
			t.Fatal("empty index retained historical bucket backing")
		}
		assertIndexModel(t, index, model)
	}
}

func TestBoundedIndexZeroAndDuplicateAllocation(t *testing.T) {
	var absent *boundedIndex[string, int]
	if absent.Len() != 0 || absent.Entry("key") != nil || absent.Set("key", 1) != nil || absent.Delete("key") {
		t.Fatal("nil index is not an empty read-only owner")
	}
	for range absent.All() {
		t.Fatal("nil index yielded a key")
	}
	index := newFixedIndex[string, int](1)
	index.Set("key", 1)
	if got := testing.AllocsPerRun(1000, func() {
		if index.Value("key") != 1 || index.Set("key", 1) == nil || index.Set("full", 2) != nil {
			t.Fatal("lookup/update/refusal semantics changed")
		}
		for key, value := range index.All() {
			if key != "key" || value != 1 {
				t.Fatal("enumeration changed")
			}
		}
	}); got != 0 {
		t.Fatalf("lookup/update/full-refusal/enumeration allocated %v objects", got)
	}
}

func TestBoundedIndexAllocationBoundaries(t *testing.T) {
	if unsafe.Sizeof(boundedEntry[string, PC92Entry]{}) != 104 || unsafe.Sizeof(boundedEntry[ingressKey, ingressObservation]{}) != 72 {
		t.Skip("qualified byte-layout evidence is for the documented 64-bit platform")
	}
	index := newBoundedIndex[string, PC92Entry](513)
	keys := make([]string, 513)
	for i := range keys {
		keys[i] = fmt.Sprintf("N%dAA", i)
	}
	for i := range 512 {
		index.Set(keys[i], PC92Entry{Call: keys[i]})
	}
	// An independent envelope includes all 513 112-byte entries and the old
	// 512/new1024 pointer arrays, with16KiB extra for both rounded arrays and
	// the small owner. No post-GC sample stands in for growth overlap.
	const envelope = 513*112 + (512+1024)*8 + 16*1024
	beforeOwned := index.AllocationBytes()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	index.Set(keys[512], PC92Entry{Call: keys[512]})
	runtime.ReadMemStats(&after)
	if peak := uint64(beforeOwned) + after.TotalAlloc - before.TotalAlloc; peak > envelope {
		t.Fatalf("growth overlap %d exceeds independent envelope %d", peak, envelope)
	}
	for key := range index.All() {
		if key != keys[0] {
			index.Delete(key)
		}
	}
	beforeOwned = index.AllocationBytes()
	runtime.ReadMemStats(&before)
	index.Compact()
	runtime.ReadMemStats(&after)
	if peak := uint64(beforeOwned) + after.TotalAlloc - before.TotalAlloc; peak > envelope {
		t.Fatalf("shrink overlap %d exceeds prior high-water envelope %d", peak, envelope)
	}
	if index.Len() != 1 || len(index.buckets) != 1 || index.Value(keys[0]).Call != keys[0] {
		t.Fatal("compaction changed the surviving value")
	}
	runtime.KeepAlive(index)
}

func FuzzBoundedIndexModel(f *testing.F) {
	f.Add([]byte{0, 1, 2, 0, 2, 3, 1, 1, 0, 3, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 8192 {
			t.Skip()
		}
		index := newBoundedIndex[uint8, uint8](64)
		model := make(map[uint8]uint8)
		for i := 0; i+2 < len(data); i += 3 {
			key, value := data[i+1], data[i+2]
			switch data[i] % 4 {
			case 0:
				_, exists := model[key]
				fits := exists || len(model) < 64
				if (index.Set(key, value) != nil) != fits {
					t.Fatal("admission differs from independent cardinality model")
				}
				if fits {
					model[key] = value
				}
			case 1:
				_, exists := model[key]
				if index.Delete(key) != exists {
					t.Fatal("delete differs from model")
				}
				delete(model, key)
			case 2:
				index.Compact()
			case 3:
				for current := range index.All() {
					if current%3 == key%3 {
						index.Delete(current)
						delete(model, current)
					}
				}
			}
			assertIndexModel(t, index, model)
		}
	})
}
