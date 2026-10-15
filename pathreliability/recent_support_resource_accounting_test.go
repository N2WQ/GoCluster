package pathreliability

import (
	"fmt"
	"math"
	"testing"
)

// These checks concern process accounting. RuntimeResidual is only the seed
// difference from known new owners; it never identifies or discounts runtime
// work. The controller independently checks all arithmetic, live owner stacks
// and the conservative pair bound, excluding every preexisting warm owner.
func recentResourceValidateAccounting(r *recentResourceReceipt) error {
	previous := r.Start
	for i, epoch := range []recentResourceStats{r.Start, r.Seed, r.PostGC} {
		if !recentResourceStatsValid(epoch) || epoch.TotalAlloc < previous.TotalAlloc || epoch.Mallocs < previous.Mallocs || epoch.Frees < previous.Frees || epoch.TotalAlloc-epoch.HeapAlloc < previous.TotalAlloc-previous.HeapAlloc {
			return fmt.Errorf("invalid/nonmonotonic resource snapshot %d", i)
		}
		previous = epoch
	}
	if r.SeedDelta != recentResourceStatsDifference(r.Seed, r.Start) || r.PostGCDelta != recentResourceStatsDifference(r.PostGC, r.Start) {
		return fmt.Errorf("resource delta does not match absolute snapshots")
	}
	for _, delta := range []recentResourceDelta{r.SeedDelta, r.PostGCDelta} {
		if delta.TotalAlloc < int64(r.ExpectedHeapBytes) || delta.Mallocs < int64(r.ExpectedAllocations) {
			return fmt.Errorf("allocated bytes/objects below verified new-owner minimum")
		}
	}
	r.RuntimeResidual = recentResourceDelta{r.SeedDelta.HeapAlloc - int64(r.ExpectedHeapBytes),
		r.SeedDelta.HeapObjects - int64(r.ExpectedAllocations), r.SeedDelta.Mallocs - int64(r.ExpectedAllocations),
		r.SeedDelta.Frees, r.SeedDelta.TotalAlloc - int64(r.ExpectedHeapBytes)}
	totals, err := recentResourceValidateClasses(r)
	if err != nil {
		return err
	}
	for i, delta := range []recentResourceDelta{r.SeedDelta, r.PostGCDelta} {
		if delta.TotalAlloc < totals[i].bytes || delta.Mallocs < totals[i].mallocs || delta.Frees < totals[i].frees {
			return fmt.Errorf("global accounting below public size-class lower bound at epoch %d", i)
		}
	}
	return nil
}

func recentResourceStatsValid(s recentResourceStats) bool {
	return s.TotalAlloc <= math.MaxInt64 && s.Mallocs <= math.MaxInt64 && s.Frees <= s.Mallocs &&
		s.TotalAlloc >= s.HeapAlloc && s.HeapObjects == s.Mallocs-s.Frees
}

func recentResourceStatsDifference(after, before recentResourceStats) recentResourceDelta {
	return recentResourceDelta{int64(after.HeapAlloc) - int64(before.HeapAlloc), int64(after.HeapObjects) - int64(before.HeapObjects),
		int64(after.Mallocs) - int64(before.Mallocs), int64(after.Frees) - int64(before.Frees), int64(after.TotalAlloc) - int64(before.TotalAlloc)}
}

type recentResourceClassTotal struct{ bytes, mallocs, frees int64 }

func recentResourceValidateClasses(r *recentResourceReceipt) (totals [2]recentResourceClassTotal, err error) {
	r.ObservedAllocatorBytes = 0
	for _, c := range r.Classes {
		if c.StartMallocs > c.SeedMallocs || c.SeedMallocs > c.PostGCMallocs || c.StartFrees > c.SeedFrees || c.SeedFrees > c.PostGCFrees || c.PostGCMallocs > math.MaxInt64 || c.PostGCFrees > c.PostGCMallocs || c.SeedFrees > c.SeedMallocs || c.StartFrees > c.StartMallocs {
			return totals, fmt.Errorf("invalid/nonmonotonic size-class %d", c.Size)
		}
		if c.SeedMallocDelta != int64(c.SeedMallocs-c.StartMallocs) || c.PostGCMallocDelta != int64(c.PostGCMallocs-c.StartMallocs) || c.SeedFreeDelta != int64(c.SeedFrees-c.StartFrees) || c.PostGCFreeDelta != int64(c.PostGCFrees-c.StartFrees) {
			return totals, fmt.Errorf("size-class %d counter subtraction disagrees", c.Size)
		}
		var minimum int64
		if c.Size == r.ExpectedAllocatorBytes {
			minimum = int64(r.Buckets)
		}
		if c.Size == 8 || c.Size == 192 {
			minimum += int64(r.Sidecars)
		}
		if c.SeedMallocDelta < minimum || c.PostGCMallocDelta < minimum {
			return totals, fmt.Errorf("size-class %d allocations below new-owner minimum", c.Size)
		}
		if c.Size == r.ExpectedAllocatorBytes {
			r.ObservedAllocatorBytes = c.Size
		}
		for i, values := range [][2]int64{{c.SeedMallocDelta, c.SeedFreeDelta}, {c.PostGCMallocDelta, c.PostGCFreeDelta}} {
			if err := recentResourceAddClass(&totals[i], c.Size, values[0], values[1]); err != nil {
				return totals, err
			}
		}
	}
	if r.ObservedAllocatorBytes != r.ExpectedAllocatorBytes {
		return totals, fmt.Errorf("bucket allocator class was not observed")
	}
	return totals, nil
}

func recentResourceAddClass(total *recentResourceClassTotal, size uint32, mallocs, frees int64) error {
	if mallocs > math.MaxInt64-total.mallocs || frees > math.MaxInt64-total.frees || size != 0 && mallocs > (math.MaxInt64-total.bytes)/int64(size) {
		return fmt.Errorf("size-class accounting overflow")
	}
	total.bytes += mallocs * int64(size)
	total.mallocs += mallocs
	total.frees += frees
	return nil
}

func TestRecentResourceAccountingAllocationBound(t *testing.T) {
	for _, name := range []string{"quiet", "new32_old16_freed", "temporary64_freed", "old_objects_freed"} {
		t.Run(name, func(t *testing.T) {
			r := recentResourceAccountingVector()
			switch name {
			case "new32_old16_freed":
				recentResourceVectorAllocate(&r, 32)
				r.PostGC.HeapAlloc -= 16
				r.PostGC.HeapObjects--
				r.PostGC.Frees++
				r.Classes[1].PostGCFrees++
			case "temporary64_freed":
				recentResourceVectorAllocate(&r, 64)
				r.PostGC.HeapAlloc -= 64
				r.PostGC.HeapObjects--
				r.PostGC.Frees++
				r.Classes[len(r.Classes)-1].PostGCFrees++
			case "old_objects_freed":
				r.PostGC.HeapAlloc -= 16
				r.PostGC.HeapObjects--
				r.PostGC.Frees++
				r.Classes[1].PostGCFrees++
			}
			recentResourceVectorDeltas(&r)
			if err := recentResourceValidateAccounting(&r); err != nil {
				t.Fatal(err)
			}
			if r.ObservedAllocatorBytes != 480 {
				t.Fatal("allocator class not proved")
			}
			if name == "new32_old16_freed" && (r.RuntimeResidual.TotalAlloc != 32 || r.PostGCDelta.TotalAlloc != 1192) {
				t.Fatal("free supplied allocation credit")
			}
			if name == "temporary64_freed" && r.PostGCDelta.TotalAlloc != 1224 {
				t.Fatal("temporary allocation was discarded")
			}
		})
	}
}

func TestRecentResourceAccountingRejectsInvalid(t *testing.T) {
	cases := []struct {
		name   string
		change func(*recentResourceReceipt)
	}{
		{"missing_total", func(r *recentResourceReceipt) { r.Seed.TotalAlloc = 0 }},
		{"decreasing_total", func(r *recentResourceReceipt) { r.PostGC.TotalAlloc-- }},
		{"decreasing_mallocs", func(r *recentResourceReceipt) { r.PostGC.Mallocs--; r.PostGC.HeapObjects-- }},
		{"decreasing_frees", func(r *recentResourceReceipt) { r.PostGC.Frees--; r.PostGC.HeapObjects++ }},
		{"bad_object_identity", func(r *recentResourceReceipt) { r.Seed.HeapObjects++ }},
		{"heap_above_total", func(r *recentResourceReceipt) { r.Seed.HeapAlloc = r.Seed.TotalAlloc + 1 }},
		{"freed_bytes_decrease", func(r *recentResourceReceipt) { r.Seed.HeapAlloc++ }},
		{"below_owner_bytes", func(r *recentResourceReceipt) { r.Seed.TotalAlloc = r.Start.TotalAlloc + 1159 }},
		{"below_owner_objects", func(r *recentResourceReceipt) { r.Seed.Mallocs = r.Start.Mallocs + 3; r.Seed.HeapObjects-- }},
		{"unobserved_class", func(r *recentResourceReceipt) { r.ExpectedAllocatorBytes = 448 }},
		{"missing_bucket_malloc", func(r *recentResourceReceipt) { r.Classes[3].SeedMallocs--; r.Classes[3].PostGCMallocs-- }},
		{"missing_sidecar_malloc", func(r *recentResourceReceipt) { r.Classes[2].SeedMallocs--; r.Classes[2].PostGCMallocs-- }},
		{"class_decreased", func(r *recentResourceReceipt) { r.Classes[1].SeedMallocs = 0 }},
		{"class_frees_impossible", func(r *recentResourceReceipt) { r.Classes[1].PostGCFrees = 2 }},
		{"extra_class_underreported_bytes", func(r *recentResourceReceipt) {
			recentResourceVectorAllocate(r, 32)
			r.Seed.TotalAlloc -= 32
			r.PostGC.TotalAlloc -= 32
		}},
		{"class_free_underreported", func(r *recentResourceReceipt) { r.Classes[1].PostGCFrees++ }},
		{"counter_range", func(r *recentResourceReceipt) { r.Seed.TotalAlloc = math.MaxUint64 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := recentResourceAccountingVector()
			tc.change(&r)
			recentResourceVectorDeltas(&r)
			if recentResourceValidateAccounting(&r) == nil {
				t.Fatal("invalid counters accepted")
			}
		})
	}
	t.Run("delta_arithmetic", func(t *testing.T) {
		r := recentResourceAccountingVector()
		r.SeedDelta.TotalAlloc++
		if recentResourceValidateAccounting(&r) == nil {
			t.Fatal("wrong subtraction accepted")
		}
	})
	t.Run("class_delta_arithmetic", func(t *testing.T) {
		r := recentResourceAccountingVector()
		r.Classes[3].SeedMallocDelta--
		if recentResourceValidateAccounting(&r) == nil {
			t.Fatal("wrong class subtraction accepted")
		}
	})
}

// Literal new owners: two480-byte buckets plus8/192 sidecar=1160bytes/four
// objects. One old16-byte object permits unrelated old frees in the vectors.
func recentResourceAccountingVector() recentResourceReceipt {
	r := recentResourceReceipt{Buckets: 2, Sidecars: 1, ExpectedAllocations: 4, ExpectedHeapBytes: 1160, ExpectedAllocatorBytes: 480,
		Start: recentResourceStats{10000, 10, 20, 10, 20000}, Seed: recentResourceStats{11160, 14, 24, 10, 21160}, PostGC: recentResourceStats{11160, 14, 24, 10, 21160},
		Classes: []recentResourceClass{{Size: 8, SeedMallocs: 1, PostGCMallocs: 1}, {Size: 16, StartMallocs: 1, SeedMallocs: 1, PostGCMallocs: 1}, {Size: 192, SeedMallocs: 1, PostGCMallocs: 1}, {Size: 480, SeedMallocs: 2, PostGCMallocs: 2}}}
	recentResourceVectorDeltas(&r)
	return r
}

func recentResourceVectorAllocate(r *recentResourceReceipt, size uint32) {
	r.Seed.HeapAlloc += uint64(size)
	r.Seed.TotalAlloc += uint64(size)
	r.Seed.HeapObjects++
	r.Seed.Mallocs++
	r.PostGC = r.Seed
	r.Classes = append(r.Classes, recentResourceClass{Size: size, SeedMallocs: 1, PostGCMallocs: 1})
}

func recentResourceVectorDeltas(r *recentResourceReceipt) {
	r.SeedDelta = recentResourceStatsDifference(r.Seed, r.Start)
	r.PostGCDelta = recentResourceStatsDifference(r.PostGC, r.Start)
	for i := range r.Classes {
		c := &r.Classes[i]
		c.SeedMallocDelta = int64(c.SeedMallocs) - int64(c.StartMallocs)
		c.PostGCMallocDelta = int64(c.PostGCMallocs) - int64(c.StartMallocs)
		c.SeedFreeDelta = int64(c.SeedFrees) - int64(c.StartFrees)
		c.PostGCFreeDelta = int64(c.PostGCFrees) - int64(c.StartFrees)
	}
}
