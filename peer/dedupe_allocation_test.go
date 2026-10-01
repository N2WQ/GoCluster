package peer

import (
	"runtime"
	"testing"
	"time"
	"unsafe"
)

// This oracle was read independently from Go 1.26.4's
// src/internal/runtime/gc/sizeclasses.go (SHA256
// 7626B00BBB416C25C5279B29AA73A072D74ACE3444E58B178A6FF855A2DEEC2C).
// It deliberately does not call the production allocation estimator. Strings
// contain no pointers; larger objects use whole 8192-byte runtime pages.
var dedupeOracleClasses = [...]int{
	8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224,
	240, 256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768,
	896, 1024, 1152, 1280, 1408, 1536, 1792, 2048, 2304, 2688, 3072,
	3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472,
	9728, 10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480,
	21760, 24576, 27264, 28672, 32768,
}

func dedupeOracleAllocation(size int) int {
	if size == 0 {
		return 0
	}
	for _, class := range dedupeOracleClasses {
		if size <= class {
			return class
		}
	}
	return ((size + 8191) / 8192) * 8192
}

// For any admitted key distribution, allocated key storage is at most
// ceil(5*keyBytes/4)+7*count, plus tiny-allocator slack below. Every item uses one
// <=48-byte pointer-bearing allocation, one 8-byte bucket slot, and one 8-byte
// expiry slot. Fixed bucket counts equal the power-of-two production limits.
// Two 8192-byte allowances cover conservative array header/page rounding;
// another 4096 covers cache/index structs and tiny-allocator fragments on the
// qualified runtime (two allocation Ps). No index or heap growth generation
// exists in a cache. This is owned storage, not a bound on runtime/OS overhead.
func dedupeCompleteAllowance(limit, byteLimit int) int {
	return (5*byteLimit+3)/4 + 7*limit + 48*limit + 16*limit + 2*8192 + 4096
}

func TestDedupeAllocationBound(t *testing.T) {
	for size := 1; size <= 65536; size++ {
		if allocated := dedupeOracleAllocation(size); 4*allocated > 5*size+28 {
			t.Fatalf("string size%d allocation%d exceeds independent inequality", size, allocated)
		}
	}
	// Above 32768, rounding adds at most 8191 bytes, which is less than n/4.
	// Check page transitions through the largest cache key-byte limit as well.
	for size := 32769; size <= 64<<20; size += 8192 {
		if allocated := dedupeOracleAllocation(size); 4*allocated > 5*size+28 {
			t.Fatalf("large string size%d allocation%d exceeds independent inequality", size, allocated)
		}
	}
	if size := unsafe.Sizeof(dedupeItem{}); size > 32 || dedupeOracleAllocation(int(size)+8) > 48 {
		t.Fatalf("cache item changed layout: %d", size)
	}
	if unsafe.Sizeof(dedupeCache{})+unsafe.Sizeof(boundedIndex[string, int64]{}) > 512 {
		t.Fatal("cache metadata no longer fits fixed slack")
	}
	spot := dedupeCompleteAllowance(131072, 64<<20)
	other := 2*dedupeCompleteAllowance(65536, 8<<20) + dedupeCompleteAllowance(8192, 2<<20)
	if spot > 96<<20 || other > 32<<20 {
		t.Fatalf("complete cache envelope exceeded: spot%d other%d", spot, other)
	}
	t.Logf("distribution-independent complete allocation bounds: spot=%d other=%d", spot, other)
}

func TestDedupeFixedFullAllocation(t *testing.T) {
	cases := []struct {
		name     string
		limit    int
		keyBytes int
		keySize  int
	}{
		{"spot", 131072, 64 << 20, 512},
		{"pc92", 65536, 8 << 20, 128},
		{"pc93", 65536, 8 << 20, 128},
		{"bulletin", 8192, 2 << 20, 256},
	}
	// Input fixtures belong to the test driver, outside cache ownership. Build
	// them before each allocation measurement so caller strings are not mistaken
	// for the cache's separately cloned exact keys.
	caches := make([]*dedupeCache, 0, len(cases))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			keys := make([]string, tc.limit)
			for i := range keys {
				keys[i] = qualificationKey(tc.name, i, tc.keySize)
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			cache := newBoundedDedupe(600*time.Second, tc.limit, tc.keyBytes)
			if len(cache.items.buckets) != tc.limit || cap(cache.expiry) != tc.limit {
				t.Fatal("production bucket or expiry capacity changed the allocation proof")
			}
			bucketStart := &cache.items.buckets[0]
			heapStart := &cache.expiry[:cap(cache.expiry)][0]
			now := time.Now()
			for _, key := range keys {
				if cache.admit(key, now) != dedupeAccepted {
					t.Fatal("supported full count/byte occupancy refused")
				}
			}
			runtime.ReadMemStats(&after)
			allocated := after.TotalAlloc - before.TotalAlloc
			if allocated > uint64(dedupeCompleteAllowance(tc.limit, tc.keyBytes)) {
				t.Fatalf("cache allocation=%d exceeds proved allowance=%d", allocated, dedupeCompleteAllowance(tc.limit, tc.keyBytes))
			}
			if len(cache.expiry) != tc.limit || cache.items.Len() != tc.limit || cache.bytes != tc.keyBytes {
				t.Fatal("measurement did not reach both full limits")
			}
			if cache.admit(keys[0], now.Add(600*time.Second)) != dedupeDuplicate {
				t.Fatal("full cache duplicate refused")
			}
			cache.prune(now.Add(601 * time.Second))
			if cache.items.Len() != 0 || len(cache.expiry) != 0 || cache.bytes != 0 {
				t.Fatal("full expiry failed to release owned entries")
			}
			for _, key := range keys {
				if cache.admit(key, now.Add(601*time.Second)) != dedupeAccepted {
					t.Fatal("fixed backing could not be reused")
				}
			}
			if bucketStart != &cache.items.buckets[0] || heapStart != &cache.expiry[:cap(cache.expiry)][0] {
				t.Fatal("fixed cache backing grew or moved during fill/expiry/refill")
			}
			t.Logf("full count=%d key_bytes=%d TotalAlloc=%d allowance=%d", tc.limit, tc.keyBytes, allocated, dedupeCompleteAllowance(tc.limit, tc.keyBytes))
			caches = append(caches, cache)
			runtime.KeepAlive(keys)
		})
	}
	// Retain all four full caches together. Their complete simultaneous bound is
	// the sum proved above, not separate retained-heap samples after collection.
	for _, cache := range caches {
		if cache.items.Len() != cache.limit || cache.bytes != cache.byteLimit {
			t.Fatal("simultaneous cache occupancy lost")
		}
	}
	runtime.KeepAlive(caches)
}
