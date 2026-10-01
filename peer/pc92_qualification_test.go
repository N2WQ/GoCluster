package peer

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// These opt-in tests qualify cache storage/cleanup only. They do not stand in
// for Q1-Q6 end-to-end delivery, per-client latency, graph, handshake, or live
// DXSpider evidence. The sustained profile intentionally has no short-duration
// override; a short cache benchmark is not the approved >600-second workload.
func requireCacheQualification(t *testing.T, name string) {
	t.Helper()
	if os.Getenv("GOCLUSTER_PC92_QUALIFICATION") != name {
		t.Skip("opt-in cache qualification; see docs/pc92-qualification.md")
	}
}

func qualificationKey(prefix string, sequence, size int) string {
	key := fmt.Sprintf("%s:%012d:", prefix, sequence)
	return key + strings.Repeat("x", size-len(key))
}

func TestPC92QualificationCacheMemory(t *testing.T) {
	requireCacheQualification(t, "cache-memory")
	const entries, keyBytes, spotBudget = 131072, 373, 96 << 20
	runtime.GC()
	var before, sample runtime.MemStats
	runtime.ReadMemStats(&before)
	spot := newBoundedDedupe(600*time.Second, entries, 64<<20)
	now := time.Now()
	peakHeap := before.HeapAlloc
	for cycle := 0; cycle < 3; cycle++ {
		for i := 0; i < entries; i++ {
			if got := spot.admit(qualificationKey("spot", cycle*entries+i, keyBytes), now); got != dedupeAccepted {
				t.Fatalf("fill cycle%d key%d admission=%d", cycle, i, got)
			}
			if i%4096 == 0 {
				runtime.ReadMemStats(&sample)
				if sample.HeapAlloc > peakHeap {
					peakHeap = sample.HeapAlloc
				}
			}
		}
		count, bytes, refused := spot.occupancy()
		if count != entries || bytes != entries*keyBytes || refused != uint64(cycle) || len(spot.expiry) != entries {
			t.Fatalf("full cache/index accounting: count%d bytes%d refusals%d expiry%d", count, bytes, refused, len(spot.expiry))
		}
		first := qualificationKey("spot", cycle*entries, keyBytes)
		if spot.admit(first, now.Add(600*time.Second)) != dedupeDuplicate {
			t.Fatal("TTL boundary or full-cache duplicate evicted/refreshed")
		}
		if spot.admit(qualificationKey("new", cycle, keyBytes), now) != dedupeFull {
			t.Fatal("full cache admitted an untrackable key")
		}
		runtime.GC()
		runtime.ReadMemStats(&sample)
		live := sample.HeapAlloc - before.HeapAlloc
		if live > spotBudget {
			t.Fatalf("spot retained heap%d > budget%d", live, spotBudget)
		}
		t.Logf("spot cycle=%d entries=%d key_bytes=%d retained_heap_delta=%d sampled_process_heap_delta=%d", cycle, count, bytes, live, peakHeap-before.HeapAlloc)
		now = now.Add(601 * time.Second)
		spot.prune(now)
		count, bytes, _ = spot.occupancy()
		if count != 0 || bytes != 0 || len(spot.expiry) != 0 {
			t.Fatal("expiry left primary or secondary state")
		}
	}
	// Populate every class concurrently before measuring. No artificial refusal
	// is accepted as evidence that the simultaneous full occupancy is safe.
	for i := 0; i < entries; i++ {
		if spot.admit(qualificationKey("spot", i, keyBytes), now) != dedupeAccepted {
			t.Fatal("spot refill")
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&sample)
	spotFullHeap := sample.HeapAlloc
	other := []*dedupeCache{newBoundedDedupe(600*time.Second, 65536, 8<<20), newBoundedDedupe(600*time.Second, 65536, 8<<20), newBoundedDedupe(600*time.Second, 8192, 2<<20)}
	for index, cache := range other {
		size := 128
		if index == 2 {
			size = 256
		}
		for i := 0; i < cache.limit; i++ {
			if cache.admit(qualificationKey(fmt.Sprintf("class%d", index), i, size), now) != dedupeAccepted {
				t.Fatalf("class%d key%d refused", index, i)
			}
		}
		if len(cache.expiry) != cache.limit || cache.items.Len() != cache.limit || cache.bytes != cache.byteLimit {
			t.Fatalf("class%d not full", index)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&sample)
	if live := sample.HeapAlloc - spotFullHeap; live > 32<<20 {
		t.Fatalf("non-spot caches retained_heap_delta=%d >32MiB dedicated allocation", live)
	}
	if live := sample.HeapAlloc - before.HeapAlloc; live > (96+32)<<20 {
		t.Fatalf("concurrent caches retained_heap_delta=%d >128MiB", live)
	}
	t.Logf("simultaneous four-class retained_heap_delta=%d non_spot_delta=%d process_heap=%d heap_sys=%d", sample.HeapAlloc-before.HeapAlloc, sample.HeapAlloc-spotFullHeap, sample.HeapAlloc, sample.HeapSys)
	runtime.KeepAlive(spot)
	runtime.KeepAlive(other)
}

func TestPC92QualificationCacheSustained(t *testing.T) {
	requireCacheQualification(t, "cache-sustained")
	const load, drain = 45 * time.Minute, 11 * time.Minute
	spot := newBoundedDedupe(600*time.Second, 131072, 64<<20)
	pc92 := newBoundedDedupe(600*time.Second, 65536, 8<<20)
	pc93 := newBoundedDedupe(600*time.Second, 65536, 8<<20)
	bulletin := newBoundedDedupe(600*time.Second, 8192, 2<<20)
	classes := []*dedupeCache{spot, pc92, pc93, bulletin}
	var totals [5]int64
	rates := [5]int64{10000, 100000, 6000, 100, 20}
	started := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	nextReport := time.Minute
	for now := range ticker.C {
		if time.Since(now) > time.Second {
			t.Fatal("load driver fell more than one second behind its scheduled arrival stream")
		}
		elapsed := now.Sub(started)
		if elapsed > load {
			elapsed = load
		}
		for stream, rate := range rates {
			want := int64(elapsed) * rate / int64(time.Minute)
			for totals[stream] < want {
				i := totals[stream]
				cache, key, expected := spot, "", dedupeAccepted
				switch stream {
				case 0:
					key = qualificationSpotKey(i)
				case 1:
					if totals[0] == 0 {
						t.Fatal("duplicate stream before first new key")
					}
					window := totals[0]
					if window > 90000 {
						window = 90000
					}
					key, expected = qualificationSpotKey(totals[0]-1-i%window), dedupeDuplicate
				case 2:
					cache, key = pc92, qualificationKey("pc92", int(i), 128)
				case 3:
					cache, key = pc93, qualificationKey("pc93", int(i), 128)
				case 4:
					cache, key = bulletin, qualificationKey("bulletin", int(i), 256)
				}
				if got := cache.admit(key, now); got != expected {
					t.Fatalf("stream%d item%d admitted%d want%d elapsed%s", stream, i, got, expected, elapsed)
				}
				totals[stream]++
			}
		}
		for _, cache := range classes {
			cache.prune(now)
		}
		if elapsed >= nextReport {
			count, bytes, refused := spot.occupancy()
			t.Logf("elapsed=%s totals=%v spot_entries=%d spot_key_bytes=%d refusals=%d", elapsed, totals, count, bytes, refused)
			nextReport += time.Minute
		}
		if elapsed == load {
			break
		}
	}
	for i, rate := range rates {
		if totals[i] != rate*45 {
			t.Fatalf("stream%d totals%d expected%d", i, totals[i], rate*45)
		}
	}
	deadline := time.Now().Add(drain)
	for now := range ticker.C {
		for _, cache := range classes {
			cache.prune(now)
		}
		if !now.Before(deadline) {
			break
		}
	}
	for i, cache := range classes {
		count, bytes, refused := cache.occupancy()
		if count != 0 || bytes != 0 || refused != 0 || len(cache.expiry) != 0 {
			t.Fatalf("class%d did not drain cleanly: %d/%d/%d expiry%d", i, count, bytes, refused, len(cache.expiry))
		}
	}
	t.Logf("completed actual wall duration=%s load=%s drain=%s; totals=%v", time.Since(started), load, drain, totals)
}

func qualificationSpotKey(sequence int64) string {
	frameType := "PC26"
	if sequence%10 < 4 {
		frameType = "PC61"
	} else if sequence%10 < 8 {
		frameType = "PC11"
	}
	return fmt.Sprintf("dx:%s:K1ABC:K2ABC:14000.0:%d", frameType, sequence)
}

func BenchmarkPC92CacheDuplicate(b *testing.B) {
	cache := newBoundedDedupe(600*time.Second, 131072, 64<<20)
	now := time.Now()
	key := qualificationKey("spot", 1, 373)
	if cache.admit(key, now) != dedupeAccepted {
		b.Fatal("setup")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cache.admit(key, now) != dedupeDuplicate {
			b.Fatal("duplicate")
		}
	}
}

func BenchmarkPC92CacheAdmitAndExpire(b *testing.B) {
	cache := newBoundedDedupe(600*time.Second, 131072, 64<<20)
	keys := make([]string, 131072)
	for i := range keys {
		keys[i] = qualificationKey("spot", i, 373)
	}
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// A 6 ms interarrival is exactly 10,000 new keys/minute. A wrapped
		// key has aged past TTL before reuse, exercising steady-state expiry.
		at := now.Add(time.Duration(i) * 6 * time.Millisecond)
		if cache.admit(keys[i%len(keys)], at) != dedupeAccepted {
			b.Fatal("admission refused")
		}
	}
	b.StopTimer()
	if cache.items.Len() > 131072 || len(cache.expiry) != cache.items.Len() {
		b.Fatal("unbounded cache/index")
	}
}

func BenchmarkPC92CacheConcentratedExpiry(b *testing.B) {
	now := time.Now()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cache := newBoundedDedupe(600*time.Second, 131072, 64<<20)
		for j := 0; j < 131072; j++ {
			if cache.admit(qualificationKey("spot", j, 373), now) != dedupeAccepted {
				b.Fatal("setup")
			}
		}
		b.StartTimer()
		cache.prune(now.Add(601 * time.Second))
		b.StopTimer()
		if cache.items.Len() != 0 || len(cache.expiry) != 0 || cache.bytes != 0 {
			b.Fatal("incomplete expiry")
		}
	}
}
