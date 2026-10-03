package dedup

import (
	"encoding/binary"
	"runtime"
	"testing"
	"time"
)

func BenchmarkDedupeCompleteKeyDuplicate(b *testing.B) {
	b.Run("primary", func(b *testing.B) {
		d := NewDeduplicator(time.Minute, false, 1)
		s := policySpot(time.Unix(60, 0), false, -10)
		d.processSpot(s)
		<-d.outputChan
		b.ReportAllocs()
		for b.Loop() {
			d.processSpot(s)
		}
		if processed, duplicates, size := d.GetStats(); processed != uint64(b.N)+1 || duplicates != uint64(b.N) || size != 1 {
			b.Fatal("duplicate benchmark changed behavior")
		}
	})
	b.Run("secondary", func(b *testing.B) {
		d := NewSecondaryDeduper(time.Minute, false)
		s := policySpot(time.Unix(60, 0), false, -10)
		if !d.ShouldForward(s) {
			b.Fatal("first input suppressed")
		}
		b.ReportAllocs()
		for b.Loop() {
			if d.ShouldForward(s) {
				b.Fatal("duplicate forwarded")
			}
		}
	})
}

func BenchmarkDedupeCleanupPopulated(b *testing.B) {
	d := NewDeduplicator(time.Minute, false, 1)
	now := time.Unix(60, 0)
	for i := range 100000 {
		var key [42]byte
		binary.LittleEndian.PutUint64(key[:], uint64(i))
		d.shards[i&63].cache[key] = cachedEntry{when: now}
	}
	b.ReportAllocs()
	for b.Loop() {
		d.cleanupAt(now, nil)
	}
	if _, _, size := d.GetStats(); size != 100000 {
		b.Fatal("cleanup lost fresh inputs")
	}
}

// Report ordinary shared-ingestion map growth separately from the governed
// peer 480-MiB envelope. This is a controlled live-heap observation on this Go
// build, not a portable map-backing proof or a new cardinality policy.
func TestSharedDedupeKeyBackingMeasurement(t *testing.T) {
	if testing.Short() {
		t.Skip("live-heap comparative measurement")
	}
	const count = 100000
	old := measureKeyMapBacking[uint32](count, func(i int) uint32 { return uint32(i) })
	primary := measureKeyMapBacking[[42]byte](count, func(i int) (key [42]byte) { binary.LittleEndian.PutUint64(key[:], uint64(i)); return })
	secondary := measureKeyMapBacking[[32]byte](count, func(i int) (key [32]byte) { binary.LittleEndian.PutUint64(key[:], uint64(i)); return })
	t.Logf("100000 entries across64 shards; old32=%d primary42=%d secondary32=%d primary_delta=%d secondary_delta=%d bytes; outside peer480MiB accounting", old, primary, secondary, primary-old, secondary-old)
}

func measureKeyMapBacking[K comparable](count int, key func(int) K) int64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var shards [64]map[K]cachedEntry
	for i := range shards {
		shards[i] = make(map[K]cachedEntry)
	}
	for i := range count {
		shards[i&63][key(i)] = cachedEntry{when: time.Unix(60, 0)}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(shards)
	return int64(after.HeapAlloc) - int64(before.HeapAlloc)
}
