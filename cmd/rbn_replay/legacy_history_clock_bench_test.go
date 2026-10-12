// File role: Measures real replay maintenance dispatch without timers, workers,
// query pruning, or rebuilding the bounded retained population inside timing.
package main

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/spot"
)

func BenchmarkReplayLegacyClockMaintenance(b *testing.B) {
	for _, due := range []bool{false, true} {
		for _, capacity := range []int{16, 64} {
			b.Run(fmt.Sprintf("due_%t/receivers_%d", due, capacity), func(b *testing.B) {
				at := time.Date(2020, 2, 14, 12, 0, 0, 123, time.UTC)
				store := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{Window: time.Hour,
					Shards: 1, MaxEntries: 32, MaxSpottersPerCall: capacity, CleanupInterval: 10 * time.Minute})
				for key := 0; key < 32; key++ {
					for receiver := 0; receiver < capacity; receiver++ {
						store.RecordAt(fmt.Sprintf("K%dABC", key), "20m", "CW", fmt.Sprintf("W%03d", receiver), at, at)
					}
				}
				r := &replayRunner{recentBandStore: store, nextLegacyCleanup: at.Add(10 * time.Minute)}
				if replayLegacyClockKeys(store) != 32 || store.ActiveCallCount(at) != 32 {
					b.Fatal("maintenance cost fixture failed actual bounded admission")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if due {
						r.nextLegacyCleanup = at
					}
					r.maintainLegacyHistory(at)
				}
				b.StopTimer()
				if !r.nextLegacyCleanup.Equal(at.Add(10*time.Minute)) || replayLegacyClockKeys(store) != 32 || store.ActiveCallCount(at) != 32 {
					b.Fatal("dispatch changed eligible population or its next scalar deadline")
				}
			})
		}
	}
}
