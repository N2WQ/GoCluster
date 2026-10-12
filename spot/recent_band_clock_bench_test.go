// File role: Measures actual bounded legacy admission, eligible queries, and
// explicit-time maintenance with correctness guards outside measured loops.
package spot

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func BenchmarkRecentBandClockQuery(b *testing.B) {
	for _, capacity := range []int{16, 64} {
		for _, normalized := range []bool{false, true} {
			b.Run(fmt.Sprintf("receivers_%d/normalized_%t", capacity, normalized), func(b *testing.B) {
				now := time.Date(2030, 2, 14, 12, 0, 0, 123, time.UTC)
				s := newRecentBandClockStore(capacity)
				for i := 0; i < capacity; i++ {
					s.RecordAt("K1ABC", "20m", "CW", fmt.Sprintf("W%03d", i), now, now)
				}
				query := s.RecentSupportCount
				if normalized {
					query = s.RecentSupportCountNormalized
				}
				if query("K1ABC", "20m", "CW", now) != capacity {
					b.Fatal("query benchmark did not populate actual receiver capacity")
				}
				b.ReportAllocs()
				b.ResetTimer()
				count := 0
				for i := 0; i < b.N; i++ {
					count = query("K1ABC", "20m", "CW", now)
				}
				b.StopTimer()
				if count != capacity {
					b.Fatal("query benchmark lost eligible support")
				}
				recentBandClockAssertCoupling(b, s, false)
			})
		}
	}
}

func BenchmarkRecentBandClockAdmission(b *testing.B) {
	for _, path := range []string{"existing", "reject_future", "receiver_overflow", "key_churn"} {
		b.Run(path, func(b *testing.B) {
			now := time.Date(2030, 2, 14, 12, 0, 0, 123, time.UTC)
			s := NewRecentBandStoreWithOptions(RecentBandOptions{Window: time.Hour, Shards: 1, MaxEntries: 64, MaxSpottersPerCall: 64})
			calls, receivers := make([]string, 65), make([]string, 65)
			for i := range calls {
				calls[i], receivers[i] = fmt.Sprintf("K%dABC", i), fmt.Sprintf("W%03d", i)
				if i < 64 {
					s.RecordAt("K1ABC", "20m", "CW", receivers[i], now, now)
					if path == "key_churn" {
						s.RecordAt(calls[i], "40m", "CW", "W1AAA", now, now)
					}
				}
			}
			before := recentBandClockSnapshot(b, s)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				at := now.Add(time.Duration(i+1) * time.Nanosecond)
				switch path {
				case "existing":
					s.RecordAt("K1ABC", "20m", "CW", receivers[0], at, at)
				case "reject_future":
					s.RecordAt("K1NEW", "20m", "CW", "W1FUT", now.Add(time.Nanosecond), now)
				case "receiver_overflow":
					s.RecordAt("K1ABC", "20m", "CW", receivers[(i+64)%len(receivers)], at, at)
				case "key_churn":
					s.RecordAt(calls[(i+64)%len(calls)], "40m", "CW", "W1AAA", at, at)
				}
			}
			b.StopTimer()
			recentBandClockAssertCoupling(b, s, false)
			if path == "reject_future" && !reflect.DeepEqual(before, recentBandClockSnapshot(b, s)) {
				b.Fatal("future-rejection benchmark changed physical history")
			}
			if path == "existing" || path == "receiver_overflow" {
				if n := s.RecentSupportCount("K1ABC", "20m", "CW", now.Add(time.Duration(b.N+1)*time.Nanosecond)); n != 64 {
					b.Fatalf("bounded receiver benchmark retained %d, want 64", n)
				}
			}
		})
	}
}

func BenchmarkRecentBandClockCleanup(b *testing.B) {
	for _, capacity := range []int{16, 64} {
		b.Run(fmt.Sprintf("receivers_%d", capacity), func(b *testing.B) {
			now := time.Date(2030, 2, 14, 12, 0, 0, 123, time.UTC)
			s := NewRecentBandStoreWithOptions(RecentBandOptions{Window: time.Hour, Shards: 4, MaxEntries: 64, MaxSpottersPerCall: capacity})
			for key := 0; key < 16; key++ {
				for receiver := 0; receiver < capacity; receiver++ {
					s.RecordAt(fmt.Sprintf("K%dABC", key), "20m", "CW", fmt.Sprintf("W%03d", receiver), now, now)
				}
			}
			if s.ActiveCallCount(now) != 16 {
				b.Fatal("maintenance benchmark failed to construct its physical population")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.CleanupAt(now)
			}
			b.StopTimer()
			if s.ActiveCallCount(now) != 16 {
				b.Fatal("maintenance benchmark deleted still-eligible keys")
			}
			recentBandClockAssertCoupling(b, s, false)
			s.CleanupAt(now.Add(time.Hour + time.Nanosecond))
			recentBandClockAssertCoupling(b, s, true)
		})
	}
}
