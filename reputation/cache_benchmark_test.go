package reputation

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func benchmarkLookupCache(b *testing.B, addresses int) (*ttlCache, []string, LookupResult, time.Time) {
	b.Helper()
	c := newTTLCache(32, time.Hour, time.Minute, 50000)
	now := time.Unix(1700000000, 0).UTC()
	value := LookupResult{ASN: "64500", CountryCode: "US", CountryName: "United States", ContinentCode: "NA", Source: "ipinfo", FetchedAt: now}
	keys := make([]string, addresses)
	for i := range keys {
		keys[i] = fmt.Sprintf("198.18.%d.%d", i/256, i%256)
		c.set(keys[i], value, now, false)
	}
	for _, key := range keys {
		if got, ok, negative := c.get(key, now); !ok || negative || got != value {
			b.Fatalf("warmed fixture missed %q: %+v %v %v", key, got, ok, negative)
		}
	}
	return c, keys, value, now
}

func BenchmarkLookupCacheHit(b *testing.B) {
	for _, addresses := range []int{50, 1000} {
		b.Run(strconv.Itoa(addresses), func(b *testing.B) {
			c, keys, value, now := benchmarkLookupCache(b, addresses)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got, ok, negative := c.get(keys[i%len(keys)], now); !ok || negative || got != value {
					b.Fatal("warmed lookup changed")
				}
			}
		})
	}
}

func BenchmarkLookupCacheOverwrite(b *testing.B) {
	c, keys, value, now := benchmarkLookupCache(b, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.set(keys[i%len(keys)], value, now, false)
	}
	b.StopTimer()
	for _, key := range keys {
		if got, ok, _ := c.get(key, now); !ok || got != value {
			b.Fatal("overwrite changed result")
		}
	}
}

func BenchmarkLookupCacheParallelMix(b *testing.B) {
	for _, addresses := range []int{50, 1000} {
		b.Run(strconv.Itoa(addresses), func(b *testing.B) {
			c, keys, value, now := benchmarkLookupCache(b, addresses)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := keys[i%len(keys)]
					if i%8 == 0 {
						c.set(key, value, now, false)
					} else if got, ok, negative := c.get(key, now); !ok || negative || got != value {
						b.Error("parallel warmed lookup changed")
						return
					}
					i++
				}
			})
		})
	}
}

func BenchmarkLookupCacheOverflowLive(b *testing.B) {
	const capacity = 50000
	c := newTTLCache(1, time.Hour, time.Minute, capacity)
	now := time.Unix(1700000000, 0).UTC()
	value := LookupResult{ASN: "64500", Source: "cymru", FetchedAt: now}
	for i := 0; i < capacity; i++ {
		c.set(strconv.Itoa(i), value, now, false)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Each input is new, so every timed operation pressures a full shard.
		c.set(strconv.Itoa(capacity+i), value, now, false)
	}
}

func BenchmarkLookupCacheOverflowExpired(b *testing.B) {
	// Use a fixed iteration count (for example -test.benchtime=10x). Rebuilding
	// the expired population is deliberately untimed and costs O(capacity).
	if b.N > 100 {
		b.Skip("full-population expiry transition requires -test.benchtime=10x")
	}
	const capacity = 50000
	now := time.Unix(1700000000, 0).UTC()
	value := LookupResult{ASN: "64500", Source: "cymru", FetchedAt: now}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c := newTTLCache(1, time.Minute, time.Minute, capacity)
		for j := 0; j < capacity; j++ {
			c.set(strconv.Itoa(j), value, now, false)
		}
		b.StartTimer()
		c.set("overflow", value, now.Add(time.Minute+time.Nanosecond), false)
		b.StopTimer()
		if got, hit, negative := c.get("overflow", now.Add(time.Minute+time.Nanosecond)); !hit || negative || got != value {
			b.Fatal("overflow admission failed")
		}
	}
}

func BenchmarkLookupCacheSharedParallel(b *testing.B) {
	for _, addresses := range []int{50, 1000} {
		b.Run(strconv.Itoa(addresses), func(b *testing.B) {
			main, keys, value, now := benchmarkLookupCache(b, addresses)
			dns := &ttlCache{store: main.store, namespace: cacheCymru, ttl: time.Hour, negativeTT: time.Minute}
			dnsValue := value
			dnsValue.Source = "cymru"
			for _, key := range keys {
				dns.set(key, dnsValue, now, false)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					view, want := main, value
					if i%2 != 0 {
						view, want = dns, dnsValue
					}
					key := keys[i%len(keys)]
					if (i/2)%8 == 0 {
						view.set(key, want, now, false)
					} else if got, hit, negative := view.get(key, now); !hit || negative || got != want {
						b.Error("shared namespace hit changed")
						return
					}
					i++
				}
			})
		})
	}
}

func BenchmarkLookupCacheSharedSkewOverflow(b *testing.B) {
	main := newTTLCache(32, time.Hour, time.Minute, 50000)
	dns := &ttlCache{store: main.store, namespace: cacheCymru, ttl: time.Hour, negativeTT: time.Minute}
	quota := main.store.shards[0].quota
	keys := make([]string, 0, quota+1)
	for i := 0; len(keys) < quota+1; i++ {
		key := fmt.Sprintf("198.18.%d.%d", i/256, i%256)
		if main.shardIndex(key) == 0 {
			keys = append(keys, key)
		}
	}
	now := time.Unix(1700000000, 0).UTC()
	value := LookupResult{ASN: "64500", Source: "fixture", FetchedAt: now}
	for i := 0; i < quota; i++ {
		view := main
		if i%2 != 0 {
			view = dns
		}
		view.set(keys[i], value, now, false)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		at := (quota + i) % len(keys)
		view := main
		if at%2 != 0 {
			view = dns
		}
		view.set(keys[at], value, now, false)
	}
	b.StopTimer()
	stats := main.store.snapshot()
	if stats.Entries != quota || stats.Evictions != uint64(b.N) {
		b.Fatalf("skew fixture did not pressure every operation: %+v", stats)
	}
}
