package spot

import (
	"fmt"
	"testing"
	"time"
)

// Precompute an equal population in each actual shard. The measured loop has
// one new key per shard per second, always reaching the existing entry cap
// before the 600-second window expires. Formatting is outside the timer.
func BenchmarkV16WhoSpotsMeRecordAtCapacity(b *testing.B) {
	store := NewWhoSpotsMeStoreWithOptions(WhoSpotsMeOptions{Window: 600 * time.Second})
	const perShard = 513
	calls := make([][]string, len(store.shards))
	wanted := perShard * len(calls)
	for n := 0; wanted != 0; n++ {
		call := fmt.Sprintf("K%dV", n)
		key, ok := store.normalizeKey(call, "20m")
		if !ok {
			b.Fatal("invalid benchmark identity")
		}
		index := int(hashWhoSpotsMeKey(key) % uint64(len(calls)))
		if len(calls[index]) < perShard {
			calls[index] = append(calls[index], call)
			wanted--
		}
	}
	base := time.Unix(1_700_000_000, 0).UTC()
	for round := 0; round < perShard-1; round++ {
		for _, row := range calls {
			store.Record(row[round], "20m", 291, "NA", base.Add(time.Duration(round)*time.Second))
		}
	}
	if got := store.ActiveKeyCount(); got != 32768 {
		b.Fatalf("warm population=%d", got)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		round := perShard - 1 + i/len(calls)
		store.Record(calls[i%len(calls)][round%perShard], "20m", 291, "NA", base.Add(time.Duration(round)*time.Second))
	}
	b.StopTimer()
	if got := store.ActiveKeyCount(); got != 32768 {
		b.Fatalf("churn population=%d", got)
	}
}

func BenchmarkV16WhoSpotsMeScrub(b *testing.B) {
	for _, tc := range []struct {
		name                       string
		buckets, others, countries int
	}{
		{"dense_one_country", 600, 32, 1},
		{"sparse_many_countries", 2, 0, 256},
		{"dense_many_countries", 8, 512, 32},
	} {
		b.Run(tc.name, func(b *testing.B) {
			store := NewWhoSpotsMeStore(600 * time.Second)
			key, _ := store.normalizeKey("W1AW", "20m")
			victim := &whoSpotsMeEntry{totals: make(map[whoSpotsMeCountryKey]int), lastSeen: 1}
			type record struct {
				bucket int
				key    whoSpotsMeRecordKey
			}
			var records []record
			for bucket := 0; bucket < tc.buckets; bucket++ {
				counts := make(map[whoSpotsMeRecordKey]int)
				store.buckets[bucket].counts = counts
				for i := 0; i < tc.others; i++ {
					other, _ := store.normalizeKey(fmt.Sprintf("K%dV", bucket*tc.others+i), "20m")
					country := whoSpotsMeCountryKey{adif: 291, continent: "NA"}
					counts[whoSpotsMeRecordKey{key: other, country: country}] = 1
					store.shardFor(other).entries[other] = &whoSpotsMeEntry{totals: map[whoSpotsMeCountryKey]int{country: 1}, lastSeen: 2}
				}
			}
			for country := 0; country < tc.countries; country++ {
				c := whoSpotsMeCountryKey{adif: country + 1, continent: "EU"}
				for bucket := country % tc.buckets; bucket < tc.buckets; bucket += tc.countries {
					r := record{bucket: bucket, key: whoSpotsMeRecordKey{key: key, country: c}}
					records = append(records, r)
					store.buckets[bucket].counts[r.key] = 1
					victim.totals[c]++
				}
			}
			shard := store.shardFor(key)
			shard.entries[key] = victim
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				store.mu.Lock()
				shard.mu.Lock()
				store.evictOneEntryLocked(shard)
				shard.mu.Unlock()
				store.mu.Unlock()
				b.StopTimer()
				if shard.entries[key] != nil {
					b.Fatal("fixture did not evict its unique oldest victim")
				}
				shard.entries[key] = victim
				for _, r := range records {
					store.buckets[r.bucket].counts[r.key] = 1
				}
				b.StartTimer()
			}
		})
	}
}

func v16HarmonicSettings() HarmonicSettings {
	return HarmonicSettings{Enabled: true, RecencyWindow: 120 * time.Second,
		MaxHarmonicMultiple: 4, FrequencyToleranceHz: 25, MinReportDelta: 6}
}

func BenchmarkV16HarmonicSteadyExpiry(b *testing.B) {
	const live = 10000
	detector := NewHarmonicDetector(v16HarmonicSettings())
	base := time.Unix(1_700_000_000, 0).UTC()
	spots := make([]Spot, live+1)
	for i := range spots {
		spots[i] = *NewSpotNormalized(fmt.Sprintf("K%dV", i), "W1AW", 7011, "CW")
		spots[i].Report = 20
	}
	for i := 0; i < live; i++ {
		now := base.Add(time.Duration(i) * 12 * time.Millisecond)
		spots[i].Time = now
		detector.ShouldDrop(&spots[i], now)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := (live + i) % len(spots)
		now := base.Add(time.Duration(live+i) * 12 * time.Millisecond)
		spots[index].Time = now
		if drop, _, _, _ := detector.ShouldDrop(&spots[index], now); drop {
			b.Fatal("steady-expiry fundamental was suppressed")
		}
	}
	b.StopTimer()
	if len(detector.entries) != live+1 {
		b.Fatalf("strict-expiry population=%d", len(detector.entries))
	}
}

func BenchmarkV16HarmonicRefresh(b *testing.B) {
	detector := NewHarmonicDetector(v16HarmonicSettings())
	base := time.Unix(1_700_000_000, 0).UTC()
	fundamental := NewSpotNormalized("W1AW", "K1V", 7011, "CW")
	fundamental.Time, fundamental.Report = base, 20
	detector.ShouldDrop(fundamental, base)
	harmonic := NewSpotNormalized("W1AW", "K2V", 14022, "CW")
	harmonic.Time, harmonic.Report = base.Add(time.Second), 10
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if drop, freq, corroborators, delta := detector.ShouldDrop(harmonic, base.Add(time.Second)); !drop || freq != 7011 || corroborators != 1 || delta != 10 {
			b.Fatal("refresh fixture did not suppress its known harmonic")
		}
	}
}
