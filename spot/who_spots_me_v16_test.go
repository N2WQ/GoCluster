package spot

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// This is the frozen pre-v16 scrub algorithm. The expected bucket maps use
// its exhaustive key predicate, never the new country-directed traversal.
func v16FrozenWhoScrub(buckets []whoSpotsMeBucket, key whoSpotsMeKey) {
	for i := range buckets {
		for record := range buckets[i].counts {
			if record.key == key {
				delete(buckets[i].counts, record)
			}
		}
	}
}

func v16CopyWhoBuckets(in []whoSpotsMeBucket) []whoSpotsMeBucket {
	out := make([]whoSpotsMeBucket, len(in))
	for i, bucket := range in {
		out[i].second = bucket.second
		if bucket.counts != nil {
			out[i].counts = make(map[whoSpotsMeRecordKey]int, len(bucket.counts))
			for key, count := range bucket.counts {
				out[i].counts[key] = count
			}
		}
	}
	return out
}

func TestV16WhoSpotsMeScrubMatchesFrozenScan(t *testing.T) {
	for _, countries := range []int{1, 4, 16} {
		for _, others := range []int{0, 1, 7, 32} {
			t.Run(fmt.Sprintf("countries%d-others%d", countries, others), func(t *testing.T) {
				store := NewWhoSpotsMeStoreWithOptions(WhoSpotsMeOptions{Window: 8 * time.Second, MaxCountriesPerEntry: 16})
				base := time.Unix(100, 0).UTC()
				for second := 0; second < 8; second++ {
					for country := 0; country < countries; country++ {
						if (second+country)%3 != 0 {
							store.Record("W1AW", "20m", country+1, "EU", base.Add(time.Duration(second)*time.Second))
							store.Record("W1AW", "20m", country+1, "EU", base.Add(time.Duration(second)*time.Second))
						}
					}
					for i := 0; i < others; i++ {
						store.Record(fmt.Sprintf("K%dV", i), "20m", 291, "NA", base.Add(time.Duration(second)*time.Second))
					}
				}
				key, _ := store.normalizeKey("W1AW", "20m")
				want := v16CopyWhoBuckets(store.buckets)
				v16FrozenWhoScrub(want, key)
				shard := store.shardFor(key)
				store.mu.Lock()
				shard.mu.Lock()
				store.scrubKeyFromBucketsLocked(key, shard.entries[key].totals)
				shard.mu.Unlock()
				store.mu.Unlock()
				if !reflect.DeepEqual(store.buckets, want) {
					t.Fatal("victim scrub differs from frozen full scan")
				}
			})
		}
	}
}

func TestV16WhoSpotsMeSparseDenseScrub(t *testing.T) {
	store := NewWhoSpotsMeStore(10 * time.Second)
	key := whoSpotsMeKey{call: "W1AW", band: "20m"}
	countries := map[whoSpotsMeCountryKey]int{}
	for country := 1; country <= 4; country++ {
		countries[whoSpotsMeCountryKey{adif: country, continent: "EU"}] = 1
	}
	for bucket, count := range []int{1, 4, 8} {
		store.buckets[bucket].counts = map[whoSpotsMeRecordKey]int{}
		for i := 1; i <= count; i++ {
			record := whoSpotsMeRecordKey{key: key, country: whoSpotsMeCountryKey{adif: i, continent: "EU"}}
			if i > 4 {
				record.key.call = "K1V"
			}
			store.buckets[bucket].counts[record] = 1
		}
	}
	want := v16CopyWhoBuckets(store.buckets)
	v16FrozenWhoScrub(want, key)
	store.mu.Lock()
	store.scrubKeyFromBucketsLocked(key, countries)
	store.mu.Unlock()
	if !reflect.DeepEqual(store.buckets, want) {
		t.Fatal("sparse/equal/dense branches changed exact deletion")
	}
}

func TestV16WhoSpotsMeVictimBucketCoupling(t *testing.T) {
	store := NewWhoSpotsMeStoreWithOptions(WhoSpotsMeOptions{Window: 6 * time.Second, Shards: 1, MaxEntries: 1})
	base := time.Unix(100, 0).UTC()
	store.Record("W1AW", "20m", 291, "NA", base)
	store.Record("W1AW", "20m", 230, "EU", base.Add(time.Second))
	store.Record("W1AW", "20m", 291, "NA", base.Add(2*time.Second))
	store.Record("K1V", "20m", 291, "NA", base.Add(3*time.Second))
	store.Record("W1AW", "20m", 230, "EU", base.Add(4*time.Second))
	want := map[string][]WhoSpotsMeCountryCount{"EU": {{ADIF: 230, Count: 1}}}
	for second := 4; second < 10; second++ {
		if got := store.CountryCountsByContinent("W1AW", "20m", base.Add(time.Duration(second)*time.Second)); !reflect.DeepEqual(got, want) {
			t.Fatalf("second%d: old bucket corrupted new generation: %v", second, got)
		}
	}
	if got := store.CountryCountsByContinent("W1AW", "20m", base.Add(10*time.Second)); got != nil {
		t.Fatalf("new generation did not expire exactly: %v", got)
	}
	if store.ActiveKeyCount() != 0 {
		t.Fatal("full expiry retained an owner")
	}
	for _, bucket := range store.buckets {
		if len(bucket.counts) != 0 {
			t.Fatal("full expiry retained bucket records")
		}
	}
}

func TestV16WhoSpotsMeTimeAndVictimRules(t *testing.T) {
	store := NewWhoSpotsMeStoreWithOptions(WhoSpotsMeOptions{Window: 5 * time.Second, Shards: 1, MaxEntries: 2, MaxCountriesPerEntry: 1})
	base := time.Unix(100, 0).UTC()
	store.Record("W1AW", "20m", 291, "NA", base)
	store.Record("K1V", "20m", 230, "EU", base.Add(time.Second))
	store.Record("W1AW", "20m", 230, "EU", base.Add(2*time.Second)) // refused country must not refresh recency
	store.Record("K2V", "20m", 291, "NA", base.Add(2*time.Second))
	if got := store.CountryCountsByContinent("W1AW", "20m", base.Add(2*time.Second)); got != nil {
		t.Fatal("country refusal refreshed victim recency")
	}
	store.Record("K2V", "20m", 291, "NA", base) // accepted backward time becomes oldest
	store.Record("K3V", "20m", 291, "NA", base.Add(3*time.Second))
	if got := store.CountryCountsByContinent("K2V", "20m", base.Add(3*time.Second)); got != nil {
		t.Fatal("backward lastSeen assignment was not preserved")
	}
	store.Record("K3V", "20m", 291, "NA", base.Add(-2*time.Second)) // exact stale cutoff
	if got := store.CountryCountsByContinent("K3V", "20m", base.Add(3*time.Second)); !reflect.DeepEqual(got, map[string][]WhoSpotsMeCountryCount{"NA": {{ADIF: 291, Count: 1}}}) {
		t.Fatal("exact stale cutoff admitted a record")
	}
	store.cleanup(base.Add(time.Hour))
	if store.ActiveKeyCount() != 0 {
		t.Fatal("large jump retained owners")
	}
	for _, call := range []string{"K4V", "K5V", "K6V"} {
		store.Record(call, "20m", 291, "NA", base.Add(time.Hour))
	}
	if store.ActiveKeyCount() != 2 || store.CountryCountsByContinent("K6V", "20m", base.Add(time.Hour)) == nil {
		t.Fatal("equal-age eviction violated existing eligible-victim set")
	}
}

func TestV16WhoSpotsMeConcurrentQueriesCleanup(t *testing.T) {
	store := NewWhoSpotsMeStoreWithOptions(WhoSpotsMeOptions{Window: 5 * time.Second, Shards: 4, MaxEntries: 16})
	base := time.Unix(100, 0).UTC()
	var workers sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		workers.Go(func() {
			for i := 0; i < 500; i++ {
				at := base.Add(time.Duration(i/10) * time.Second)
				switch worker {
				case 0:
					store.Record(fmt.Sprintf("K%dV", i%32), "20m", 291, "NA", at)
				case 1:
					store.CountryCountsByContinent("K1V", "20m", at)
				case 2:
					store.cleanup(at)
				}
			}
		})
	}
	workers.Wait()
	store.cleanup(base.Add(time.Hour))
	if store.ActiveKeyCount() != 0 {
		t.Fatal("concurrent owners did not converge")
	}
}
