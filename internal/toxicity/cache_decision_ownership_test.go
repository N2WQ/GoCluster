package toxicity

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestCacheDecisionCategoriesAreDetached(t *testing.T) {
	now := time.Unix(100, 0)
	cache := NewCache(2, time.Minute)
	input := Decision{Status: spot.ToxicityToxic, Categories: []string{"original"}, Model: "fixture"}
	cache.Put("one", input, now)
	input.Categories[0] = "changed input"
	first, ok := cache.Get("one", now)
	if !ok || first.Status != spot.ToxicityToxic || first.Model != "fixture" || len(first.Categories) != 1 || first.Categories[0] != "original" {
		t.Fatalf("cache retained caller categories: %#v, hit=%v", first, ok)
	}
	first.Categories[0] = "changed read"
	second, ok := cache.Get("one", now)
	if !ok || len(second.Categories) != 1 || second.Categories[0] != "original" {
		t.Fatalf("cache returned shared categories: %#v, hit=%v", second, ok)
	}
	cache.Put("one", Decision{Status: spot.ToxicitySafe, Categories: input.Categories}, now)
	input.Categories[0] = "changed replacement"
	replaced, ok := cache.Get("one", now)
	if !ok || replaced.Status != spot.ToxicitySafe || len(replaced.Categories) != 1 || replaced.Categories[0] != "changed input" || cache.Len() != 1 {
		t.Fatalf("replacement was shared or duplicated: %#v, hit=%v, len=%d", replaced, ok, cache.Len())
	}
}

func TestCacheChurnKeepsBothIndexesBounded(t *testing.T) {
	const capacity = 8
	now := time.Unix(100, 0)
	cache := NewCache(capacity, time.Minute)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("comment-%d", i)
		cache.Put(key, Decision{Status: spot.ToxicitySafe, Categories: []string{key}}, now)
		if _, ok := cache.Get(key, now); !ok {
			t.Fatalf("fresh decision %d was lost", i)
		}
		cache.mu.Lock()
		entries := 0
		valid := true
		for _, bucket := range cache.items {
			entries += len(bucket)
			for _, elem := range bucket {
				entry, ok := cacheEntryFromElement(elem)
				if !ok || entry == nil {
					valid = false
				}
			}
		}
		buckets, lru := len(cache.items), cache.lru.Len()
		cache.mu.Unlock()
		if !valid {
			t.Fatal("invalid retained cache element")
		}
		want := min(i+1, capacity)
		if entries != want || lru != want || buckets > capacity {
			t.Fatalf("churn %d: entries=%d lru=%d buckets=%d want=%d", i, entries, lru, buckets, want)
		}
	}
	if cache.Evictions() != 1000-capacity {
		t.Fatalf("evictions=%d", cache.Evictions())
	}
	// TTL is lazy: reading every retained key must remove both coupled indexes.
	for i := 1000 - capacity; i < 1000; i++ {
		if _, ok := cache.Get(fmt.Sprintf("comment-%d", i), now.Add(time.Minute)); ok {
			t.Fatalf("decision %d survived its exact expiry", i)
		}
	}
	if cache.Len() != 0 || len(cache.items) != 0 {
		t.Fatalf("expired state retained: len=%d buckets=%d", cache.Len(), len(cache.items))
	}
}
