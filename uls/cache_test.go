package uls

import (
	"testing"
	"time"
)

func TestLicenseCacheTTLExpiry(t *testing.T) {
	cache := newLicenseCache(50*time.Millisecond, 4)
	now := time.Now().UTC()
	cache.set("K1ABC", LookupResult{Available: true, Found: true}, now)

	if v, ok := cache.get("K1ABC", now.Add(10*time.Millisecond)); !ok || !v.Found {
		t.Fatalf("expected cached value before TTL expiry")
	}
	if _, ok := cache.get("K1ABC", now.Add(100*time.Millisecond)); ok {
		t.Fatalf("expected cached value to expire")
	}
}

func TestLicenseCacheEvictsWhenFull(t *testing.T) {
	cache := newLicenseCache(5*time.Minute, 1)
	now := time.Now().UTC()
	cache.set("K1ABC", LookupResult{Available: true, Found: true}, now)
	cache.set("N2WQ", LookupResult{Available: true}, now.Add(time.Millisecond))

	cache.mu.Lock()
	size := len(cache.entries)
	cache.mu.Unlock()
	if size != 1 {
		t.Fatalf("expected cache size 1, got %d", size)
	}
	_, ok1 := cache.get("K1ABC", now.Add(2*time.Millisecond))
	_, ok2 := cache.get("N2WQ", now.Add(2*time.Millisecond))
	if ok1 && ok2 {
		t.Fatalf("expected at most one entry after eviction")
	}
	if !ok1 && !ok2 {
		t.Fatalf("expected one entry to remain after eviction")
	}
}
