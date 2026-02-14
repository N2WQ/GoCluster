package spot

import (
	"testing"
	"time"
)

func TestRecentBandStoreAdmissionAndExpiry(t *testing.T) {
	now := time.Date(2026, 2, 14, 12, 0, 0, 0, time.UTC)
	store := NewRecentBandStoreWithOptions(RecentBandOptions{
		Window:             time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})

	store.Record("K1ABC", "40m", "CW", "W1AAA", now.Add(-30*time.Minute))
	store.Record("K1ABC", "40m", "CW", "W2BBB", now.Add(-20*time.Minute))

	if got := store.RecentSupportCount("K1ABC", "40m", "CW", now); got != 2 {
		t.Fatalf("expected 2 unique spotters, got %d", got)
	}
	if !store.HasRecentSupport("K1ABC", "40m", "CW", 2, now) {
		t.Fatalf("expected recent support admission at threshold=2")
	}
	if store.HasRecentSupport("K1ABC", "40m", "CW", 3, now) {
		t.Fatalf("expected no admission at threshold=3")
	}

	expiredAt := now.Add(2 * time.Hour)
	if store.HasRecentSupport("K1ABC", "40m", "CW", 2, expiredAt) {
		t.Fatalf("expected admission to expire outside window")
	}
	if got := store.RecentSupportCount("K1ABC", "40m", "CW", expiredAt); got != 0 {
		t.Fatalf("expected 0 unique spotters after expiry, got %d", got)
	}
}

func TestRecentBandStoreNormalizesKeys(t *testing.T) {
	now := time.Now().UTC()
	store := NewRecentBandStoreWithOptions(RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})

	store.Record("k1abc", "40M", "cw", "w1aaa", now)
	store.Record("K1ABC", "40m", "CW", "W2BBB", now)

	if !store.HasRecentSupport("K1ABC", "40m", "CW", 2, now) {
		t.Fatalf("expected key normalization across case differences")
	}
}
