package spot

import (
	"testing"
	"time"
)

func TestCallQualityPinnedTTL(t *testing.T) {
	pin := true
	store := NewCallQualityStoreWithOptions(CallQualityOptions{
		Shards:          1,
		TTL:             time.Second,
		MaxEntries:      10,
		CleanupInterval: time.Hour,
		PinPriors:       &pin,
	})
	now := time.Date(2026, 2, 4, 0, 0, 0, 0, time.UTC)
	store.AddPinned("W1AW", 0, 1000, 2)
	store.addWithTime("W1AW", 14000, 1000, 1, now)

	if got := store.getWithTime("W1AW", 14000, 1000, now); got != 1 {
		t.Fatalf("expected dynamic score 1 before TTL, got %d", got)
	}
	if got := store.getWithTime("W1AW", 14000, 1000, now.Add(2*time.Second)); got != 2 {
		t.Fatalf("expected pinned score 2 after TTL expiry, got %d", got)
	}
}

func TestCallQualityCapEvictsOldest(t *testing.T) {
	pin := true
	store := NewCallQualityStoreWithOptions(CallQualityOptions{
		Shards:          1,
		TTL:             time.Hour,
		MaxEntries:      2,
		CleanupInterval: time.Hour,
		PinPriors:       &pin,
	})
	now := time.Date(2026, 2, 4, 1, 0, 0, 0, time.UTC)
	store.addWithTime("AAA", 7000, 1000, 1, now)
	store.addWithTime("BBB", 7000, 1000, 1, now.Add(1*time.Second))
	store.addWithTime("CCC", 7000, 1000, 1, now.Add(2*time.Second))

	if got := store.getWithTime("AAA", 7000, 1000, now.Add(3*time.Second)); got != 0 {
		t.Fatalf("expected oldest entry to be evicted, got %d", got)
	}
	if got := store.getWithTime("BBB", 7000, 1000, now.Add(3*time.Second)); got == 0 {
		t.Fatalf("expected BBB to remain after eviction")
	}
	if got := store.getWithTime("CCC", 7000, 1000, now.Add(3*time.Second)); got == 0 {
		t.Fatalf("expected CCC to remain after eviction")
	}
}

func TestCallQualityCapEvictsDeterministicOnTimeTie(t *testing.T) {
	pin := true
	store := NewCallQualityStoreWithOptions(CallQualityOptions{
		Shards:          1,
		TTL:             time.Hour,
		MaxEntries:      1,
		CleanupInterval: time.Hour,
		PinPriors:       &pin,
	})
	now := time.Date(2026, 2, 4, 1, 30, 0, 0, time.UTC)

	// Force equal timestamps so eviction ordering is only determined by key tie-break.
	store.addWithTime("BBB", 7000, 1000, 1, now)
	store.addWithTime("AAA", 7000, 1000, 1, now)

	if got := store.getWithTime("AAA", 7000, 1000, now.Add(time.Second)); got != 0 {
		t.Fatalf("expected deterministic eviction of AAA (lexicographic tie-break), got %d", got)
	}
	if got := store.getWithTime("BBB", 7000, 1000, now.Add(time.Second)); got == 0 {
		t.Fatalf("expected BBB to remain after eviction tie-break")
	}
}

func TestCallQualityPinnedNotEvicted(t *testing.T) {
	pin := true
	store := NewCallQualityStoreWithOptions(CallQualityOptions{
		Shards:          1,
		TTL:             time.Hour,
		MaxEntries:      1,
		CleanupInterval: time.Hour,
		PinPriors:       &pin,
	})
	now := time.Date(2026, 2, 4, 2, 0, 0, 0, time.UTC)
	store.AddPinned("PIN", 0, 1000, 3)
	store.addWithTime("AAA", 7000, 1000, 1, now)
	store.addWithTime("BBB", 7000, 1000, 1, now.Add(1*time.Second))

	if got := store.getWithTime("PIN", 0, 1000, now.Add(2*time.Second)); got != 3 {
		t.Fatalf("expected pinned score 3 to remain, got %d", got)
	}
}
