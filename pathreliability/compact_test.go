package pathreliability

import (
	"testing"
	"time"
)

func TestStoreCompactPreservesEntries(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	store := NewStore(cfg, []string{"20m"})

	shard := &store.shards[0]
	now := time.Now().UTC().Unix()
	shard.mu.Lock()
	shard.buckets[1] = &bucket{weight: 1, lastUpdate: now}
	shard.buckets[2] = &bucket{weight: 1, lastUpdate: now}
	shard.peak = 10
	shard.mu.Unlock()

	compacted := store.Compact(5, 0.5)
	if compacted == 0 {
		t.Fatalf("expected at least one shard to compact")
	}

	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if got := len(shard.buckets); got != 2 {
		t.Fatalf("expected 2 buckets after compaction, got %d", got)
	}
	if shard.peak != 2 {
		t.Fatalf("expected peak reset to 2, got %d", shard.peak)
	}
}
