package peer

import (
	"sync"
	"time"
)

// dedupeCache is a time-bounded set for seen keys.
type dedupeCache struct {
	mu    sync.Mutex
	items map[string]time.Time
	ttl   time.Duration
}

// Purpose: Construct a time-bounded dedupe cache.
// Key aspects: Stores keys with timestamps and TTL for pruning.
// Upstream: Peer session dedup logic.
// Downstream: dedupeCache.markSeen, dedupeCache.prune.
func newDedupeCache(ttl time.Duration) *dedupeCache {
	return &dedupeCache{
		items: make(map[string]time.Time),
		ttl:   ttl,
	}
}

// Purpose: Record a key as seen if it is new.
// Key aspects: Returns false for duplicates; guarded by a mutex.
// Upstream: Peer input dedupe checks.
// Downstream: dedupeCache.items map.
func (c *dedupeCache) markSeen(key string, now time.Time) bool {
	if c == nil || key == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; ok {
		return false
	}
	c.items[key] = now
	return true
}

// Purpose: Remove expired entries from the dedupe cache.
// Key aspects: Uses TTL to evict old keys.
// Upstream: Periodic cleanup in peer sessions.
// Downstream: dedupeCache.items map.
func (c *dedupeCache) prune(now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.items) == 0 {
		return
	}
	ttl := c.ttl
	for k, ts := range c.items {
		if now.Sub(ts) > ttl {
			delete(c.items, k)
		}
	}
}
