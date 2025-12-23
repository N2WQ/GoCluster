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

func newDedupeCache(ttl time.Duration) *dedupeCache {
	return &dedupeCache{
		items: make(map[string]time.Time),
		ttl:   ttl,
	}
}

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
