package reputation

import (
	"strings"
	"sync"
	"time"
)

type cacheEntry struct {
	value     LookupResult
	expiresAt time.Time
	negative  bool
}

type ttlCache struct {
	shards     []cacheShard
	ttl        time.Duration
	negativeTT time.Duration
	maxEntries int
}

type cacheShard struct {
	mu    sync.Mutex
	items map[string]cacheEntry
}

func newTTLCache(shards int, ttl, negativeTTL time.Duration, maxEntries int) *ttlCache {
	if shards <= 0 {
		shards = 16
	}
	if maxEntries <= 0 {
		maxEntries = 100000
	}
	c := &ttlCache{
		shards:     make([]cacheShard, shards),
		ttl:        ttl,
		negativeTT: negativeTTL,
		maxEntries: maxEntries,
	}
	for i := range c.shards {
		c.shards[i].items = make(map[string]cacheEntry)
	}
	return c
}

func (c *ttlCache) get(key string, now time.Time) (LookupResult, bool, bool) {
	if c == nil || strings.TrimSpace(key) == "" {
		return LookupResult{}, false, false
	}
	shard := &c.shards[c.shardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry, ok := shard.items[key]
	if !ok {
		return LookupResult{}, false, false
	}
	if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
		delete(shard.items, key)
		return LookupResult{}, false, false
	}
	return entry.value, true, entry.negative
}

func (c *ttlCache) set(key string, value LookupResult, now time.Time, negative bool) {
	if c == nil || strings.TrimSpace(key) == "" {
		return
	}
	ttl := c.ttl
	if negative {
		ttl = c.negativeTT
	}
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	shard := &c.shards[c.shardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.items[key] = cacheEntry{value: value, expiresAt: expiresAt, negative: negative}
	if c.maxEntries > 0 && len(shard.items) > c.maxEntries {
		c.sweepShardLocked(shard, now)
	}
}

func (c *ttlCache) sweepShardLocked(shard *cacheShard, now time.Time) {
	if shard == nil {
		return
	}
	limit := c.maxEntries
	for key := range shard.items {
		entry := shard.items[key]
		if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			delete(shard.items, key)
		}
	}
	if limit > 0 && len(shard.items) <= limit {
		return
	}
	for key := range shard.items {
		delete(shard.items, key)
		if len(shard.items) <= limit {
			break
		}
	}
}

func (c *ttlCache) shardIndex(key string) int {
	hash := fnv32a(key)
	return int(hash % uint32(len(c.shards)))
}

func fnv32a(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	hash := uint32(offset32)
	for i := 0; i < len(s); i++ {
		hash ^= uint32(s[i])
		hash *= prime32
	}
	return hash
}
