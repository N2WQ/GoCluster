// File role: Keeps FCC/ISED facts in one bounded CLOCK cache. Source identity,
// generation and UTC date prevent authority mixing without extra side indexes.
package uls

import (
	"sync"
	"time"
)

const (
	defaultLicenseCacheTTL        = 6 * time.Hour
	defaultLicenseCacheMaxEntries = 200000
)

type cacheEntry struct {
	value      LookupResult
	at         time.Time
	generation uint64
	day        int64
	used       bool
	slot       int
}

type cacheSlot struct {
	key licenseCacheKey
}

// A fixed source namespace prevents identical calls from sharing authority.
// Generation and UTC date are entry properties, never growing key dimensions.
type licenseCacheKey struct {
	source licenseSourceID
	call   string
}

type ttlCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	max     int
	entries map[licenseCacheKey]*cacheEntry
	slots   []cacheSlot
	hand    int
}

func newLicenseCache(ttl time.Duration, maxEntries int) *ttlCache {
	if ttl <= 0 {
		ttl = defaultLicenseCacheTTL
	}
	if maxEntries <= 0 {
		maxEntries = defaultLicenseCacheMaxEntries
	}
	return &ttlCache{
		ttl:     ttl,
		max:     maxEntries,
		entries: make(map[licenseCacheKey]*cacheEntry, maxEntries),
		slots:   make([]cacheSlot, maxEntries),
	}
}

func (c *ttlCache) get(key licenseCacheKey, generation uint64, day int64, now time.Time) (LookupResult, bool) {
	if c == nil || key.call == "" {
		return LookupResult{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		return LookupResult{}, false
	}
	if entry.generation != generation || entry.day != day || c.ttl > 0 && now.Sub(entry.at) > c.ttl {
		c.deleteEntryLocked(key, entry)
		return LookupResult{}, false
	}
	entry.used = true
	return entry.value, true
}

func (c *ttlCache) set(key licenseCacheKey, generation uint64, day int64, value LookupResult, now time.Time) {
	if c == nil || key.call == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.entries[key]; entry != nil {
		entry.value = value
		entry.at = now
		entry.generation, entry.day = generation, day
		entry.used = true
		return
	}
	if c.max <= 0 || len(c.slots) == 0 {
		return
	}
	slot := c.findSlotLocked(now)
	if slot < 0 {
		return
	}
	entry := &cacheEntry{
		value:      value,
		at:         now,
		generation: generation,
		day:        day,
		used:       true,
		slot:       slot,
	}
	c.entries[key] = entry
	c.slots[slot].key = key
}

func (c *ttlCache) deleteEntryLocked(key licenseCacheKey, entry *cacheEntry) {
	delete(c.entries, key)
	if entry.slot >= 0 && entry.slot < len(c.slots) && c.slots[entry.slot].key == key {
		c.slots[entry.slot].key = licenseCacheKey{}
	}
}

func (c *ttlCache) findSlotLocked(now time.Time) int {
	if len(c.entries) < c.max {
		for i := 0; i < len(c.slots); i++ {
			idx := (c.hand + i) % len(c.slots)
			if c.slots[idx].key.call == "" {
				c.hand = (idx + 1) % len(c.slots)
				return idx
			}
		}
	}
	limit := len(c.slots) * 2
	for i := 0; i < limit; i++ {
		idx := c.hand
		c.hand = (c.hand + 1) % len(c.slots)
		key := c.slots[idx].key
		if key.call == "" {
			return idx
		}
		entry := c.entries[key]
		if entry == nil {
			c.slots[idx].key = licenseCacheKey{}
			return idx
		}
		if c.ttl > 0 && now.Sub(entry.at) > c.ttl {
			c.deleteEntryLocked(key, entry)
			return idx
		}
		if entry.used {
			entry.used = false
			continue
		}
		c.deleteEntryLocked(key, entry)
		return idx
	}
	return -1
}

// removeSource runs only at source ownership transitions. The sweep is bounded
// by the existing aggregate cap and leaves the other source's entries intact.
func (c *ttlCache) removeSource(source licenseSourceID) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, entry := range c.entries {
		if key.source == source {
			c.deleteEntryLocked(key, entry)
		}
	}
}
