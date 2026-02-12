package main

import (
	"container/list"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/cty"
	"dxcluster/gridstore"
)

const callMetaCacheShardCount = 16

// callMetaCache stores per-call grid/CTY/known metadata with a bounded LRU.
// Grid lookups normalize to a base identity (uppercase, trimmed, hyphen suffix stripped)
// to avoid duplicate entries across source variants.
type callMetaCache struct {
	shards   []callMetaCacheShard
	gridTTL  time.Duration
	capacity int

	ctyLookups atomic.Uint64
	ctyHits    atomic.Uint64
}

type callMetaCacheShard struct {
	mu      sync.Mutex
	max     int
	order   *list.List
	entries map[string]*list.Element
}

type callMetaEntry struct {
	call string

	grid          string
	gridValid     bool
	gridDerived   bool
	gridUpdatedAt time.Time

	isKnown      bool
	knownChecked bool

	cty        *cty.PrefixInfo
	ctyValid   bool
	ctyChecked bool
}

type ctyCacheMetrics struct {
	Lookups uint64
	Hits    uint64
}

// Purpose: Construct the unified call metadata cache.
// Key aspects: Bounds total entries and applies TTL for grid freshness.
// Upstream: main startup.
// Downstream: shard allocation and LRU initialization.
func newCallMetaCache(capacity int, gridTTL time.Duration) *callMetaCache {
	if capacity <= 0 {
		capacity = 100000
	}
	if capacity < callMetaCacheShardCount {
		capacity = callMetaCacheShardCount
	}
	perShard := capacity / callMetaCacheShardCount
	if perShard <= 0 {
		perShard = 1
	}
	shards := make([]callMetaCacheShard, callMetaCacheShardCount)
	for i := range shards {
		shards[i] = callMetaCacheShard{
			max:     perShard,
			order:   list.New(),
			entries: make(map[string]*list.Element, perShard),
		}
	}
	return &callMetaCache{
		shards:   shards,
		gridTTL:  gridTTL,
		capacity: capacity,
	}
}

// Clear clears all cached entries (used on CTY/SCP reload).
// Key aspects: Resets shard maps under lock.
// Upstream: CTY/SCP refresh handlers.
// Downstream: shard state reset.
func (c *callMetaCache) Clear() {
	if c == nil {
		return
	}
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		shard.order = list.New()
		shard.entries = make(map[string]*list.Element, shard.max)
		shard.mu.Unlock()
	}
}

// CTYMetrics returns CTY cache metrics for stats display.
// Key aspects: Uses atomic counters for lock-free reads.
// Upstream: formatMemoryLine.
// Downstream: atomic loads.
func (c *callMetaCache) CTYMetrics() ctyCacheMetrics {
	if c == nil {
		return ctyCacheMetrics{}
	}
	return ctyCacheMetrics{
		Lookups: c.ctyLookups.Load(),
		Hits:    c.ctyHits.Load(),
	}
}

// EntryCount counts entries for coarse memory estimation.
// Key aspects: Locks each shard briefly to sum sizes.
// Upstream: formatMemoryLine.
// Downstream: shard entry maps.
func (c *callMetaCache) EntryCount() int {
	if c == nil {
		return 0
	}
	total := 0
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		total += len(shard.entries)
		shard.mu.Unlock()
	}
	return total
}

// LookupCTY looks up CTY prefix info with cache and DB fallback.
// Key aspects: Caches hits and misses; returns cached=true on hits.
// Upstream: ingest validation and output metadata refresh.
// Downstream: cty.LookupCallsignPortable.
func (c *callMetaCache) LookupCTY(call string, db *cty.CTYDatabase) (*cty.PrefixInfo, bool, bool) {
	if c == nil || call == "" || db == nil {
		if db == nil {
			return nil, false, false
		}
		if shouldRejectCTYCall(call) {
			return nil, false, false
		}
		info, ok := db.LookupCallsignPortable(call)
		return info, ok, false
	}
	c.ctyLookups.Add(1)
	shard := c.shardFor(call)
	shard.mu.Lock()
	if elem, ok := shard.entries[call]; ok {
		entry := elem.Value.(*callMetaEntry)
		shard.order.MoveToFront(elem)
		if entry.ctyChecked {
			if entry.ctyValid {
				c.ctyHits.Add(1)
				info := entry.cty
				shard.mu.Unlock()
				return info, true, true
			}
			c.ctyHits.Add(1)
			shard.mu.Unlock()
			return nil, false, true
		}
	}
	shard.mu.Unlock()

	if shouldRejectCTYCall(call) {
		shard.mu.Lock()
		entry := shard.getOrCreate(call)
		entry.ctyChecked = true
		entry.ctyValid = false
		entry.cty = nil
		shard.mu.Unlock()
		return nil, false, false
	}

	info, ok := db.LookupCallsignPortable(call)

	shard.mu.Lock()
	entry := shard.getOrCreate(call)
	entry.ctyChecked = true
	entry.ctyValid = ok
	if ok {
		entry.cty = info
	} else {
		entry.cty = nil
	}
	shard.mu.Unlock()
	return info, ok, false
}

// LookupGrid looks up a cached grid by callsign with TTL enforcement.
// Key aspects: Returns derived flag and counts cache lookups/hits when metrics are provided.
// Upstream: grid backfill path.
// Downstream: per-shard LRU and TTL checks.
func (c *callMetaCache) LookupGrid(call string, metrics *gridMetrics) (string, bool, bool) {
	if c == nil {
		return "", false, false
	}
	call = normalizeCallForMetadata(call)
	if call == "" {
		return "", false, false
	}
	if metrics != nil {
		metrics.cacheLookups.Add(1)
	}
	shard := c.shardFor(call)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	elem, ok := shard.entries[call]
	if !ok {
		return "", false, false
	}
	entry := elem.Value.(*callMetaEntry)
	shard.order.MoveToFront(elem)
	if entry.gridValid {
		if c.gridTTL > 0 && !entry.gridUpdatedAt.IsZero() && time.Since(entry.gridUpdatedAt) > c.gridTTL {
			entry.gridValid = false
			entry.grid = ""
			entry.gridDerived = false
			return "", false, false
		}
		if metrics != nil {
			metrics.cacheHits.Add(1)
		}
		return entry.grid, entry.gridDerived, true
	}
	return "", false, false
}

// UpdateGrid updates cached grid and indicates whether persistence is needed.
// Key aspects: Honors derived-vs-actual precedence and suppresses writes when unchanged.
// Upstream: grid update hook.
// Downstream: per-shard LRU mutation.
func (c *callMetaCache) UpdateGrid(call, grid string, derived bool) bool {
	if c == nil {
		return false
	}
	call = normalizeCallForMetadata(call)
	if call == "" || grid == "" {
		return false
	}
	shard := c.shardFor(call)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	now := time.Now().UTC()
	if elem, ok := shard.entries[call]; ok {
		entry := elem.Value.(*callMetaEntry)
		shard.order.MoveToFront(elem)
		expired := c.gridTTL > 0 && !entry.gridUpdatedAt.IsZero() && now.Sub(entry.gridUpdatedAt) > c.gridTTL
		if entry.gridValid && entry.grid == grid && entry.gridDerived == derived && !expired {
			return false
		}
		if derived && entry.gridValid && !entry.gridDerived {
			return false
		}
		entry.grid = grid
		entry.gridValid = true
		entry.gridDerived = derived
		entry.gridUpdatedAt = now
		return true
	}
	entry := shard.addEntry(call)
	entry.grid = grid
	entry.gridValid = true
	entry.gridDerived = derived
	entry.gridUpdatedAt = now
	return true
}

// ApplyRecord applies a persisted record into the cache.
// Key aspects: Prefer newer grid timestamps; keep CTY/known metadata when present.
// Upstream: grid lookup backfill.
// Downstream: per-shard LRU mutation.
func (c *callMetaCache) ApplyRecord(rec gridstore.Record) {
	if c == nil {
		return
	}
	call := normalizeCallForMetadata(rec.Call)
	if call == "" {
		return
	}
	shard := c.shardFor(call)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry := shard.getOrCreate(call)

	if rec.Grid.Valid {
		grid := strings.ToUpper(strings.TrimSpace(rec.Grid.String))
		if grid != "" {
			incomingDerived := rec.GridDerived
			if !(entry.gridValid && !entry.gridDerived && incomingDerived) {
				if !entry.gridValid || rec.UpdatedAt.After(entry.gridUpdatedAt) || (entry.gridDerived && !incomingDerived) {
					entry.grid = grid
					entry.gridValid = true
					entry.gridDerived = incomingDerived
					entry.gridUpdatedAt = rec.UpdatedAt
				}
			}
		}
	}
	entry.isKnown = rec.IsKnown
	entry.knownChecked = true
	if rec.CTYValid {
		entry.ctyChecked = true
		entry.ctyValid = true
		entry.cty = &cty.PrefixInfo{
			Country:   rec.CTYCountry,
			ADIF:      rec.CTYADIF,
			CQZone:    rec.CTYCQZone,
			ITUZone:   rec.CTYITUZone,
			Continent: rec.CTYContinent,
		}
	}
}

// SetKnown marks known-call status in cache when available.
// Key aspects: Stores both hits and misses to avoid repeated checks.
// Upstream: known calls lookup hooks.
// Downstream: per-shard LRU mutation.
func (c *callMetaCache) SetKnown(call string, known bool) {
	if c == nil || call == "" {
		return
	}
	shard := c.shardFor(call)
	shard.mu.Lock()
	entry := shard.getOrCreate(call)
	entry.isKnown = known
	entry.knownChecked = true
	shard.mu.Unlock()
}

func (c *callMetaCache) shardFor(key string) *callMetaCacheShard {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return &c.shards[hash%uint32(len(c.shards))]
}

func (s *callMetaCacheShard) getOrCreate(call string) *callMetaEntry {
	if elem, ok := s.entries[call]; ok {
		s.order.MoveToFront(elem)
		return elem.Value.(*callMetaEntry)
	}
	return s.addEntry(call)
}

func (s *callMetaCacheShard) addEntry(call string) *callMetaEntry {
	entry := &callMetaEntry{call: call}
	elem := s.order.PushFront(entry)
	s.entries[call] = elem
	if s.max > 0 && len(s.entries) > s.max {
		if tail := s.order.Back(); tail != nil {
			s.order.Remove(tail)
			if evicted, ok := tail.Value.(*callMetaEntry); ok {
				delete(s.entries, evicted.call)
			}
		}
	}
	return entry
}
