// File role: Owns bounded call metadata caching with separate grid/CTY authority.
// Crawler notes: Grid age/provenance comes from reports or disk; classification
// belongs to one CTY snapshot token. See ADR-0340 and cty_cache_test.go.
package cluster

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/cty"
	"dxcluster/gridstore"
	"dxcluster/strutil"
)

const callMetaCacheShardCount = 16

// callMetaCache stores per-call grid and CTY metadata with a bounded LRU.
// Grid lookups normalize to a base identity (uppercase, trimmed, hyphen suffix stripped)
// to avoid duplicate entries across source variants.
type callMetaCache struct {
	shards    []callMetaCacheShard
	gridTTL   time.Duration
	capacity  int
	authority *gridCoordinator // stable before CTY scheduling or recovery

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

	cty        *cty.PrefixInfo
	ctySource  *cty.SnapshotIdentity
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
	c := &callMetaCache{
		shards:   shards,
		gridTTL:  gridTTL,
		capacity: capacity,
	}
	c.authority = newGridCoordinator(c)
	return c
}

// Clear clears all cached entries (used on CTY reload).
// Key aspects: Resets shard maps under lock.
// Upstream: CTY refresh handlers.
// Downstream: shard state reset.
func (c *callMetaCache) Clear() {
	if c == nil {
		return
	}
	c.authority.invalidateCache()
}

// clearEntries is called only by the authority owner, before shard locks.
func (c *callMetaCache) clearEntries() {
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
	source := db.SnapshotIdentity()
	shard := c.shardFor(call)
	shard.mu.Lock()
	if elem, ok := shard.entries[call]; ok {
		if entry, valid := elem.Value.(*callMetaEntry); valid && entry != nil {
			shard.order.MoveToFront(elem)
			if entry.ctyChecked && entry.ctySource == source {
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
		} else {
			shard.order.Remove(elem)
			delete(shard.entries, call)
		}
	}
	shard.mu.Unlock()

	if shouldRejectCTYCall(call) {
		c.storeCTYResult(call, source, nil, false)
		return nil, false, false
	}

	info, ok := db.LookupCallsignPortable(call)
	c.storeCTYResult(call, source, info, ok)
	return info, ok, false
}

// storeCTYResult publishes a resolved result with the identity captured from
// that lookup's database. An old in-flight lookup may finish after Clear/new
// publication; its tag prevents a new-snapshot caller from reusing the result.
// The ordinary bounded entry owns only a tiny token and copied PrefixInfo.
func (c *callMetaCache) storeCTYResult(call string, source *cty.SnapshotIdentity, info *cty.PrefixInfo, ok bool) {
	shard := c.shardFor(call)
	shard.mu.Lock()
	entry := shard.getOrCreate(call)
	entry.ctyChecked = true
	entry.ctyValid = ok
	entry.ctySource = source
	if ok {
		entry.cty = info
	} else {
		entry.cty = nil
	}
	shard.mu.Unlock()
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
	entry, valid := elem.Value.(*callMetaEntry)
	if !valid || entry == nil {
		shard.order.Remove(elem)
		delete(shard.entries, call)
		return "", false, false
	}
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
		entry, valid := elem.Value.(*callMetaEntry)
		if !valid || entry == nil {
			shard.order.Remove(elem)
			delete(shard.entries, call)
			entry = shard.addEntry(call)
		} else {
			shard.order.MoveToFront(elem)
		}
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

// unchangedReported is a shard-only fast path. Failed checks change no grid
// state; the authority path rechecks. Equal updates still refresh LRU recency.
func (c *callMetaCache) unchangedReported(call, grid string) bool {
	if c == nil {
		return false
	}
	shard := c.shardFor(call)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	elem := shard.entries[call]
	if elem == nil {
		return false
	}
	entry, ok := elem.Value.(*callMetaEntry)
	if !ok || entry == nil || !entry.gridValid || entry.gridDerived || entry.grid != grid {
		return false
	}
	if c.gridTTL > 0 && !entry.gridUpdatedAt.IsZero() && time.Since(entry.gridUpdatedAt) > c.gridTTL {
		return false
	}
	shard.order.MoveToFront(elem)
	return true
}

// ApplyRecord hydrates a persisted grid and provenance, never classification.
// Legacy disk CTY fields lack coordinates and file identity; only LookupCTY
// using the caller's active snapshot may establish a checked CTY hit or miss.
// Upstream: grid lookup backfill.
// Downstream: per-shard LRU mutation.
func (c *callMetaCache) ApplyRecord(rec gridstore.Record) {
	c.selectGridRecord(rec)
}

// selectGridRecord returns the exact selected tuple, including rejection of a
// stale candidate. It does not reset disk age or apply cache TTL to that reply.
func (c *callMetaCache) selectGridRecord(rec gridstore.Record) gridLookupResult {
	result, _ := c.selectGridRecordSource(rec)
	return result
}

func (c *callMetaCache) selectGridRecordSource(rec gridstore.Record) (gridLookupResult, bool) {
	if c == nil {
		return recordGridResult(&rec), true
	}
	call := normalizeCallForMetadata(rec.Call)
	if call == "" {
		return gridLookupResult{}, false
	}
	shard := c.shardFor(call)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry := shard.getOrCreate(call)

	if rec.Grid.Valid {
		grid := strutil.NormalizeUpper(rec.Grid.String)
		if grid != "" {
			incomingDerived := rec.GridDerived
			if !entry.gridValid || entry.gridDerived || !incomingDerived {
				if !entry.gridValid || rec.UpdatedAt.After(entry.gridUpdatedAt) || (entry.gridDerived && !incomingDerived) {
					entry.grid = grid
					entry.gridValid = true
					entry.gridDerived = incomingDerived
					entry.gridUpdatedAt = rec.UpdatedAt
				}
			}
		}
	}
	selected := gridLookupResult{grid: entry.grid, derived: entry.gridDerived, ok: entry.gridValid}
	incoming := recordGridResult(&rec)
	return selected, selected == incoming && entry.gridUpdatedAt.Equal(rec.UpdatedAt)
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
		if entry, valid := elem.Value.(*callMetaEntry); valid && entry != nil {
			s.order.MoveToFront(elem)
			return entry
		}
		s.order.Remove(elem)
		delete(s.entries, call)
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
