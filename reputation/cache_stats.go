// File role: Coherent, read-only lookup-cache pressure and retention diagnostics.
package reputation

// LookupCacheStats describes the one combined main/Cymru cache owner. Retained
// entries include unread expired records. OwnedStringBytes excludes fixed slot,
// map and allocator overhead, time.Time locations, and upstream data stores;
// it is not a process-memory measurement.
type LookupCacheStats struct {
	MaxEntries       int
	Entries          int
	MainEntries      int
	CymruEntries     int
	AllocatedSlots   int
	OwnedStringBytes int
	Evictions        uint64
	Expirations      uint64
	Bypasses         uint64
	Shards           []LookupCacheShardStats
}

// LookupCacheShardStats exposes occupancy versus a shard's exact allowance.
// Evictions count capacity removals, Expirations count get-observed expiry, and
// Bypasses count oversized writes; admitted updates and short blank keys do not
// count. The aggregate includes oversized-key bypasses needing no shard access.
type LookupCacheShardStats struct {
	MaxEntries       int
	Entries          int
	MainEntries      int
	CymruEntries     int
	AllocatedSlots   int
	OwnedStringBytes int
	Evictions        uint64
	Expirations      uint64
	Bypasses         uint64
}

// LookupCacheStats returns a coherent detached snapshot without touching TTL or
// FIFO order. Disabled/nil gates return zero. No UI or logging stream is added.
func (g *Gate) LookupCacheStats() LookupCacheStats {
	if g == nil || g.lookupCache == nil || g.lookupCache.store == nil {
		return LookupCacheStats{}
	}
	return g.lookupCache.store.snapshot()
}

func (s *lookupCacheStore) snapshot() LookupCacheStats {
	// Normal operations take only one shard lock. Snapshots take all locks in
	// index order and release before returning/formatting; no other path takes
	// multiple locks, so aggregate counters and membership describe one state.
	stats := LookupCacheStats{MaxEntries: s.maxEntries, Shards: make([]LookupCacheShardStats, len(s.shards))}
	for i := range s.shards {
		s.shards[i].mu.Lock()
	}
	for i := range s.shards {
		shard := &s.shards[i]
		part := LookupCacheShardStats{
			MaxEntries: shard.quota, Entries: len(shard.items),
			MainEntries: shard.counts[cacheMain], CymruEntries: shard.counts[cacheCymru],
			AllocatedSlots: len(shard.slots), OwnedStringBytes: shard.ownedBytes,
			Evictions: shard.evictions, Expirations: shard.expirations, Bypasses: shard.bypasses,
		}
		stats.Shards[i] = part
		stats.Entries += part.Entries
		stats.MainEntries += part.MainEntries
		stats.CymruEntries += part.CymruEntries
		stats.AllocatedSlots += part.AllocatedSlots
		stats.OwnedStringBytes += part.OwnedStringBytes
		stats.Evictions += part.Evictions
		stats.Expirations += part.Expirations
		stats.Bypasses += part.Bypasses
	}
	for i := range s.earlyBypasses {
		stats.Bypasses += s.earlyBypasses[i].Load()
	}
	for i := len(s.shards) - 1; i >= 0; i-- {
		s.shards[i].mu.Unlock()
	}
	return stats
}
