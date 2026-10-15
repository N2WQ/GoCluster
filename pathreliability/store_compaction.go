// File role: Releases retained path-map backing storage after contraction.
// Crawler notes: The store-wide snapshot is advisory. Each shard rechecks live
// occupancy under its write lock; compaction never copies or replaces buckets.
package pathreliability

// distributedContraction examines only the existing bounded shard collection.
// Peaks describe retained map growth, including peaks reached at different
// times. No update-time global counter or server-lifetime history is needed.
func (s *Store) distributedContraction(minPeak int, shrinkRatio float64) bool {
	current, peak := 0, 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		count := len(sh.buckets)
		current += count
		peak += max(sh.peak, count)
		sh.mu.RUnlock()
	}
	return peak >= minPeak && float64(current) < float64(peak)*shrinkRatio
}

func (sh *shard) compactIfContracted(minPeak int, shrinkRatio float64, distributed bool) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	current := len(sh.buckets)
	sh.peak = max(sh.peak, current)

	// Preserve the existing large-shard threshold, including its integer floor.
	local := sh.peak >= minPeak && current < int(float64(sh.peak)*shrinkRatio)
	// The added route releases maps whose individual peaks never reach minPeak.
	// Keep the ratio untruncated: 8/17 is below one half, even though int(8.5)=8.
	small := distributed && sh.peak < minPeak && float64(current) < float64(sh.peak)*shrinkRatio
	if !local && !small {
		return false
	}
	// Refill since the advisory snapshot is reflected in current and peak above.
	next := make(map[uint64]*bucket, current)
	for key, value := range sh.buckets {
		if value != nil {
			next[key] = value
		}
	}
	sh.buckets = next
	sh.peak = len(next)
	return true
}
