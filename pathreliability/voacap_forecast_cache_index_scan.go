// File role: Rebuilds the persisted VOACAP indexes for exceptional startup or
// recovery. Candidate ownership is capped; rejected keys are pruned in chunks.
package pathreliability

import (
	"container/heap"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
)

const voacapForecastCachePruneChunkSize = 256

// A scan may see arbitrarily many old DB records. It owns at most capacity
// candidates and one fixed-size deletion chunk, rather than retaining every
// decoded record or rejected key. The private candidate indexes remain untrusted
// until the iterator and every pruning commit have succeeded.
func (s *VOACAPForecastCacheStore) rebuildIndexLocked(now time.Time, currentSSN int, cfg VOACAPFallbackConfig, hydrate bool) (loaded []voacapForecastCacheLoadedEntry, stats VOACAPForecastCacheRestoreStats, err error) {
	if s == nil || s.db == nil {
		return nil, stats, nil
	}
	s.clearIndexLocked(false)
	defer func() {
		if err != nil {
			s.clearIndexLocked(true)
		}
	}()
	options := &pebble.IterOptions{
		LowerBound: []byte(voacapForecastCacheEntryKeyPrefix),
		UpperBound: voacapForecastCacheEntryKeyUpperBound(),
	}
	var iter *pebble.Iterator
	if s.faults != nil && s.faults.newIter != nil {
		iter, err = s.faults.newIter(s.db, options)
	} else {
		iter, err = s.db.NewIter(options)
	}
	if err != nil {
		return nil, stats, fmt.Errorf("voacap forecast cache: iterator: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = iter.Close()
		}
	}()
	s.index = make(map[string]*voacapForecastCacheIndexEntry)
	var payloads map[string]voacapForecastCacheLoadedEntry
	if hydrate {
		payloads = make(map[string]voacapForecastCacheLoadedEntry)
	}
	deleteKeys := make([][]byte, 0, voacapForecastCachePruneChunkSize)
	processed := 0
	for iter.First(); iter.Valid(); iter.Next() {
		processed++
		if s.faults != nil && s.faults.scanStep != nil {
			if err = s.faults.scanStep(processed); err != nil {
				return nil, stats, fmt.Errorf("voacap forecast cache: scan: %w", err)
			}
		}
		dbKey := string(iter.Key())
		key, entry, reason := decodeCurrentForecastCacheEntry([]byte(dbKey), iter.Value(), now, currentSSN, cfg)
		pruneKey := dbKey
		if reason == voacapForecastCacheRejectNone {
			pruneKey = s.retainScannedEntryLocked(dbKey, key, entry, cfg.MaxCacheEntries, payloads)
			if pruneKey != "" {
				stats.Overflow++
			}
		} else {
			countForecastCacheReject(&stats, reason)
		}
		if pruneKey != "" {
			deleteKeys = append(deleteKeys, []byte(pruneKey))
			if len(deleteKeys) == voacapForecastCachePruneChunkSize {
				if err = s.flushForecastCachePruneLocked(deleteKeys, &stats); err != nil {
					return nil, stats, err
				}
				clear(deleteKeys)
				deleteKeys = deleteKeys[:0]
			}
		}
	}
	if err = iter.Error(); err != nil {
		return nil, stats, fmt.Errorf("voacap forecast cache: scan: %w", err)
	}
	err = iter.Close()
	closed = true
	if err != nil {
		return nil, stats, fmt.Errorf("voacap forecast cache: iterator close: %w", err)
	}
	if err = s.flushForecastCachePruneLocked(deleteKeys, &stats); err != nil {
		return nil, stats, err
	}
	for _, e := range s.index {
		s.eligibility.add(e)
	}
	if hydrate {
		loaded = sortedForecastCachePayloads(payloads)
	}
	stats.Loaded = len(s.index)
	s.indexContext = forecastCacheIndexContext(now, currentSSN, cfg)
	s.indexReady = true
	s.indexDirty = false
	s.publishIndexStatsLocked()
	return loaded, stats, nil
}

func (s *VOACAPForecastCacheStore) retainScannedEntryLocked(dbKey string, key voacapCacheKey, entry voacapCacheEntry, capacity int, payloads map[string]voacapForecastCacheLoadedEntry) string {
	if capacity <= 0 {
		return dbKey
	}
	pruneKey := ""
	if len(s.index) >= capacity {
		victim := s.retention[0]
		if !forecastCacheRetentionBetter(entry.storedAt.UnixNano(), dbKey, victim.storedAtNS, victim.dbKey) {
			return dbKey
		}
		pruneKey = victim.dbKey
		heap.Pop(&s.retention)
		delete(s.index, victim.dbKey)
		delete(payloads, victim.dbKey)
	}
	e := forecastCacheIndexEntry(dbKey, entry.storedAt.UnixNano(), entry.forecast.WindowStartUTC.UnixNano(), entry.forecast.Records)
	s.index[dbKey] = e
	heap.Push(&s.retention, e)
	if payloads != nil {
		payloads[dbKey] = voacapForecastCacheLoadedEntry{key: key, entry: entry, dbKey: []byte(dbKey)}
	}
	return pruneKey
}

func (s *VOACAPForecastCacheStore) flushForecastCachePruneLocked(keys [][]byte, stats *VOACAPForecastCacheRestoreStats) error {
	if len(keys) == 0 {
		return nil
	}
	if err := s.deleteKeysLocked(keys); err != nil {
		return err
	}
	stats.Pruned += len(keys)
	return nil
}

func countForecastCacheReject(stats *VOACAPForecastCacheRestoreStats, reason voacapForecastCacheRejectReason) {
	switch reason {
	case voacapForecastCacheRejectExpired:
		stats.Expired++
	case voacapForecastCacheRejectStaleGeneration:
		stats.StaleGeneration++
	case voacapForecastCacheRejectStaleWindow:
		stats.StaleWindow++
	default:
		stats.Invalid++
	}
}

func sortedForecastCachePayloads(payloads map[string]voacapForecastCacheLoadedEntry) []voacapForecastCacheLoadedEntry {
	loaded := make([]voacapForecastCacheLoadedEntry, 0, len(payloads))
	for dbKey := range payloads {
		loaded = append(loaded, payloads[dbKey])
	}
	sort.Slice(loaded, func(i, j int) bool {
		if loaded[i].entry.storedAt.Equal(loaded[j].entry.storedAt) {
			return string(loaded[i].dbKey) < string(loaded[j].dbKey)
		}
		return loaded[i].entry.storedAt.After(loaded[j].entry.storedAt)
	})
	return loaded
}
