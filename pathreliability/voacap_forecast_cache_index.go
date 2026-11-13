// File role: Maintains bounded, payload-free indexes for the persisted VOACAP
// cache. Ordinary completions touch their key and actual victims only; startup,
// recovery, configuration/generation changes and backward clocks rebuild from DB.
package pathreliability

import (
	"container/heap"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
)

// VOACAPForecastCacheIndexSnapshot is a cached description of persisted-cache
// bookkeeping. Attached but not Ready means the DB has not been certified;
// zero entries in that state does not establish that the DB is empty.
type VOACAPForecastCacheIndexSnapshot struct {
	Entries            int
	RetentionEntries   int
	EligibilityEntries int
	Attached           bool
	Ready              bool
	Dirty              bool
}

// IndexSnapshot reads the last published state without waiting behind disk I/O.
func (s *VOACAPForecastCacheStore) IndexSnapshot() VOACAPForecastCacheIndexSnapshot {
	if s == nil {
		return VOACAPForecastCacheIndexSnapshot{}
	}
	s.indexStatsMu.RLock()
	stats := s.indexStats
	s.indexStatsMu.RUnlock()
	return stats
}

type voacapForecastCacheIndexEntry struct {
	dbKey            string
	storedAtNS       int64
	windowStartNS    int64
	windowEndNS      int64
	expiresNS        int64
	originNS         int64
	hourMask         uint32
	retentionIndex   int
	eligibilityIndex int
	subtreeHourMask  uint32
	subtreeMinStart  int64
	subtreeMaxStart  int64
	subtreeMinEnd    int64
	subtreeMinExpiry int64
}

type voacapForecastCacheIndexContext struct {
	maxEntries int
	lastNow    time.Time
}

// Hooks are nil in production. Tests can fail iterator creation, an arbitrary
// scan step, or a commit (including a real commit followed by an error) without
// changing the concrete Pebble owner or adding an alternate production backend.
type voacapForecastCacheFaults struct {
	newIter  func(*pebble.DB, *pebble.IterOptions) (*pebble.Iterator, error)
	scanStep func(int) error
	commit   func(*pebble.Batch, *pebble.WriteOptions) error
}

func forecastCacheIndexContext(now time.Time, _ int, cfg VOACAPFallbackConfig) voacapForecastCacheIndexContext {
	return voacapForecastCacheIndexContext{maxEntries: cfg.MaxCacheEntries, lastNow: now}
}

func (c voacapForecastCacheIndexContext) matches(now time.Time, _ int, cfg VOACAPFallbackConfig) bool {
	return c.maxEntries == cfg.MaxCacheEntries && !now.Before(c.lastNow)
}

func (s *VOACAPForecastCacheStore) publishIndexStatsLocked() {
	stats := VOACAPForecastCacheIndexSnapshot{
		Entries: len(s.index), RetentionEntries: len(s.retention),
		EligibilityEntries: len(s.eligibility), Attached: s.db != nil,
		Ready: s.indexReady, Dirty: s.indexDirty,
	}
	s.indexStatsMu.Lock()
	s.indexStats = stats
	s.indexStatsMu.Unlock()
}

func (s *VOACAPForecastCacheStore) clearIndexLocked(dirty bool) {
	s.index = nil
	s.retention = nil
	s.eligibility = nil
	s.indexContext = voacapForecastCacheIndexContext{}
	s.indexReady = false
	s.indexDirty = dirty
	s.publishIndexStatsLocked()
}

func (s *VOACAPForecastCacheStore) commitForecastCacheBatchLocked(batch *pebble.Batch) error {
	if s.faults != nil && s.faults.commit != nil {
		return s.faults.commit(batch, pebble.NoSync)
	}
	return batch.Commit(pebble.NoSync)
}

func forecastCacheIndexEntry(dbKey string, storedAtNS, windowStartNS, windowEndNS, expiresNS, originNS int64, records []VOACAPHourlyForecast) *voacapForecastCacheIndexEntry {
	return &voacapForecastCacheIndexEntry{dbKey: dbKey, storedAtNS: storedAtNS, windowStartNS: windowStartNS, windowEndNS: windowEndNS, expiresNS: expiresNS, originNS: originNS, hourMask: forecastCacheHourMask(records), retentionIndex: -1, eligibilityIndex: -1}
}

func (s *VOACAPForecastCacheStore) addIndexEntryLocked(e *voacapForecastCacheIndexEntry) {
	s.index[e.dbKey] = e
	heap.Push(&s.retention, e)
	s.eligibility.add(e)
}

func (s *VOACAPForecastCacheStore) removeIndexEntryLocked(e *voacapForecastCacheIndexEntry, batch *pebble.Batch) error {
	if err := batch.Delete([]byte(e.dbKey), nil); err != nil {
		return fmt.Errorf("voacap forecast cache: delete: %w", err)
	}
	heap.Remove(&s.retention, e.retentionIndex)
	s.eligibility.remove(e)
	delete(s.index, e.dbKey)
	return nil
}

// The private speculative indexes are never read outside mu. A failed or
// uncertain commit clears them; only a successful commit publishes trust.
func (s *VOACAPForecastCacheStore) storeIndexedForecastLocked(key voacapCacheKey, record voacapForecastCacheDiskRecord, value []byte, cfg VOACAPFallbackConfig, now time.Time) (err error) {
	if !s.indexReady || !s.indexContext.matches(now, 0, cfg) {
		if _, _, err = s.rebuildIndexLocked(now, 0, cfg, false); err != nil {
			return err
		}
	}
	defer func() {
		if err != nil {
			s.clearIndexLocked(true)
		}
	}()
	s.indexReady = false
	batch := s.db.NewBatch()
	defer batch.Close()
	if err = s.pruneIndexedEntriesLocked(batch, now, cfg); err != nil {
		return err
	}
	dbKey := voacapForecastCacheKeyBytes(key)
	_, reason := validateCurrentForecastCacheRecord(dbKey, record, now, 0, cfg)
	if reason != voacapForecastCacheRejectNone {
		return fmt.Errorf("voacap forecast cache: invalid replacement (%d)", reason)
	}
	if err = s.stageIndexedForecastLocked(dbKey, record, value, batch, cfg.MaxCacheEntries); err != nil {
		return err
	}

	if !batch.Empty() {
		if err = s.commitForecastCacheBatchLocked(batch); err != nil {
			return fmt.Errorf("voacap forecast cache: persist: %w", err)
		}
	}
	s.indexContext = forecastCacheIndexContext(now, 0, cfg)
	s.indexReady = true
	s.indexDirty = false
	s.publishIndexStatsLocked()
	return nil
}

func (s *VOACAPForecastCacheStore) pruneIndexedEntriesLocked(batch *pebble.Batch, now time.Time, cfg VOACAPFallbackConfig) error {
	for {
		e := s.eligibility.findInvalid(now, cfg.ForecastHours)
		if e == nil {
			return nil
		}
		if err := s.removeIndexEntryLocked(e, batch); err != nil {
			return err
		}
	}
}

func (s *VOACAPForecastCacheStore) stageIndexedForecastLocked(dbKey []byte, record voacapForecastCacheDiskRecord, value []byte, batch *pebble.Batch, capacity int) error {
	existing := s.index[string(dbKey)]
	if existing != nil {
		if existing.originNS > record.Metadata.OriginUnixNS {
			return nil
		}
		// Reuse the owned key/node. Refresh changes no index cardinality and cannot
		// accumulate stale heap nodes across the lifetime of a popular path.
		existing.storedAtNS = record.StoredAtUnixNS
		existing.windowStartNS = record.WindowUnixNS
		existing.windowEndNS = record.Metadata.WindowEndUnixNS
		existing.expiresNS = record.Metadata.ExpiresUnixNS
		existing.originNS = record.Metadata.OriginUnixNS
		existing.hourMask = forecastCacheHourMask(record.Records)
		heap.Fix(&s.retention, existing.retentionIndex)
		s.eligibility.fix(existing)
	} else {
		if capacity <= 0 {
			return nil
		}
		if len(s.index) >= capacity {
			victim := s.retention[0]
			if !forecastCacheRetentionBetter(record.StoredAtUnixNS, string(dbKey), victim.storedAtNS, victim.dbKey) {
				return nil
			}
			if err := s.removeIndexEntryLocked(victim, batch); err != nil {
				return err
			}
		}
		s.addIndexEntryLocked(forecastCacheIndexEntry(string(dbKey), record.StoredAtUnixNS, record.WindowUnixNS, record.Metadata.WindowEndUnixNS, record.Metadata.ExpiresUnixNS, record.Metadata.OriginUnixNS, record.Records))
	}
	if err := batch.Set(dbKey, value, nil); err != nil {
		return fmt.Errorf("voacap forecast cache: set: %w", err)
	}
	return nil
}

func forecastCacheHourMask(records []VOACAPHourlyForecast) uint32 {
	var mask uint32
	for _, record := range records {
		// now.Hour is always 0..23; other record hours cannot match the existing
		// selector and must not wrap the mask's bit shift.
		if record.HourUTC >= 0 && record.HourUTC < 24 {
			mask |= uint32(1) << uint(record.HourUTC)
		}
	}
	return mask
}

func forecastCacheRetentionBetter(aTime int64, aKey string, bTime int64, bKey string) bool {
	return aTime > bTime || aTime == bTime && aKey < bKey
}
