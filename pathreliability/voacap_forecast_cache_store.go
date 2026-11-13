// File role: Owns the Pebble-backed restart cache for completed VOACAP forecast
// windows while keeping live prediction lookups memory-only.
// Crawler notes: This store persists derived forecast records only; SSN state,
// warm-up queues, worker lifecycle, and VOACAP runner output files live
// elsewhere.
package pathreliability

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"dxcluster/internal/voacap"
	"sync"
	"time"

	"dxcluster/internal/pebbleutil"

	"github.com/cockroachdb/pebble"
)

const (
	voacapForecastCacheSchemaVersion     = 2
	voacapForecastCacheModelGeneration   = 2
	voacapForecastCacheEntryKeyPrefix    = "voacapfc/v2/e/"
	voacapForecastCacheLegacyKeyPrefix   = "voacapfc/v1/e/"
	voacapForecastCacheMaximumValueBytes = 64 * 1024
	voacapForecastCacheFutureStoredSkew  = time.Minute
)

// VOACAPForecastCacheRestoreStats reports startup hydration work for the
// derived VOACAP prediction cache. Non-loaded entries are intentionally pruned
// when they no longer match the current forecast contract.
type VOACAPForecastCacheRestoreStats struct {
	Loaded          int
	Pruned          int
	Expired         int
	Invalid         int
	StaleGeneration int
	StaleWindow     int
	Overflow        int
	SSNUnavailable  bool
	Retired         int
	RebuildEligible int
}

// VOACAPForecastCacheStore owns the Pebble cache for completed VOACAP forecast
// windows. The runtime still serves lookups from VOACAPClosedFallback memory;
// Pebble is only a restart hydration and worker-completion persistence surface.
type VOACAPForecastCacheStore struct {
	path string
	db   *pebble.DB
	mu   sync.Mutex

	// The disk mutex owns these bounded, payload-free indexes. A failed or
	// uncertain write discards all derived state before the next operation.
	index         map[string]*voacapForecastCacheIndexEntry
	retention     voacapForecastCacheRetentionHeap
	eligibility   voacapForecastCacheEligibilityHeap
	indexContext  voacapForecastCacheIndexContext
	indexReady    bool
	indexDirty    bool
	legacyRetired bool
	faults        *voacapForecastCacheFaults

	// Diagnostics never acquire mu or wait behind Pebble I/O.
	indexStatsMu sync.RWMutex
	indexStats   VOACAPForecastCacheIndexSnapshot
}

type voacapForecastCacheLoadedEntry struct {
	key   voacapCacheKey
	entry voacapCacheEntry
	dbKey []byte
}

type voacapForecastCacheDiskRecord struct {
	SchemaVersion   int                             `json:"schema_version"`
	ModelGeneration int                             `json:"model_generation"`
	Key             voacapForecastCacheDiskKey      `json:"key"`
	StoredAtUnixNS  int64                           `json:"stored_at_unix_ns"`
	WindowUnixNS    int64                           `json:"window_unix_ns"`
	Records         []VOACAPHourlyForecast          `json:"records"`
	Metadata        voacapForecastCacheDiskMetadata `json:"metadata"`
}

type voacapForecastCacheDiskMetadata struct {
	SolarInput      voacap.SolarInputSnapshot `json:"solar_input"`
	WindowEndUnixNS int64                     `json:"window_end_unix_ns"`
	ExpiresUnixNS   int64                     `json:"expires_unix_ns"`
	OriginUnixNS    int64                     `json:"origin_unix_ns"`
	Flags           uint32                    `json:"flags"`
}

type voacapForecastCacheDiskKey struct {
	UserCell     uint16 `json:"user_cell"`
	DXCell       uint16 `json:"dx_cell"`
	Band         string `json:"band"`
	FrequencyKHz int    `json:"frequency_khz"`
	Year         int    `json:"year"`
	Month        int    `json:"month"`
	SSN          int    `json:"ssn"`
	Direction    string `json:"direction"`
}

type voacapForecastCacheRejectReason uint8

const (
	voacapForecastCacheRejectNone voacapForecastCacheRejectReason = iota
	voacapForecastCacheRejectInvalid
	voacapForecastCacheRejectStaleGeneration
	voacapForecastCacheRejectExpired
	voacapForecastCacheRejectStaleWindow
)

// OpenVOACAPForecastCacheStore opens the dedicated Pebble DB for restart
// reuse of VOACAP forecast windows.
func OpenVOACAPForecastCacheStore(path string) (*VOACAPForecastCacheStore, error) {
	path, err := pebbleutil.PrepareDir(path)
	if err != nil {
		return nil, voacapForecastCacheOpenDirError(err)
	}
	db, err := pebbleutil.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("voacap forecast cache: open: %w", err)
	}
	return &VOACAPForecastCacheStore{path: path, db: db, indexStats: VOACAPForecastCacheIndexSnapshot{Attached: true}}, nil
}

func voacapForecastCacheOpenDirError(err error) error {
	if errors.Is(err, pebbleutil.ErrEmptyPath) {
		return errors.New("voacap forecast cache: path is empty")
	}
	var notDir *pebbleutil.NotDirectoryError
	if errors.As(err, &notDir) {
		return fmt.Errorf("voacap forecast cache: %s exists and is not a directory", notDir.Path)
	}
	var opErr *pebbleutil.DirOpError
	if errors.As(err, &opErr) {
		switch opErr.Op {
		case pebbleutil.DirOpStat:
			return fmt.Errorf("voacap forecast cache: stat path: %w", opErr.Err)
		case pebbleutil.DirOpMkdir:
			return fmt.Errorf("voacap forecast cache: mkdir: %w", opErr.Err)
		}
	}
	return fmt.Errorf("voacap forecast cache: %w", err)
}

func (s *VOACAPForecastCacheStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	s.clearIndexLocked(false)
	return err
}

func (s *VOACAPForecastCacheStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *VOACAPForecastCacheStore) loadCurrent(now time.Time, cfg VOACAPFallbackConfig) ([]voacapForecastCacheLoadedEntry, VOACAPForecastCacheRestoreStats, error) {
	return s.loadCurrentRetiring(now, cfg, nil)
}

func (s *VOACAPForecastCacheStore) storeForecast(key voacapCacheKey, entry voacapCacheEntry, cfg VOACAPFallbackConfig, now time.Time) error {
	if s == nil {
		return nil
	}
	now = normalizeVOACAPForecastCacheNow(now)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	// A failed startup retirement must not leave old payloads outside the new
	// index's capacity while later successful writes fill the v2 namespace.
	if !s.legacyRetired {
		var stats VOACAPForecastCacheRestoreStats
		if err := s.retireLegacyLocked(now, nil, &stats); err != nil {
			s.clearIndexLocked(true)
			return err
		}
		s.legacyRetired = true
	}
	record := diskRecordFromForecastCache(key, entry)
	value, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("voacap forecast cache: encode: %w", err)
	}
	return s.storeIndexedForecastLocked(key, record, value, cfg, now)
}

func (s *VOACAPForecastCacheStore) scanCurrentLocked(now time.Time, currentSSN int, cfg VOACAPFallbackConfig) ([]voacapForecastCacheLoadedEntry, VOACAPForecastCacheRestoreStats, error) {
	return s.rebuildIndexLocked(now, currentSSN, cfg, true)
}

func (s *VOACAPForecastCacheStore) deleteKeysLocked(keys [][]byte) error {
	if s == nil || s.db == nil || len(keys) == 0 {
		return nil
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	for _, key := range keys {
		if err := batch.Delete(key, nil); err != nil {
			return fmt.Errorf("voacap forecast cache: delete: %w", err)
		}
	}
	if err := s.commitForecastCacheBatchLocked(batch); err != nil {
		return fmt.Errorf("voacap forecast cache: prune: %w", err)
	}
	return nil
}

func decodeCurrentForecastCacheEntry(dbKey, value []byte, now time.Time, currentSSN int, cfg VOACAPFallbackConfig) (voacapCacheKey, voacapCacheEntry, voacapForecastCacheRejectReason) {
	if len(value) > voacapForecastCacheMaximumValueBytes || len(dbKey) > 128 {
		return voacapCacheKey{}, voacapCacheEntry{}, voacapForecastCacheRejectInvalid
	}
	var record voacapForecastCacheDiskRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return voacapCacheKey{}, voacapCacheEntry{}, voacapForecastCacheRejectInvalid
	}
	key, reason := validateCurrentForecastCacheRecord(dbKey, record, now, currentSSN, cfg)
	if reason != voacapForecastCacheRejectNone {
		return voacapCacheKey{}, voacapCacheEntry{}, reason
	}
	records := make([]VOACAPHourlyForecast, len(record.Records))
	copy(records, record.Records)
	entry := forecastCacheEntryFromDiskRecord(record)
	entry.forecast.Records = records
	return key, entry, voacapForecastCacheRejectNone
}

func forecastCacheEntryFromDiskRecord(record voacapForecastCacheDiskRecord) voacapCacheEntry {
	return voacapCacheEntry{storedAt: time.Unix(0, record.StoredAtUnixNS).UTC(), forecast: VOACAPClosedForecast{WindowStartUTC: time.Unix(0, record.WindowUnixNS).UTC(), Records: record.Records}, metadata: &voacapForecastMetadata{solar: record.Metadata.SolarInput, originNS: record.Metadata.OriginUnixNS}, windowEndNS: record.Metadata.WindowEndUnixNS, expiresNS: record.Metadata.ExpiresUnixNS, hourMask: forecastCacheHourMask(record.Records), flags: record.Metadata.Flags}
}

// Validity is frozen per value. Solar changes and a missing represented hour
// do not authorize deleting another still-unexpired forecast owner.
func validateCurrentForecastCacheRecord(dbKey []byte, record voacapForecastCacheDiskRecord, now time.Time, _ int, _ VOACAPFallbackConfig) (voacapCacheKey, voacapForecastCacheRejectReason) {
	if record.SchemaVersion != voacapForecastCacheSchemaVersion || record.ModelGeneration != voacapForecastCacheModelGeneration {
		return voacapCacheKey{}, voacapForecastCacheRejectStaleGeneration
	}
	key, ok := record.Key.cacheKey()
	if !ok || len(dbKey) > 128 || string(dbKey) != string(voacapForecastCacheKeyBytes(key)) {
		return voacapCacheKey{}, voacapForecastCacheRejectInvalid
	}
	m := record.Metadata
	if len(record.Records) < 1 || len(record.Records) > 24 || record.StoredAtUnixNS <= 0 || record.WindowUnixNS <= 0 || m.Flags != voacapMetadataInitialized || !m.SolarInput.Valid() || m.OriginUnixNS < record.WindowUnixNS || m.OriginUnixNS > record.StoredAtUnixNS || m.ExpiresUnixNS < record.StoredAtUnixNS {
		return voacapCacheKey{}, voacapForecastCacheRejectInvalid
	}
	start := time.Unix(0, record.WindowUnixNS).UTC()
	end := time.Unix(0, m.WindowEndUnixNS).UTC()
	duration := end.Sub(start)
	if !start.Equal(start.Truncate(time.Hour)) || duration <= 0 || duration > 24*time.Hour || duration%time.Hour != 0 || m.WindowEndUnixNS <= record.WindowUnixNS {
		return voacapCacheKey{}, voacapForecastCacheRejectInvalid
	}
	var mask uint32
	for _, row := range record.Records {
		if row.HourUTC < 0 || row.HourUTC > 23 || mask&(1<<uint(row.HourUTC)) != 0 || math.IsNaN(row.FrequencyMHz) || math.IsInf(row.FrequencyMHz, 0) || bandForMHz(row.FrequencyMHz) != key.band {
			return voacapCacheKey{}, voacapForecastCacheRejectInvalid
		}
		offset := (row.HourUTC - start.Hour() + 24) % 24
		if time.Duration(offset)*time.Hour >= duration {
			return voacapCacheKey{}, voacapForecastCacheRejectInvalid
		}
		mask |= 1 << uint(row.HourUTC)
	}
	if time.Unix(0, record.StoredAtUnixNS).After(now.Add(voacapForecastCacheFutureStoredSkew)) {
		return voacapCacheKey{}, voacapForecastCacheRejectInvalid
	}
	if now.UnixNano() > m.ExpiresUnixNS {
		return voacapCacheKey{}, voacapForecastCacheRejectExpired
	}
	if now.Before(start) || !now.Before(end) {
		return voacapCacheKey{}, voacapForecastCacheRejectStaleWindow
	}
	return key, voacapForecastCacheRejectNone
}

func diskRecordFromForecastCache(key voacapCacheKey, entry voacapCacheEntry) voacapForecastCacheDiskRecord {
	metadata := voacapForecastCacheDiskMetadata{Flags: entry.flags}
	if entry.metadata != nil {
		metadata.SolarInput = entry.metadata.solar
		metadata.WindowEndUnixNS = entry.windowEndNS
		metadata.ExpiresUnixNS = entry.expiresNS
		metadata.OriginUnixNS = entry.metadata.originNS
	}
	return voacapForecastCacheDiskRecord{SchemaVersion: voacapForecastCacheSchemaVersion, ModelGeneration: voacapForecastCacheModelGeneration, Key: diskKeyFromForecastCacheKey(key), StoredAtUnixNS: entry.storedAt.UTC().UnixNano(), WindowUnixNS: entry.forecast.WindowStartUTC.UTC().UnixNano(), Records: entry.forecast.Records, Metadata: metadata}
}

func diskKeyFromForecastCacheKey(key voacapCacheKey) voacapForecastCacheDiskKey {
	return voacapForecastCacheDiskKey{UserCell: uint16(key.userCell), DXCell: uint16(key.dxCell), Band: key.band, FrequencyKHz: key.frequencyKHz, Direction: key.directionName()}
}

// The internal compact ID must never alter or silently alias a persisted key.
func (key voacapCacheKey) directionName() string {
	if key.direction != voacapDirectionBidirectionalID {
		panic("noncanonical VOACAP cache direction")
	}
	return voacapDirectionBidirectional
}

func (key voacapForecastCacheDiskKey) cacheKey() (voacapCacheKey, bool) {
	// Use the fixed band literal, not a substring of arbitrarily padded JSON.
	band := bandForMHz(float64(key.FrequencyKHz) / 1000)
	if key.UserCell == 0 || key.DXCell == 0 || band == "" || normalizeBand(key.Band) != band || key.FrequencyKHz <= 0 || strings.TrimSpace(key.Direction) != voacapDirectionBidirectional {
		return voacapCacheKey{}, false
	}
	return voacapCacheKey{userCell: CellID(key.UserCell), dxCell: CellID(key.DXCell), band: band, frequencyKHz: key.FrequencyKHz, direction: voacapDirectionBidirectionalID}, true
}

func voacapForecastCacheKeyBytes(key voacapCacheKey) []byte {
	return []byte(fmt.Sprintf("%s%04x/%04x/%s/%06d/%s", voacapForecastCacheEntryKeyPrefix, uint16(key.userCell), uint16(key.dxCell), key.band, key.frequencyKHz, key.directionName()))
}

func voacapForecastCacheEntryKeyUpperBound() []byte {
	return append([]byte(voacapForecastCacheEntryKeyPrefix), 0xff)
}

func (s *VOACAPForecastCacheStore) loadCurrentRetiring(now time.Time, cfg VOACAPFallbackConfig, retire func(voacapCacheKey)) ([]voacapForecastCacheLoadedEntry, VOACAPForecastCacheRestoreStats, error) {
	now = normalizeVOACAPForecastCacheNow(now)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.legacyRetired = false
	loaded, stats, err := s.scanCurrentLocked(now, 0, cfg)
	if err == nil {
		err = s.retireLegacyLocked(now, retire, &stats)
	}
	if err != nil {
		s.clearIndexLocked(true)
		return nil, stats, err
	}
	s.legacyRetired = true
	return loaded, stats, nil
}

// Old geometry is never served. A successful bounded deletion publishes only
// the known path identity to the existing transient delay owner, not payloads.
func (s *VOACAPForecastCacheStore) retireLegacyLocked(now time.Time, retire func(voacapCacheKey), stats *VOACAPForecastCacheRestoreStats) error {
	if s.db == nil {
		return nil
	}
	options := &pebble.IterOptions{LowerBound: []byte(voacapForecastCacheLegacyKeyPrefix), UpperBound: append([]byte(voacapForecastCacheLegacyKeyPrefix), 0xff)}
	var iter *pebble.Iterator
	var err error
	if s.faults != nil && s.faults.newIter != nil {
		iter, err = s.faults.newIter(s.db, options)
	} else {
		iter, err = s.db.NewIter(options)
	}
	if err != nil {
		return fmt.Errorf("voacap legacy cache: iterator: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = iter.Close()
		}
	}()
	keys := make([][]byte, 0, voacapForecastCachePruneChunkSize)
	eligible := make([]voacapCacheKey, 0, voacapForecastCachePruneChunkSize)
	flush := func() error {
		if err := s.deleteKeysLocked(keys); err != nil {
			return err
		}
		stats.Pruned += len(keys)
		stats.Retired += len(keys)
		if retire != nil {
			for _, key := range eligible {
				retire(key)
				stats.RebuildEligible++
			}
		}
		clear(keys)
		keys = keys[:0]
		eligible = eligible[:0]
		return nil
	}
	for iter.First(); iter.Valid(); iter.Next() {
		rawKey := iter.Key()
		if key, ok := recognizedLegacyForecastKey(rawKey, iter.Value(), now); ok {
			eligible = append(eligible, key)
		}
		keys = append(keys, append([]byte(nil), rawKey...))
		if len(keys) == voacapForecastCachePruneChunkSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("voacap legacy cache: scan: %w", err)
	}
	err = iter.Close()
	closed = true
	if err != nil {
		return fmt.Errorf("voacap legacy cache: iterator close: %w", err)
	}
	return flush()
}

func recognizedLegacyForecastKey(dbKey, value []byte, now time.Time) (voacapCacheKey, bool) {
	if len(dbKey) > 128 || len(value) > voacapForecastCacheMaximumValueBytes {
		return voacapCacheKey{}, false
	}
	var record voacapForecastCacheDiskRecord
	if json.Unmarshal(value, &record) != nil || record.SchemaVersion != 1 || record.ModelGeneration != 1 || record.Key.Year <= 0 || record.Key.Month < 1 || record.Key.Month > 12 || record.Key.SSN <= 0 || record.StoredAtUnixNS <= 0 || record.WindowUnixNS <= 0 || len(record.Records) < 1 || len(record.Records) > 24 {
		return voacapCacheKey{}, false
	}
	key, ok := record.Key.cacheKey()
	if !ok {
		return voacapCacheKey{}, false
	}
	canonical := fmt.Sprintf("%s%04x/%04x/%s/%06d/%04d/%02d/%04d/%s", voacapForecastCacheLegacyKeyPrefix, record.Key.UserCell, record.Key.DXCell, key.band, key.frequencyKHz, record.Key.Year, record.Key.Month, record.Key.SSN, key.directionName())
	if string(dbKey) != canonical {
		return voacapCacheKey{}, false
	}
	start := time.Unix(0, record.WindowUnixNS).UTC()
	stored := time.Unix(0, record.StoredAtUnixNS).UTC()
	end := start.Add(8 * time.Hour)
	monthEnd := time.Date(start.Year(), start.Month()+1, 1, 0, 0, 0, 0, time.UTC)
	if monthEnd.Before(end) {
		end = monthEnd
	}
	if start.Year() != record.Key.Year || int(start.Month()) != record.Key.Month || !start.Equal(start.Truncate(time.Hour)) || now.Before(start) || !now.Before(end) || stored.After(now.Add(voacapForecastCacheFutureStoredSkew)) || now.Sub(stored) > 8*time.Hour {
		return voacapCacheKey{}, false
	}
	var mask uint32
	for _, row := range record.Records {
		if row.HourUTC < 0 || row.HourUTC > 23 || mask&(1<<uint(row.HourUTC)) != 0 || row.FrequencyMHz <= 0 || math.IsNaN(row.FrequencyMHz) || math.IsInf(row.FrequencyMHz, 0) {
			return voacapCacheKey{}, false
		}
		mask |= 1 << uint(row.HourUTC)
	}
	return key, true
}

func normalizeVOACAPForecastCacheNow(now time.Time) time.Time {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return now.UTC()
}
