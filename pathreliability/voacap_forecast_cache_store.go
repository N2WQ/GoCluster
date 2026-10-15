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
	"strings"
	"sync"
	"time"

	"dxcluster/internal/pebbleutil"

	"github.com/cockroachdb/pebble"
)

const (
	voacapForecastCacheSchemaVersion    = 1
	voacapForecastCacheModelGeneration  = 1
	voacapForecastCacheEntryKeyPrefix   = "voacapfc/v1/e/"
	voacapForecastCacheFutureStoredSkew = time.Minute
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
	index        map[string]*voacapForecastCacheIndexEntry
	retention    voacapForecastCacheRetentionHeap
	eligibility  voacapForecastCacheEligibilityHeap
	indexContext voacapForecastCacheIndexContext
	indexReady   bool
	indexDirty   bool
	faults       *voacapForecastCacheFaults

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
	SchemaVersion   int                        `json:"schema_version"`
	ModelGeneration int                        `json:"model_generation"`
	Key             voacapForecastCacheDiskKey `json:"key"`
	StoredAtUnixNS  int64                      `json:"stored_at_unix_ns"`
	WindowUnixNS    int64                      `json:"window_unix_ns"`
	Records         []VOACAPHourlyForecast     `json:"records"`
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

func (s *VOACAPForecastCacheStore) loadCurrent(now time.Time, currentSSN int, cfg VOACAPFallbackConfig) ([]voacapForecastCacheLoadedEntry, VOACAPForecastCacheRestoreStats, error) {
	now = normalizeVOACAPForecastCacheNow(now)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanCurrentLocked(now, currentSSN, cfg)
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
	var record voacapForecastCacheDiskRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return voacapCacheKey{}, voacapCacheEntry{}, voacapForecastCacheRejectInvalid
	}
	key, reason := validateCurrentForecastCacheRecord(dbKey, record, now, currentSSN, cfg)
	if reason != voacapForecastCacheRejectNone {
		return voacapCacheKey{}, voacapCacheEntry{}, reason
	}
	// Hydrated forecasts can live for the cache lifetime. Do not retain the
	// JSON decoder's growth capacity (for example, beyond 24 hourly rows).
	records := make([]VOACAPHourlyForecast, len(record.Records))
	copy(records, record.Records)
	return key, voacapCacheEntry{
		storedAt: time.Unix(0, record.StoredAtUnixNS).UTC(),
		forecast: VOACAPClosedForecast{
			WindowStartUTC: time.Unix(0, record.WindowUnixNS).UTC(),
			Records:        records,
		},
	}, voacapForecastCacheRejectNone
}

// Both recovery decoding and ordinary writes use this predicate. In particular,
// record existence is exactly forecastRecordForHour's HourUTC equality test;
// persistence does not introduce new numerical forecast admission policy.
func validateCurrentForecastCacheRecord(dbKey []byte, record voacapForecastCacheDiskRecord, now time.Time, currentSSN int, cfg VOACAPFallbackConfig) (voacapCacheKey, voacapForecastCacheRejectReason) {
	if record.SchemaVersion != voacapForecastCacheSchemaVersion || record.ModelGeneration != voacapForecastCacheModelGeneration {
		return voacapCacheKey{}, voacapForecastCacheRejectStaleGeneration
	}
	key, ok := record.Key.cacheKey()
	if !ok {
		return voacapCacheKey{}, voacapForecastCacheRejectInvalid
	}
	if string(dbKey) != string(voacapForecastCacheKeyBytes(key)) {
		return voacapCacheKey{}, voacapForecastCacheRejectInvalid
	}
	if len(record.Records) == 0 || record.StoredAtUnixNS <= 0 || record.WindowUnixNS <= 0 {
		return voacapCacheKey{}, voacapForecastCacheRejectInvalid
	}
	entry := voacapCacheEntry{
		storedAt: time.Unix(0, record.StoredAtUnixNS).UTC(),
		forecast: VOACAPClosedForecast{
			WindowStartUTC: time.Unix(0, record.WindowUnixNS).UTC(),
			Records:        record.Records,
		},
	}
	if entry.storedAt.After(now.Add(voacapForecastCacheFutureStoredSkew)) {
		return voacapCacheKey{}, voacapForecastCacheRejectInvalid
	}
	if now.Sub(entry.storedAt) > time.Duration(cfg.CacheTTLSeconds)*time.Second {
		return voacapCacheKey{}, voacapForecastCacheRejectExpired
	}
	windowStart := forecastWindowStart(now)
	if key.ssn != currentSSN || key.year != windowStart.Year() || key.month != int(windowStart.Month()) {
		return voacapCacheKey{}, voacapForecastCacheRejectStaleWindow
	}
	if _, ok := forecastRecordForHour(entry.forecast, now, cfg.ForecastHours); !ok {
		return voacapCacheKey{}, voacapForecastCacheRejectStaleWindow
	}
	return key, voacapForecastCacheRejectNone
}

func diskRecordFromForecastCache(key voacapCacheKey, entry voacapCacheEntry) voacapForecastCacheDiskRecord {
	return voacapForecastCacheDiskRecord{
		SchemaVersion:   voacapForecastCacheSchemaVersion,
		ModelGeneration: voacapForecastCacheModelGeneration,
		Key:             diskKeyFromForecastCacheKey(key),
		StoredAtUnixNS:  entry.storedAt.UTC().UnixNano(),
		WindowUnixNS:    entry.forecast.WindowStartUTC.UTC().UnixNano(),
		Records:         append([]VOACAPHourlyForecast(nil), entry.forecast.Records...),
	}
}

func diskKeyFromForecastCacheKey(key voacapCacheKey) voacapForecastCacheDiskKey {
	return voacapForecastCacheDiskKey{
		UserCell:     uint16(key.userCell),
		DXCell:       uint16(key.dxCell),
		Band:         key.band,
		FrequencyKHz: key.frequencyKHz,
		Year:         key.year,
		Month:        key.month,
		SSN:          key.ssn,
		Direction:    key.direction,
	}
}

func (key voacapForecastCacheDiskKey) cacheKey() (voacapCacheKey, bool) {
	band := normalizeBand(key.Band)
	if key.UserCell == 0 || key.DXCell == 0 || band == "" || key.FrequencyKHz <= 0 || key.Year <= 0 || key.Month < 1 || key.Month > 12 || key.SSN <= 0 {
		return voacapCacheKey{}, false
	}
	direction := strings.TrimSpace(key.Direction)
	if direction != voacapDirectionBidirectional {
		return voacapCacheKey{}, false
	}
	return voacapCacheKey{
		userCell:     CellID(key.UserCell),
		dxCell:       CellID(key.DXCell),
		band:         band,
		frequencyKHz: key.FrequencyKHz,
		year:         key.Year,
		month:        key.Month,
		ssn:          key.SSN,
		direction:    direction,
	}, true
}

func voacapForecastCacheKeyBytes(key voacapCacheKey) []byte {
	return []byte(fmt.Sprintf("%s%04x/%04x/%s/%06d/%04d/%02d/%04d/%s",
		voacapForecastCacheEntryKeyPrefix,
		uint16(key.userCell),
		uint16(key.dxCell),
		key.band,
		key.frequencyKHz,
		key.year,
		key.month,
		key.ssn,
		key.direction,
	))
}

func voacapForecastCacheEntryKeyUpperBound() []byte {
	upper := append([]byte(voacapForecastCacheEntryKeyPrefix), 0xff)
	return upper
}

func normalizeVOACAPForecastCacheNow(now time.Time) time.Time {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return now.UTC()
}
