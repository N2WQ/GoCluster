package pathreliability

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

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
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("voacap forecast cache: path is empty")
	}
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return nil, fmt.Errorf("voacap forecast cache: %s exists and is not a directory", path)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("voacap forecast cache: stat path: %w", err)
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("voacap forecast cache: mkdir: %w", err)
	}
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("voacap forecast cache: open: %w", err)
	}
	return &VOACAPForecastCacheStore{path: path, db: db}, nil
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
	if err := s.db.Set(voacapForecastCacheKeyBytes(key), value, pebble.NoSync); err != nil {
		return fmt.Errorf("voacap forecast cache: set: %w", err)
	}
	if _, _, err := s.scanCurrentLocked(now, key.ssn, cfg); err != nil {
		return err
	}
	return nil
}

func (s *VOACAPForecastCacheStore) scanCurrentLocked(now time.Time, currentSSN int, cfg VOACAPFallbackConfig) ([]voacapForecastCacheLoadedEntry, VOACAPForecastCacheRestoreStats, error) {
	var stats VOACAPForecastCacheRestoreStats
	if s == nil || s.db == nil {
		return nil, stats, nil
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(voacapForecastCacheEntryKeyPrefix),
		UpperBound: voacapForecastCacheEntryKeyUpperBound(),
	})
	if err != nil {
		return nil, stats, fmt.Errorf("voacap forecast cache: iterator: %w", err)
	}
	defer iter.Close()

	loaded := make([]voacapForecastCacheLoadedEntry, 0)
	deleteKeys := make([][]byte, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		dbKey := append([]byte(nil), iter.Key()...)
		key, entry, reason := decodeCurrentForecastCacheEntry(dbKey, iter.Value(), now, currentSSN, cfg)
		switch reason {
		case voacapForecastCacheRejectNone:
			loaded = append(loaded, voacapForecastCacheLoadedEntry{
				key:   key,
				entry: entry,
				dbKey: dbKey,
			})
		case voacapForecastCacheRejectExpired:
			stats.Expired++
			deleteKeys = append(deleteKeys, dbKey)
		case voacapForecastCacheRejectStaleGeneration:
			stats.StaleGeneration++
			deleteKeys = append(deleteKeys, dbKey)
		case voacapForecastCacheRejectStaleWindow:
			stats.StaleWindow++
			deleteKeys = append(deleteKeys, dbKey)
		default:
			stats.Invalid++
			deleteKeys = append(deleteKeys, dbKey)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, stats, fmt.Errorf("voacap forecast cache: scan: %w", err)
	}

	sort.Slice(loaded, func(i, j int) bool {
		return loaded[i].entry.storedAt.After(loaded[j].entry.storedAt)
	})
	if len(loaded) > cfg.MaxCacheEntries {
		for i := cfg.MaxCacheEntries; i < len(loaded); i++ {
			deleteKeys = append(deleteKeys, loaded[i].dbKey)
			stats.Overflow++
		}
		loaded = loaded[:cfg.MaxCacheEntries]
	}
	stats.Loaded = len(loaded)
	if len(deleteKeys) > 0 {
		if err := s.deleteKeysLocked(deleteKeys); err != nil {
			return nil, stats, err
		}
		stats.Pruned = len(deleteKeys)
	}
	return loaded, stats, nil
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
	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("voacap forecast cache: prune: %w", err)
	}
	return nil
}

func decodeCurrentForecastCacheEntry(dbKey, value []byte, now time.Time, currentSSN int, cfg VOACAPFallbackConfig) (voacapCacheKey, voacapCacheEntry, voacapForecastCacheRejectReason) {
	var record voacapForecastCacheDiskRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return voacapCacheKey{}, voacapCacheEntry{}, voacapForecastCacheRejectInvalid
	}
	if record.SchemaVersion != voacapForecastCacheSchemaVersion || record.ModelGeneration != voacapForecastCacheModelGeneration {
		return voacapCacheKey{}, voacapCacheEntry{}, voacapForecastCacheRejectStaleGeneration
	}
	key, ok := record.Key.cacheKey()
	if !ok {
		return voacapCacheKey{}, voacapCacheEntry{}, voacapForecastCacheRejectInvalid
	}
	if string(dbKey) != string(voacapForecastCacheKeyBytes(key)) {
		return voacapCacheKey{}, voacapCacheEntry{}, voacapForecastCacheRejectInvalid
	}
	if len(record.Records) == 0 || record.StoredAtUnixNS <= 0 || record.WindowUnixNS <= 0 {
		return voacapCacheKey{}, voacapCacheEntry{}, voacapForecastCacheRejectInvalid
	}
	entry := voacapCacheEntry{
		storedAt: time.Unix(0, record.StoredAtUnixNS).UTC(),
		forecast: VOACAPClosedForecast{
			WindowStartUTC: time.Unix(0, record.WindowUnixNS).UTC(),
			Records:        append([]VOACAPHourlyForecast(nil), record.Records...),
		},
	}
	if entry.storedAt.After(now.Add(voacapForecastCacheFutureStoredSkew)) {
		return voacapCacheKey{}, voacapCacheEntry{}, voacapForecastCacheRejectInvalid
	}
	if now.Sub(entry.storedAt) > time.Duration(cfg.CacheTTLSeconds)*time.Second {
		return voacapCacheKey{}, voacapCacheEntry{}, voacapForecastCacheRejectExpired
	}
	windowStart := forecastWindowStart(now)
	if key.ssn != currentSSN || key.year != windowStart.Year() || key.month != int(windowStart.Month()) {
		return voacapCacheKey{}, voacapCacheEntry{}, voacapForecastCacheRejectStaleWindow
	}
	if _, ok := forecastRecordForHour(entry.forecast, now, cfg.ForecastHours); !ok {
		return voacapCacheKey{}, voacapCacheEntry{}, voacapForecastCacheRejectStaleWindow
	}
	return key, entry, voacapForecastCacheRejectNone
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
