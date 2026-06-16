package pathreliability

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestVOACAPForecastCacheStoreRoundTripCurrentForecast(t *testing.T) {
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)

	key := testVOACAPForecastCacheKey(now, 1)
	entry := testVOACAPForecastCacheEntry(now, -14, 20)
	if err := store.storeForecast(key, entry, cfg, now); err != nil {
		t.Fatalf("storeForecast() error: %v", err)
	}

	loaded, stats, err := store.loadCurrent(now, 112, cfg)
	if err != nil {
		t.Fatalf("loadCurrent() error: %v", err)
	}
	if stats.Loaded != 1 || stats.Pruned != 0 || len(loaded) != 1 {
		t.Fatalf("unexpected restore stats=%+v loaded=%d", stats, len(loaded))
	}
	got := loaded[0]
	if got.key != key {
		t.Fatalf("restored key = %+v, want %+v", got.key, key)
	}
	if !got.entry.storedAt.Equal(entry.storedAt) {
		t.Fatalf("storedAt = %s, want %s", got.entry.storedAt, entry.storedAt)
	}
	if len(got.entry.forecast.Records) != 1 || got.entry.forecast.Records[0].FT8SNRDB != -14 {
		t.Fatalf("unexpected restored forecast: %+v", got.entry.forecast)
	}
	if got.entry.forecast.OutputPath != "" || got.entry.forecast.Elapsed != 0 {
		t.Fatalf("runtime output metadata must not be persisted: %+v", got.entry.forecast)
	}
}

func TestVOACAPForecastCacheStorePrunesNonCurrentRecords(t *testing.T) {
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)

	writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 1), testVOACAPForecastCacheEntry(now, -14, 20), nil)
	writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 2), testVOACAPForecastCacheEntry(now.Add(-2*time.Hour), -15, 20), nil)
	writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 3), testVOACAPForecastCacheEntry(now, -16, 21), nil)
	writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 4), testVOACAPForecastCacheEntry(now, -17, 20), func(record *voacapForecastCacheDiskRecord) {
		record.ModelGeneration = 0
	})
	writeTestVOACAPForecastCacheRawValue(t, store, []byte(voacapForecastCacheEntryKeyPrefix+"bad"), []byte("{"))

	loaded, stats, err := store.loadCurrent(now, 112, cfg)
	if err != nil {
		t.Fatalf("loadCurrent() error: %v", err)
	}
	if len(loaded) != 1 || stats.Loaded != 1 {
		t.Fatalf("loaded=%d stats=%+v, want one current record", len(loaded), stats)
	}
	if stats.Expired != 1 || stats.StaleWindow != 1 || stats.StaleGeneration != 1 || stats.Invalid != 1 || stats.Pruned != 4 {
		t.Fatalf("unexpected prune stats: %+v", stats)
	}

	_, stats, err = store.loadCurrent(now, 112, cfg)
	if err != nil {
		t.Fatalf("second loadCurrent() error: %v", err)
	}
	if stats.Pruned != 0 || stats.Loaded != 1 {
		t.Fatalf("second load should see only the retained current record, stats=%+v", stats)
	}
}

func TestVOACAPForecastCacheStoreEnforcesMaxEntries(t *testing.T) {
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries = 2
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)

	writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 1), testVOACAPForecastCacheEntry(now.Add(-3*time.Minute), -14, 20), nil)
	writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 2), testVOACAPForecastCacheEntry(now.Add(-2*time.Minute), -15, 20), nil)
	writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 3), testVOACAPForecastCacheEntry(now.Add(-time.Minute), -16, 20), nil)

	loaded, stats, err := store.loadCurrent(now, 112, cfg)
	if err != nil {
		t.Fatalf("loadCurrent() error: %v", err)
	}
	if len(loaded) != 2 || stats.Overflow != 1 || stats.Pruned != 1 {
		t.Fatalf("unexpected overflow stats=%+v loaded=%d", stats, len(loaded))
	}
	if loaded[0].key.dxCell != 3 || loaded[1].key.dxCell != 2 {
		t.Fatalf("loaded entries should keep newest first, got %+v", loaded)
	}
}

func TestOpenVOACAPForecastCacheStoreRejectsEmptyPath(t *testing.T) {
	store, err := OpenVOACAPForecastCacheStore(" \t ")
	if err == nil {
		_ = store.Close()
		t.Fatalf("OpenVOACAPForecastCacheStore() succeeded, want empty-path error")
	}
	if err.Error() != "voacap forecast cache: path is empty" {
		t.Fatalf("OpenVOACAPForecastCacheStore() error = %v", err)
	}
}

func TestOpenVOACAPForecastCacheStoreRejectsNonDirectory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "voacap-forecast-cache")
	if err := os.WriteFile(dbPath, []byte("not a pebble db"), 0o644); err != nil {
		t.Fatalf("write db path file: %v", err)
	}

	store, err := OpenVOACAPForecastCacheStore(dbPath)
	if err == nil {
		_ = store.Close()
		t.Fatalf("OpenVOACAPForecastCacheStore() succeeded, want non-directory error")
	}
	if !strings.Contains(err.Error(), "exists and is not a directory") {
		t.Fatalf("OpenVOACAPForecastCacheStore() error = %v, want non-directory error", err)
	}
}

func openTestVOACAPForecastCacheStore(t *testing.T) *VOACAPForecastCacheStore {
	t.Helper()
	store, err := OpenVOACAPForecastCacheStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenVOACAPForecastCacheStore() error: %v", err)
	}
	return store
}

func closeTestVOACAPForecastCacheStore(t *testing.T, store *VOACAPForecastCacheStore) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func testVOACAPForecastCacheKey(now time.Time, dxCell CellID) voacapCacheKey {
	windowStart := forecastWindowStart(now)
	return voacapCacheKey{
		userCell:     1,
		dxCell:       dxCell,
		band:         "20m",
		frequencyKHz: 14100,
		year:         windowStart.Year(),
		month:        int(windowStart.Month()),
		ssn:          112,
		direction:    voacapDirectionBidirectional,
	}
}

func testVOACAPForecastCacheEntry(now time.Time, snr int, hour int) voacapCacheEntry {
	windowStart := forecastWindowStart(now)
	return voacapCacheEntry{
		storedAt: now.Add(-time.Minute).UTC(),
		forecast: VOACAPClosedForecast{
			WindowStartUTC: windowStart,
			Records: []VOACAPHourlyForecast{
				{
					FT8SNRDB:                        snr,
					VOACAPSNRDBHz:                   snr + 30,
					HourUTC:                         hour,
					FrequencyMHz:                    14.1,
					ReqSNRReliability:               0.82,
					HasReqSNRReliability:            true,
					ReceiveFT8SNRDB:                 snr,
					TransmitFT8SNRDB:                snr - 1,
					ReceiveVOACAPSNRDBHz:            snr + 30,
					TransmitVOACAPSNRDBHz:           snr + 29,
					ReceiveReqSNRReliability:        0.82,
					TransmitReqSNRReliability:       0.78,
					HasDirectionalReqSNRReliability: true,
					HasDirectionalSNR:               true,
				},
			},
			OutputPath: "runtime-output-should-not-persist.out",
			Elapsed:    2 * time.Second,
		},
	}
}

func writeTestVOACAPForecastCacheRecord(t *testing.T, store *VOACAPForecastCacheStore, key voacapCacheKey, entry voacapCacheEntry, mutate func(*voacapForecastCacheDiskRecord)) {
	t.Helper()
	record := diskRecordFromForecastCache(key, entry)
	if mutate != nil {
		mutate(&record)
	}
	value, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal disk record: %v", err)
	}
	writeTestVOACAPForecastCacheRawValue(t, store, voacapForecastCacheKeyBytes(key), value)
}

func writeTestVOACAPForecastCacheRawValue(t *testing.T, store *VOACAPForecastCacheStore, key, value []byte) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.db.Set(key, value, pebble.NoSync); err != nil {
		t.Fatalf("write raw cache record: %v", err)
	}
}
