package pathreliability

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestVOACAPForecastCacheRetentionStartupCanonicalTies(t *testing.T) {
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries = 12
	for name, order := range voacapRetentionInsertionOrders() {
		t.Run(name, func(t *testing.T) {
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			writeVOACAPRetentionFixture(t, store, cfg, now, order, false)
			if records := readRawVOACAPRetentionRecords(t, store); len(records) != 24 {
				t.Fatalf("raw fixture has %d entries, want 24 before hydration", len(records))
			}

			loaded, stats, err := store.loadCurrent(now, 112, cfg)
			if err != nil {
				t.Fatalf("loadCurrent() error: %v", err)
			}
			if stats.Loaded != 12 || stats.Pruned != 12 || stats.Overflow != 12 || stats.Invalid != 0 || stats.Expired != 0 || stats.StaleGeneration != 0 || stats.StaleWindow != 0 {
				t.Fatalf("unexpected first hydration stats: %+v", stats)
			}
			assertVOACAPRetentionLoadedCells(t, loaded, []CellID{3, 6, 9, 12, 15, 18, 21, 24, 2, 5, 8, 11})
			assertRawVOACAPRetentionKeys(t, store, voacapRetentionExpectedKeys())

			// Reopening must retain the same disk result before hydration can repair it.
			reopened := reopenVOACAPRetentionStore(t, store)
			assertRawVOACAPRetentionKeys(t, reopened, voacapRetentionExpectedKeys())
			assertVOACAPRetentionHydration(t, reopened, cfg, now, []CellID{3, 6, 9, 12, 15, 18, 21, 24, 2, 5, 8, 11})
		})
	}
}

func TestVOACAPForecastCacheRetentionRuntimeCanonicalTies(t *testing.T) {
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries = 12
	for name, order := range voacapRetentionInsertionOrders() {
		t.Run(name, func(t *testing.T) {
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			writeVOACAPRetentionFixture(t, store, cfg, now, order, true)

			// This read uses Pebble directly: loadCurrent must not mask a write-time defect.
			assertRawVOACAPRetentionKeys(t, store, voacapRetentionExpectedKeys())
			reopened := reopenVOACAPRetentionStore(t, store)
			assertRawVOACAPRetentionKeys(t, reopened, voacapRetentionExpectedKeys())
			assertVOACAPRetentionHydration(t, reopened, cfg, now, []CellID{3, 6, 9, 12, 15, 18, 21, 24, 2, 5, 8, 11})
		})
	}
}

func TestVOACAPForecastCacheRetentionEqualTimestampOverflow(t *testing.T) {
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries = 8
	wantKeys := []string{
		"voacapfc/v1/e/0001/0001/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0002/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0003/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0004/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0005/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0006/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0007/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0008/20m/014100/2026/06/0112/bidirectional",
	}
	for _, route := range []string{"startup", "completion"} {
		t.Run(route, func(t *testing.T) {
			for name, order := range voacapRetentionInsertionOrders() {
				t.Run(name, func(t *testing.T) {
					store := openTestVOACAPForecastCacheStore(t)
					defer closeTestVOACAPForecastCacheStore(t, store)
					for _, cell := range order {
						key := testVOACAPForecastCacheKey(now, cell)
						entry := testVOACAPForecastCacheEntry(now, -int(cell), 20)
						if route == "completion" {
							if err := store.storeForecast(key, entry, cfg, now); err != nil {
								t.Fatalf("storeForecast(cell=%d) error: %v", cell, err)
							}
						} else {
							writeTestVOACAPForecastCacheRecord(t, store, key, entry, nil)
						}
					}
					if route == "startup" {
						loaded, stats, err := store.loadCurrent(now, 112, cfg)
						if err != nil || stats.Loaded != 8 || stats.Pruned != 16 || stats.Overflow != 16 {
							t.Fatalf("initial hydration: stats=%+v error=%v", stats, err)
						}
						assertVOACAPRetentionLoadedCells(t, loaded, []CellID{1, 2, 3, 4, 5, 6, 7, 8})
					}
					assertRawVOACAPRetentionKeys(t, store, wantKeys)
					reopened := reopenVOACAPRetentionStore(t, store)
					assertRawVOACAPRetentionKeys(t, reopened, wantKeys)
					assertVOACAPRetentionHydration(t, reopened, cfg, now, []CellID{1, 2, 3, 4, 5, 6, 7, 8})
				})
			}
		})
	}
}

func TestVOACAPForecastCacheRetentionRefreshAndReinsert(t *testing.T) {
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	cfg.MaxCacheEntries = 12
	tests := []struct {
		name       string
		reinsert   bool
		refreshAt  time.Time
		wantCells  []CellID
		wantKeys   []string
		wantEleven bool
	}{
		{
			name:      "same_time_refresh",
			refreshAt: now.Add(-2 * time.Minute),
			wantCells: []CellID{3, 6, 9, 12, 15, 18, 21, 24, 2, 5, 8, 10},
			wantKeys:  voacapRetentionExpectedKeysWithTen(),
		},
		{
			name:       "newer_refresh",
			refreshAt:  now.Add(-time.Minute + time.Nanosecond),
			wantCells:  []CellID{11, 3, 6, 9, 12, 15, 18, 21, 24, 2, 5, 8},
			wantKeys:   voacapRetentionExpectedKeys(),
			wantEleven: true,
		},
		{
			name:      "same_time_reinsert",
			reinsert:  true,
			refreshAt: now.Add(-2 * time.Minute),
			wantCells: []CellID{3, 6, 9, 12, 15, 18, 21, 24, 2, 5, 8, 10},
			wantKeys:  voacapRetentionExpectedKeysWithTen(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := openTestVOACAPForecastCacheStore(t)
			defer closeTestVOACAPForecastCacheStore(t, store)
			writeVOACAPRetentionFixture(t, store, cfg, now, voacapRetentionInsertionOrders()["ascending"], false)
			if _, _, err := store.loadCurrent(now, 112, cfg); err != nil {
				t.Fatalf("initial loadCurrent() error: %v", err)
			}
			assertRawVOACAPRetentionKeys(t, store, voacapRetentionExpectedKeys())

			if tc.reinsert {
				deleteRawVOACAPRetentionKey(t, store, voacapRetentionElevenKey)
				// Restart after the explicit deletion so no future index can be stale.
				store = reopenVOACAPRetentionStore(t, store)
				loaded, stats, err := store.loadCurrent(now, 112, cfg)
				if err != nil || len(loaded) != 11 || stats.Pruned != 0 {
					t.Fatalf("hydrate deletion: loaded=%d stats=%+v error=%v", len(loaded), stats, err)
				}
				storeVOACAPRetentionEntry(t, store, cfg, now, 10, now.Add(-2*time.Minute), -77)
				assertRawVOACAPRetentionKeys(t, store, tc.wantKeys)
			}

			storeVOACAPRetentionEntry(t, store, cfg, now, 11, tc.refreshAt, -88)
			if !tc.reinsert {
				records := assertRawVOACAPRetentionKeys(t, store, voacapRetentionExpectedKeys())
				assertVOACAPRetentionRawForecast(t, records, voacapRetentionElevenKey, tc.refreshAt, -88)
				storeVOACAPRetentionEntry(t, store, cfg, now, 10, now.Add(-2*time.Minute), -77)
			}

			records := assertRawVOACAPRetentionKeys(t, store, tc.wantKeys)
			if tc.wantEleven {
				assertVOACAPRetentionRawForecast(t, records, voacapRetentionElevenKey, tc.refreshAt, -88)
			} else {
				assertVOACAPRetentionRawForecast(t, records, voacapRetentionTenKey, now.Add(-2*time.Minute), -77)
			}
			reopened := reopenVOACAPRetentionStore(t, store)
			assertRawVOACAPRetentionKeys(t, reopened, tc.wantKeys)
			assertVOACAPRetentionHydration(t, reopened, cfg, now, tc.wantCells)
		})
	}
}

func TestVOACAPClosedFallbackWaitFencesBlockedPersistence(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.DelaySeconds = 0
	cfg.VOACAPFallback.ForecastHours = 2
	now := time.Now().UTC()
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			return VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -20, HourUTC: job.WindowStartUTC.Hour(), FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -18, HourUTC: job.WindowStartUTC.Add(time.Hour).Hour(), FrequencyMHz: job.FrequencyMHz},
				},
			}, nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	if _, err := fallback.AttachForecastCacheStore(store, now); err != nil {
		t.Fatalf("AttachForecastCacheStore() error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	store.mu.Lock()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(store.mu.Unlock) }
	// Release the persistence lock before shutdown even when an assertion fails.
	defer func() {
		release()
		cancel()
		fallback.Wait()
	}()

	window, status := fallback.CheckForecastWindowWait(testClosedRequest(), now, time.Second)
	if status != VOACAPForecastWindowReady || len(window.Records) != 2 || fallback.Snapshot().CacheEntries != 1 {
		t.Fatalf("forecast must be visible while persistence is blocked: status=%v window=%+v snapshot=%+v", status, window, fallback.Snapshot())
	}
	cancel()
	waitEntered := make(chan struct{})
	waitDone := make(chan struct{})
	go func() {
		close(waitEntered)
		fallback.Wait()
		close(waitDone)
	}()
	select {
	case <-waitEntered:
	case <-time.After(time.Second):
		t.Fatal("Wait goroutine did not start")
	}
	select {
	case <-waitDone:
		t.Fatal("Wait returned while forecast persistence was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after forecast persistence was released")
	}
	records := readRawVOACAPRetentionRecords(t, store)
	key := fallback.cacheKey(testClosedRequest(), 112, now)
	record, ok := records[string(voacapForecastCacheKeyBytes(key))]
	if !ok || len(records) != 1 || len(record.Records) != 2 || record.Records[0].FT8SNRDB != -20 || record.Records[1].FT8SNRDB != -18 {
		t.Fatalf("raw persisted forecast missing after Wait, before hydration: %+v", records)
	}
}

const (
	voacapRetentionTenKey    = "voacapfc/v1/e/0001/000a/20m/014100/2026/06/0112/bidirectional"
	voacapRetentionElevenKey = "voacapfc/v1/e/0001/000b/20m/014100/2026/06/0112/bidirectional"
)

// These literal key sets are an oracle for byte ordering, independent of the
// production key formatter and retention comparator.
func voacapRetentionExpectedKeys() []string {
	return []string{
		"voacapfc/v1/e/0001/0002/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0003/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0005/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0006/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0008/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0009/20m/014100/2026/06/0112/bidirectional",
		voacapRetentionElevenKey,
		"voacapfc/v1/e/0001/000c/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/000f/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0012/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0015/20m/014100/2026/06/0112/bidirectional",
		"voacapfc/v1/e/0001/0018/20m/014100/2026/06/0112/bidirectional",
	}
}

func voacapRetentionExpectedKeysWithTen() []string {
	keys := voacapRetentionExpectedKeys()
	keys[6] = voacapRetentionTenKey
	return keys
}

func voacapRetentionInsertionOrders() map[string][]CellID {
	return map[string][]CellID{
		"ascending":  {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24},
		"descending": {24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		"mixed":      {24, 1, 14, 3, 22, 5, 21, 4, 11, 18, 7, 17, 15, 10, 2, 12, 13, 20, 9, 16, 8, 6, 19, 23},
	}
}

func writeVOACAPRetentionFixture(t *testing.T, store *VOACAPForecastCacheStore, cfg VOACAPFallbackConfig, now time.Time, order []CellID, completion bool) {
	t.Helper()
	for _, cell := range order {
		entry := testVOACAPForecastCacheEntry(now, -int(cell), 20)
		switch cell % 3 {
		case 0:
			entry.storedAt = now.Add(-time.Minute)
		case 1:
			entry.storedAt = now.Add(-3 * time.Minute)
		case 2:
			entry.storedAt = now.Add(-2 * time.Minute)
		}
		key := testVOACAPForecastCacheKey(now, cell)
		if completion {
			if err := store.storeForecast(key, entry, cfg, now); err != nil {
				t.Fatalf("storeForecast(cell=%d) error: %v", cell, err)
			}
		} else {
			writeTestVOACAPForecastCacheRecord(t, store, key, entry, nil)
		}
	}
}

func storeVOACAPRetentionEntry(t *testing.T, store *VOACAPForecastCacheStore, cfg VOACAPFallbackConfig, now time.Time, cell CellID, storedAt time.Time, snr int) {
	t.Helper()
	entry := testVOACAPForecastCacheEntry(now, snr, 20)
	entry.storedAt = storedAt
	if err := store.storeForecast(testVOACAPForecastCacheKey(now, cell), entry, cfg, now); err != nil {
		t.Fatalf("storeForecast(cell=%d) error: %v", cell, err)
	}
}

func readRawVOACAPRetentionRecords(t *testing.T, store *VOACAPForecastCacheStore) map[string]voacapForecastCacheDiskRecord {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	iter, err := store.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(voacapForecastCacheEntryKeyPrefix),
		UpperBound: voacapForecastCacheEntryKeyUpperBound(),
	})
	if err != nil {
		t.Fatalf("raw cache iterator: %v", err)
	}
	defer iter.Close()
	records := make(map[string]voacapForecastCacheDiskRecord)
	for iter.First(); iter.Valid(); iter.Next() {
		var record voacapForecastCacheDiskRecord
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			t.Fatalf("raw cache record %q: %v", iter.Key(), err)
		}
		records[string(iter.Key())] = record
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("raw cache scan: %v", err)
	}
	return records
}

func assertRawVOACAPRetentionKeys(t *testing.T, store *VOACAPForecastCacheStore, want []string) map[string]voacapForecastCacheDiskRecord {
	t.Helper()
	records := readRawVOACAPRetentionRecords(t, store)
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	if !slices.Equal(keys, want) {
		t.Fatalf("raw retained keys = %q, want %q", keys, want)
	}
	return records
}

func assertVOACAPRetentionRawForecast(t *testing.T, records map[string]voacapForecastCacheDiskRecord, key string, storedAt time.Time, snr int) {
	t.Helper()
	record, ok := records[key]
	if !ok || record.StoredAtUnixNS != storedAt.UnixNano() || len(record.Records) != 1 || record.Records[0].FT8SNRDB != snr {
		t.Fatalf("raw record %q = %+v, want storedAt=%s SNR=%d", key, record, storedAt, snr)
	}
}

func assertVOACAPRetentionLoadedCells(t *testing.T, loaded []voacapForecastCacheLoadedEntry, want []CellID) {
	t.Helper()
	cells := make([]CellID, len(loaded))
	for i, entry := range loaded {
		cells[i] = entry.key.dxCell
	}
	if !slices.Equal(cells, want) {
		t.Fatalf("loaded cells = %v, want %v in newest/canonical-key order", cells, want)
	}
}

func assertVOACAPRetentionHydration(t *testing.T, store *VOACAPForecastCacheStore, cfg VOACAPFallbackConfig, now time.Time, want []CellID) {
	t.Helper()
	loaded, stats, err := store.loadCurrent(now, 112, cfg)
	if err != nil {
		t.Fatalf("loadCurrent() error: %v", err)
	}
	if stats.Loaded != len(want) || stats.Pruned != 0 {
		t.Fatalf("restart hydration stats = %+v, want loaded=%d without repair", stats, len(want))
	}
	assertVOACAPRetentionLoadedCells(t, loaded, want)
}

func reopenVOACAPRetentionStore(t *testing.T, store *VOACAPForecastCacheStore) *VOACAPForecastCacheStore {
	t.Helper()
	path := store.Path()
	closeTestVOACAPForecastCacheStore(t, store)
	reopened, err := OpenVOACAPForecastCacheStore(path)
	if err != nil {
		t.Fatalf("reopen forecast cache: %v", err)
	}
	t.Cleanup(func() { closeTestVOACAPForecastCacheStore(t, reopened) })
	return reopened
}

func deleteRawVOACAPRetentionKey(t *testing.T, store *VOACAPForecastCacheStore, key string) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.db.Delete([]byte(key), pebble.NoSync); err != nil {
		t.Fatalf("raw cache deletion %q: %v", key, err)
	}
}
