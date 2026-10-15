package pathreliability

import (
	"encoding/json"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// TestVOACAPDailyForecastRetainedHeap measures the complete retained hydration
// shape: 50,000 normal keys, each with 24 hourly rows, owned by a live fallback
// and its attached store. Keep this fixture identical in S1 and the candidate.
// The external paired comparison applies the 20 MiB additional-heap limit.
func TestVOACAPDailyForecastRetainedHeap(t *testing.T) {
	output := os.Getenv("GOCLUSTER_VOACAP_HEAP_OUTPUT")
	if output == "" {
		t.Skip("paired daily-forecast retained-heap measurement is opt-in")
	}
	const entries = 50000
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.MaxCacheEntries = entries
	cfg.VOACAPFallback.ForecastHours = 24
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	seedVOACAPDailyRetentionFixture(t, store, now, entries)
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	attachVOACAPDailyRetentionFixture(t, fallback, store, now, entries)
	// The attach frame has returned, so loaded-entry scratch cannot inflate the
	// result. The fallback keeps every forecast alive through this collection.
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(fallback)
	report := struct {
		Entries int    `json:"entries"`
		Before  uint64 `json:"before_bytes"`
		After   uint64 `json:"after_bytes"`
		Delta   int64  `json:"delta_bytes"`
	}{entries, before.HeapAlloc, after.HeapAlloc, int64(after.HeapAlloc) - int64(before.HeapAlloc)}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("50000-key, 24-row live hydration retained heap delta: %d bytes", report.Delta)
}

func TestVOACAPForecastHydrationCompactsDailyPayload(t *testing.T) {
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.ForecastHours = 24
	store := openTestVOACAPForecastCacheStore(t)
	defer closeTestVOACAPForecastCacheStore(t, store)
	seedVOACAPDailyRetentionFixture(t, store, now, 1)
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
	attachVOACAPDailyRetentionFixture(t, fallback, store, now, 1)
	key := testVOACAPForecastCacheKey(now, 1)
	fallback.mu.Lock()
	got := fallback.cache[key].forecast.Records
	fallback.mu.Unlock()
	want := voacapDailyRetentionRecords(now)
	if len(got) != 24 || cap(got) > 25 {
		t.Fatalf("hydrated daily payload len=%d cap=%d, want 24 rows with at most one spare slot", len(got), cap(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("hydrated row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	// A full slice expression can fake a small cap while retaining a larger backing
	// array. The opt-in paired heap measurement above checks physical retention.
}

func attachVOACAPDailyRetentionFixture(t *testing.T, fallback *VOACAPClosedFallback, store *VOACAPForecastCacheStore, now time.Time, entries int) {
	t.Helper()
	stats, err := fallback.AttachForecastCacheStore(store, now)
	if err != nil || stats.Loaded != entries || stats.Pruned != 0 || stats.SSNUnavailable {
		t.Fatalf("daily fixture hydration: stats=%+v error=%v, want %d loaded", stats, err, entries)
	}
	fallback.mu.Lock()
	defer fallback.mu.Unlock()
	if len(fallback.cache) != entries {
		t.Fatalf("daily fixture retained %d forecasts, want %d", len(fallback.cache), entries)
	}
	for key, entry := range fallback.cache {
		if len(entry.forecast.Records) != 24 {
			t.Fatalf("daily fixture key %+v retained %d hourly rows, want 24", key, len(entry.forecast.Records))
		}
	}
}

func seedVOACAPDailyRetentionFixture(t *testing.T, store *VOACAPForecastCacheStore, now time.Time, entries int) {
	t.Helper()
	entry := voacapCacheEntry{
		storedAt: now.Add(-time.Minute),
		forecast: VOACAPClosedForecast{WindowStartUTC: now, Records: voacapDailyRetentionRecords(now)},
	}
	// Bound setup scratch as well: a full-day JSON corpus is much larger than
	// the one-row fixture, and one giant batch would confound retained memory.
	const chunk = 256
	for first := 1; first <= entries; first += chunk {
		batch := store.db.NewBatch()
		for i := first; i <= min(first+chunk-1, entries); i++ {
			key := testVOACAPForecastCacheKey(now, CellID(i))
			value, err := json.Marshal(diskRecordFromForecastCache(key, entry))
			if err != nil {
				_ = batch.Close()
				t.Fatal(err)
			}
			if err := batch.Set(voacapForecastCacheKeyBytes(key), value, nil); err != nil {
				_ = batch.Close()
				t.Fatal(err)
			}
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			_ = batch.Close()
			t.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Compact([]byte(voacapForecastCacheEntryKeyPrefix), voacapForecastCacheEntryKeyUpperBound(), true); err != nil {
		t.Fatal(err)
	}
}

func voacapDailyRetentionRecords(now time.Time) []VOACAPHourlyForecast {
	records := make([]VOACAPHourlyForecast, 24)
	for i := range records {
		hour := (now.Hour() + i) % 24
		snr := hour - 30
		records[i] = VOACAPHourlyForecast{
			FT8SNRDB: snr, VOACAPSNRDBHz: snr + 30, HourUTC: hour,
			FrequencyMHz:      14.1,
			ReqSNRReliability: float64(hour+1) / 32, HasReqSNRReliability: true,
			ReceiveFT8SNRDB: snr + 1, TransmitFT8SNRDB: snr - 1,
			ReceiveVOACAPSNRDBHz: snr + 31, TransmitVOACAPSNRDBHz: snr + 29,
			ReceiveReqSNRReliability: float64(hour+2) / 32, TransmitReqSNRReliability: float64(hour) / 32,
			HasDirectionalReqSNRReliability: true, HasDirectionalSNR: true,
		}
	}
	return records
}
