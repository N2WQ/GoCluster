package pathreliability

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestVOACAPClosedFallbackDelaysEnqueuesAndReturnsCachedClosed(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.CacheTTLSeconds = 8 * 3600
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return testCompleteForecast(job, cfg.VOACAPFallback.ForecastHours, VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -34, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
				},
			}), nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		cancel()
		fallback.Wait()
	}()

	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	fallback.now = func() time.Time { return now }
	req := testClosedRequest()
	if _, ok := fallback.CheckClosed(req, now); ok {
		t.Fatalf("first insufficient lookup should only start the delay window")
	}
	if calls.Load() != 0 {
		t.Fatalf("forecaster called before delay elapsed")
	}
	if _, ok := fallback.CheckClosed(req, now.Add(14*time.Minute)); ok {
		t.Fatalf("lookup before delay elapsed should not return a result")
	}
	if _, ok := fallback.CheckClosed(req, now.Add(15*time.Minute)); ok {
		t.Fatalf("enqueue lookup should not synchronously return a result")
	}
	waitUntil(t, func() bool { return calls.Load() == 1 && fallback.Snapshot().CacheEntries == 1 })

	res, ok := fallback.CheckClosed(req, now.Add(16*time.Minute))
	if !ok {
		t.Fatalf("expected cached closed result")
	}
	if res.Source != SourceVOACAPClosed || res.Glyph != cfg.GlyphSymbols.Closed || res.Class != classUnlikely {
		t.Fatalf("unexpected cached result: %+v", res)
	}
	if res.VOACAPFT8SNRDB != -34 || res.VOACAPSSN != 112 || res.VOACAPHourUTC != 20 || res.VOACAPFrequencyMHz != 14.1 {
		t.Fatalf("unexpected VOACAP diagnostics: %+v", res)
	}
}

func TestVOACAPClosedFallbackCachesOpenVerdictWithoutReturningGlyph(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.CacheTTLSeconds = 8 * 3600
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return testCompleteForecast(job, cfg.VOACAPFallback.ForecastHours, VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -20, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
				},
			}), nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		cancel()
		fallback.Wait()
	}()

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	fallback.now = func() time.Time { return now }
	if _, ok := fallback.CheckClosed(req, now); ok {
		t.Fatalf("open enqueue should not synchronously return a result")
	}
	waitUntil(t, func() bool { return calls.Load() == 1 && fallback.Snapshot().CacheEntries == 1 })
	if _, ok := fallback.CheckClosed(req, now.Add(time.Second)); ok {
		t.Fatalf("open VOACAP verdict must not replace insufficient bucket result")
	}
	forecast, ok := fallback.CheckForecast(req, now.Add(time.Second))
	if !ok {
		t.Fatalf("expected cached open current-hour forecast")
	}
	if forecast.Record.FT8SNRDB != -20 || forecast.Record.HourUTC != 20 || forecast.SSN != 112 {
		t.Fatalf("unexpected cached open forecast: %+v", forecast)
	}
	if calls.Load() != 1 {
		t.Fatalf("open cached verdict should avoid duplicate forecast, calls=%d", calls.Load())
	}
}

func TestVOACAPClosedFallbackCheckCachedForecastDoesNotMutateOnMiss(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	var calls atomic.Int32
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{
		fn: func(context.Context, VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return VOACAPClosedForecast{}, nil
		},
	}, fixedSSNProvider{ssn: 112})

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	if _, ok := fallback.CheckCachedForecast(req, now); ok {
		t.Fatalf("empty cache should not return a forecast")
	}
	if calls.Load() != 0 {
		t.Fatalf("cache-only lookup must not call forecaster, calls=%d", calls.Load())
	}
	snap := fallback.Snapshot()
	if snap.DelayEntries != 0 || snap.InflightEntries != 0 || snap.QueueDepth != 0 {
		t.Fatalf("cache-only miss mutated fallback state: %+v", snap)
	}
	if stats := fallback.StatsSnapshot(); stats.HasActivity() {
		t.Fatalf("cache-only miss mutated fallback stats: %+v", stats)
	}

	key := fallback.cacheKey(req)
	fallback.cache[key] = testCacheEntryWithConfig(voacapCacheEntry{
		forecast: VOACAPClosedForecast{
			WindowStartUTC: forecastWindowStart(now),
			Records: []VOACAPHourlyForecast{
				{FT8SNRDB: -15, HourUTC: 20, FrequencyMHz: 14.1},
			},
		},
		storedAt: now.Add(-time.Minute),
	}, cfg)
	forecast, ok := fallback.CheckCachedForecast(req, now)
	if !ok {
		t.Fatalf("expected existing current-hour cache hit")
	}
	if forecast.Record.FT8SNRDB != -15 || forecast.Record.HourUTC != 20 || forecast.SSN != 112 {
		t.Fatalf("unexpected cache-only forecast: %+v", forecast)
	}
	if stats := fallback.StatsSnapshot(); stats.HasActivity() {
		t.Fatalf("cache-only hit mutated fallback stats: %+v", stats)
	}

	fallback.cache[key] = testCacheEntryWithConfig(voacapCacheEntry{
		forecast: VOACAPClosedForecast{
			WindowStartUTC: forecastWindowStart(now),
			Records: []VOACAPHourlyForecast{
				{FT8SNRDB: -15, HourUTC: 20, FrequencyMHz: 14.1},
			},
		},
		storedAt: now.Add(-2 * time.Hour),
	}, cfg)
	if _, ok := fallback.CheckCachedForecast(req, now); ok {
		t.Fatalf("expired cache entry should not return a forecast")
	}
	if snap := fallback.Snapshot(); snap.CacheEntries != 1 || snap.DelayEntries != 0 || snap.InflightEntries != 0 || snap.QueueDepth != 0 {
		t.Fatalf("cache-only expired miss should not prune or enqueue: %+v", snap)
	}
}

func TestVOACAPClosedFallbackCheckForecastWindowReturnsRemainingDirectionalRows(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.MergeReceiveWeight = 0.6
	cfg.MergeTransmitWeight = 0.4
	cfg.VOACAPFallback.CacheTTLSeconds = int((4 * time.Hour).Seconds())
	cfg.VOACAPFallback.ForecastHours = 4
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})

	req := testClosedRequest()
	req.ReceiveNoisePenaltyDB = 5
	windowStart := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	key := fallback.cacheKey(req)
	fallback.mu.Lock()
	fallback.addCacheLocked(key, testCacheEntryWithConfig(voacapCacheEntry{
		forecast: VOACAPClosedForecast{
			WindowStartUTC: windowStart,
			Records: []VOACAPHourlyForecast{
				{HourUTC: 20, FrequencyMHz: 14.1, ReceiveFT8SNRDB: -8, TransmitFT8SNRDB: -12, HasDirectionalSNR: true},
				{HourUTC: 21, FrequencyMHz: 14.1, ReceiveFT8SNRDB: -10, TransmitFT8SNRDB: -20, HasDirectionalSNR: true},
				{HourUTC: 22, FrequencyMHz: 14.1, ReceiveFT8SNRDB: -15, TransmitFT8SNRDB: -5, HasDirectionalSNR: true},
				{HourUTC: 23, FrequencyMHz: 14.1, ReceiveFT8SNRDB: -30, TransmitFT8SNRDB: -15, HasDirectionalSNR: true},
			},
		},
		storedAt: windowStart.Add(-time.Minute),
	}, cfg))
	fallback.mu.Unlock()

	window, ok := fallback.CheckForecastWindow(req, windowStart.Add(90*time.Minute))
	if !ok {
		t.Fatalf("expected cached forecast window")
	}
	if !window.WindowStartUTC.Equal(windowStart) {
		t.Fatalf("WindowStartUTC = %s, want %s", window.WindowStartUTC, windowStart)
	}
	if len(window.Records) != 3 {
		t.Fatalf("records = %d, want remaining 3: %+v", len(window.Records), window.Records)
	}
	first := window.Records[0]
	if first.Record.HourUTC != 21 {
		t.Fatalf("first returned hour = %d, want current hour 21", first.Record.HourUTC)
	}
	if got, want := first.ReceiveDB(), -15.0; got != want {
		t.Fatalf("receive DB = %v, want %v", got, want)
	}
	if got, want := first.TransmitDB(), -20.0; got != want {
		t.Fatalf("transmit DB = %v, want %v", got, want)
	}
	if got, want := first.EffectiveDB(), -17.0; got != want {
		t.Fatalf("effective DB = %v, want %v", got, want)
	}
	if first.Record.FT8SNRDB != -17 {
		t.Fatalf("rounded effective SNR = %d, want -17", first.Record.FT8SNRDB)
	}
	stats := fallback.StatsSnapshot()
	if stats.CacheHit != 1 {
		t.Fatalf("CacheHit = %d, want 1 in stats %+v", stats.CacheHit, stats)
	}
}

func TestVOACAPClosedFallbackCheckForecastWindowDelaysAndEnqueues(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.CacheTTLSeconds = 8 * 3600
	cfg.VOACAPFallback.ForecastHours = 2
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return testCompleteForecast(job, cfg.VOACAPFallback.ForecastHours, VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -20, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -18, HourUTC: 21, FrequencyMHz: job.FrequencyMHz},
				},
			}), nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		cancel()
		fallback.Wait()
	}()

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	fallback.now = func() time.Time { return now }
	if _, ok := fallback.CheckForecastWindow(req, now); ok {
		t.Fatalf("enqueue lookup should not synchronously return a window")
	}
	waitUntil(t, func() bool { return calls.Load() == 1 && fallback.Snapshot().CacheEntries == 1 })
	window, ok := fallback.CheckForecastWindow(req, now.Add(time.Second))
	if !ok {
		t.Fatalf("expected cached forecast window after background run")
	}
	if len(window.Records) != 2 {
		t.Fatalf("window records = %d, want 2: %+v", len(window.Records), window.Records)
	}
	if calls.Load() != 1 {
		t.Fatalf("cached window should avoid duplicate forecast, calls=%d", calls.Load())
	}
}

func TestVOACAPClosedFallbackRestoresPersistedCurrentForecastAndBypassesDelay(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.DelaySeconds = 900
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	store := openTestVOACAPForecastCacheStore(t)
	key := testVOACAPForecastCacheKey(now, 2)
	if err := store.storeForecast(key, testCacheEntryWithConfig(testVOACAPForecastCacheEntry(now, -31, 20), cfg.VOACAPFallback), cfg.VOACAPFallback, now); err != nil {
		t.Fatalf("storeForecast() error: %v", err)
	}

	var calls atomic.Int32
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{
		fn: func(context.Context, VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return VOACAPClosedForecast{}, nil
		},
	}, fixedSSNProvider{ssn: 112})
	defer func() {
		if err := fallback.CloseForecastCacheStore(); err != nil {
			t.Fatalf("CloseForecastCacheStore() error: %v", err)
		}
	}()
	stats, err := fallback.AttachForecastCacheStore(store, now)
	if err != nil {
		t.Fatalf("AttachForecastCacheStore() error: %v", err)
	}
	if stats.Loaded != 1 || stats.Pruned != 0 || stats.SSNUnavailable {
		t.Fatalf("unexpected restore stats: %+v", stats)
	}

	got := fallback.CheckForecastDetailed(testClosedRequest(), now)
	if got.Status != VOACAPForecastCheckReady || got.CacheMiss {
		t.Fatalf("restored current forecast should be immediately ready, got %+v", got)
	}
	if calls.Load() != 0 {
		t.Fatalf("restored cache hit should not call forecaster, calls=%d", calls.Load())
	}
	snap := fallback.Snapshot()
	if snap.DelayEntries != 0 || snap.InflightEntries != 0 || snap.QueueDepth != 0 {
		t.Fatalf("restored cache hit should bypass warm-up state, snapshot=%+v", snap)
	}
	fallbackStats := fallback.StatsSnapshot()
	if fallbackStats.CacheHit != 1 || fallbackStats.DelayWait != 0 || fallbackStats.Queued != 0 {
		t.Fatalf("restored cache hit should report only cache hit, stats=%+v", fallbackStats)
	}
}

func TestVOACAPClosedFallbackRetainsFuturePersistedRowsAndUsesDelay(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.DelaySeconds = 900
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	store := openTestVOACAPForecastCacheStore(t)
	stale := testVOACAPForecastCacheEntry(now, -31, 21)
	writeTestVOACAPForecastCacheRecord(t, store, testVOACAPForecastCacheKey(now, 2), testCacheEntryWithConfig(stale, cfg), nil)

	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
	defer func() {
		if err := fallback.CloseForecastCacheStore(); err != nil {
			t.Fatalf("CloseForecastCacheStore() error: %v", err)
		}
	}()
	stats, err := fallback.AttachForecastCacheStore(store, now)
	if err != nil {
		t.Fatalf("AttachForecastCacheStore() error: %v", err)
	}
	if stats.Loaded != 1 || stats.StaleWindow != 0 || stats.Pruned != 0 {
		t.Fatalf("temporally current rows should survive a current-hour hole, stats=%+v", stats)
	}

	got := fallback.CheckForecastDetailed(testClosedRequest(), now)
	if got.Status != VOACAPForecastCheckDelayWait || !got.CacheMiss {
		t.Fatalf("stale restored record should fall back to delay, got %+v", got)
	}
	if snap := fallback.Snapshot(); snap.DelayEntries != 1 || snap.CacheEntries != 1 {
		t.Fatalf("delay state after stale restore mismatch: %+v", snap)
	}
}

func TestVOACAPClosedFallbackRestoresWithoutCurrentSSN(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	store := openTestVOACAPForecastCacheStore(t)
	if err := store.storeForecast(testVOACAPForecastCacheKey(now, 2), testCacheEntryWithConfig(testVOACAPForecastCacheEntry(now, -31, 20), cfg.VOACAPFallback), cfg.VOACAPFallback, now); err != nil {
		t.Fatalf("storeForecast() error: %v", err)
	}

	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{})
	defer func() {
		if err := fallback.CloseForecastCacheStore(); err != nil {
			t.Fatalf("CloseForecastCacheStore() error: %v", err)
		}
	}()
	stats, err := fallback.AttachForecastCacheStore(store, now)
	if err != nil {
		t.Fatalf("AttachForecastCacheStore() error: %v", err)
	}
	if stats.SSNUnavailable || stats.Loaded != 1 || fallback.Snapshot().CacheEntries != 1 {
		t.Fatalf("restore must not depend on current SSN, stats=%+v snapshot=%+v", stats, fallback.Snapshot())
	}
	got := fallback.CheckForecastDetailed(testClosedRequest(), now)
	if got.Status != VOACAPForecastCheckReady || got.Forecast.SSN != 112 {
		t.Fatalf("restored actual input should remain usable without current SSN, got %+v", got)
	}
}

func TestVOACAPClosedFallbackPersistsSuccessfulWorkerForecast(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.CacheTTLSeconds = 8 * 3600
	cfg.VOACAPFallback.DelaySeconds = 0
	cfg.VOACAPFallback.ForecastHours = 2
	now := time.Now().UTC()
	store := openTestVOACAPForecastCacheStore(t)

	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			windowStart := job.WindowStartUTC
			return testCompleteForecast(job, cfg.VOACAPFallback.ForecastHours, VOACAPClosedForecast{
				WindowStartUTC: windowStart,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -20, HourUTC: windowStart.Hour(), FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -18, HourUTC: windowStart.Add(time.Hour).Hour(), FrequencyMHz: job.FrequencyMHz},
				},
				OutputPath: "runtime-output-should-not-persist.out",
				Elapsed:    time.Second,
			}), nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	fallback.now = func() time.Time { return now }
	stats, err := fallback.AttachForecastCacheStore(store, now)
	if err != nil {
		t.Fatalf("AttachForecastCacheStore() error: %v", err)
	}
	if stats.Loaded != 0 {
		t.Fatalf("empty store loaded unexpected entries: %+v", stats)
	}
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		cancel()
		fallback.Wait()
		if err := fallback.CloseForecastCacheStore(); err != nil {
			t.Fatalf("CloseForecastCacheStore() error: %v", err)
		}
	}()

	if _, ok := fallback.CheckForecastWindow(testClosedRequest(), now); ok {
		t.Fatalf("enqueue lookup should not synchronously return a forecast")
	}
	waitUntil(t, func() bool { return calls.Load() == 1 && fallback.Snapshot().CacheEntries == 1 })
	// Memory publication precedes persistence; worker shutdown is the disk fence.
	cancel()
	fallback.Wait()
	records := readRawVOACAPRetentionRecords(t, store)
	key := fallback.cacheKey(testClosedRequest())
	record, ok := records[string(voacapForecastCacheKeyBytes(key))]
	if !ok || len(records) != 1 || record.StoredAtUnixNS <= 0 || record.WindowUnixNS != forecastWindowStart(now).UnixNano() || len(record.Records) != 2 {
		t.Fatalf("raw persisted worker forecast missing or invalid before hydration: %+v", records)
	}
	if record.Records[0].FT8SNRDB != -20 || record.Records[1].FT8SNRDB != -18 || record.Records[0].HourUTC != forecastWindowStart(now).Hour() || record.Records[1].HourUTC != forecastWindowStart(now).Add(time.Hour).Hour() {
		t.Fatalf("raw persisted worker rows = %+v, want requested two-hour forecast", record.Records)
	}

	loaded, loadStats, err := store.loadCurrent(time.Now().UTC(), cfg.VOACAPFallback)
	if err != nil {
		t.Fatalf("loadCurrent() error: %v", err)
	}
	if len(loaded) != 1 || loadStats.Loaded != 1 {
		t.Fatalf("persisted worker forecast missing, stats=%+v loaded=%d", loadStats, len(loaded))
	}
	if loaded[0].entry.forecast.OutputPath != "" || loaded[0].entry.forecast.Elapsed != 0 {
		t.Fatalf("persisted forecast should not retain runtime output metadata: %+v", loaded[0].entry.forecast)
	}
}

func TestVOACAPClosedFallbackCheckForecastWindowWaitRefreshesEmptyCache(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.CacheTTLSeconds = 8 * 3600
	cfg.VOACAPFallback.ForecastHours = 2
	cfg.VOACAPFallback.DelaySeconds = 900
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return testCompleteForecast(job, cfg.VOACAPFallback.ForecastHours, VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -20, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -18, HourUTC: 21, FrequencyMHz: job.FrequencyMHz},
				},
			}), nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		cancel()
		fallback.Wait()
	}()

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	fallback.now = func() time.Time { return now }
	window, status := fallback.CheckForecastWindowWait(req, now, time.Second)
	if status != VOACAPForecastWindowReady {
		t.Fatalf("status = %v, want ready", status)
	}
	if len(window.Records) != 2 {
		t.Fatalf("window records = %d, want 2: %+v", len(window.Records), window.Records)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
	waitUntil(t, func() bool { return fallback.Snapshot().WorkerDurationCount == 1 })
	if snap := fallback.Snapshot(); snap.DelayEntries != 0 || snap.InflightEntries != 0 || snap.CacheEntries != 1 {
		t.Fatalf("unexpected snapshot after refresh: %+v", snap)
	}
}

func TestVOACAPClosedFallbackCheckForecastWindowWaitTimeoutContinuesWorker(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.CacheTTLSeconds = 8 * 3600
	cfg.VOACAPFallback.ForecastHours = 2
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var releaseOnce sync.Once
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			once.Do(func() { close(started) })
			select {
			case <-ctx.Done():
				return VOACAPClosedForecast{}, ctx.Err()
			case <-release:
			}
			return testCompleteForecast(job, cfg.VOACAPFallback.ForecastHours, VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -20, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -18, HourUTC: 21, FrequencyMHz: job.FrequencyMHz},
				},
			}), nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		releaseOnce.Do(func() { close(release) })
		cancel()
		fallback.Wait()
	}()

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	fallback.now = func() time.Time { return now }
	window, status := fallback.CheckForecastWindowWait(req, now, 10*time.Millisecond)
	if status != VOACAPForecastWindowRefreshing {
		t.Fatalf("status = %v, want refreshing", status)
	}
	if len(window.Records) != 0 {
		t.Fatalf("timeout should not return records, got %+v", window.Records)
	}
	<-started
	releaseOnce.Do(func() { close(release) })
	waitUntil(t, func() bool { return fallback.Snapshot().CacheEntries == 1 && fallback.Snapshot().InflightEntries == 0 })
	window, ok := fallback.CheckForecastWindow(req, now)
	if !ok || len(window.Records) != 2 {
		t.Fatalf("expected background refresh to populate cache, ok=%v window=%+v", ok, window)
	}
}

func TestVOACAPClosedFallbackCheckForecastWindowWaitRefreshesPartialCache(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.CacheTTLSeconds = 8 * 3600
	cfg.VOACAPFallback.ForecastHours = 2
	cfg.VOACAPFallback.DelaySeconds = 900
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return testCompleteForecast(job, cfg.VOACAPFallback.ForecastHours, VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -20, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -18, HourUTC: 21, FrequencyMHz: job.FrequencyMHz},
				},
			}), nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		cancel()
		fallback.Wait()
	}()

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	fallback.now = func() time.Time { return now }
	oldWindowStart := now.Add(-time.Hour)
	key := fallback.cacheKey(req)
	fallback.mu.Lock()
	fallback.addCacheLocked(key, testCacheEntryWithConfig(voacapCacheEntry{
		forecast: VOACAPClosedForecast{
			WindowStartUTC: oldWindowStart,
			Records: []VOACAPHourlyForecast{
				{FT8SNRDB: -15, HourUTC: 20, FrequencyMHz: 14.1},
			},
		},
		storedAt: now.Add(-time.Minute),
	}, cfg))
	fallback.mu.Unlock()

	window, status := fallback.CheckForecastWindowWait(req, now, 0)
	if status != VOACAPForecastWindowRefreshing {
		t.Fatalf("status = %v, want refreshing", status)
	}
	if len(window.Records) != 1 || window.Records[0].Record.HourUTC != 20 {
		t.Fatalf("expected partial cached window, got %+v", window.Records)
	}
	waitUntil(t, func() bool { return calls.Load() == 1 && fallback.Snapshot().InflightEntries == 0 })
	refreshed, ok := fallback.CheckForecastWindow(req, now)
	if !ok || len(refreshed.Records) != 2 {
		t.Fatalf("expected refreshed full window, ok=%v window=%+v", ok, refreshed)
	}
}

func TestVOACAPClosedFallbackCheckForecastWindowWaitFailureClearsInflight(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			return VOACAPClosedForecast{}, fmt.Errorf("forecast failed")
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		cancel()
		fallback.Wait()
	}()

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	fallback.now = func() time.Time { return now }
	window, status := fallback.CheckForecastWindowWait(req, now, time.Second)
	if status != VOACAPForecastWindowFailed {
		t.Fatalf("status = %v, want failed", status)
	}
	if len(window.Records) != 0 {
		t.Fatalf("failure should not return records, got %+v", window.Records)
	}
	waitUntil(t, func() bool { return fallback.Snapshot().WorkerDurationCount == 1 })
	if snap := fallback.Snapshot(); snap.InflightEntries != 0 || snap.DelayEntries != 1 {
		t.Fatalf("failure should clear inflight and add delay, snapshot=%+v", snap)
	}
}

func TestVOACAPClosedFallbackReevaluatesCachedForecastPerMode(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.CacheTTLSeconds = 8 * 3600
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return testCompleteForecast(job, cfg.VOACAPFallback.ForecastHours, VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -14, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
				},
			}), nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		cancel()
		fallback.Wait()
	}()

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	fallback.now = func() time.Time { return now }
	if _, ok := fallback.CheckClosed(req, now); ok {
		t.Fatalf("FT8 enqueue should not synchronously return a result")
	}
	waitUntil(t, func() bool { return calls.Load() == 1 && fallback.Snapshot().CacheEntries == 1 })
	if _, ok := fallback.CheckClosed(req, now.Add(time.Second)); ok {
		t.Fatalf("FT8 threshold should keep -14 open")
	}
	req.Mode = "PSK"
	res, ok := fallback.CheckClosed(req, now.Add(2*time.Second))
	if !ok {
		t.Fatalf("PSK threshold should classify cached -14 forecast as closed")
	}
	if res.Source != SourceVOACAPClosed || res.VOACAPFT8SNRDB != -14 {
		t.Fatalf("unexpected PSK cached closed result: %+v", res)
	}
	if calls.Load() != 1 {
		t.Fatalf("cached forecast should not rerun for mode threshold re-evaluation, calls=%d", calls.Load())
	}
}

func TestVOACAPClosedFallbackReusesForecastWindowAcrossHours(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.CacheTTLSeconds = 8 * 3600
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return testCompleteForecast(job, cfg.VOACAPFallback.ForecastHours, VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -34, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -31, HourUTC: 21, FrequencyMHz: job.FrequencyMHz},
				},
			}), nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		cancel()
		fallback.Wait()
	}()

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	fallback.now = func() time.Time { return now }
	if _, ok := fallback.CheckClosed(req, now); ok {
		t.Fatalf("enqueue should not synchronously return a result")
	}
	waitUntil(t, func() bool { return calls.Load() == 1 && fallback.Snapshot().CacheEntries == 1 })

	res, ok := fallback.CheckClosed(req, now.Add(time.Second))
	if !ok || res.VOACAPFT8SNRDB != -34 || res.VOACAPHourUTC != 20 {
		t.Fatalf("expected hour 20 cached closed result, got ok=%v result=%+v", ok, res)
	}
	res, ok = fallback.CheckClosed(req, now.Add(time.Hour))
	if !ok || res.VOACAPFT8SNRDB != -31 || res.VOACAPHourUTC != 21 {
		t.Fatalf("expected hour 21 cached closed result, got ok=%v result=%+v", ok, res)
	}
	if calls.Load() != 1 {
		t.Fatalf("forecast window should be reused across hours, calls=%d", calls.Load())
	}
}

func TestVOACAPClosedFallbackReusesForecastWindowAcrossMidnight(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.CacheTTLSeconds = 8 * 3600
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return testCompleteForecast(job, cfg.VOACAPFallback.ForecastHours, VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -34, HourUTC: 23, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -33, HourUTC: 0, FrequencyMHz: job.FrequencyMHz},
				},
			}), nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		cancel()
		fallback.Wait()
	}()

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 23, 0, 0, 0, time.UTC)
	fallback.now = func() time.Time { return now }
	if _, ok := fallback.CheckClosed(req, now); ok {
		t.Fatalf("enqueue should not synchronously return a result")
	}
	waitUntil(t, func() bool { return calls.Load() == 1 && fallback.Snapshot().CacheEntries == 1 })

	res, ok := fallback.CheckClosed(req, now.Add(time.Hour))
	if !ok || res.VOACAPFT8SNRDB != -33 || res.VOACAPHourUTC != 0 {
		t.Fatalf("expected midnight cached closed result, got ok=%v result=%+v", ok, res)
	}
	if calls.Load() != 1 {
		t.Fatalf("forecast window should be reused across midnight, calls=%d", calls.Load())
	}
}

func TestVOACAPClosedFallbackDoesNotReuseForecastOutsideHorizon(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.DelaySeconds = 0
	cfg.VOACAPFallback.CacheTTLSeconds = int((48 * time.Hour).Seconds())
	cfg.VOACAPFallback.ForecastHours = 2
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return testCompleteForecast(job, cfg.VOACAPFallback.ForecastHours, VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -34, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -33, HourUTC: 21, FrequencyMHz: job.FrequencyMHz},
				},
			}), nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		cancel()
		fallback.Wait()
	}()

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	fallback.now = func() time.Time { return now }
	if _, ok := fallback.CheckClosed(req, now); ok {
		t.Fatalf("enqueue should not synchronously return a result")
	}
	waitUntil(t, func() bool { return calls.Load() == 1 && fallback.Snapshot().CacheEntries == 1 })
	if res, ok := fallback.CheckClosed(req, now.Add(time.Second)); !ok || res.VOACAPHourUTC != 20 {
		t.Fatalf("expected cached hour 20 result inside horizon, got ok=%v result=%+v", ok, res)
	}
	if res, ok := fallback.CheckClosed(req, now.Add(24*time.Hour)); ok {
		t.Fatalf("must not reuse same UTC hour outside forecast horizon, got %+v", res)
	}
	waitUntil(t, func() bool { return calls.Load() == 2 })
}

func TestVOACAPClosedFallbackStatsExplainCacheLifecycle(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.CacheTTLSeconds = 8 * 3600
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return testCompleteForecast(job, cfg.VOACAPFallback.ForecastHours, VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -34, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
				},
			}), nil
		},
	}
	fallback := newTestClosedFallback(t, cfg, forecaster, fixedSSNProvider{ssn: 112})
	ctx, cancel := context.WithCancel(context.Background())
	fallback.Start(ctx)
	defer func() {
		cancel()
		fallback.Wait()
	}()

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	fallback.now = func() time.Time { return now }
	if _, ok := fallback.CheckForecast(req, now); ok {
		t.Fatalf("enqueue lookup should not synchronously return a forecast")
	}
	waitUntil(t, func() bool { return calls.Load() == 1 && fallback.Snapshot().CacheEntries == 1 })
	if _, ok := fallback.CheckForecast(req, now.Add(time.Second)); !ok {
		t.Fatalf("expected cached current-hour forecast")
	}

	stats := fallback.StatsSnapshot()
	if stats.Queued != 1 || stats.RunSuccess != 1 || stats.CacheHit != 1 {
		t.Fatalf("unexpected lifecycle stats: %+v", stats)
	}
	if !stats.HasActivity() {
		t.Fatalf("expected lifecycle stats to report activity")
	}
	if after := fallback.StatsSnapshot(); after.HasActivity() {
		t.Fatalf("expected snapshot to reset stats, got %+v", after)
	}
}

func TestVOACAPClosedFallbackStatsReportNoCurrentHour(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.DelaySeconds = 0
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})

	req := testClosedRequest()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	key := fallback.cacheKey(req)
	fallback.mu.Lock()
	fallback.addCacheLocked(key, testCacheEntryWithConfig(voacapCacheEntry{
		forecast: VOACAPClosedForecast{
			WindowStartUTC: now,
			Records: []VOACAPHourlyForecast{
				{FT8SNRDB: -34, HourUTC: 21, FrequencyMHz: 14.1},
			},
		},
		storedAt: now,
	}, cfg))
	fallback.mu.Unlock()

	if _, ok := fallback.CheckForecast(req, now); ok {
		t.Fatalf("cache without current-hour record should not return a forecast")
	}
	stats := fallback.StatsSnapshot()
	if stats.NoCurrentHour != 1 {
		t.Fatalf("NoCurrentHour = %d, want 1 in stats %+v", stats.NoCurrentHour, stats)
	}
}

func TestVOACAPClosedFallbackDetailedStatuses(t *testing.T) {
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	req := testClosedRequest()

	t.Run("invalid request reasons", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			mutate func(*VOACAPClosedRequest)
			reason VOACAPInvalidRequestReason
			assert func(t *testing.T, stats VOACAPFallbackStats)
		}{
			{
				name: "unsupported band",
				mutate: func(req *VOACAPClosedRequest) {
					req.Band = "2m"
				},
				reason: VOACAPInvalidUnsupportedBand,
				assert: func(t *testing.T, stats VOACAPFallbackStats) {
					t.Helper()
					if stats.InvalidUnsupportedBand != 1 {
						t.Fatalf("InvalidUnsupportedBand = %d, want 1 in %+v", stats.InvalidUnsupportedBand, stats)
					}
				},
			},
			{
				name: "empty band",
				mutate: func(req *VOACAPClosedRequest) {
					req.Band = ""
				},
				reason: VOACAPInvalidEmptyUnknownBand,
				assert: func(t *testing.T, stats VOACAPFallbackStats) {
					t.Helper()
					if stats.InvalidEmptyUnknownBand != 1 {
						t.Fatalf("InvalidEmptyUnknownBand = %d, want 1 in %+v", stats.InvalidEmptyUnknownBand, stats)
					}
				},
			},
			{
				name: "unknown band",
				mutate: func(req *VOACAPClosedRequest) {
					req.Band = "unknown"
				},
				reason: VOACAPInvalidEmptyUnknownBand,
				assert: func(t *testing.T, stats VOACAPFallbackStats) {
					t.Helper()
					if stats.InvalidEmptyUnknownBand != 1 {
						t.Fatalf("InvalidEmptyUnknownBand = %d, want 1 in %+v", stats.InvalidEmptyUnknownBand, stats)
					}
				},
			},
			{
				name: "unknown marker band",
				mutate: func(req *VOACAPClosedRequest) {
					req.Band = "???"
				},
				reason: VOACAPInvalidEmptyUnknownBand,
				assert: func(t *testing.T, stats VOACAPFallbackStats) {
					t.Helper()
					if stats.InvalidEmptyUnknownBand != 1 {
						t.Fatalf("InvalidEmptyUnknownBand = %d, want 1 in %+v", stats.InvalidEmptyUnknownBand, stats)
					}
				},
			},
			{
				name: "user grid",
				mutate: func(req *VOACAPClosedRequest) {
					req.UserGrid = "BAD"
				},
				reason: VOACAPInvalidUserGrid,
				assert: func(t *testing.T, stats VOACAPFallbackStats) {
					t.Helper()
					if stats.InvalidUserGrid != 1 {
						t.Fatalf("InvalidUserGrid = %d, want 1 in %+v", stats.InvalidUserGrid, stats)
					}
				},
			},
			{
				name: "DX grid",
				mutate: func(req *VOACAPClosedRequest) {
					req.DXGrid = "BAD"
				},
				reason: VOACAPInvalidDXGrid,
				assert: func(t *testing.T, stats VOACAPFallbackStats) {
					t.Helper()
					if stats.InvalidDXGrid != 1 {
						t.Fatalf("InvalidDXGrid = %d, want 1 in %+v", stats.InvalidDXGrid, stats)
					}
				},
			},
			{
				name: "user cell",
				mutate: func(req *VOACAPClosedRequest) {
					req.UserCell = InvalidCell
				},
				reason: VOACAPInvalidUserCell,
				assert: func(t *testing.T, stats VOACAPFallbackStats) {
					t.Helper()
					if stats.InvalidUserCell != 1 {
						t.Fatalf("InvalidUserCell = %d, want 1 in %+v", stats.InvalidUserCell, stats)
					}
				},
			},
			{
				name: "DX cell",
				mutate: func(req *VOACAPClosedRequest) {
					req.DXCell = InvalidCell
				},
				reason: VOACAPInvalidDXCell,
				assert: func(t *testing.T, stats VOACAPFallbackStats) {
					t.Helper()
					if stats.InvalidDXCell != 1 {
						t.Fatalf("InvalidDXCell = %d, want 1 in %+v", stats.InvalidDXCell, stats)
					}
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cfg := testVOACAPFallbackConfig()
				fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
				req := testClosedRequest()
				tc.mutate(&req)
				got := fallback.CheckForecastDetailed(req, now)
				if got.Status != VOACAPForecastCheckInvalidRequest || got.InvalidReason != tc.reason || got.CacheMiss {
					t.Fatalf("unexpected invalid request status: %+v", got)
				}
				stats := fallback.StatsSnapshot()
				if stats.InvalidRequest != 1 {
					t.Fatalf("InvalidRequest = %d, want 1 in %+v", stats.InvalidRequest, stats)
				}
				tc.assert(t, stats)
			})
		}
	})

	t.Run("ssn unavailable", func(t *testing.T) {
		cfg := testVOACAPFallbackConfig()
		fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{})
		got := fallback.CheckForecastDetailed(req, now)
		if got.Status != VOACAPForecastCheckSSNUnavailable || got.CacheMiss {
			t.Fatalf("unexpected SSN unavailable status: %+v", got)
		}
	})

	t.Run("delay wait", func(t *testing.T) {
		cfg := testVOACAPFallbackConfig()
		fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
		got := fallback.CheckForecastDetailed(req, now)
		if got.Status != VOACAPForecastCheckDelayWait || !got.CacheMiss {
			t.Fatalf("unexpected delay-wait status: %+v", got)
		}
	})

	t.Run("not running", func(t *testing.T) {
		cfg := testVOACAPFallbackConfig()
		cfg.VOACAPFallback.DelaySeconds = 0
		fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
		got := fallback.CheckForecastDetailed(req, now)
		if got.Status != VOACAPForecastCheckNotRunning || !got.CacheMiss {
			t.Fatalf("unexpected not-running status: %+v", got)
		}
	})

	t.Run("inflight", func(t *testing.T) {
		cfg := testVOACAPFallbackConfig()
		cfg.VOACAPFallback.DelaySeconds = 0
		fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
		key := fallback.cacheKey(req)
		fallback.mu.Lock()
		fallback.inflight[key] = &voacapPendingWork{key: key, published: make(chan struct{})}
		fallback.mu.Unlock()
		got := fallback.CheckForecastDetailed(req, now)
		if got.Status != VOACAPForecastCheckInflight || !got.CacheMiss {
			t.Fatalf("unexpected inflight status: %+v", got)
		}
	})

	t.Run("queue full", func(t *testing.T) {
		cfg := testVOACAPFallbackConfig()
		cfg.VOACAPFallback.DelaySeconds = 0
		cfg.VOACAPFallback.MaxQueueDepth = 1
		fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
		fallback.running.Store(true)
		fallback.queue <- &voacapPendingWork{published: make(chan struct{})}
		got := fallback.CheckForecastDetailed(req, now)
		if got.Status != VOACAPForecastCheckQueueFull || !got.CacheMiss {
			t.Fatalf("unexpected queue-full status: %+v", got)
		}
	})

	t.Run("queued", func(t *testing.T) {
		cfg := testVOACAPFallbackConfig()
		cfg.VOACAPFallback.DelaySeconds = 0
		fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
		fallback.running.Store(true)
		got := fallback.CheckForecastDetailed(req, now)
		if got.Status != VOACAPForecastCheckQueued || !got.CacheMiss {
			t.Fatalf("unexpected queued status: %+v", got)
		}
	})

	t.Run("cache hit", func(t *testing.T) {
		cfg := testVOACAPFallbackConfig()
		fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
		key := fallback.cacheKey(req)
		fallback.mu.Lock()
		fallback.addCacheLocked(key, testCacheEntryWithConfig(voacapCacheEntry{
			forecast: VOACAPClosedForecast{
				WindowStartUTC: now,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -19, HourUTC: 20, FrequencyMHz: 14.1},
				},
			},
			storedAt: now,
		}, cfg))
		fallback.mu.Unlock()
		got := fallback.CheckForecastDetailed(req, now)
		if got.Status != VOACAPForecastCheckReady || got.CacheMiss || got.Forecast.Record.FT8SNRDB != -19 {
			t.Fatalf("unexpected cache-hit status: %+v", got)
		}
	})

	t.Run("no current hour plus terminal state", func(t *testing.T) {
		cfg := testVOACAPFallbackConfig()
		cfg.VOACAPFallback.DelaySeconds = 0
		fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
		key := fallback.cacheKey(req)
		fallback.mu.Lock()
		fallback.addCacheLocked(key, testCacheEntryWithConfig(voacapCacheEntry{
			forecast: VOACAPClosedForecast{
				WindowStartUTC: now,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -19, HourUTC: 21, FrequencyMHz: 14.1},
				},
			},
			storedAt: now,
		}, cfg))
		fallback.mu.Unlock()
		got := fallback.CheckForecastDetailed(req, now)
		if got.Status != VOACAPForecastCheckNotRunning || !got.CacheMiss || !got.NoCurrentHour {
			t.Fatalf("unexpected no-current-hour status: %+v", got)
		}
	})
}

func TestVOACAPClosedFallbackCheckCachedForecastInvalidRequestDoesNotRecordStats(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
	req := testClosedRequest()
	req.UserCell = InvalidCell

	if _, ok := fallback.CheckCachedForecast(req, time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)); ok {
		t.Fatalf("invalid cache-only request should not return a forecast")
	}
	if stats := fallback.StatsSnapshot(); stats.InvalidRequest != 0 || stats.InvalidUserCell != 0 {
		t.Fatalf("cache-only invalid request should not mutate stats: %+v", stats)
	}
}

func TestVOACAPClosedFallbackStatsCountRunFailuresButNotShutdown(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	runErr := errors.New("voacap failed")
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{
		fn: func(context.Context, VOACAPClosedJob) (VOACAPClosedForecast, error) {
			return VOACAPClosedForecast{}, runErr
		},
	}, fixedSSNProvider{ssn: 112})

	fallback.runJob(context.Background(), VOACAPClosedJob{Request: testClosedRequest()})
	if stats := fallback.StatsSnapshot(); stats.RunFailure != 1 {
		t.Fatalf("RunFailure = %d, want 1 in stats %+v", stats.RunFailure, stats)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fallback.runJob(ctx, VOACAPClosedJob{Request: testClosedRequest()})
	if stats := fallback.StatsSnapshot(); stats.RunFailure != 0 {
		t.Fatalf("shutdown-canceled run should not count as failure, got %+v", stats)
	}
}

func TestVOACAPClosedFallbackBoundsDelayMap(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.MaxDelayEntries = 2
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})

	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	for i, grid := range []string{"FN31", "FN32", "FN33"} {
		req := testClosedRequest()
		req.UserCell = CellID(i + 1)
		req.UserGrid = grid
		_, _ = fallback.CheckClosed(req, now.Add(time.Duration(i)*time.Second))
	}
	if got := fallback.Snapshot().DelayEntries; got != 2 {
		t.Fatalf("delay entries = %d, want bounded 2", got)
	}
}

func TestVOACAPClosedFallbackAppliesNoiseToReceiveDirection(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.MergeReceiveWeight = 0.6
	cfg.MergeTransmitWeight = 0.4
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	key := fallback.cacheKey(testClosedRequest())
	fallback.addCacheLocked(key, testCacheEntryWithConfig(voacapCacheEntry{
		storedAt: now,
		forecast: VOACAPClosedForecast{
			WindowStartUTC: now,
			Records: []VOACAPHourlyForecast{
				{
					HourUTC:           20,
					FrequencyMHz:      14.1,
					ReceiveFT8SNRDB:   -10,
					TransmitFT8SNRDB:  -20,
					HasDirectionalSNR: true,
				},
			},
		},
	}, cfg))

	req := testClosedRequest()
	req.ReceiveNoisePenaltyDB = 5
	forecast, ok := fallback.CheckForecast(req, now)
	if !ok {
		t.Fatalf("expected cached directional forecast")
	}
	if got, want := forecast.EffectiveDB(), -17.0; got != want {
		t.Fatalf("effective VOACAP SNR = %v, want %v", got, want)
	}
	if forecast.Record.FT8SNRDB != -17 {
		t.Fatalf("rounded diagnostic SNR = %d, want -17", forecast.Record.FT8SNRDB)
	}
}

func TestVOACAPFallbackBandsUseConfiguredCenterFrequencies(t *testing.T) {
	cfg := defaultVOACAPFallbackConfig()
	cfg.CenterFrequenciesMHz = []float64{14.1, 14.2, 7.15, 50.125, 144.2}
	bands := VOACAPFallbackBands(cfg)
	want := []string{"20m", "40m", "6m"}
	if strings.Join(bands, ",") != strings.Join(want, ",") {
		t.Fatalf("VOACAPFallbackBands() = %v, want %v", bands, want)
	}
	freq, ok := VOACAPFallbackCenterFrequencyMHz(cfg, "20m")
	if !ok || freq != 14.1 {
		t.Fatalf("20m center frequency = %v ok=%v, want 14.1 true", freq, ok)
	}
	if _, ok := VOACAPFallbackCenterFrequencyMHz(cfg, "2m"); ok {
		t.Fatalf("2m should not be supported by configured VOACAP fallback frequencies")
	}
}

func BenchmarkVOACAPClosedFallbackCheckForecastDetailedCacheHit(b *testing.B) {
	cfg := testVOACAPFallbackConfig()
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	fallback, err := NewVOACAPClosedFallback(cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112}, nil)
	if err != nil {
		b.Fatalf("NewVOACAPClosedFallback() error: %v", err)
	}
	store, err := OpenVOACAPForecastCacheStore(b.TempDir())
	if err != nil {
		b.Fatalf("OpenVOACAPForecastCacheStore() error: %v", err)
	}
	defer func() {
		if err := fallback.CloseForecastCacheStore(); err != nil {
			b.Fatalf("CloseForecastCacheStore() error: %v", err)
		}
	}()
	if _, err := fallback.AttachForecastCacheStore(store, now); err != nil {
		b.Fatalf("AttachForecastCacheStore() error: %v", err)
	}
	req := testClosedRequest()
	key := fallback.cacheKey(req)
	fallback.addCacheLocked(key, testCacheEntryWithConfig(voacapCacheEntry{
		storedAt: now,
		forecast: VOACAPClosedForecast{
			WindowStartUTC: forecastWindowStart(now),
			Records: []VOACAPHourlyForecast{
				{FT8SNRDB: -19, HourUTC: 20, FrequencyMHz: 14.1},
			},
		},
	}, cfg))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := fallback.CheckForecastDetailed(req, now)
		if got.Status != VOACAPForecastCheckReady {
			b.Fatalf("status = %v, want ready", got.Status)
		}
	}
}

type fixedSSNProvider struct {
	ssn int
	ok  bool
}

func (p fixedSSNProvider) CurrentSSN(time.Time) (int, bool) {
	if !p.ok && p.ssn == 0 {
		return 0, false
	}
	return p.ssn, true
}

type fakeClosedForecaster struct {
	fn func(context.Context, VOACAPClosedJob) (VOACAPClosedForecast, error)
}

func (f fakeClosedForecaster) ForecastClosed(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
	if f.fn == nil {
		return VOACAPClosedForecast{}, nil
	}
	return f.fn(ctx, job)
}

func newTestClosedFallback(t *testing.T, cfg Config, forecaster VOACAPClosedForecaster, ssn VOACAPSSNProvider) *VOACAPClosedFallback {
	t.Helper()
	fallback, err := NewVOACAPClosedFallback(cfg, forecaster, ssn, nil)
	if err != nil {
		t.Fatalf("NewVOACAPClosedFallback() error: %v", err)
	}
	if fallback == nil {
		t.Fatalf("expected enabled fallback")
	}
	return fallback
}

func testVOACAPFallbackConfig() Config {
	cfg := DefaultConfig()
	cfg.GlyphSymbols.Closed = "!"
	cfg.VOACAPFallback = defaultVOACAPFallbackConfig()
	cfg.VOACAPFallback.Enabled = true
	cfg.VOACAPFallback.ForecastHours = 8
	cfg.VOACAPFallback.DelaySeconds = 900
	cfg.VOACAPFallback.CacheTTLSeconds = 3600
	cfg.VOACAPFallback.MaxCacheEntries = 8
	cfg.VOACAPFallback.MaxDelayEntries = 8
	cfg.VOACAPFallback.MaxQueueDepth = 2
	cfg.VOACAPFallback.CenterFrequenciesMHz = []float64{14.1}
	return cfg
}

func testClosedRequest() VOACAPClosedRequest {
	return VOACAPClosedRequest{
		UserCell: CellID(1),
		DXCell:   CellID(2),
		UserGrid: "FN31",
		DXGrid:   "FN32",
		Band:     "20m",
		Mode:     "FT8",
	}
}

func waitUntil(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before deadline")
}
