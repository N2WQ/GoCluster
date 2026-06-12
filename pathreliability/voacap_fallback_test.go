package pathreliability

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/internal/voacap"
)

func TestVOACAPClosedFallbackDelaysEnqueuesAndReturnsCachedClosed(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -34, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
				},
			}, nil
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
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -20, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
				},
			}, nil
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

	key := fallback.cacheKey(req, 112, now)
	fallback.cache[key] = voacapCacheEntry{
		forecast: VOACAPClosedForecast{
			WindowStartUTC: forecastWindowStart(now),
			Records: []VOACAPHourlyForecast{
				{FT8SNRDB: -15, HourUTC: 20, FrequencyMHz: 14.1},
			},
		},
		storedAt: now.Add(-time.Minute),
	}
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

	fallback.cache[key] = voacapCacheEntry{
		forecast: VOACAPClosedForecast{
			WindowStartUTC: forecastWindowStart(now),
			Records: []VOACAPHourlyForecast{
				{FT8SNRDB: -15, HourUTC: 20, FrequencyMHz: 14.1},
			},
		},
		storedAt: now.Add(-2 * time.Hour),
	}
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
	key := fallback.cacheKey(req, 112, windowStart)
	fallback.mu.Lock()
	fallback.addCacheLocked(key, voacapCacheEntry{
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
	})
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
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -20, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -18, HourUTC: 21, FrequencyMHz: job.FrequencyMHz},
				},
			}, nil
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

func TestVOACAPClosedFallbackCheckForecastWindowWaitRefreshesEmptyCache(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.ForecastHours = 2
	cfg.VOACAPFallback.DelaySeconds = 900
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -20, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -18, HourUTC: 21, FrequencyMHz: job.FrequencyMHz},
				},
			}, nil
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
	if snap := fallback.Snapshot(); snap.DelayEntries != 0 || snap.InflightEntries != 0 || snap.CacheEntries != 1 {
		t.Fatalf("unexpected snapshot after refresh: %+v", snap)
	}
}

func TestVOACAPClosedFallbackCheckForecastWindowWaitTimeoutContinuesWorker(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
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
			return VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -20, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -18, HourUTC: 21, FrequencyMHz: job.FrequencyMHz},
				},
			}, nil
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
	cfg.VOACAPFallback.ForecastHours = 2
	cfg.VOACAPFallback.DelaySeconds = 900
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -20, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -18, HourUTC: 21, FrequencyMHz: job.FrequencyMHz},
				},
			}, nil
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
	oldWindowStart := now.Add(-time.Hour)
	key := fallback.cacheKey(req, 112, oldWindowStart)
	fallback.mu.Lock()
	fallback.addCacheLocked(key, voacapCacheEntry{
		forecast: VOACAPClosedForecast{
			WindowStartUTC: oldWindowStart,
			Records: []VOACAPHourlyForecast{
				{FT8SNRDB: -15, HourUTC: 20, FrequencyMHz: 14.1},
			},
		},
		storedAt: now.Add(-time.Minute),
	})
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
	window, status := fallback.CheckForecastWindowWait(req, now, time.Second)
	if status != VOACAPForecastWindowFailed {
		t.Fatalf("status = %v, want failed", status)
	}
	if len(window.Records) != 0 {
		t.Fatalf("failure should not return records, got %+v", window.Records)
	}
	if snap := fallback.Snapshot(); snap.InflightEntries != 0 || snap.DelayEntries != 1 {
		t.Fatalf("failure should clear inflight and add delay, snapshot=%+v", snap)
	}
}

func TestVOACAPClosedFallbackReevaluatesCachedForecastPerMode(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -14, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
				},
			}, nil
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
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -34, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -31, HourUTC: 21, FrequencyMHz: job.FrequencyMHz},
				},
			}, nil
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
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -34, HourUTC: 23, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -33, HourUTC: 0, FrequencyMHz: job.FrequencyMHz},
				},
			}, nil
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
			return VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -34, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
					{FT8SNRDB: -33, HourUTC: 21, FrequencyMHz: job.FrequencyMHz},
				},
			}, nil
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
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			return VOACAPClosedForecast{
				WindowStartUTC: job.WindowStartUTC,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -34, HourUTC: 20, FrequencyMHz: job.FrequencyMHz},
				},
			}, nil
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
	key := fallback.cacheKey(req, 112, now)
	fallback.mu.Lock()
	fallback.addCacheLocked(key, voacapCacheEntry{
		forecast: VOACAPClosedForecast{
			WindowStartUTC: now,
			Records: []VOACAPHourlyForecast{
				{FT8SNRDB: -34, HourUTC: 21, FrequencyMHz: 14.1},
			},
		},
		storedAt: now,
	})
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
		key := fallback.cacheKey(req, 112, now)
		fallback.mu.Lock()
		fallback.inflight[key] = struct{}{}
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
		fallback.queue <- VOACAPClosedJob{}
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
		key := fallback.cacheKey(req, 112, now)
		fallback.mu.Lock()
		fallback.addCacheLocked(key, voacapCacheEntry{
			forecast: VOACAPClosedForecast{
				WindowStartUTC: now,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -19, HourUTC: 20, FrequencyMHz: 14.1},
				},
			},
			storedAt: now,
		})
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
		key := fallback.cacheKey(req, 112, now)
		fallback.mu.Lock()
		fallback.addCacheLocked(key, voacapCacheEntry{
			forecast: VOACAPClosedForecast{
				WindowStartUTC: now,
				Records: []VOACAPHourlyForecast{
					{FT8SNRDB: -19, HourUTC: 21, FrequencyMHz: 14.1},
				},
			},
			storedAt: now,
		})
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

func TestClosedForecastFromRecordsBuildsHourlyWindow(t *testing.T) {
	records := []voacapPredictionRecordForTest{
		{HourUTC: 1, FrequencyMHz: 14.1, FT8SNRDB: -35, VOACAPSNRDBHz: 5, Reliability: 0.72, HasReliability: true},
		{HourUTC: 1, FrequencyMHz: 14.2, FT8SNRDB: -32, VOACAPSNRDBHz: 8, Reliability: 0.81, HasReliability: true},
		{HourUTC: 2, FrequencyMHz: 14.1, FT8SNRDB: -30},
		{HourUTC: 1, FrequencyMHz: 7.1, FT8SNRDB: -10},
	}
	forecast, err := closedForecastFromRecords(testPredictionRecords(records), "20m")
	if err != nil {
		t.Fatalf("closedForecastFromRecords() error: %v", err)
	}
	if len(forecast.Records) != 2 {
		t.Fatalf("forecast records = %d, want 2: %+v", len(forecast.Records), forecast)
	}
	if forecast.Records[0].HourUTC != 1 || forecast.Records[0].FT8SNRDB != -32 || forecast.Records[0].VOACAPSNRDBHz != 8 {
		t.Fatalf("hour 1 should keep the strongest same-hour SNR, got %+v", forecast.Records[0])
	}
	if !forecast.Records[0].HasReqSNRReliability || forecast.Records[0].ReqSNRReliability != 0.81 {
		t.Fatalf("hour 1 should keep REL from the strongest same-hour SNR, got %+v", forecast.Records[0])
	}
	if forecast.Records[1].HourUTC != 2 || forecast.Records[1].FT8SNRDB != -30 {
		t.Fatalf("hour 2 should be retained, got %+v", forecast.Records[1])
	}
}

func TestVOACAPRunnerClosedForecasterDeckUsesSafeLabels(t *testing.T) {
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	forecaster := NewVOACAPRunnerClosedForecaster(cfg)
	deck, err := forecaster.buildDeck(VOACAPClosedJob{
		Request: VOACAPClosedRequest{
			UserGrid: "FN05",
			DXGrid:   "DM81XX",
			Band:     "20m",
		},
		SSN:            147,
		WindowStartUTC: time.Date(2026, time.June, 9, 3, 0, 0, 0, time.UTC),
	}, voacapDeckTransmit)
	if err != nil {
		t.Fatalf("buildDeck() error: %v", err)
	}
	text := string(deck)
	if !strings.Contains(text, "LABEL     TRANSMITTER") || !strings.Contains(text, " RECEIVER") {
		t.Fatalf("fallback deck should use safe alphabetic labels:\n%s", text)
	}
	if strings.Contains(text, "LABEL     FN05") || strings.Contains(text, "DM81XX") {
		t.Fatalf("fallback deck label must not contain grid strings:\n%s", text)
	}
	if !strings.Contains(text, "CIRCUIT   45.50N   079.00W    31.98N    102.04W") {
		t.Fatalf("fallback deck should preserve grid-center coordinates:\n%s", text)
	}
	if !strings.Contains(text, "TIME          3   10    1    1") {
		t.Fatalf("fallback deck should start at the job window hour:\n%s", text)
	}
}

func TestVOACAPRunnerClosedForecasterDeckSelectsMethodByDistance(t *testing.T) {
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	forecaster := NewVOACAPRunnerClosedForecaster(cfg)
	tests := []struct {
		name     string
		userGrid string
		dxGrid   string
		want     string
	}{
		{name: "short path", userGrid: "FN31", dxGrid: "FN32", want: "METHOD       20    0"},
		{name: "long path", userGrid: "FN31", dxGrid: "QF56", want: "METHOD       30    0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deck, err := forecaster.buildDeck(VOACAPClosedJob{
				Request: VOACAPClosedRequest{
					UserGrid: tt.userGrid,
					DXGrid:   tt.dxGrid,
					Band:     "20m",
				},
				SSN:            147,
				WindowStartUTC: time.Date(2026, time.June, 9, 3, 0, 0, 0, time.UTC),
			}, voacapDeckTransmit)
			if err != nil {
				t.Fatalf("buildDeck() error: %v", err)
			}
			if text := string(deck); !strings.Contains(text, tt.want) {
				t.Fatalf("fallback deck method mismatch, want %q:\n%s", tt.want, text)
			}
		})
	}
}

func TestVOACAPRunnerClosedForecasterDeckUsesWindowStartHour(t *testing.T) {
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	forecaster := NewVOACAPRunnerClosedForecaster(cfg)
	deck, err := forecaster.buildDeck(VOACAPClosedJob{
		Request: VOACAPClosedRequest{
			UserGrid: "FN05",
			DXGrid:   "DM81XX",
			Band:     "20m",
		},
		SSN:            147,
		WindowStartUTC: time.Date(2026, time.June, 9, 17, 0, 0, 0, time.UTC),
	}, voacapDeckTransmit)
	if err != nil {
		t.Fatalf("buildDeck() error: %v", err)
	}
	if text := string(deck); !strings.Contains(text, "TIME         17   24    1    1") {
		t.Fatalf("fallback deck should cover the current UTC hour:\n%s", text)
	}
}

func TestVOACAPRunnerClosedForecasterDeckSupportsReceiveDirection(t *testing.T) {
	cfg := testVOACAPFallbackConfig().VOACAPFallback
	forecaster := NewVOACAPRunnerClosedForecaster(cfg)
	deck, err := forecaster.buildDeck(VOACAPClosedJob{
		Request: VOACAPClosedRequest{
			UserGrid: "FN05",
			DXGrid:   "DM81XX",
			Band:     "20m",
		},
		SSN:            147,
		WindowStartUTC: time.Date(2026, time.June, 9, 3, 0, 0, 0, time.UTC),
	}, voacapDeckReceive)
	if err != nil {
		t.Fatalf("buildDeck() error: %v", err)
	}
	if text := string(deck); !strings.Contains(text, "CIRCUIT   31.98N   102.04W    45.50N    079.00W") {
		t.Fatalf("receive-direction deck should reverse circuit coordinates:\n%s", text)
	}
}

func TestVOACAPClosedFallbackAppliesNoiseToReceiveDirection(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.MergeReceiveWeight = 0.6
	cfg.MergeTransmitWeight = 0.4
	fallback := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	key := fallback.cacheKey(testClosedRequest(), 112, now)
	fallback.addCacheLocked(key, voacapCacheEntry{
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
	})

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

func TestCombineDirectionalForecastsRequiresCommonHours(t *testing.T) {
	receive := VOACAPClosedForecast{Records: []VOACAPHourlyForecast{
		{HourUTC: 20, FrequencyMHz: 14.1, FT8SNRDB: -10, VOACAPSNRDBHz: 24, ReqSNRReliability: 0.91, HasReqSNRReliability: true},
		{HourUTC: 21, FrequencyMHz: 14.1, FT8SNRDB: -12, VOACAPSNRDBHz: 22},
	}}
	transmit := VOACAPClosedForecast{Records: []VOACAPHourlyForecast{
		{HourUTC: 20, FrequencyMHz: 14.1, FT8SNRDB: -20, VOACAPSNRDBHz: 14, ReqSNRReliability: 0.84, HasReqSNRReliability: true},
	}}
	combined, err := combineDirectionalForecasts(receive, transmit, "20m")
	if err != nil {
		t.Fatalf("combineDirectionalForecasts() error: %v", err)
	}
	if len(combined.Records) != 1 {
		t.Fatalf("expected one common-hour record, got %d", len(combined.Records))
	}
	record := combined.Records[0]
	if !record.HasDirectionalSNR || record.ReceiveFT8SNRDB != -10 || record.TransmitFT8SNRDB != -20 {
		t.Fatalf("unexpected combined record: %+v", record)
	}
	if !record.HasDirectionalReqSNRReliability || record.ReceiveReqSNRReliability != 0.91 || record.TransmitReqSNRReliability != 0.84 {
		t.Fatalf("unexpected directional REL fields: %+v", record)
	}
	if rel, ok := (VOACAPCachedForecast{Record: record}).ReqSNRReliability(); !ok || rel != 0.84 {
		t.Fatalf("effective REL = %v ok=%v, want 0.84 true", rel, ok)
	}

	_, err = combineDirectionalForecasts(receive, VOACAPClosedForecast{}, "20m")
	if err == nil {
		t.Fatalf("expected error with no common bidirectional hours")
	}
}

func TestVOACAPRunnerClosedForecasterLiveSafeLabelDeck(t *testing.T) {
	if os.Getenv("GOCLUSTER_RUN_LIVE_VOACAP") != "1" {
		t.Skip("set GOCLUSTER_RUN_LIVE_VOACAP=1 to run against local VOACAP")
	}
	cfg := defaultVOACAPFallbackConfig()
	cfg.VOACAPHome = voacap.DefaultVOACAPHome
	cfg.VOACAPTimeoutSeconds = 120
	cfg.OutputNamePrefix = "gocluster_voacap_v9_live"
	forecaster := NewVOACAPRunnerClosedForecaster(cfg)
	forecast, err := forecaster.ForecastClosed(context.Background(), VOACAPClosedJob{
		Request: VOACAPClosedRequest{
			UserCell: CellID(1020),
			DXCell:   CellID(1476),
			UserGrid: "FN05",
			DXGrid:   "DM81XX",
			Band:     "40m",
			Mode:     "FT8",
		},
		SSN:            147,
		WindowStartUTC: time.Date(2026, time.June, 9, 3, 0, 0, 0, time.UTC),
		FrequencyMHz:   7.1,
	})
	if err != nil {
		t.Fatalf("ForecastClosed() live error: %v", err)
	}
	if len(forecast.Records) == 0 {
		t.Fatalf("live forecast missing hourly records: %+v", forecast)
	}
	if forecast.Records[0].FrequencyMHz <= 0 {
		t.Fatalf("live forecast missing frequency: %+v", forecast)
	}
	if forecast.OutputPath == "" {
		t.Fatalf("live forecast missing output path: %+v", forecast)
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

type voacapPredictionRecordForTest struct {
	HourUTC        int
	FrequencyMHz   float64
	FT8SNRDB       int
	VOACAPSNRDBHz  int
	Reliability    float64
	HasReliability bool
}

func testPredictionRecords(in []voacapPredictionRecordForTest) []voacap.PredictionRecord {
	out := make([]voacap.PredictionRecord, 0, len(in))
	for _, record := range in {
		out = append(out, voacap.PredictionRecord{
			HourUTC:        record.HourUTC,
			FrequencyMHz:   record.FrequencyMHz,
			FT8SNRDB:       record.FT8SNRDB,
			VOACAPSNRDBHz:  record.VOACAPSNRDBHz,
			Reliability:    record.Reliability,
			HasReliability: record.HasReliability,
		})
	}
	return out
}
