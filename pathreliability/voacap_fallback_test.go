package pathreliability

import (
	"context"
	"errors"
	"os"
	"strings"
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
		{HourUTC: 1, FrequencyMHz: 14.1, FT8SNRDB: -35, VOACAPSNRDBHz: 5},
		{HourUTC: 1, FrequencyMHz: 14.2, FT8SNRDB: -32, VOACAPSNRDBHz: 8},
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
	})
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
	})
	if err != nil {
		t.Fatalf("buildDeck() error: %v", err)
	}
	if text := string(deck); !strings.Contains(text, "TIME         17   24    1    1") {
		t.Fatalf("fallback deck should cover the current UTC hour:\n%s", text)
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
	HourUTC       int
	FrequencyMHz  float64
	FT8SNRDB      int
	VOACAPSNRDBHz int
}

func testPredictionRecords(in []voacapPredictionRecordForTest) []voacap.PredictionRecord {
	out := make([]voacap.PredictionRecord, 0, len(in))
	for _, record := range in {
		out = append(out, voacap.PredictionRecord{
			HourUTC:       record.HourUTC,
			FrequencyMHz:  record.FrequencyMHz,
			FT8SNRDB:      record.FT8SNRDB,
			VOACAPSNRDBHz: record.VOACAPSNRDBHz,
		})
	}
	return out
}
