package pathreliability

import (
	"context"
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
				Closed:       true,
				FT8SNRDB:     -34,
				HourUTC:      3,
				FrequencyMHz: job.FrequencyMHz,
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
	waitUntil(t, func() bool { return calls.Load() == 1 })

	res, ok := fallback.CheckClosed(req, now.Add(16*time.Minute))
	if !ok {
		t.Fatalf("expected cached closed result")
	}
	if res.Source != SourceVOACAPClosed || res.Glyph != cfg.GlyphSymbols.Closed || res.Class != classUnlikely {
		t.Fatalf("unexpected cached result: %+v", res)
	}
	if res.VOACAPFT8SNRDB != -34 || res.VOACAPSSN != 112 || res.VOACAPFrequencyMHz != 14.1 {
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
			return VOACAPClosedForecast{Closed: false, FT8SNRDB: -20, FrequencyMHz: job.FrequencyMHz}, nil
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
	waitUntil(t, func() bool { return calls.Load() == 1 })
	if _, ok := fallback.CheckClosed(req, now.Add(time.Second)); ok {
		t.Fatalf("open VOACAP verdict must not replace insufficient bucket result")
	}
	if calls.Load() != 1 {
		t.Fatalf("open cached verdict should avoid duplicate forecast, calls=%d", calls.Load())
	}
}

func TestVOACAPClosedFallbackReevaluatesCachedForecastPerMode(t *testing.T) {
	cfg := testVOACAPFallbackConfig()
	cfg.VOACAPFallback.DelaySeconds = 0
	var calls atomic.Int32
	forecaster := fakeClosedForecaster{
		fn: func(ctx context.Context, job VOACAPClosedJob) (VOACAPClosedForecast, error) {
			calls.Add(1)
			if job.ThresholdDB != -29 {
				t.Fatalf("FT8 job threshold = %v, want -29", job.ThresholdDB)
			}
			return VOACAPClosedForecast{Closed: false, FT8SNRDB: -14, FrequencyMHz: job.FrequencyMHz}, nil
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
	waitUntil(t, func() bool { return calls.Load() == 1 })
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

func TestClosedForecastFromRecordsUsesBestBandSNR(t *testing.T) {
	records := []voacapPredictionRecordForTest{
		{HourUTC: 1, FrequencyMHz: 14.1, FT8SNRDB: -35},
		{HourUTC: 2, FrequencyMHz: 14.1, FT8SNRDB: -30},
		{HourUTC: 1, FrequencyMHz: 7.1, FT8SNRDB: -10},
	}
	forecast, err := closedForecastFromRecords(testPredictionRecords(records), "20m", -29)
	if err != nil {
		t.Fatalf("closedForecastFromRecords() error: %v", err)
	}
	if !forecast.Closed || forecast.FT8SNRDB != -30 || forecast.HourUTC != 2 {
		t.Fatalf("unexpected closed forecast: %+v", forecast)
	}
	forecast, err = closedForecastFromRecords(testPredictionRecords(records), "20m", -31)
	if err != nil {
		t.Fatalf("closedForecastFromRecords() error: %v", err)
	}
	if forecast.Closed {
		t.Fatalf("best SNR -30 should be open at threshold -31")
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
		ThresholdDB:    thresholdsForModeDB("FT8", DefaultConfig()).Closed,
	})
	if err != nil {
		t.Fatalf("ForecastClosed() live error: %v", err)
	}
	if forecast.FrequencyMHz <= 0 {
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
	HourUTC      int
	FrequencyMHz float64
	FT8SNRDB     int
}

func testPredictionRecords(in []voacapPredictionRecordForTest) []voacap.PredictionRecord {
	out := make([]voacap.PredictionRecord, 0, len(in))
	for _, record := range in {
		out = append(out, voacap.PredictionRecord{
			HourUTC:      record.HourUTC,
			FrequencyMHz: record.FrequencyMHz,
			FT8SNRDB:     record.FT8SNRDB,
		})
	}
	return out
}
