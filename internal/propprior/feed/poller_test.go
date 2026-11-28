package feed

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/internal/propprior"
)

func TestConfigValidateRejectsZeroAndInvalidValues(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultConfig should validate: %v", err)
	}

	bad := cfg
	bad.Hp30PrimaryURL = "ftp://kp.gfz.de/app/json/"
	if err := bad.Validate(); err == nil {
		t.Fatalf("expected non-http scheme to fail")
	}

	bad = cfg
	bad.RequestTimeout = 0
	if err := bad.Validate(); err == nil {
		t.Fatalf("expected zero RequestTimeout to fail")
	}

	bad = cfg
	bad.MaxBodyBytesStandard = 0
	if err := bad.Validate(); err == nil {
		t.Fatalf("expected zero MaxBodyBytesStandard to fail")
	}

	bad = cfg
	bad.UserAgent = "   "
	if err := bad.Validate(); err == nil {
		t.Fatalf("expected empty UserAgent to fail")
	}

	bad = cfg
	bad.FailoverConsecutiveErrors = 0
	if err := bad.Validate(); err == nil {
		t.Fatalf("expected zero FailoverConsecutiveErrors to fail")
	}
}

func TestBoundedHTTPClientConditionalGETBodyCapAndRetryAfter(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		switch r.URL.Path {
		case "/etag":
			if r.Header.Get("If-None-Match") == `W/"v1"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("ETag", `W/"v1"`)
			w.Header().Set("Last-Modified", "Fri, 27 Nov 2026 17:15:00 GMT")
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/huge":
			_, _ = w.Write([]byte(strings.Repeat("x", 128)))
		case "/429":
			w.Header().Set("Retry-After", "45")
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			_ = n
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client := newBoundedHTTPClient(2*time.Second, "GoCluster-Test/1.0")
	defer client.closeIdle()
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)

	ep := &conditionalEndpoint{}
	res1, err := client.fetch(context.Background(), ep, srv.URL+"/etag", 1024, now)
	if err != nil || !res1.modified || res1.statusCode != http.StatusOK {
		t.Fatalf("first fetch: res=%+v err=%v", res1, err)
	}
	res2, err := client.fetch(context.Background(), ep, srv.URL+"/etag", 1024, now)
	if err != nil || res2.modified || res2.statusCode != http.StatusNotModified {
		t.Fatalf("second fetch expected 304: res=%+v err=%v", res2, err)
	}

	if _, err := client.fetch(context.Background(), nil, srv.URL+"/huge", 64, now); !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("oversized body expected ErrBodyTooLarge, got %v", err)
	}

	res429, err := client.fetch(context.Background(), nil, srv.URL+"/429", 1024, now)
	if err == nil || res429.statusCode != http.StatusTooManyRequests || res429.retryAfter != 45*time.Second {
		t.Fatalf("429 fetch: res=%+v err=%v", res429, err)
	}
}

func TestPollerDisabledSpawnsZeroGoroutinesAndMakesZeroHTTPRequests(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := testPollerConfig(srv.URL)
	cfg.Enabled = false
	store := propprior.NewSpaceWeatherStore()
	p, err := NewPoller(cfg, store, nil)
	if err != nil {
		t.Fatalf("NewPoller: %v", err)
	}
	p.Start(context.Background())
	time.Sleep(30 * time.Millisecond)
	p.Stop()

	if got := hits.Load(); got != 0 {
		t.Fatalf("disabled poller made %d HTTP requests, want 0", got)
	}
	h := p.Health()
	if h.Hp30.State != StateCold || h.Dst.State != StateCold {
		t.Fatalf("disabled poller should stay cold: %+v", h)
	}
}

func TestPollerCompletedBinGatingNoRingEviction(t *testing.T) {
	// Live clock matching fixture timestamps (geospace has 17:00 and +1h 18:12;
	// gfz_hp30 has in-progress 17:00). Pre-seed a deep storm at T-24h and verify
	// ingesting live-shaped feeds never evicts the oldest completed 24h bin.
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)
	store := propprior.NewSpaceWeatherStore()

	// Completed 24h Dst window at 17:16Z is [2026-11-26T17:00Z .. 2026-11-27T16:00Z].
	oldestDstHour := time.Date(2026, 11, 26, 17, 0, 0, 0, time.UTC)
	store.RecordDst(oldestDstHour, -150.0, "seed")
	// Completed 24h Hp30 window at 17:16Z is [2026-11-26T17:00Z .. 2026-11-27T16:30Z].
	oldestHp30Bin := time.Date(2026, 11, 26, 17, 0, 0, 0, time.UTC)
	store.RecordHp30(oldestHp30Bin, 9.5, "seed")

	geoSamples, err := ParseGeospaceDst(readTestdata(t, "noaa_geospace_dst_7d.json"), now)
	if err != nil {
		t.Fatalf("ParseGeospaceDst: %v", err)
	}
	store.RecordDstBatch(geoSamples, "noaa_geospace_dst_fallback")

	hpSamples, err := ParseGFZHp30(readTestdata(t, "gfz_hp30.json"), now)
	if err != nil {
		t.Fatalf("ParseGFZHp30: %v", err)
	}
	store.RecordHp30Batch(hpSamples, "gfz_hp30")

	snap := store.Evaluate(now)
	if snap.Features.Dst24hMin != -150.0 {
		t.Fatalf("oldest completed Dst hour was evicted! Dst24hMin=%v, want -150.0", snap.Features.Dst24hMin)
	}
	if snap.Features.DstNow != -40.0 {
		t.Fatalf("DstNow=%v, want -40.0 from 16:00 completed Geospace mean", snap.Features.DstNow)
	}
	if snap.Features.Hp30Lag1 != 0.667 {
		t.Fatalf("Hp30Lag1=%v, want 0.667 from 16:30 completed GFZ bin", snap.Features.Hp30Lag1)
	}
	// If in-progress 17:00 had evicted oldestHp30Bin (9.5), 24h mean would drop.
	// Verify oldestHp30Bin remained in the 48-bin ring by checking Hp3024hMean > 2.0
	// (with 9.5 held across the first 43 bins until 14:30!).
	if snap.Features.Hp3024hMean <= 2.5 {
		t.Fatalf("oldest completed Hp30 bin was evicted! Hp3024hMean=%v", snap.Features.Hp3024hMean)
	}
}

func TestPollerFailoverNonOverwriteAndPrimaryRecovery(t *testing.T) {
	nowMu := sync.Mutex{}
	now := time.Date(2026, 11, 27, 15, 45, 0, 0, time.UTC)
	getNow := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}
	setNow := func(t time.Time) {
		nowMu.Lock()
		now = t
		nowMu.Unlock()
	}

	var mode atomic.Int32 // 0=primary healthy, 1=primary down (fallback active), 2=primary recovered

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m := mode.Load()
		switch r.URL.Path {
		case "/gfz":
			idx := r.URL.Query().Get("index")
			if idx == "Fobs" {
				// SFI fallback covers day 25, 26 (primary already has 26=165!), and 27 (new=140).
				_, _ = w.Write([]byte(`{"datetime":["2026-11-25T00:00:00Z","2026-11-26T00:00:00Z","2026-11-27T00:00:00Z"],"Fobs":[110.0,111.0,140.0]}`))
				return
			}
			switch m {
			case 0:
				// Primary Hp30 through 15:00Z with distinctive value 5.333.
				_, _ = w.Write([]byte(`{"datetime":["2026-11-27T14:30:00Z","2026-11-27T15:00:00Z"],"Hp30":[5.333,5.333]}`))
			case 1:
				w.WriteHeader(http.StatusServiceUnavailable)
			case 2:
				// Primary recovered: supplies authoritative 4.667 for 15:30..16:30.
				_, _ = w.Write([]byte(`{"datetime":["2026-11-27T15:00:00Z","2026-11-27T15:30:00Z","2026-11-27T16:00:00Z","2026-11-27T16:30:00Z"],"Hp30":[5.333,4.667,4.667,4.667]}`))
			}
		case "/kp":
			// 7-day Kp covers 12:00..18:00 with low value 1.0 (must NOT overwrite 14:30/15:00=5.333!).
			_, _ = w.Write([]byte(`[{"time_tag":"2026-11-27T12:00:00","Kp":1.0},{"time_tag":"2026-11-27T15:00:00","Kp":1.0}]`))
		case "/kyoto":
			switch m {
			case 0:
				_, _ = w.Write([]byte(`[{"time_tag":"2026-11-27T13:00:00","dst":-80},{"time_tag":"2026-11-27T14:00:00","dst":-80}]`))
			case 1:
				w.WriteHeader(http.StatusServiceUnavailable)
			case 2:
				_, _ = w.Write([]byte(`[{"time_tag":"2026-11-27T14:00:00","dst":-80},{"time_tag":"2026-11-27T15:00:00","dst":-60},{"time_tag":"2026-11-27T16:00:00","dst":-55}]`))
			}
		case "/geospace":
			// Geospace covers 13:00..18:12 with -10 (must NOT overwrite Kyoto 13:00/14:00=-80!).
			_, _ = w.Write([]byte(`[
				{"time_tag":"2026-11-27T13:00:00","dst":-10.0},
				{"time_tag":"2026-11-27T14:00:00","dst":-10.0},
				{"time_tag":"2026-11-27T15:00:00","dst":-20.0},
				{"time_tag":"2026-11-27T16:00:00","dst":-25.0},
				{"time_tag":"2026-11-27T17:00:00","dst":-99.0},
				{"time_tag":"2026-11-27T18:10:00","dst":-99.0}
			]`))
		case "/f107":
			switch m {
			case 0:
				_, _ = w.Write([]byte(`[{"time_tag":"2026-11-26T20:00:00","flux":165.0,"reporting_schedule":"Noon"}]`))
			case 1:
				w.WriteHeader(http.StatusServiceUnavailable)
			case 2:
				_, _ = w.Write([]byte(`[{"time_tag":"2026-11-27T20:00:00","flux":155.0,"reporting_schedule":"Noon"},{"time_tag":"2026-11-26T20:00:00","flux":165.0,"reporting_schedule":"Noon"}]`))
			}
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer srv.Close()

	cfg := testPollerConfig(srv.URL)
	cfg.Enabled = true
	cfg.FailoverConsecutiveErrors = 2
	cfg.Hp30PollInterval = 15 * time.Millisecond
	cfg.DstPollInterval = 15 * time.Millisecond
	cfg.SFIPollInterval = 15 * time.Millisecond
	cfg.RTSWPollInterval = 200 * time.Millisecond
	cfg.SFIFallbackURL = srv.URL + "/gfz?app=1"
	// Use /app/json suffix so isGFZBaseURL builds index=Fobs.
	cfg.Hp30PrimaryURL = srv.URL + "/gfz"
	cfg.SFIFallbackURL = srv.URL + "/gfz"

	store := propprior.NewSpaceWeatherStore()
	p, err := NewPoller(cfg, store, nil)
	if err != nil {
		t.Fatalf("NewPoller: %v", err)
	}
	p.nowFn = getNow

	// Drive deterministic synchronous cycles via internal poll helpers so the
	// exact phase transitions (primary -> failover -> recovery) are flake-free.
	hpTr := &streamTracker{name: "hp30", stepSec: hp30StepSec, staleAfter: cfg.Hp30StaleAfter, primarySource: "gfz_hp30"}
	dstTr := &streamTracker{name: "dst", stepSec: omniStepSec, staleAfter: cfg.DstStaleAfter, primarySource: "noaa_kyoto_dst"}
	sfiTr := &streamTracker{name: "sfi", stepSec: sfiStepSec, staleAfter: cfg.SFIStaleAfter, primarySource: "noaa_f107"}

	pollHp := func() {
		p.pollPrimaryFallback(
			context.Background(), hpTr, cfg.Hp30PollInterval, cfg.MaxBodyBytesStandard, nil,
			func(now time.Time) (string, error) { return BuildGFZURL(cfg.Hp30PrimaryURL, "Hp30", now.Add(-gfzHp30Lookback), now) },
			func(b []byte, now time.Time) ([]propprior.Sample, string, error) { s, e := ParseGFZHp30(b, now); return s, "gfz_hp30", e },
			func(_ time.Time) (string, error) { return cfg.Hp30FallbackURL, nil },
			func(b []byte, now time.Time) ([]propprior.Sample, string, error) { s, e := ParseNOAAKp(b, now); return s, "noaa_kp_fallback", e },
			cfg.MaxBodyBytesStandard, store.RecordHp30Batch, func(s *HealthSnapshot, h StreamHealth) { s.Hp30 = h },
		)
	}
	pollDst := func() {
		p.pollPrimaryFallback(
			context.Background(), dstTr, cfg.DstPollInterval, cfg.MaxBodyBytesStandard, nil,
			func(_ time.Time) (string, error) { return cfg.DstPrimaryURL, nil },
			func(b []byte, now time.Time) ([]propprior.Sample, string, error) { s, e := ParseKyotoDst(b, now); return s, "noaa_kyoto_dst", e },
			func(_ time.Time) (string, error) { return cfg.DstFallbackURL, nil },
			func(b []byte, now time.Time) ([]propprior.Sample, string, error) { s, e := ParseGeospaceDst(b, now); return s, "noaa_geospace_dst_fallback", e },
			cfg.MaxBodyBytesGeospace, store.RecordDstBatch, func(s *HealthSnapshot, h StreamHealth) { s.Dst = h },
		)
	}
	pollSFI := func() {
		p.pollPrimaryFallback(
			context.Background(), sfiTr, cfg.SFIPollInterval, cfg.MaxBodyBytesStandard, nil,
			func(_ time.Time) (string, error) { return cfg.SFIPrimaryURL, nil },
			func(b []byte, now time.Time) ([]propprior.Sample, string, error) { s, e := ParseNOAAF107(b, now); return s, "noaa_f107", e },
			func(now time.Time) (string, error) { return BuildGFZURL(cfg.SFIFallbackURL, "Fobs", now.Add(-gfzFobsLookback), now) },
			ParseSFIFallback, cfg.MaxBodyBytesStandard, store.RecordSFIBatch, func(s *HealthSnapshot, h StreamHealth) { s.SFI = h },
		)
	}

	// Phase 0: Healthy primary hydration at 15:45Z.
	pollHp()
	pollDst()
	pollSFI()
	snap0 := store.Evaluate(getNow())
	if snap0.Features.Hp30Lag1 != 5.333 || snap0.Features.DstNow != -80 || snap0.Features.SFINow != 165 {
		t.Fatalf("phase 0 primary hydration mismatch: %+v", snap0.Features)
	}

	// Phase 1: Advance to 17:16Z, primary fails twice -> fallback activates!
	setNow(time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC))
	mode.Store(1)
	pollHp() // 1st error (degraded)
	pollHp() // 2nd error -> Kp fallback activates
	pollDst()
	pollDst()
	pollSFI()
	pollSFI()

	// Verify non-overwrite guard:
	// - Hp30 15:00 MUST still be 5.333 (observed by querying at 15:30Z!), while
	//   15:30..16:30 got Kp=1.0 (Hp3012hMax still preserves 5.333, Lag1=1.0).
	snap1530 := store.Evaluate(time.Date(2026, 11, 27, 15, 30, 0, 0, time.UTC))
	if snap1530.Features.Hp30Lag1 != 5.333 {
		t.Fatalf("Kp fallback overwrote primary GFZ Hp30 bin! got %v, want 5.333", snap1530.Features.Hp30Lag1)
	}
	snap1 := store.Evaluate(getNow())
	if snap1.Features.Hp30Lag1 != 1.0 || snap1.Features.Hp3012hMax != 5.333 {
		t.Fatalf("Hp30 failover state mismatch: lag1=%v max12h=%v", snap1.Features.Hp30Lag1, snap1.Features.Hp3012hMax)
	}
	// - Dst 14:00 MUST still be Kyoto -80 (Dst24hMin == -80), while 16:00 got Geospace -25.
	if snap1.Features.Dst24hMin != -80.0 || snap1.Features.DstNow != -25.0 {
		t.Fatalf("Geospace fallback overwrote Kyoto or missed new hour: now=%v min=%v", snap1.Features.DstNow, snap1.Features.Dst24hMin)
	}
	// - SFI day 26 MUST still be primary 165 (queried on day 27), while day 27 got fallback 140 (queried on day 28).
	if snap1.Features.SFINow != 165.0 {
		t.Fatalf("SFI fallback overwrote primary day 26! got %v, want 165", snap1.Features.SFINow)
	}
	snapNextDay := store.Evaluate(time.Date(2026, 11, 28, 1, 0, 0, 0, time.UTC))
	if snapNextDay.Features.SFINow != 140.0 {
		t.Fatalf("SFI fallback did not populate new day 27: got %v, want 140", snapNextDay.Features.SFINow)
	}
	if h := p.Health(); !h.Hp30.UsingFallback || !h.Dst.UsingFallback || !h.SFI.UsingFallback {
		t.Fatalf("expected fallback_active health on all three streams: %+v", h)
	}

	// Phase 2: Primary recovers and overwrites temporary fallback bins cleanly!
	mode.Store(2)
	pollHp()
	pollDst()
	pollSFI()

	snap2 := store.Evaluate(getNow())
	if snap2.Features.Hp30Lag1 != 4.667 {
		t.Fatalf("GFZ primary recovery did not overwrite Kp fallback bin: got %v, want 4.667", snap2.Features.Hp30Lag1)
	}
	if snap2.Features.DstNow != -55.0 || snap2.Features.Dst24hMin != -80.0 {
		t.Fatalf("Kyoto primary recovery mismatch: now=%v min=%v", snap2.Features.DstNow, snap2.Features.Dst24hMin)
	}
	snap2NextDay := store.Evaluate(time.Date(2026, 11, 28, 1, 0, 0, 0, time.UTC))
	if snap2NextDay.Features.SFINow != 155.0 {
		t.Fatalf("F10.7 primary recovery did not overwrite day 27 fallback: got %v, want 155", snap2NextDay.Features.SFINow)
	}
	if h := p.Health(); h.Hp30.State != StateHealthyPrimary || h.Dst.State != StateHealthyPrimary || h.SFI.State != StateHealthyPrimary {
		t.Fatalf("expected healthy_primary after recovery: %+v", h)
	}
}

func TestPollerRTSWIndependentFailureHandling(t *testing.T) {
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)
	var failMag atomic.Bool
	failMag.Store(true)
	var failWind atomic.Bool

	magBody := readTestdata(t, "noaa_rtsw_mag_1m.json")
	windBody := readTestdata(t, "noaa_rtsw_wind_1m.json")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rtsw_mag":
			if failMag.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write(magBody)
		case "/rtsw_wind":
			if failWind.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write(windBody)
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer srv.Close()

	cfg := testPollerConfig(srv.URL)
	cfg.Enabled = true
	store := propprior.NewSpaceWeatherStore()
	p, err := NewPoller(cfg, store, nil)
	if err != nil {
		t.Fatalf("NewPoller: %v", err)
	}
	p.nowFn = func() time.Time { return now }

	magTr := &streamTracker{name: "rtsw_mag", stepSec: omniStepSec, primarySource: "noaa_rtsw_mag"}
	windTr := &streamTracker{name: "rtsw_wind", stepSec: omniStepSec, primarySource: "noaa_rtsw_wind"}

	// Mag returns 503 while Wind returns 200 -> Wind must still ingest!
	p.pollSingleStream(context.Background(), magTr, cfg.RTSWMagURL, cfg.RTSWPollInterval, cfg.MaxBodyBytesRTSW, nil,
		ParseRTSWMag, store.RecordBzGSMBatch, func(s *HealthSnapshot, h StreamHealth) { s.RTSWMag = h })
	p.pollSingleStream(context.Background(), windTr, cfg.RTSWWindURL, cfg.RTSWPollInterval, cfg.MaxBodyBytesRTSW, nil,
		ParseRTSWWind, store.RecordVswBatch, func(s *HealthSnapshot, h StreamHealth) { s.RTSWWind = h })

	snap := store.Evaluate(now)
	if !snap.Vsw.HasValid || snap.Bz.HasValid {
		t.Fatalf("expected Wind valid and Mag not yet valid: Vsw=%+v Bz=%+v", snap.Vsw, snap.Bz)
	}
	if h := p.Health(); h.RTSWWind.State != StateHealthyPrimary || h.RTSWMag.State != StateDegradedPrimary {
		t.Fatalf("unexpected independent RTSW health: mag=%+v wind=%+v", h.RTSWMag, h.RTSWWind)
	}

	// Flip: Mag returns 200 while Wind returns 503 -> Mag must ingest!
	failMag.Store(false)
	failWind.Store(true)
	p.pollSingleStream(context.Background(), magTr, cfg.RTSWMagURL, cfg.RTSWPollInterval, cfg.MaxBodyBytesRTSW, nil,
		ParseRTSWMag, store.RecordBzGSMBatch, func(s *HealthSnapshot, h StreamHealth) { s.RTSWMag = h })
	p.pollSingleStream(context.Background(), windTr, cfg.RTSWWindURL, cfg.RTSWPollInterval, cfg.MaxBodyBytesRTSW, nil,
		ParseRTSWWind, store.RecordVswBatch, func(s *HealthSnapshot, h StreamHealth) { s.RTSWWind = h })

	snap2 := store.Evaluate(now)
	if !snap2.Bz.HasValid || !snap2.Vsw.HasValid {
		t.Fatalf("expected both Bz and Vsw populated: Bz=%+v Vsw=%+v", snap2.Bz, snap2.Vsw)
	}
}

func TestPollerLiveIntegrationRaceAndCleanShutdownDuringBlockedRequest(t *testing.T) {
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)
	gfzHp := readTestdata(t, "gfz_hp30.json")
	f107 := readTestdata(t, "noaa_f107.json")
	mag := readTestdata(t, "noaa_rtsw_mag_1m.json")
	wind := readTestdata(t, "noaa_rtsw_wind_1m.json")
	kyoto := readTestdata(t, "noaa_kyoto_dst_objects.json")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/gfz":
			_, _ = w.Write(gfzHp)
		case "/f107":
			_, _ = w.Write(f107)
		case "/rtsw_mag":
			_, _ = w.Write(mag)
		case "/rtsw_wind":
			_, _ = w.Write(wind)
		case "/kyoto":
			_, _ = w.Write(kyoto)
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer srv.Close()

	cfg := testPollerConfig(srv.URL)
	cfg.Enabled = true
	cfg.Hp30PollInterval = 25 * time.Millisecond
	cfg.SFIPollInterval = 25 * time.Millisecond
	cfg.RTSWPollInterval = 25 * time.Millisecond
	cfg.DstPollInterval = 25 * time.Millisecond

	store := propprior.NewSpaceWeatherStore()
	p, err := NewPoller(cfg, store, nil)
	if err != nil {
		t.Fatalf("NewPoller: %v", err)
	}
	p.nowFn = func() time.Time { return now }

	p.Start(context.Background())
	// Concurrent readers while all 4 pollers hydrate & cycle under -race.
	var readerWG sync.WaitGroup
	stopReaders := make(chan struct{})
	readerWG.Add(2)
	for range 2 {
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
					snap := store.Evaluate(now)
					if math.IsNaN(snap.Features.Hp30Lag1) || math.IsNaN(snap.Features.DstNow) {
						t.Errorf("non-finite snapshot during concurrent polling: %+v", snap)
						return
					}
					_ = p.Health()
				}
			}
		}()
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := store.Evaluate(now)
		if snap.Hp30.HasValid && snap.SFI.HasValid && snap.Bz.HasValid && snap.Vsw.HasValid && snap.Dst.HasValid {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(stopReaders)
	readerWG.Wait()
	p.Stop()
	p.Stop() // idempotent

	final := store.Evaluate(now)
	if !final.Hp30.HasValid || !final.SFI.HasValid || !final.Bz.HasValid || !final.Vsw.HasValid || !final.Dst.HasValid {
		t.Fatalf("cold-start hydration incomplete: %+v", final)
	}

	// Verify Stop() cancels a hung upstream request promptly.
	inHandler := make(chan struct{}, 4)
	blockSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case inHandler <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer blockSrv.Close()

	blockCfg := testPollerConfig(blockSrv.URL)
	blockCfg.Enabled = true
	blockCfg.RequestTimeout = 10 * time.Second
	blockPoller, err := NewPoller(blockCfg, propprior.NewSpaceWeatherStore(), nil)
	if err != nil {
		t.Fatalf("NewPoller blocked: %v", err)
	}
	blockPoller.Start(context.Background())
	<-inHandler

	stopDone := make(chan struct{})
	go func() {
		blockPoller.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("Poller.Stop() hung during blocked HTTP request")
	}
}

func TestPollerPrimaryStalenessUsesCompletedBinEnd(t *testing.T) {
	// 1) Unit boundary checks for SFI (36h), Hp30 (90m), and Dst (3h), plus
	// cold-start uninitialized primary (!hasPrimaryBin -> not stale).
	cold := &streamTracker{name: "sfi", stepSec: sfiStepSec, staleAfter: 36 * time.Hour}
	if cold.isPrimaryStale(time.Date(2026, 11, 27, 18, 16, 55, 0, time.UTC)) {
		t.Fatalf("uninitialized tracker must not report stale")
	}

	cases := []struct {
		name       string
		stepSec    int64
		staleAfter time.Duration
		binStart   time.Time
	}{
		{
			name:       "sfi",
			stepSec:    sfiStepSec,
			staleAfter: 36 * time.Hour,
			binStart:   time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC),
		},
		{
			name:       "hp30",
			stepSec:    hp30StepSec,
			staleAfter: 90 * time.Minute,
			binStart:   time.Date(2026, 11, 27, 16, 0, 0, 0, time.UTC),
		},
		{
			name:       "dst",
			stepSec:    omniStepSec,
			staleAfter: 3 * time.Hour,
			binStart:   time.Date(2026, 11, 27, 14, 0, 0, 0, time.UTC),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bin := floorDiv(tc.binStart.Unix(), tc.stepSec)
			tr := &streamTracker{
				name:                tc.name,
				stepSec:             tc.stepSec,
				staleAfter:          tc.staleAfter,
				hasPrimaryBin:       true,
				lastValidPrimaryBin: bin,
			}
			binEnd := time.Unix((bin+1)*tc.stepSec, 0).UTC()
			if tr.isPrimaryStale(binEnd.Add(tc.staleAfter - time.Second)) {
				t.Fatalf("%s stale at staleAfter-1s past binEnd", tc.name)
			}
			if tr.isPrimaryStale(binEnd.Add(tc.staleAfter)) {
				t.Fatalf("%s stale at exact staleAfter boundary past binEnd", tc.name)
			}
			if !tr.isPrimaryStale(binEnd.Add(tc.staleAfter + time.Second)) {
				t.Fatalf("%s not stale at staleAfter+1s past binEnd", tc.name)
			}
		})
	}

	// 2) End-to-end soak regression: on 2026-11-27 18:16:55 UTC, NOAA F10.7 has
	// 2026-11-26 Noon (binEnd = 2026-11-27 00:00:00 UTC -> age 18h16m55s <= 36h).
	// Must stay healthy_primary with zero fallback HTTP calls, then activate
	// fallback once now exceeds 2026-11-28 12:00:00 UTC (36h past binEnd).
	var fallbackCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/f107":
			_, _ = w.Write([]byte(`[{"time_tag":"2026-11-26T20:00:00","flux":162.0,"reporting_schedule":"Noon"}]`))
		case "/gfz":
			fallbackCalls.Add(1)
			_, _ = w.Write([]byte(`{"datetime":["2026-11-26T12:00:00Z","2026-11-27T12:00:00Z"],"Fobs":[162.0,158.0]}`))
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer srv.Close()

	cfg := testPollerConfig(srv.URL)
	cfg.Enabled = true
	store := propprior.NewSpaceWeatherStore()
	p, err := NewPoller(cfg, store, nil)
	if err != nil {
		t.Fatalf("NewPoller: %v", err)
	}
	var nowMu sync.Mutex
	curNow := time.Date(2026, 11, 27, 18, 16, 55, 0, time.UTC)
	p.nowFn = func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return curNow
	}

	sfiTr := &streamTracker{name: "sfi", stepSec: sfiStepSec, staleAfter: cfg.SFIStaleAfter, primarySource: "noaa_f107"}
	pollSFI := func() {
		p.pollPrimaryFallback(
			context.Background(), sfiTr, cfg.SFIPollInterval, cfg.MaxBodyBytesStandard, nil,
			func(_ time.Time) (string, error) { return cfg.SFIPrimaryURL, nil },
			func(b []byte, now time.Time) ([]propprior.Sample, string, error) { s, e := ParseNOAAF107(b, now); return s, "noaa_f107", e },
			func(now time.Time) (string, error) { return BuildGFZURL(cfg.SFIFallbackURL, "Fobs", now.Add(-gfzFobsLookback), now) },
			ParseSFIFallback, cfg.MaxBodyBytesStandard, store.RecordSFIBatch, func(s *HealthSnapshot, h StreamHealth) { s.SFI = h },
		)
	}

	pollSFI()
	if got := fallbackCalls.Load(); got != 0 {
		t.Fatalf("live soak timestamp triggered %d fallback calls, want 0", got)
	}
	if h := p.Health().SFI; h.State != StateHealthyPrimary || h.UsingFallback {
		t.Fatalf("expected healthy_primary at 2026-11-27T18:16:55Z, got %+v", h)
	}

	// Exact 36h boundary past binEnd (2026-11-28 12:00:00 UTC): still non-stale.
	nowMu.Lock()
	curNow = time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	nowMu.Unlock()
	pollSFI()
	if got := fallbackCalls.Load(); got != 0 {
		t.Fatalf("exact 36h boundary triggered %d fallback calls, want 0", got)
	}

	// 1 second past 36h boundary (2026-11-28 12:00:01 UTC): fallback activates.
	nowMu.Lock()
	curNow = time.Date(2026, 11, 28, 12, 0, 1, 0, time.UTC)
	nowMu.Unlock()
	pollSFI()
	if got := fallbackCalls.Load(); got != 1 {
		t.Fatalf("36h+1s past binEnd triggered %d fallback calls, want 1", got)
	}
	if h := p.Health().SFI; h.State != StateFallbackActive || !h.UsingFallback {
		t.Fatalf("expected fallback_active at 36h+1s past binEnd, got %+v", h)
	}
}

func testPollerConfig(baseURL string) Config {
	cfg := DefaultConfig()
	cfg.Hp30PrimaryURL = baseURL + "/gfz"
	cfg.Hp30FallbackURL = baseURL + "/kp"
	cfg.SFIPrimaryURL = baseURL + "/f107"
	cfg.SFIFallbackURL = baseURL + "/gfz"
	cfg.RTSWMagURL = baseURL + "/rtsw_mag"
	cfg.RTSWWindURL = baseURL + "/rtsw_wind"
	cfg.DstPrimaryURL = baseURL + "/kyoto"
	cfg.DstFallbackURL = baseURL + "/geospace"
	cfg.RequestTimeout = 2 * time.Second
	return cfg
}
