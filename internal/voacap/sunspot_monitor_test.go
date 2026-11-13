package voacap

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSunspotMonitorReplayCatchupAndRoundedGenerations(t *testing.T) {
	var body atomic.Value
	body.Store("[" + noonFluxJSON("2026-06-10T20:00:00", "140", "123.17362") + "," +
		noonFluxJSON("2026-06-08T20:00:00", "100", "100") + "]")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer server.Close()
	monitor := newTestSunspotMonitor(t, server)
	firstNow := mustTime(t, "2026-06-10T21:00:00Z")
	if err := monitor.Poll(context.Background(), firstNow); err != nil {
		t.Fatal(err)
	}
	s := monitor.Snapshot()
	if s.RecentFluxEWMA != 120 || s.ForecastSSN != 73 || !s.ForecastInitialized ||
		s.Input.Provenance.BlendedFluxSFU != 121.58681 {
		t.Fatalf("cold chronological replay: %+v", s)
	}
	// A one-SSN change below the retired 12% hold advances immediately.
	body.Store("[" + noonFluxJSON("2026-06-14T20:00:00", "123", "121.89128") + "," +
		noonFluxJSON("2026-06-12T20:00:00", "126", "121.89128") + "," +
		noonFluxJSON("2026-06-10T20:00:00", "140", "123.17362") + "]")
	secondNow := mustTime(t, "2026-06-14T21:00:00Z")
	if err := monitor.Poll(context.Background(), secondNow); err != nil {
		t.Fatal(err)
	}
	s = monitor.Snapshot()
	if s.RecentFluxEWMA != 123 || s.ForecastSSN != 74 || !s.LastObservedAtUTC.Equal(mustTime(t, "2026-06-14T20:00:00Z")) {
		t.Fatalf("missed observations not replayed: %+v", s)
	}
	before, _ := monitor.CurrentSolarInput(secondNow)
	if err := monitor.Poll(context.Background(), secondNow.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	after, _ := monitor.CurrentSolarInput(secondNow)
	if before != after {
		t.Fatalf("repeated feed changed accepted input: before=%+v after=%+v", before, after)
	}
	// Same integer SSN still captures its new actual input provenance.
	body.Store("[" + noonFluxJSON("2026-06-16T20:00:00", "123", "121.89128") + "]")
	if err := monitor.Poll(context.Background(), secondNow.Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	s = monitor.Snapshot()
	if s.ForecastSSN != 74 || !s.LastForecastAtUTC.Equal(secondNow) || s.Input.Provenance.RecentObservedUnix != mustTime(t, "2026-06-16T20:00:00Z").Unix() {
		t.Fatalf("same-SSN provenance/generation mismatch: %+v", s)
	}
}

func TestSunspotMonitorLateMeanAndIndependentWatermark(t *testing.T) {
	var body atomic.Value
	body.Store("[" + noonFluxJSON("2026-06-10T20:00:00", "140", "null") + "," +
		noonFluxJSON("2026-06-08T20:00:00", "100", "null") + "]")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer server.Close()
	m := newTestSunspotMonitor(t, server)
	now := mustTime(t, "2026-06-10T21:00:00Z")
	if err := m.Poll(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.CurrentSSN(now); ok || !m.Snapshot().MeanMissing || m.Snapshot().RecentFluxEWMA != 120 {
		t.Fatalf("partial first input must remain unavailable: %+v", m.Snapshot())
	}
	// An earlier mean can arrive after newer flux: its independent watermark
	// advances without reprocessing either accepted Noon flux observation.
	body.Store("[" + noonFluxJSON("2026-06-08T20:00:00", "900", "80") + "]")
	if err := m.Poll(context.Background(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	s := m.Snapshot()
	if s.RecentFluxEWMA != 120 || s.PublishedMeanSFU != 80 || s.ForecastSSN != 47 {
		t.Fatalf("late earlier mean/watermark: %+v", s)
	}
	body.Store("[" + noonFluxJSON("2026-06-10T20:00:00", "900", "123.17362") + "]")
	if err := m.Poll(context.Background(), now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	accepted, ok := m.CurrentSolarInput(now)
	if !ok || accepted.SSN != 73 || accepted.Provenance.RecentEWMAFluxSFU != 120 {
		t.Fatalf("late current mean double-smoothed flux: %+v", accepted)
	}
	body.Store("[" + noonFluxJSON("2026-06-10T20:00:00", "900", "900") + "]")
	if err := m.Poll(context.Background(), now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.CurrentSolarInput(now); got != accepted {
		t.Fatalf("accepted equal-timestamp correction changed input: %+v", got)
	}
	body.Store("[" + noonFluxJSON("2026-06-12T20:00:00", "140", "\"invalid\"") + "]")
	if err := m.Poll(context.Background(), now.Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	s = m.Snapshot()
	if s.RecentFluxEWMA != 130 || s.PublishedMeanSFU != 123.17362 ||
		!s.MeanObservedAtUTC.Equal(mustTime(t, "2026-06-10T20:00:00Z")) ||
		s.Input.Provenance.MeanObservedUnix != accepted.Provenance.MeanObservedUnix {
		t.Fatalf("invalid mean rejected flux or refreshed carried mean age: %+v", s)
	}
}

func TestSunspotMonitorWeightEndpointsAndZero(t *testing.T) {
	for _, tc := range []struct {
		name, flux, mean string
		weight           float64
		wantSSN          int
		wantFlags        uint16
	}{
		{"recent_only", "121.58681", "null", 1, 73, SolarInputRecentPresent | SolarInputBlendPresent},
		{"mean_only", "null", "122.44564", 0, 74, SolarInputMeanPresent | SolarInputBlendPresent},
		{"valid_zero", "0", "0", .5, 0, SolarInputRecentPresent | SolarInputMeanPresent | SolarInputBlendPresent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, "["+noonFluxJSON("2026-06-08T20:00:00", tc.flux, tc.mean)+"]")
			}))
			defer server.Close()
			cfg := testSolarMonitorConfig(server)
			cfg.RecentWeight = tc.weight
			m, err := NewSunspotMonitor(cfg)
			if err != nil {
				t.Fatal(err)
			}
			now := mustTime(t, "2026-06-12T20:00:00Z")
			if err := m.Poll(context.Background(), now); err != nil {
				t.Fatal(err)
			}
			input, ok := m.CurrentSolarInput(now)
			ssn, ssnOK := m.CurrentSSN(now)
			if !ok || !ssnOK || input.SSN != tc.wantSSN || ssn != tc.wantSSN || input.Provenance.Flags != tc.wantFlags {
				t.Fatalf("weight/zero lost: %+v ok=%v ssn=%d/%v", input, ok, ssn, ssnOK)
			}
			s := m.Snapshot()
			if (tc.weight == 1 && s.MeanStale) || (tc.weight == 0 && s.RecentStale) {
				t.Fatalf("noncontributor marked stale: %+v", s)
			}
		})
	}
}

func TestSunspotMonitorValidatorsAndFailureRetainInput(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.Header().Set("ETag", "\"bad\"")
			fmt.Fprint(w, "not-json")
		case 2:
			if r.Header.Get("If-None-Match") != "" {
				t.Error("parse-failure validator was retained")
			}
			w.Header().Set("ETag", "\"good\"")
			fmt.Fprint(w, "["+noonFluxJSON("2026-06-08T20:00:00", "121.58681", "121.58681")+"]")
		case 3:
			if r.Header.Get("If-None-Match") != "\"good\"" {
				t.Error("usable validator missing")
			}
			w.WriteHeader(http.StatusNotModified)
		case 4:
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			fmt.Fprint(w, "["+noonFluxJSON("2026-06-10T20:00:00", "1e308", "1e308")+"]")
		}
	}))
	defer server.Close()
	m := newTestSunspotMonitor(t, server)
	now := mustTime(t, "2026-06-08T21:00:00Z")
	if err := m.Poll(context.Background(), now); err == nil || m.Snapshot().LastError == "" {
		t.Fatal("parse failure not exposed")
	}
	if err := m.Poll(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	accepted, _ := m.CurrentSolarInput(now)
	for _, advance := range []time.Duration{time.Hour, 49 * time.Hour, 50 * time.Hour} {
		err := m.Poll(context.Background(), now.Add(advance))
		if (advance == time.Hour && err != nil) || (advance != time.Hour && err == nil) {
			t.Fatalf("unexpected fetch/arithmetic outcome after %s: %v", advance, err)
		}
		if got, _ := m.CurrentSolarInput(now); got != accepted {
			t.Fatalf("304/failure changed accepted input: %+v", got)
		}
	}
	if m.Snapshot().LastObservedAtUTC.Unix() != accepted.Provenance.RecentObservedUnix {
		t.Fatal("failed arithmetic partly advanced the ingestion batch")
	}
}

func TestSunspotMonitorAtomicConcurrentInput(t *testing.T) {
	m, err := NewSunspotMonitor(testSolarMonitorConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	start := mustTime(t, "2026-01-01T20:00:00Z")
	var readers sync.WaitGroup
	done := make(chan struct{})
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-done:
				return
			default:
				input, ok := m.CurrentSolarInput(start)
				if !ok {
					continue
				}
				p := input.Provenance
				want, err := ssnFromSolarFlux(p.BlendedFluxSFU)
				if err != nil || want != input.SSN || p.BlendedFluxSFU != .5*p.RecentEWMAFluxSFU+.5*p.PublishedMeanFluxSFU {
					t.Errorf("incoherent input: %+v", input)
					return
				}
			}
		}
	}()
	for i := 0; i < 200; i++ {
		at := start.Add(time.Duration(i) * 24 * time.Hour)
		if err := m.applyFluxSeries([]solarFluxObservation{{at: at, flux: float64(90 + i), mean: 110, hasFlux: true, hasMean: true}}, at); err != nil {
			t.Fatal(err)
		}
	}
	close(done)
	readers.Wait()
}

func TestSunspotMonitorRejectsInvalidPolicy(t *testing.T) {
	for _, mutate := range []func(*SunspotMonitorConfig){
		func(c *SunspotMonitorConfig) { c.FetchInterval = 0 },
		func(c *SunspotMonitorConfig) { c.RequestTimeout = 0 },
		func(c *SunspotMonitorConfig) { c.EWMAHalfLife = 0 },
		func(c *SunspotMonitorConfig) { c.EWMAHalfLife = time.Millisecond },
		func(c *SunspotMonitorConfig) { c.RecentWeight = -.01 },
		func(c *SunspotMonitorConfig) { c.RecentWeight = 1.01 },
		func(c *SunspotMonitorConfig) { c.RecentWeight = math.NaN() },
		func(c *SunspotMonitorConfig) { c.StaleAfter = 0 },
		func(c *SunspotMonitorConfig) { c.URL = strings.Repeat("x", maxSolarSourceURLBytes+1) },
	} {
		cfg := testSolarMonitorConfig(nil)
		mutate(&cfg)
		if _, err := NewSunspotMonitor(cfg); err == nil {
			t.Fatalf("invalid policy accepted: %+v", cfg)
		}
	}
}

func testSolarMonitorConfig(server *httptest.Server) SunspotMonitorConfig {
	cfg := SunspotMonitorConfig{
		URL: "http://example.invalid/flux.json", FetchInterval: time.Hour,
		RequestTimeout: time.Second, EWMAHalfLife: 48 * time.Hour, RecentWeight: .5, StaleAfter: 48 * time.Hour,
	}
	if server != nil {
		cfg.URL, cfg.HTTPClient = server.URL, server.Client()
	}
	return cfg
}

func newTestSunspotMonitor(t *testing.T, server *httptest.Server) *SunspotMonitor {
	t.Helper()
	m, err := NewSunspotMonitor(testSolarMonitorConfig(server))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func noonFluxJSON(at, flux, mean string) string {
	return fmt.Sprintf("{\"time_tag\":%q,\"frequency\":2800,\"reporting_schedule\":\"Noon\",\"flux\":%s,\"ninety_day_mean\":%s}", at, flux, mean)
}
