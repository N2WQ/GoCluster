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

func testSolarMonitorConfig(server *httptest.Server) SunspotMonitorConfig {
	cfg := SunspotMonitorConfig{URL: "http://example.invalid/count.json", FetchInterval: time.Hour, RequestTimeout: time.Second, EWMAHalfLife: 48 * time.Hour, StaleAfter: 48 * time.Hour}
	if server != nil {
		cfg.URL = server.URL
		cfg.HTTPClient = server.Client()
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
func TestSunspotMonitorReplayPrecisionAndNoReweighting(t *testing.T) {
	var body atomic.Value
	a := countRow("2026-06-08T20:00:00", "1", "1", "90", "2")
	b := countRow("2026-06-10T20:00:00", "1", "1", "92", "3")
	body.Store("[" + b + "," + a + "]")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body.Load()) }))
	defer server.Close()
	m := newTestSunspotMonitor(t, server)
	now := mustTime(t, "2026-06-10T21:00:00Z")
	if err := m.Poll(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	accepted, ok := m.CurrentSolarInput(now)
	if !ok || accepted.SSN != 101 || accepted.Provenance.SmoothedValue != 101 || accepted.Provenance.LatestValue != 102 || accepted.Provenance.SourceID != SolarSourceNOAASunspotReport {
		t.Fatalf("input: %+v", accepted)
	}
	// An accepted timestamp's correction is deliberately deferred, even when the
	// body changes. Neither repeated polls nor fetch times advance smoothing.
	body.Store("[" + strings.Replace(b, `"Numspot":92`, `"Numspot":999`, 1) + "," + a + "]")
	for i := 0; i < 3; i++ {
		if err := m.Poll(context.Background(), now.Add(time.Duration(i)*time.Hour)); err != nil {
			t.Fatal(err)
		}
		got, _ := m.CurrentSolarInput(now)
		if got != accepted {
			t.Fatalf("accepted sample reweighted: %+v", got)
		}
	}
	seed := newTestSunspotMonitor(t, nil)
	at := now.Add(-time.Hour)
	if err := seed.applyCountSeries([]countReportSample{{at, 27}}, now); err != nil {
		t.Fatal(err)
	}
	if got, _ := seed.CurrentSSN(now); got != 27 {
		t.Fatalf("first sample=%d", got)
	}
	// A fractional oracle checks the monitor boundary, so rounding the stored
	// EWMA (while leaving its arithmetic helper correct) cannot pass unnoticed.
	precision := newTestSunspotMonitor(t, nil)
	if err := precision.applyCountSeries([]countReportSample{{at.Add(-24 * time.Hour), 100}, {at, 200}}, now); err != nil {
		t.Fatal(err)
	}
	if got, ready := precision.CurrentSolarInput(now); !ready || got.SSN != 129 || math.Abs(got.Provenance.SmoothedValue-129.28932188134524) > 1e-12 {
		t.Fatalf("monitor rounded the intermediate EWMA: %+v", got)
	}
}
func TestSunspotMonitorValidatorsAndFailureRetainInput(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.Header().Set("ETag", `"bad"`)
			fmt.Fprint(w, "not-json")
		case 2:
			if r.Header.Get("If-None-Match") != "" {
				t.Error("bad validator retained")
			}
			w.Header().Set("ETag", `"good"`)
			fmt.Fprint(w, "["+countRow("2026-06-08T20:00:00", "1", "1", "90", "2")+"]")
		case 3:
			if r.Header.Get("If-None-Match") != `"good"` {
				t.Error("good validator missing")
			}
			w.WriteHeader(http.StatusNotModified)
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	m := newTestSunspotMonitor(t, server)
	now := mustTime(t, "2026-06-08T21:00:00Z")
	if err := m.Poll(context.Background(), now); err == nil {
		t.Fatal("invalid representation accepted")
	}
	if err := m.Poll(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	accepted, _ := m.CurrentSolarInput(now)
	if err := m.Poll(context.Background(), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := m.Poll(context.Background(), now.Add(49*time.Hour)); err == nil {
		t.Fatal("fetch failure hidden")
	}
	if got, _ := m.CurrentSolarInput(now); got != accepted || !m.Snapshot().RecentStale {
		t.Fatalf("failure lost accepted input/health: %+v", m.Snapshot())
	}
}
func TestSunspotMonitorAtomicConcurrentInput(t *testing.T) {
	m := newTestSunspotMonitor(t, nil)
	start := mustTime(t, "2026-01-01T20:00:00Z")
	done := make(chan struct{})
	var readers sync.WaitGroup
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
				want, err := roundedCountSSN(input.Provenance.SmoothedValue)
				if err != nil || want != input.SSN || !input.Provenance.Valid() {
					t.Errorf("incoherent %+v", input)
					return
				}
			}
		}
	}()
	for i := 0; i < 200; i++ {
		at := start.Add(time.Duration(i) * 24 * time.Hour)
		if err := m.applyCountSeries([]countReportSample{{at, float64(90 + i)}}, at); err != nil {
			t.Fatal(err)
		}
	}
	close(done)
	readers.Wait()
	if got := testing.AllocsPerRun(1000, func() { _, _ = m.CurrentSolarInput(start) }); got != 0 {
		t.Fatalf("snapshot allocations=%g", got)
	}
}
func TestSunspotMonitorRejectsInvalidPolicy(t *testing.T) {
	for _, mutate := range []func(*SunspotMonitorConfig){func(c *SunspotMonitorConfig) { c.FetchInterval = 0 }, func(c *SunspotMonitorConfig) { c.RequestTimeout = 0 }, func(c *SunspotMonitorConfig) { c.EWMAHalfLife = 0 }, func(c *SunspotMonitorConfig) { c.EWMAHalfLife = time.Millisecond }, func(c *SunspotMonitorConfig) { c.StaleAfter = 0 }, func(c *SunspotMonitorConfig) { c.MaxResponseBytes = math.MaxInt64 }, func(c *SunspotMonitorConfig) { c.URL = strings.Repeat("x", maxSolarSourceURLBytes+1) }} {
		cfg := testSolarMonitorConfig(nil)
		mutate(&cfg)
		if _, err := NewSunspotMonitor(cfg); err == nil {
			t.Fatal("invalid policy accepted")
		}
	}
	cfg := testSolarMonitorConfig(nil)
	cfg.URL = ""
	m, err := NewSunspotMonitor(cfg)
	if err != nil || m.cfg.URL != NOAASunspotReportURL {
		t.Fatalf("source default %+v %v", m, err)
	}
}
func TestSunspotMonitorStartCancellationJoinsFetch(t *testing.T) {
	entered := make(chan struct{})
	finished := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(entered)
		<-r.Context().Done()
		close(finished)
	}))
	defer server.Close()
	m := newTestSunspotMonitor(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	m.Start(ctx)
	<-entered
	cancel()
	m.Wait()
	<-finished
	if calls.Load() != 1 {
		t.Fatalf("poll loops=%d", calls.Load())
	}
}
func TestSunspotMonitorTransportBounds(t *testing.T) {
	for _, tc := range []struct {
		name, body, validator string
		limit                 int64
	}{{"body", strings.Repeat("x", 33), "", 32}, {"validator", "[]", strings.Repeat("x", maxSolarValidatorBytes+1), 100}} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("ETag", tc.validator)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			cfg := testSolarMonitorConfig(server)
			cfg.MaxResponseBytes = tc.limit
			m, err := NewSunspotMonitor(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err = m.Poll(context.Background(), time.Now()); err == nil {
				t.Fatal("transport bound not enforced")
			}
			if _, ok := m.CurrentSSN(time.Now()); ok {
				t.Fatal("invalid transport published input")
			}
		})
	}
}

type solarTestHTTPClient func(*http.Request) (*http.Response, error)

func (f solarTestHTTPClient) Do(req *http.Request) (*http.Response, error) { return f(req) }

type solarFailingBody struct{ closed bool }

func (*solarFailingBody) Read([]byte) (int, error) { return 0, fmt.Errorf("body read failed") }
func (b *solarFailingBody) Close() error           { b.closed = true; return nil }

// Preserve the transport cleanup regression independently of the solar source:
// a failed read must close the response and cannot publish incomplete input.
func TestSunspotMonitorReadFailureClosesBody(t *testing.T) {
	body := &solarFailingBody{}
	cfg := testSolarMonitorConfig(nil)
	cfg.HTTPClient = solarTestHTTPClient(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
	})
	m, err := NewSunspotMonitor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Poll(context.Background(), time.Now()); err == nil || !body.closed || !strings.Contains(err.Error(), "body read failed") {
		t.Fatalf("read failure/closure absent: %v closed=%v", err, body.closed)
	}
	if _, ready := m.CurrentSolarInput(time.Now()); ready {
		t.Fatal("failed body read published solar input")
	}
}
