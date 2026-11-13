package voacap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSunspotMonitorIndependentAgeWarningsAndRecovery(t *testing.T) {
	var phase atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch phase.Load() {
		case 0:
			fmt.Fprint(w, "["+noonFluxJSON("2026-06-08T20:00:00", "100", "110")+"]")
		case 1:
			w.WriteHeader(http.StatusNotModified)
		case 2:
			w.WriteHeader(http.StatusServiceUnavailable)
		case 3:
			fmt.Fprint(w, "["+noonFluxJSON("2026-06-10T20:00:00", "140", "null")+"]")
		case 4:
			fmt.Fprint(w, "["+noonFluxJSON("2026-06-10T20:00:00", "900", "123.17362")+"]")
		default:
			fmt.Fprint(w, "not-json")
		}
	}))
	defer server.Close()
	var logs bytes.Buffer
	cfg := testSolarMonitorConfig(server)
	cfg.Logger = log.New(&logs, "", 0)
	m, err := NewSunspotMonitor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	at := mustTime(t, "2026-06-08T20:00:00Z")
	if err := m.Poll(context.Background(), at); err != nil {
		t.Fatal(err)
	}
	phase.Store(1)
	if err := m.Poll(context.Background(), at.Add(48*time.Hour-time.Nanosecond)); err != nil || logs.Len() != 0 {
		t.Fatalf("warning before boundary: %s %v", logs.String(), err)
	}
	phase.Store(2)
	if err := m.Poll(context.Background(), at.Add(48*time.Hour)); err == nil {
		t.Fatal("fetch failure hidden")
	}
	s := m.Snapshot()
	if !s.RecentStale || !s.MeanStale || strings.Count(logs.String(), "Warning:") != 2 {
		t.Fatalf("failed poll did not evaluate both ages: %+v logs=%s", s, logs.String())
	}
	phase.Store(5)
	_ = m.Poll(context.Background(), at.Add(48*time.Hour+time.Minute))
	phase.Store(1)
	if err := m.Poll(context.Background(), at.Add(48*time.Hour+2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if strings.Count(logs.String(), "Warning:") != 2 {
		t.Fatalf("repeated error/304 spammed warning: %s", logs.String())
	}
	phase.Store(3)
	if err := m.Poll(context.Background(), at.Add(48*time.Hour+3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	s = m.Snapshot()
	if s.RecentStale || !s.MeanStale || s.RecentRecoveryAtUTC.IsZero() || !s.MeanRecoveryAtUTC.IsZero() {
		t.Fatalf("partial source recovery conflated ages: %+v", s)
	}
	phase.Store(4)
	if err := m.Poll(context.Background(), at.Add(48*time.Hour+4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	s = m.Snapshot()
	if s.RecentStale || s.MeanStale || s.MeanRecoveryAtUTC.IsZero() || s.RecentFluxEWMA != 120 ||
		strings.Count(logs.String(), "recovered") != 2 || s.ForecastSSN != 73 {
		t.Fatalf("late mean recovery/double EWMA: %+v logs=%s", s, logs.String())
	}
}

func TestSunspotMonitorMissingComponentsHaveNoInventedAge(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer server.Close()
	var logs bytes.Buffer
	cfg := testSolarMonitorConfig(server)
	cfg.Logger = log.New(&logs, "", 0)
	m, err := NewSunspotMonitor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = m.Poll(context.Background(), mustTime(t, "2030-01-01T00:00:00Z"))
	s := m.Snapshot()
	if !s.RecentMissing || !s.MeanMissing || s.RecentStale || s.MeanStale || s.ForecastInitialized || logs.Len() != 0 {
		t.Fatalf("missing input assigned an age or forecast: %+v logs=%s", s, logs.String())
	}
}

func TestSunspotMonitorKnownNoncontributorDoesNotWarn(t *testing.T) {
	at := mustTime(t, "2026-06-08T20:00:00Z")
	for _, weight := range []float64{0, 1} {
		cfg := testSolarMonitorConfig(nil)
		cfg.RecentWeight = weight
		var logs bytes.Buffer
		cfg.Logger = log.New(&logs, "", 0)
		m, err := NewSunspotMonitor(cfg)
		if err != nil {
			t.Fatal(err)
		}
		rows := []solarFluxObservation{{at: at, flux: 100, mean: 110, hasFlux: true, hasMean: true}}
		rows = append(rows, solarFluxObservation{at: at.Add(72 * time.Hour), flux: 120, mean: 121, hasFlux: weight == 1, hasMean: weight == 0})
		if err := m.applyFluxSeries(rows, at.Add(72*time.Hour)); err != nil {
			t.Fatal(err)
		}
		m.finishPoll(at.Add(72*time.Hour), nil)
		s := m.Snapshot()
		if s.RecentMissing || s.MeanMissing || s.RecentStale || s.MeanStale || logs.Len() != 0 {
			t.Fatalf("weight %v warned about old noncontributor: %+v logs=%s", weight, s, logs.String())
		}
	}
}

func TestSunspotMonitorStartCancellationDuringHTTPAndRepeatedStart(t *testing.T) {
	started := make(chan struct{}, 1)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		started <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	cfg := testSolarMonitorConfig(server)
	cfg.RequestTimeout = 5 * time.Second
	m, err := NewSunspotMonitor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	m.Start(ctx)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("poll never reached HTTP")
	}
	cancel()
	done := make(chan struct{})
	go func() { m.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("monitor did not join canceled HTTP")
	}
	if calls.Load() != 1 || m.Snapshot().LastError == "" {
		t.Fatalf("repeated Start or cancellation outcome: calls=%d snapshot=%+v", calls.Load(), m.Snapshot())
	}
}

type solarTestHTTPClient func(*http.Request) (*http.Response, error)

func (f solarTestHTTPClient) Do(r *http.Request) (*http.Response, error) { return f(r) }

type solarFailingBody struct{ closed bool }

func (*solarFailingBody) Read([]byte) (int, error) { return 0, errors.New("body read failed") }
func (b *solarFailingBody) Close() error           { b.closed = true; return nil }

func TestSunspotMonitorHTTPBoundsAndReadFailureCloseBodies(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		tag  string
	}{
		{"body_limit", strings.Repeat("x", 33), ""},
		{"validator_limit", "[]", strings.Repeat("x", maxSolarValidatorBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testSolarMonitorConfig(nil)
			cfg.MaxResponseBytes = 32
			cfg.HTTPClient = solarTestHTTPClient(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Etag": []string{tc.tag}}, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			m, err := NewSunspotMonitor(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.Poll(context.Background(), time.Now()); err == nil || !strings.Contains(err.Error(), "exceed") {
				t.Fatalf("limit failure absent: %v", err)
			}
		})
	}
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
}
