package voacap

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSunspotMonitorRoundsEWMAAndAdvancesForecastGeneration(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		switch call {
		case 1:
			fmt.Fprint(w, noaaSunspotBody("2026-06-08T12:00:00", 90))
		default:
			fmt.Fprint(w, noaaSunspotBody("2026-06-08T20:00:00", 114))
		}
	}))
	defer server.Close()

	monitor := newTestSunspotMonitor(t, server)
	if err := monitor.Poll(context.Background(), mustTime(t, "2026-06-08T12:01:00Z")); err != nil {
		t.Fatalf("first Poll() error: %v", err)
	}
	if got, ok := monitor.CurrentSSN(time.Time{}); !ok || got != 100 {
		t.Fatalf("first forecast SSN = %d ok=%v, want 100 true", got, ok)
	}

	if err := monitor.Poll(context.Background(), mustTime(t, "2026-06-08T20:01:00Z")); err != nil {
		t.Fatalf("second Poll() error: %v", err)
	}
	snap := monitor.Snapshot()
	if snap.EWMA != 112 {
		t.Fatalf("EWMA = %.2f, want 112", snap.EWMA)
	}
	if snap.ForecastSSN != 112 || !snap.ForecastInitialized {
		t.Fatalf("forecast generation = %d initialized=%v, want 112 true", snap.ForecastSSN, snap.ForecastInitialized)
	}
	if !strings.Contains(snap.LastForecastReason, "threshold") {
		t.Fatalf("forecast reason = %q, want threshold", snap.LastForecastReason)
	}
}

func TestSunspotMonitorUsesValidatorsAndSkipsNotModified(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("ETag", `"one"`)
			fmt.Fprint(w, noaaSunspotBody("2026-06-08T12:00:00", 90))
			return
		}
		if got := r.Header.Get("If-None-Match"); got != `"one"` {
			t.Fatalf("If-None-Match = %q, want ETag", got)
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()

	monitor := newTestSunspotMonitor(t, server)
	if err := monitor.Poll(context.Background(), mustTime(t, "2026-06-08T12:01:00Z")); err != nil {
		t.Fatalf("first Poll() error: %v", err)
	}
	if err := monitor.Poll(context.Background(), mustTime(t, "2026-06-08T12:31:00Z")); err != nil {
		t.Fatalf("second Poll() error: %v", err)
	}
	snap := monitor.Snapshot()
	if snap.LastObservedAtUTC != mustTime(t, "2026-06-08T12:00:00Z") {
		t.Fatalf("observation changed after 304: %s", snap.LastObservedAtUTC)
	}
	if snap.LastFetchAtUTC != mustTime(t, "2026-06-08T12:31:00Z") {
		t.Fatalf("fetch timestamp = %s, want second poll", snap.LastFetchAtUTC)
	}
}

func TestSunspotMonitorDoesNotStoreValidatorsAfterParseFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.Header().Set("ETag", `"bad"`)
			fmt.Fprint(w, `not-json`)
		default:
			if got := r.Header.Get("If-None-Match"); got != "" {
				t.Fatalf("If-None-Match = %q after parse failure, want empty", got)
			}
			w.Header().Set("ETag", `"good"`)
			fmt.Fprint(w, noaaSunspotBody("2026-06-08T12:00:00", 90))
		}
	}))
	defer server.Close()

	monitor := newTestSunspotMonitor(t, server)
	if err := monitor.Poll(context.Background(), mustTime(t, "2026-06-08T12:01:00Z")); err == nil {
		t.Fatalf("first Poll() should fail on invalid JSON")
	}
	if got := monitor.Snapshot().ETag; got != "" {
		t.Fatalf("ETag stored after parse failure: %q", got)
	}
	if err := monitor.Poll(context.Background(), mustTime(t, "2026-06-08T12:31:00Z")); err != nil {
		t.Fatalf("second Poll() error: %v", err)
	}
	if got := monitor.Snapshot().ETag; got != `"good"` {
		t.Fatalf("ETag after successful parse = %q, want good validator", got)
	}
}

func TestSunspotMonitorStartStopsOnContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, noaaSunspotBody("2026-06-08T12:00:00", 90))
	}))
	defer server.Close()

	monitor := newTestSunspotMonitor(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	monitor.Start(ctx)
	cancel()
	done := make(chan struct{})
	go func() {
		monitor.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sunspot monitor did not stop after cancellation")
	}
}

func newTestSunspotMonitor(t *testing.T, server *httptest.Server) *SunspotMonitor {
	t.Helper()
	monitor, err := NewSunspotMonitor(SunspotMonitorConfig{
		URL:                server.URL,
		FetchInterval:      time.Hour,
		RequestTimeout:     time.Second,
		EWMAHalfLife:       8 * time.Hour,
		RecomputeThreshold: 0.12,
		HTTPClient:         server.Client(),
	})
	if err != nil {
		t.Fatalf("NewSunspotMonitor() error: %v", err)
	}
	return monitor
}

func noaaSunspotBody(observedAt string, numspot int) string {
	return fmt.Sprintf(`[{"time_tag":%q,"Observatory":"TEST","Station":"T","Numspot":%d}]`, observedAt, numspot)
}
