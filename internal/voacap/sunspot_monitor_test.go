package voacap

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestSunspotMonitorRestoresPersistedStateBeforePolling(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "ssn-state.json")
	state := sunspotMonitorState{
		Version:             sunspotMonitorStateVersion,
		ETag:                `"persisted"`,
		LastModified:        "Tue, 09 Jun 2026 12:00:00 GMT",
		LastFetchAtUTC:      mustTime(t, "2026-06-09T12:31:00Z"),
		LastObservedAtUTC:   mustTime(t, "2026-06-09T12:00:00Z"),
		LastRawSSN:          100,
		EWMA:                112,
		EWMAInitialized:     true,
		ForecastSSN:         112,
		ForecastInitialized: true,
		LastRecomputeDelta:  0.12,
		LastForecastAtUTC:   mustTime(t, "2026-06-09T12:31:00Z"),
		LastForecastReason:  "EWMA delta reached recompute threshold",
	}
	if err := saveSunspotMonitorState(statePath, state); err != nil {
		t.Fatalf("save state fixture: %v", err)
	}

	monitor, err := NewSunspotMonitor(SunspotMonitorConfig{
		URL:                "http://example.invalid/sunspot_report.json",
		FetchInterval:      time.Hour,
		RequestTimeout:     time.Second,
		StatePath:          statePath,
		EWMAHalfLife:       8 * time.Hour,
		RecomputeThreshold: 0.12,
	})
	if err != nil {
		t.Fatalf("NewSunspotMonitor() error: %v", err)
	}
	restored, err := monitor.LoadState()
	if err != nil || !restored {
		t.Fatalf("LoadState() restored=%v err=%v, want true nil", restored, err)
	}
	if got, ok := monitor.CurrentSSN(time.Time{}); !ok || got != 112 {
		t.Fatalf("CurrentSSN after restore = %d ok=%v, want 112 true", got, ok)
	}
	snap := monitor.Snapshot()
	if snap.ETag != `"persisted"` || snap.LastObservedAtUTC != state.LastObservedAtUTC {
		t.Fatalf("unexpected restored snapshot: %+v", snap)
	}
}

func TestSunspotMonitorLoadStateColdStartsForMissingFile(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "missing", "ssn-state.json")
	monitor, err := NewSunspotMonitor(SunspotMonitorConfig{
		URL:                "http://example.invalid/sunspot_report.json",
		FetchInterval:      time.Hour,
		RequestTimeout:     time.Second,
		StatePath:          statePath,
		EWMAHalfLife:       8 * time.Hour,
		RecomputeThreshold: 0.12,
	})
	if err != nil {
		t.Fatalf("NewSunspotMonitor() error: %v", err)
	}
	restored, err := monitor.LoadState()
	if err != nil || restored {
		t.Fatalf("LoadState() restored=%v err=%v, want false nil", restored, err)
	}
	if _, ok := monitor.CurrentSSN(time.Time{}); ok {
		t.Fatalf("CurrentSSN should be unavailable after missing state cold start")
	}
}

func TestSunspotMonitorLoadStateRejectsMalformedState(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "ssn-state.json")
	if err := os.WriteFile(statePath, []byte(`{`), 0o644); err != nil {
		t.Fatalf("write malformed state: %v", err)
	}
	monitor, err := NewSunspotMonitor(SunspotMonitorConfig{
		URL:                "http://example.invalid/sunspot_report.json",
		FetchInterval:      time.Hour,
		RequestTimeout:     time.Second,
		StatePath:          statePath,
		EWMAHalfLife:       8 * time.Hour,
		RecomputeThreshold: 0.12,
	})
	if err != nil {
		t.Fatalf("NewSunspotMonitor() error: %v", err)
	}
	restored, err := monitor.LoadState()
	if err == nil || !restored {
		t.Fatalf("LoadState() restored=%v err=%v, want true error", restored, err)
	}
	if _, ok := monitor.CurrentSSN(time.Time{}); ok {
		t.Fatalf("CurrentSSN should be unavailable after rejected state")
	}
}

func TestSunspotMonitorPersistsStateAfterSuccessfulPoll(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state", "ssn-state.json")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"one"`)
		w.Header().Set("Last-Modified", "Tue, 09 Jun 2026 12:00:00 GMT")
		fmt.Fprint(w, noaaSunspotBody("2026-06-09T12:00:00", 90))
	}))
	defer server.Close()

	monitor := newTestSunspotMonitorWithState(t, server, statePath)
	if err := monitor.Poll(context.Background(), mustTime(t, "2026-06-09T12:31:00Z")); err != nil {
		t.Fatalf("Poll() error: %v", err)
	}
	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read persisted state: %v", err)
	}
	var state sunspotMonitorState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("unmarshal persisted state: %v", err)
	}
	if state.Version != sunspotMonitorStateVersion ||
		state.ETag != `"one"` ||
		state.LastModified != "Tue, 09 Jun 2026 12:00:00 GMT" ||
		state.LastObservedAtUTC != mustTime(t, "2026-06-09T12:00:00Z") ||
		state.EWMA != 100 ||
		state.ForecastSSN != 100 ||
		!state.ForecastInitialized {
		t.Fatalf("unexpected persisted state: %+v", state)
	}
}

func TestSunspotMonitorRestoredStateIgnoresStaleObservation(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "ssn-state.json")
	if err := saveSunspotMonitorState(statePath, sunspotMonitorState{
		Version:             sunspotMonitorStateVersion,
		LastObservedAtUTC:   mustTime(t, "2026-06-08T12:00:00Z"),
		LastRawSSN:          100,
		EWMA:                100,
		EWMAInitialized:     true,
		ForecastSSN:         100,
		ForecastInitialized: true,
		LastForecastAtUTC:   mustTime(t, "2026-06-08T12:01:00Z"),
		LastForecastReason:  "initial forecast SSN generation",
	}); err != nil {
		t.Fatalf("save state fixture: %v", err)
	}

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			fmt.Fprint(w, noaaSunspotBody("2026-06-08T12:00:00", 190))
		default:
			fmt.Fprint(w, noaaSunspotBody("2026-06-08T20:00:00", 114))
		}
	}))
	defer server.Close()

	monitor := newTestSunspotMonitorWithState(t, server, statePath)
	restored, err := monitor.LoadState()
	if err != nil || !restored {
		t.Fatalf("LoadState() restored=%v err=%v, want true nil", restored, err)
	}
	if err := monitor.Poll(context.Background(), mustTime(t, "2026-06-08T12:31:00Z")); err != nil {
		t.Fatalf("stale Poll() error: %v", err)
	}
	if snap := monitor.Snapshot(); snap.EWMA != 100 || snap.LastRawSSN != 100 {
		t.Fatalf("stale observation changed state: %+v", snap)
	}
	if err := monitor.Poll(context.Background(), mustTime(t, "2026-06-08T20:01:00Z")); err != nil {
		t.Fatalf("fresh Poll() error: %v", err)
	}
	snap := monitor.Snapshot()
	if snap.EWMA != 112 || snap.ForecastSSN != 112 {
		t.Fatalf("fresh observation did not decay from restored EWMA: %+v", snap)
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

func newTestSunspotMonitorWithState(t *testing.T, server *httptest.Server, statePath string) *SunspotMonitor {
	t.Helper()
	monitor, err := NewSunspotMonitor(SunspotMonitorConfig{
		URL:                server.URL,
		FetchInterval:      time.Hour,
		RequestTimeout:     time.Second,
		StatePath:          statePath,
		EWMAHalfLife:       8 * time.Hour,
		RecomputeThreshold: 0.12,
		HTTPClient:         server.Client(),
	})
	if err != nil {
		t.Fatalf("NewSunspotMonitor() error: %v", err)
	}
	return monitor
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
