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

func TestSunspotMonitorStateRestoreAndColdReplayAgree(t *testing.T) {
	var body atomic.Value
	a := countRow("2026-06-08T20:00:00", "1", "1", "90", "2")
	b := countRow("2026-06-10T20:00:00", "1", "1", "190", "2")
	c := countRow("2026-06-12T20:00:00", "1", "1", "92", "3")
	body.Store("[" + a + "]")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"persisted"`)
		fmt.Fprint(w, body.Load())
	}))
	defer server.Close()
	cfg := testSolarMonitorConfig(server)
	cfg.StatePath = filepath.Join(t.TempDir(), "state.json")
	first, err := NewSunspotMonitor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if found, err := first.LoadState(); found || err != nil {
		t.Fatalf("missing state %v %v", found, err)
	}
	if err = first.Poll(context.Background(), mustTime(t, "2026-06-08T21:00:00Z")); err != nil {
		t.Fatal(err)
	}
	restored, err := NewSunspotMonitor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if found, err := restored.LoadState(); !found || err != nil {
		t.Fatalf("restore %v %v", found, err)
	}
	if got, ok := restored.CurrentSolarInput(time.Time{}); !ok || got != first.Snapshot().Input || restored.Snapshot().ETag != `"persisted"` {
		t.Fatalf("restart lost continuity %+v", restored.Snapshot())
	}
	body.Store("[" + c + "," + a + "," + b + "]")
	now := mustTime(t, "2026-06-12T21:00:00Z")
	if err = restored.Poll(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	cold := newTestSunspotMonitor(t, server)
	if err = cold.Poll(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	got, _ := restored.CurrentSolarInput(now)
	want, _ := cold.CurrentSolarInput(now)
	if got != want || got.Provenance.SmoothedValue != 126 {
		t.Fatalf("restart vs cold %+v %+v", got, want)
	}
}
func TestSunspotMonitorStateRejectsOtherSourceOrParameters(t *testing.T) {
	m := newTestSunspotMonitor(t, nil)
	at := mustTime(t, "2026-06-08T20:00:00Z")
	if err := m.applyCountSeries([]countReportSample{{at, 100}}, at.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	state := m.stateSnapshot()
	state.ETag = `"must-not-reuse"`
	path := filepath.Join(t.TempDir(), "state.json")
	if err := saveSunspotMonitorState(path, state); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*SunspotMonitorConfig){"url": func(c *SunspotMonitorConfig) { c.URL += "?other" }, "half_life": func(c *SunspotMonitorConfig) { c.EWMAHalfLife += time.Hour }} {
		t.Run(name, func(t *testing.T) {
			cfg := testSolarMonitorConfig(nil)
			cfg.StatePath = path
			mutate(&cfg)
			restored, err := NewSunspotMonitor(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if found, err := restored.LoadState(); !found || err == nil {
				t.Fatalf("incompatible restored %v %v", found, err)
			}
			if _, ok := restored.CurrentSSN(at); ok || restored.Snapshot().ETag != "" {
				t.Fatal("incompatible continuity leaked")
			}
		})
	}
	for name, mutate := range map[string]func(*sunspotMonitorState){"source": func(s *sunspotMonitorState) { s.Identity.Source = "other" }, "units": func(s *sunspotMonitorState) { s.Identity.Units = "sfu" }, "policy": func(s *sunspotMonitorState) { s.Identity.Conversion = "other" }, "input": func(s *sunspotMonitorState) { s.Input.SSN++ }, "timestamp": func(s *sunspotMonitorState) { s.LastObservedAtUTC = time.Time{} }, "raw_fraction": func(s *sunspotMonitorState) { s.LastRawCountIndex = 100.5 },
		"future_observation": func(s *sunspotMonitorState) { s.LastObservedAtUTC = at.Add(2 * time.Hour) },
		"validator":          func(s *sunspotMonitorState) { s.ETag = strings.Repeat("x", maxSolarValidatorBytes+1) }} {
		t.Run(name, func(t *testing.T) {
			bad := state
			mutate(&bad)
			if err := saveSunspotMonitorState(path, bad); err == nil {
				t.Fatal("invalid state admitted")
			}
		})
	}
	for _, body := range []string{`{"version":1,"etag":"old-Wolf"}`, `{"version":2,"etag":"old-flux"}`, `{`, strings.Repeat(" ", maxSunspotMonitorStateBytes+1)} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg := testSolarMonitorConfig(nil)
		cfg.StatePath = path
		restored, err := NewSunspotMonitor(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = restored.LoadState(); err == nil {
			t.Fatal("old/malformed/oversize accepted")
		}
		if _, ok := restored.CurrentSSN(at); ok || restored.Snapshot().ETag != "" {
			t.Fatal("rejected state leaked continuity")
		}
	}
}
func TestSunspotMonitorEmptyAndInitializedStateRoundTrip(t *testing.T) {
	for _, initialized := range []bool{false, true} {
		m := newTestSunspotMonitor(t, nil)
		at := mustTime(t, "2026-06-08T20:00:00Z")
		if initialized {
			if err := m.applyCountSeries([]countReportSample{{at, 27}}, at); err != nil {
				t.Fatal(err)
			}
		}
		state := m.stateSnapshot()
		path := filepath.Join(t.TempDir(), "state.json")
		if err := saveSunspotMonitorState(path, state); err != nil {
			t.Fatal(err)
		}
		got, found, err := loadSunspotMonitorState(path)
		if err != nil || !found || got != state {
			t.Fatalf("continuity roundtrip %+v %v %v", got, found, err)
		}
	}
}
func TestSunspotMonitorEncodedStateByteLimit(t *testing.T) {
	m := newTestSunspotMonitor(t, nil)
	s := m.stateSnapshot()
	s.Identity.URL = strings.Repeat("\x00", maxSolarSourceURLBytes)
	s.ETag = strings.Repeat("\x00", maxSolarValidatorBytes)
	s.LastModified = strings.Repeat("\x00", maxSolarValidatorBytes)
	path := filepath.Join(t.TempDir(), "state.json")
	if err := saveSunspotMonitorState(path, s); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("encoded cap %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("oversize wrote state %v", err)
	}
}
func TestSunspotMonitorSaveFailureKeepsInputAndRetries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "["+countRow("2026-06-08T20:00:00", "1", "1", "90", "2")+"]")
	}))
	defer server.Close()
	parent := filepath.Join(t.TempDir(), "blocked-parent")
	if err := os.WriteFile(parent, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testSolarMonitorConfig(server)
	cfg.StatePath = filepath.Join(parent, "state.json")
	m, err := NewSunspotMonitor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := mustTime(t, "2026-06-08T21:00:00Z")
	if err = m.Poll(context.Background(), now); err == nil {
		t.Fatal("save failure hidden")
	}
	if _, ok := m.CurrentSSN(now); !ok || !strings.Contains(m.Snapshot().LastError, "persist") {
		t.Fatalf("save failure withdrew input %+v", m.Snapshot())
	}
	if err = os.Remove(parent); err != nil {
		t.Fatal(err)
	}
	if err = m.Poll(context.Background(), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	var saved sunspotMonitorState
	if err = json.Unmarshal(data, &saved); err != nil || saved.Input.SSN != 100 {
		t.Fatalf("retry save %+v %v", saved, err)
	}
	files, err := filepath.Glob(filepath.Join(parent, ".state.json.tmp-*"))
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary file leak %v %v", files, err)
	}
}

func TestSunspotMonitorStateRejectsCausallyFutureObservation(t *testing.T) {
	m := newTestSunspotMonitor(t, nil)
	at := mustTime(t, "2026-06-08T20:00:00Z")
	accepted := at.Add(time.Hour)
	if err := m.applyCountSeries([]countReportSample{{at, 100}}, accepted); err != nil {
		t.Fatal(err)
	}
	s := m.stateSnapshot()
	s.LastObservedAtUTC = accepted.Add(time.Hour)
	s.Input.Provenance.RecentObservedUnix = s.LastObservedAtUTC.Unix()
	// All arithmetic and source fields still agree; only causal order is wrong.
	if err := validateSunspotMonitorState(s); err == nil || !strings.Contains(err.Error(), "ordering") {
		t.Fatalf("future watermark accepted: %v", err)
	}
	s = m.stateSnapshot()
	s.LastFetchAtUTC = at.Add(-time.Second)
	if err := validateSunspotMonitorState(s); err == nil || !strings.Contains(err.Error(), "ordering") {
		t.Fatalf("observation after successful fetch accepted: %v", err)
	}
}
