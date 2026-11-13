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
	body.Store("[" + noonFluxJSON("2026-06-08T20:00:00", "100.125", "112.25") + "]")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", "\"persisted\"")
		fmt.Fprint(w, body.Load())
	}))
	defer server.Close()
	cfg := testSolarMonitorConfig(server)
	cfg.StatePath = filepath.Join(t.TempDir(), "solar-state.json")
	first, err := NewSunspotMonitor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if found, err := first.LoadState(); found || err != nil {
		t.Fatalf("missing state: found=%v err=%v", found, err)
	}
	if err := first.Poll(context.Background(), mustTime(t, "2026-06-08T21:00:00Z")); err != nil {
		t.Fatal(err)
	}
	restored, err := NewSunspotMonitor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if found, err := restored.LoadState(); !found || err != nil {
		t.Fatalf("restore: found=%v err=%v", found, err)
	}
	if got, ok := restored.CurrentSolarInput(time.Time{}); !ok || got != first.Snapshot().Input || restored.Snapshot().ETag != "\"persisted\"" {
		t.Fatalf("restart lost actual input/validators: %+v", restored.Snapshot())
	}
	body.Store("[" + noonFluxJSON("2026-06-12T20:00:00", "160.875", "114.125") + "," +
		noonFluxJSON("2026-06-08T20:00:00", "100.125", "112.25") + "," +
		noonFluxJSON("2026-06-10T20:00:00", "140.375", "113.5") + "]")
	now := mustTime(t, "2026-06-12T21:00:00Z")
	if err := restored.Poll(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	cold := newTestSunspotMonitor(t, server)
	if err := cold.Poll(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	got, _ := restored.CurrentSolarInput(now)
	want, _ := cold.CurrentSolarInput(now)
	if got != want || got.Provenance.RecentEWMAFluxSFU != 140.5625 {
		t.Fatalf("incremental/cold mismatch: got=%+v want=%+v", got, want)
	}
}

func TestSunspotMonitorStateRejectsOtherSourceOrParameters(t *testing.T) {
	fixture := newTestSunspotMonitor(t, nil)
	at := mustTime(t, "2026-06-08T20:00:00Z")
	if err := fixture.applyFluxSeries([]solarFluxObservation{{at: at, flux: 100, mean: 110, hasFlux: true, hasMean: true}}, at.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	state := fixture.stateSnapshot()
	state.ETag = "\"must-not-reuse\""
	path := filepath.Join(t.TempDir(), "state.json")
	if err := saveSunspotMonitorState(path, state); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*SunspotMonitorConfig){
		"url":       func(c *SunspotMonitorConfig) { c.URL += "?new-source" },
		"half_life": func(c *SunspotMonitorConfig) { c.EWMAHalfLife += time.Hour },
		"weight":    func(c *SunspotMonitorConfig) { c.RecentWeight = .25 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testSolarMonitorConfig(nil)
			cfg.StatePath = path
			mutate(&cfg)
			m, err := NewSunspotMonitor(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if found, err := m.LoadState(); !found || err == nil {
				t.Fatalf("incompatible state accepted: %v %v", found, err)
			}
			if _, ok := m.CurrentSSN(at); ok || m.Snapshot().ETag != "" {
				t.Fatal("incompatible state reused SSN/validator")
			}
		})
	}
	for name, mutate := range map[string]func(*sunspotMonitorState){
		"source":     func(s *sunspotMonitorState) { s.Identity.Source = "Wolf" },
		"units":      func(s *sunspotMonitorState) { s.Identity.Units = "SSN" },
		"conversion": func(s *sunspotMonitorState) { s.Identity.Conversion = "other" },
		"input":      func(s *sunspotMonitorState) { s.Input.SSN++ },
		"timestamp":  func(s *sunspotMonitorState) { s.MeanObservedAtUTC = time.Time{} },
		"validator":  func(s *sunspotMonitorState) { s.ETag = strings.Repeat("x", maxSolarValidatorBytes+1) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := state
			mutate(&bad)
			if err := saveSunspotMonitorState(path, bad); err == nil {
				t.Fatalf("invalid state admitted: %+v", bad)
			}
		})
	}
	for _, body := range []string{
		`{"version":1,"etag":"old-Wolf","ewma":112,"ewma_initialized":true,"forecast_ssn":112}`,
		`{`, strings.Repeat(" ", maxSunspotMonitorStateBytes+1),
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg := testSolarMonitorConfig(nil)
		cfg.StatePath = path
		m, err := NewSunspotMonitor(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := m.LoadState(); err == nil {
			t.Fatal("old/malformed/oversize state accepted")
		}
		if _, ok := m.CurrentSSN(at); ok || m.Snapshot().ETag != "" {
			t.Fatal("rejected state leaked continuity")
		}
	}
}

func TestSunspotMonitorPartialAndZeroStateRoundTrip(t *testing.T) {
	for _, mean := range []bool{false, true} {
		m := newTestSunspotMonitor(t, nil)
		at := mustTime(t, "2026-06-08T20:00:00Z")
		if err := m.applyFluxSeries([]solarFluxObservation{{at: at, flux: 0, mean: 0, hasFlux: true, hasMean: mean}}, at); err != nil {
			t.Fatal(err)
		}
		state := m.stateSnapshot()
		path := filepath.Join(t.TempDir(), "state.json")
		if err := saveSunspotMonitorState(path, state); err != nil {
			t.Fatal(err)
		}
		loaded, found, err := loadSunspotMonitorState(path)
		if err != nil || !found || loaded != state || loaded.InputInitialized != mean {
			t.Fatalf("partial/zero state lost: %+v found=%v err=%v", loaded, found, err)
		}
	}
}

func TestSunspotMonitorEncodedStateByteLimit(t *testing.T) {
	m := newTestSunspotMonitor(t, nil)
	s := m.stateSnapshot()
	// JSON escaping can expand bounded strings: the encoded-byte guard must
	// remain effective independently of raw validator/source string limits.
	s.Identity.URL = strings.Repeat("\x00", maxSolarSourceURLBytes)
	s.ETag = strings.Repeat("\x00", maxSolarValidatorBytes)
	s.LastModified = strings.Repeat("\x00", maxSolarValidatorBytes)
	path := filepath.Join(t.TempDir(), "state.json")
	if err := saveSunspotMonitorState(path, s); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("encoded state cap not enforced: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("oversize state wrote a file: %v", err)
	}
}

func TestSunspotMonitorSaveFailureKeepsInputAndRetries(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			fmt.Fprint(w, "["+noonFluxJSON("2026-06-08T20:00:00", "100", "110")+"]")
		} else {
			fmt.Fprint(w, "["+noonFluxJSON("2026-06-10T20:00:00", "140", "123.17362")+"]")
		}
	}))
	defer server.Close()
	parent := filepath.Join(t.TempDir(), "blocked-parent")
	if err := os.WriteFile(parent, []byte("file, not directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testSolarMonitorConfig(server)
	cfg.StatePath = filepath.Join(parent, "state.json")
	m, err := NewSunspotMonitor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := mustTime(t, "2026-06-08T21:00:00Z")
	if err := m.Poll(context.Background(), now); err == nil {
		t.Fatal("save failure hidden")
	}
	if _, ok := m.CurrentSSN(now); !ok || !strings.Contains(m.Snapshot().LastError, "persist") {
		t.Fatalf("save failure withdrew published input: %+v", m.Snapshot())
	}
	if err := os.Remove(parent); err != nil {
		t.Fatal(err)
	}
	if err := m.Poll(context.Background(), now.Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	var saved sunspotMonitorState
	if err := json.Unmarshal(data, &saved); err != nil || saved.Input.SSN != 73 || m.Snapshot().LastError != "" {
		t.Fatalf("retry did not persist current input: %+v %v", saved, err)
	}
	files, err := filepath.Glob(filepath.Join(parent, ".state.json.tmp-*"))
	if err != nil || len(files) != 0 {
		t.Fatalf("atomic state temporary files remain: %v %v", files, err)
	}
}
