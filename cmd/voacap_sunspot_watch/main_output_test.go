package main

import (
	"bytes"
	"errors"
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

type watcherFailWriter struct {
	writes int
	failAt int
	err    error
	bytes.Buffer
}

func (w *watcherFailWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == w.failAt {
		return 0, w.err
	}
	return w.Buffer.Write(p)
}

func TestWatcherReportFailurePreservesStateAttempt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		failAt int
		seed   bool
		status int
		fault  bool
	}{
		{"header", 1, false, 200, false},
		{"sample", 2, false, 200, false},
		{"unchanged-304", 2, true, 304, false},
		{"unchanged-repeat", 2, true, 200, false},
		{"output-and-state", 2, false, 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.json")
			if tc.seed {
				watcherSeedState(t, path)
			}
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				if tc.fault {
					if err := os.Mkdir(path, 0o755); err != nil {
						t.Errorf("state fault setup: %v", err)
					}
				}
				w.WriteHeader(tc.status)
				if tc.status == 200 {
					fmt.Fprint(w, watcherFixtureBody)
				}
			}))
			t.Cleanup(server.Close)
			writeErr := errors.New("fixture output failed")
			out := &watcherFailWriter{failAt: tc.failAt, err: writeErr}
			err := run(server.URL, time.Second, 8*time.Hour, 0.12, time.Second, path, true, out)
			if !errors.Is(err, writeErr) || requests.Load() != 1 || out.writes != 2 {
				t.Fatalf("report error lost or poll skipped: %v requests=%d writes=%d", err, requests.Load(), out.writes)
			}
			if tc.fault {
				var pathErr *os.PathError
				if !errors.As(err, &pathErr) || pathErr.Path != path {
					t.Fatalf("state error masked by output error: %v", err)
				}
				return
			}
			saved, loadErr := loadState(path)
			if loadErr != nil || !saved.EWMAInitialized || saved.LastRawSSN != 42 || saved.EWMA != 42 {
				t.Fatalf("report failure skipped existing state attempt: %+v %v", saved, loadErr)
			}
		})
	}
}

func TestWatcherValidationAndFetchFailurePreserveState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	watcherSeedState(t, path)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(500)
	}))
	t.Cleanup(server.Close)
	for _, tc := range []struct {
		interval, halfLife, timeout time.Duration
		threshold                   float64
	}{
		{0, time.Hour, time.Second, 0.12},
		{time.Second, 0, time.Second, 0.12},
		{time.Second, time.Hour, 0, 0.12},
		{time.Second, time.Hour, time.Second, -1},
	} {
		var out bytes.Buffer
		if err := run(server.URL, tc.interval, tc.halfLife, tc.threshold, tc.timeout, path, true, &out); err == nil || out.Len() != 0 || requests.Load() != 0 {
			t.Fatalf("invalid settings entered polling: %v requests=%d output=%q", err, requests.Load(), out.String())
		}
	}
	var out bytes.Buffer
	if err := run(server.URL, time.Second, time.Hour, 0.12, time.Second, path, true, &out); err == nil || !strings.Contains(err.Error(), "status 500") || requests.Load() != 1 {
		t.Fatalf("fetch failure not propagated: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("fetch failure changed stored state: %v", err)
	}
}
