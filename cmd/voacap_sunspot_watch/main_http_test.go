package main

import (
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

func watcherSeedState(t *testing.T, path string) {
	t.Helper()
	st := state{ETag: `"fixture-tag"`, LastModified: "Fri, 09 Oct 2026 12:00:00 GMT",
		LastObservedAtUTC: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC),
		LastRawSSN:        42, EWMA: 42, EWMAInitialized: true, LastVOACAPSSN: 42, LastVOACAPSSNInitialized: true}
	if err := saveState(path, st); err != nil {
		t.Fatal(err)
	}
}

func TestWatcherOneShotActualMain(t *testing.T) {
	for _, tc := range []struct {
		name       string
		code       int
		diagnostic string
		seed       bool
		closed     bool
	}{
		{"success", 0, "", false, false},
		{"disabled-state", 0, "", false, false},
		{"not-modified", 0, "", true, false},
		{"repeat-observation", 0, "", true, false},
		{"http-error", 1, "unexpected status 500", false, false},
		{"truncated-body", 1, "unexpected EOF", false, false},
		{"malformed-json", 1, "invalid character", false, false},
		{"empty-series", 1, "no usable NOAA", false, false},
		{"late-state-write", 1, "state.json", false, false},
		{"closed-output", 1, "write header:", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.json")
			if tc.seed {
				watcherSeedState(t, path)
			}
			var requests atomic.Int32
			var conditional atomic.Bool
			var lateFault atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				conditional.Store(r.Header.Get("If-None-Match") == `"fixture-tag"` && r.Header.Get("If-Modified-Since") == "Fri, 09 Oct 2026 12:00:00 GMT")
				w.Header().Set("ETag", `"fixture-tag"`)
				switch tc.name {
				case "http-error":
					w.WriteHeader(http.StatusInternalServerError)
					return
				case "not-modified":
					w.WriteHeader(http.StatusNotModified)
					return
				case "truncated-body":
					w.Header().Set("Content-Length", "100")
					fmt.Fprint(w, "[")
					return
				case "malformed-json":
					fmt.Fprint(w, "{bad")
					return
				case "empty-series":
					fmt.Fprint(w, "[]")
					return
				case "late-state-write":
					if err := os.Mkdir(path, 0o755); err != nil {
						t.Errorf("late fault setup: %v", err)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					lateFault.Store(true)
				}
				fmt.Fprint(w, watcherFixtureBody)
			}))
			t.Cleanup(server.Close)
			stateArg := path
			if tc.name == "disabled-state" {
				stateArg = ""
			}
			args := []string{"-url", server.URL, "-timeout", "1s", "-state", stateArg, "-once"}
			cmd, stdout, stderr, ctx, cancel := watcherMainCommand(t, args, tc.closed)
			err := cmd.Run()
			if ctx.Err() != nil {
				// Windows forced termination can also use exit 1; a deadline kill
				// must never masquerade as an observed one-shot failure.
				t.Fatalf("one-shot child missed its execution bound: %v", ctx.Err())
			}
			cancel()
			code := watcherExitCode(t, err)
			out, diag := stdout.String(), stderr.String()
			if code != tc.code || requests.Load() != 1 || (tc.diagnostic != "" && !strings.Contains(diag, tc.diagnostic)) {
				t.Fatalf("exit=%d requests=%d output=%q error=%q, want %d/%q", code, requests.Load(), out, diag, tc.code, tc.diagnostic)
			}
			if tc.code == 0 && diag != "" {
				t.Fatalf("successful invocation reported error: %q", diag)
			}
			switch tc.name {
			case "success", "disabled-state", "late-state-write":
				if !strings.Contains(out, " 2026-10-09T12:00:00Z 42 42 ") || !strings.HasSuffix(out, " * new\n") {
					t.Fatalf("expected literal new observation: %q", out)
				}
			case "not-modified", "repeat-observation":
				if !conditional.Load() || !strings.HasSuffix(out, " unchanged\n") || strings.Contains(out, " * new\n") {
					t.Fatalf("conditional/repeated path not exercised: %q", out)
				}
			case "closed-output":
				if out != "" || !strings.Contains(diag, "write sample:") {
					t.Fatalf("expected both report failures: %q / %q", out, diag)
				}
			}
			if tc.name == "late-state-write" {
				if !lateFault.Load() {
					t.Fatal("write fault was not installed after polling started")
				}
				return
			}
			wantState := tc.code == 0 && tc.name != "disabled-state" || tc.closed
			if wantState {
				body, err := os.ReadFile(path)
				var saved state
				if err != nil || json.Unmarshal(body, &saved) != nil || saved.LastRawSSN != 42 || saved.EWMA != 42 || !saved.EWMAInitialized || saved.ETag != `"fixture-tag"` {
					t.Fatalf("state attempt diverged: %s / %v", body, err)
				}
			} else if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("failed/disabled poll unexpectedly wrote state: %v", err)
			}
		})
	}
}

func TestWatcherContinuousRetryActualMain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	var requests atomic.Int32
	third := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := requests.Add(1)
		if n == 2 {
			fmt.Fprint(w, watcherFixtureBody)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		if n == 3 {
			close(third)
		}
	}))
	t.Cleanup(server.Close)
	cmd, stdout, stderr, _, cancel := watcherMainCommand(t, []string{"-url", server.URL, "-timeout", "1s", "-interval", "20ms", "-state", path}, false)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); close(done) }()
	// Cleanup owns cancellation/join even if an assertion fails before the witness.
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("continuous test child failed to join")
		}
	})
	select {
	case <-third:
	case err := <-done:
		t.Fatalf("continuous watcher stopped before retry witness: %v", err)
	case <-time.After(4 * time.Second):
		t.Fatal("continuous watcher did not retry")
	}
	cancel() // Intentional fixture termination, not graceful-retirement evidence.
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("continuous watcher child did not join")
	}
	body, err := os.ReadFile(path)
	var saved state
	if err != nil || json.Unmarshal(body, &saved) != nil || saved.LastRawSSN != 42 || requests.Load() < 3 || !strings.Contains(stdout.String(), " * new\n") || !strings.Contains(stderr.String(), "unexpected status 500") {
		t.Fatalf("retry evidence missing: requests=%d state=%s err=%v stdout=%q stderr=%q", requests.Load(), body, err, stdout.String(), stderr.String())
	}
}
