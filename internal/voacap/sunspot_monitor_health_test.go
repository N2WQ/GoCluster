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

type countTestLogger struct{ messages []string }

func (l *countTestLogger) Printf(format string, args ...any) {
	l.messages = append(l.messages, fmt.Sprintf(format, args...))
}
func TestSunspotMonitorHealthEveryOutcomeAndRecovery(t *testing.T) {
	for _, outcome := range []string{"304", "error", "malformed", "same_body"} {
		t.Run(outcome, func(t *testing.T) {
			var phase atomic.Int32
			at := mustTime(t, "2026-06-08T20:00:00Z")
			body := "[" + countRow("2026-06-08T20:00:00", "1", "1", "90", "2") + "]"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				switch phase.Load() {
				case 0:
					fmt.Fprint(w, body)
				case 1:
					switch outcome {
					case "304":
						w.WriteHeader(http.StatusNotModified)
					case "error":
						w.WriteHeader(http.StatusServiceUnavailable)
					case "malformed":
						fmt.Fprint(w, "bad")
					default:
						fmt.Fprint(w, body)
					}
				default:
					fmt.Fprint(w, "["+countRow("2026-06-10T20:00:00", "1", "1", "92", "2")+"]")
				}
			}))
			defer server.Close()
			cfg := testSolarMonitorConfig(server)
			logger := &countTestLogger{}
			cfg.Logger = logger
			m, err := NewSunspotMonitor(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err = m.Poll(context.Background(), at.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			phase.Store(1)
			_ = m.Poll(context.Background(), at.Add(48*time.Hour-time.Nanosecond))
			if m.Snapshot().RecentStale {
				t.Fatal("early stale")
			}
			_ = m.Poll(context.Background(), at.Add(48*time.Hour))
			if !m.Snapshot().RecentStale || len(logger.messages) != 1 {
				t.Fatalf("missing transition %+v %v", m.Snapshot(), logger.messages)
			}
			_ = m.Poll(context.Background(), at.Add(49*time.Hour))
			if len(logger.messages) != 1 {
				t.Fatalf("repeated alarm %v", logger.messages)
			}
			phase.Store(2)
			if err = m.Poll(context.Background(), at.Add(49*time.Hour)); err != nil {
				t.Fatal(err)
			}
			if m.Snapshot().RecentStale || len(logger.messages) != 2 || !strings.Contains(logger.messages[1], "recovered") {
				t.Fatalf("missing recovery %+v %v", m.Snapshot(), logger.messages)
			}
		})
	}
}
func TestSunspotMonitorMissingDoesNotInventAgeOrZero(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "[]") }))
	defer server.Close()
	m := newTestSunspotMonitor(t, server)
	at := mustTime(t, "2026-06-08T20:00:00Z")
	for _, now := range []time.Time{at, at.Add(72 * time.Hour)} {
		if err := m.Poll(context.Background(), now); err == nil {
			t.Fatal("absence accepted")
		}
		s := m.Snapshot()
		if !s.RecentMissing || s.RecentStale || !s.LastObservedAtUTC.IsZero() || s.ForecastInitialized {
			t.Fatalf("missing became observed %+v", s)
		}
	}
}
