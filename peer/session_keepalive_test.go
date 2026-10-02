package peer

import (
	"bufio"
	"context"
	"strings"
	"testing"
	"time"
)

func TestKeepaliveLoopSendsPC51ForLegacy(t *testing.T) {
	s, remote := newTransportTestSession(t)
	s.keepalive = 20 * time.Millisecond
	s.startWorker(s.writerLoop)
	s.startWorker(s.keepaliveLoop)
	got := readSessionWire(t, bufio.NewReader(remote), remote)
	if got != "PC51^N1REM^N0CALL^1^" {
		t.Fatalf("wire=%q", got)
	}
}

func TestKeepaliveLoopIndependentTimers(t *testing.T) {
	for _, tc := range []struct {
		name           string
		keep, config   time.Duration
		want, unwanted string
	}{
		{"config with keepalive disabled", 0, 30 * time.Millisecond, "C", "K"},
		{"keepalive with config disabled", 30 * time.Millisecond, 0, "K", "C"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager, _ := newInboundHarnessManager(t, inboundScenario{name: t.Name()})
			s, remote := newTransportTestSession(t)
			s.manager = manager
			s.pc9x = true
			s.keepalive = tc.keep
			s.configEvery = tc.config
			s.id = "N1REM"
			if err := manager.trackCandidate(s); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { manager.releaseCandidate(s); manager.unregisterSession(s) })
			if err := manager.establishSession(s); err != nil {
				t.Fatal(err)
			}
			s.startWorker(s.writerLoop)
			reader := bufio.NewReader(remote)
			// Establishment commits before its scheduled mandatory recovery.
			// Observe that one-shot pair before asserting periodic suppression.
			for _, action := range []string{"C", "A"} {
				if got := readSessionWire(t, reader, remote); !pc92TypeLine(action).match(got) {
					t.Fatalf("want recovery %s, got %q", action, got)
				}
			}
			s.startWorker(s.keepaliveLoop)
			for {
				got := readSessionWire(t, reader, remote)
				if strings.HasPrefix(got, "PC92^") {
					f, err := ParseFrame(got)
					if err != nil {
						t.Fatal(err)
					}
					if f.Fields[2] == tc.unwanted {
						t.Fatalf("disabled periodic action on wire: %q", got)
					}
					if f.Fields[2] == tc.want {
						break
					}
				}
			}
		})
	}
}

func TestPriorityLaneSaturationClosesSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &session{ctx: ctx, cancel: cancel, priorityLineCh: make(chan string, 1)}
	if err := s.sendControlLine("occupied"); err != nil {
		t.Fatal(err)
	}
	if err := s.sendControlLine("next"); err == nil {
		t.Fatal("full queue accepted control")
	}
	if ctx.Err() == nil {
		t.Fatal("full control lane did not close session")
	}
}
