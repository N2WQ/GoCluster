package main

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"dxcluster/peer"
)

// This transport has no reader or clock-driven producer. The actual protocol
// consumer still writes through its ordinary buffered writer and deadline gate.
// At most 32 markers produce 64 writes and three deadline events, so the shared
// fixture's 128-entry channels cannot block a transition or its keeper join.
type phaseMemoryConn struct{}

func (phaseMemoryConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (phaseMemoryConn) Write(p []byte) (int, error)      { return len(p), nil }
func (phaseMemoryConn) Close() error                     { return nil }
func (phaseMemoryConn) SetDeadline(time.Time) error      { return nil }
func (phaseMemoryConn) SetReadDeadline(time.Time) error  { return nil }
func (phaseMemoryConn) SetWriteDeadline(time.Time) error { return nil }
func (phaseMemoryConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (phaseMemoryConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }

type phaseMarkerSequence struct {
	lines     []string
	firstInit int
	firstPC22 int
	firstEnd  int
}

// Expected event positions follow the marker contract, independently of the
// implementation's state fields: first init marker, PC22, or post-init spot.
func phaseMarkers(data []byte) phaseMarkerSequence {
	markers := [...]string{"PC18^remote^legacy^", "PC16^remote^", "PC17^remote^",
		"PC19^remote^", "PC21^remote^", "PC22^", "PC11^remote^spot^", "PC61^remote^spot^"}
	if len(data) > 32 {
		data = data[:32]
	}
	s := phaseMarkerSequence{firstInit: len(data), firstPC22: len(data), firstEnd: len(data)}
	for i, value := range data {
		marker := int(value) % len(markers)
		s.lines = append(s.lines, markers[marker])
		if marker <= 4 && s.firstInit == len(data) {
			s.firstInit = i
		}
		if marker == 5 && s.firstPC22 == len(data) {
			s.firstPC22 = i
		}
		if s.firstEnd == len(data) && (marker == 5 || (marker >= 6 && s.firstInit < i)) {
			s.firstEnd = i
		}
	}
	return s
}

func FuzzProbePhaseTransitions(f *testing.F) {
	f.Add([]byte{0, 0, 1, 2, 3, 4, 5, 0, 5})
	f.Add([]byte{5, 0, 5, 6, 0})
	f.Add([]byte{6, 0, 7, 5, 0, 5})
	f.Add([]byte{1, 2, 3, 4, 6, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		sequence := phaseMarkers(data)
		guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
		cfg := fixtureProbeConfig()
		cfg.idleTimeout = 3 * time.Hour
		conn := newFixtureProbeConn(phaseMemoryConn{})
		tickers := newFixtureProbeTickers()
		session := newProbeSession(guard, cfg, conn, peer.NewTimestampGenerator(), time.Now, tickers.newTicker)
		t.Cleanup(func() {
			defer endGuard()
			session.cancel(context.Canceled)
			if session.keeperDone != nil {
				select {
				case <-session.keeperDone:
				case <-time.After(probeFixtureBound):
					t.Error("transition fixture could not join its keeper")
				}
			}
			if err := conn.Close(); err != nil {
				t.Errorf("close transition fixture transport: %v", err)
			}
		})
		p := probeProtocol{session: session}
		before := time.Now()
		if err := p.startPhase(cfg.loginTimeout + cfg.initTimeout); err != nil {
			t.Fatal(err)
		}
		initial := phaseEvent(t, conn, "phase", 0)
		requireFuzzPhaseInterval(t, initial, before, time.Now(), cfg.loginTimeout+cfg.initTimeout)
		active := initial.when
		for i, line := range sequence.lines {
			before = time.Now()
			priorWrites := len(conn.entryTrace())
			if err := p.handleLine(line); err != nil {
				t.Fatalf("marker %d %q: %v", i, line, err)
			}
			if i == sequence.firstInit && i < sequence.firstEnd {
				init := phaseEvent(t, conn, "phase", 1)
				requireFuzzPhaseInterval(t, init, before, time.Now(), cfg.initTimeout)
				entries := conn.entryTrace()
				if len(entries) <= priorWrites || init.order >= entries[priorWrites].order {
					t.Fatalf("first init budget did not precede its output: %+v / %+v", init, entries)
				}
				active = init.when
			}
			if i == sequence.firstEnd {
				active = time.Time{}
			}
			requireFuzzPhasePrefix(t, &p, conn, tickers, sequence, i, active)
		}
		if guard.Err() != nil {
			t.Fatalf("transition fixture exceeded its own guard: %v", guard.Err())
		}
		session.cancel(context.Canceled)
		if session.keeperDone != nil {
			awaitProbe(guard, t, session.keeperDone, "transition keeper join")
			ticker := awaitProbe(guard, t, tickers.created, "transition ticker ownership")
			awaitProbe(guard, t, ticker.stopped, "transition ticker Stop")
			if ticker.stops.Load() != 1 {
				t.Fatalf("transition ticker Stop count = %d", ticker.stops.Load())
			}
		}
	})
}

func requireFuzzPhaseInterval(t *testing.T, event probeDeadlineEvent, before, after time.Time, duration time.Duration) {
	t.Helper()
	if event.err != nil || event.when.Before(before.Add(duration)) || event.when.After(after.Add(duration)) {
		t.Fatalf("phase is not the fixed transition-time budget: %+v / %s / %s", event, before, after)
	}
}

func requireFuzzPhasePrefix(t *testing.T, p *probeProtocol, conn *fixtureProbeConn, tickers *fixtureProbeTickers, sequence phaseMarkerSequence, i int, active time.Time) {
	t.Helper()
	wantPhases, wantClear, wantKeeper := 1, 0, int32(0)
	if sequence.firstInit <= i && sequence.firstInit < sequence.firstEnd {
		wantPhases++
	}
	if sequence.firstEnd <= i {
		wantClear = 1
	}
	if sequence.firstPC22 <= i {
		wantKeeper = 1
	}
	if phaseCount(conn, "phase") != wantPhases || phaseCount(conn, "clear") != wantClear || tickers.count.Load() != wantKeeper {
		t.Fatalf("marker %d renewed phase/clear or duplicated keeper: %+v / %d", i, conn.deadlineTrace(), tickers.count.Load())
	}
	if !p.phaseDeadline.Equal(active) {
		t.Fatalf("marker %d changed fixed phase identity: %s, want %s", i, p.phaseDeadline, active)
	}
	if wantClear != 0 {
		clear := phaseEvent(t, conn, "clear", 0)
		if !clear.when.IsZero() || clear.err != nil {
			t.Fatalf("marker %d did not clear the actual write deadline: %+v", i, clear)
		}
	}
	now := time.Now()
	wantRead := active
	if active.IsZero() {
		wantRead = now.Add(p.session.cfg.idleTimeout)
	} else if err := p.admitIngress(active); !errors.Is(err, errProbeHandshakeTimeout) {
		t.Fatalf("marker %d admitted the exact fixed expiry: %v", i, err)
	}
	if got := p.readDeadline(now); !got.Equal(wantRead) {
		t.Fatalf("marker %d read bound = %s, want %s", i, got, wantRead)
	}
}
