package peer

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

func retryV14WireManager(t *testing.T) *Manager {
	t.Helper()
	cfg := completeProtocolTestConfig(config.PeeringConfig{
		MaxPeers: 1, Backoff: config.PeeringBackoff{BaseMS: 2000, MaxMS: 300000},
		Timeouts: config.PeeringTimeouts{LoginSeconds: 1, InitSeconds: 2, IdleSeconds: 5},
		Peers:    []config.PeeringPeer{{Enabled: true, Direction: config.PeeringPeerDirectionInbound, RemoteCallsign: "N1PEER", PreferPC9x: true, Password: "correct"}},
	}, "N0CALL")
	m, err := NewManager(cfg, "N0CALL", nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.pc18Banner = "GoCluster Version: test"
	old := retryV14Candidate(t, m, "N1PEER")
	m.sessions.Set(old.id, old)
	m.retryRefused(old, admissionAuthority, time.Now().Add(-3*time.Second))
	m.protocol.drainFailures()
	m.mu.Lock()
	m.retryRetireLocked(old, time.Now())
	m.sessions.Delete(old.id)
	m.mu.Unlock()
	if err := m.Start(t.Context()); err != nil {
		m.Stop()
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	return m
}

func retryV14WireCandidate(t *testing.T, m *Manager) (*session, net.Conn, *bufio.Reader, <-chan error) {
	t.Helper()
	server, client := net.Pipe()
	s := newSession(server, dirInbound, m, PeerEndpoint{host: "pipe"}, m.sessionSettings(PeerEndpoint{}))
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() { result <- s.Run(); close(done) }()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("retry candidate did not retire after cancellation")
		}
	})
	return s, client, bufio.NewReader(client), result
}

func TestPC92V14RetryAuthenticationBoundary(t *testing.T) {
	m := retryV14WireManager(t)
	m.mu.RLock()
	before := *m.retryIdentityLocked("N1PEER")
	m.mu.RUnlock()
	for i := range 8 {
		_, client, reader, result := retryV14WireCandidate(t, m)
		if got := readSessionWire(t, reader, client); got != "login:" {
			t.Fatalf("login prompt=%q", got)
		}
		call := "N1PEER"
		if i%2 == 0 {
			call = "N9DENIED"
		}
		writeSessionWire(t, client, call)
		if call == "N1PEER" {
			if got := readSessionWire(t, reader, client); got != "password:" {
				t.Fatalf("password prompt=%q", got)
			}
			// A peer that has supplied only the callsign still has no retry
			// ownership or fairness position, even when its cooldown elapsed.
			m.mu.RLock()
			got := *m.retryIdentityLocked("N1PEER")
			m.mu.RUnlock()
			if got != before {
				t.Fatal("callsign alone acquired retry ownership")
			}
			writeSessionWire(t, client, "wrong")
		}
		select {
		case err := <-result:
			if err == nil || !strings.Contains(err.Error(), "unauthorized") {
				t.Fatalf("authentication failure=%v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("invalid authentication retained candidate")
		}
		m.mu.RLock()
		got := *m.retryIdentityLocked("N1PEER")
		m.mu.RUnlock()
		if got != before {
			t.Fatal("denied arrival changed retry history, due time or ownership")
		}
	}
	s, client, reader, _ := retryV14WireCandidate(t, m)
	if got := readSessionWire(t, reader, client); got != "login:" {
		t.Fatal(got)
	}
	writeSessionWire(t, client, "N1PEER")
	if got := readSessionWire(t, reader, client); got != "password:" {
		t.Fatal(got)
	}
	writeSessionWire(t, client, "correct")
	if got := readSessionWire(t, reader, client); !strings.HasPrefix(got, "PC18^") {
		t.Fatalf("authenticated retry did not start: %q", got)
	}
	m.mu.RLock()
	r := *m.retryIdentityLocked("N1PEER")
	m.mu.RUnlock()
	if r.owner != s || !r.granted || r.delay != 2*time.Second {
		t.Fatal("authenticated startup failed ownership or reset history prematurely")
	}
}

func TestPC92V14RetryDirectionRace(t *testing.T) {
	for range 8 {
		m := retryV14WireManager(t)
		s, client, reader, result := retryV14WireCandidate(t, m)
		if got := readSessionWire(t, reader, client); got != "login:" {
			t.Fatal(got)
		}
		writeSessionWire(t, client, "N1PEER")
		if got := readSessionWire(t, reader, client); got != "password:" {
			t.Fatal(got)
		}
		type dialResult struct {
			generation uint64
			accepted   bool
		}
		reserved := make(chan dialResult, 1)
		start := make(chan struct{})
		go func() {
			<-start
			generation, ok := m.reserveRetryDial("N1PEER", time.Now())
			reserved <- dialResult{generation, ok}
		}()
		close(start)
		writeSessionWire(t, client, "correct")
		dial := <-reserved
		if dial.accepted {
			select {
			case err := <-result:
				if err == nil || !strings.Contains(err.Error(), "duplicate recovery") {
					t.Fatalf("inbound loser=%v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("inbound loser did not release candidate")
			}
			m.mu.RLock()
			r := *m.retryIdentityLocked("N1PEER")
			m.mu.RUnlock()
			if !r.dialing || r.owner != nil || r.generation != dial.generation || r.delay != 2*time.Second {
				t.Fatal("inbound loser changed outbound ownership/history")
			}
			// Cancel the reservation without counting a transport failure.
			m.mu.Lock()
			m.interruptRetriesLocked(time.Now())
			m.mu.Unlock()
			m.finishRetryDial("N1PEER", dial.generation, nil, time.Now())
		} else {
			if got := readSessionWire(t, reader, client); !strings.HasPrefix(got, "PC18^") {
				t.Fatal(got)
			}
			m.mu.RLock()
			r := *m.retryIdentityLocked("N1PEER")
			m.mu.RUnlock()
			if r.owner != s || r.dialing || !r.granted || r.delay != 2*time.Second {
				t.Fatal("outbound loser changed inbound ownership/history")
			}
		}
		_ = client.Close()
	}
}

func TestPC92V14RetryCanceledWaitDoesNotFailAttempt(t *testing.T) {
	p, old, _, _, base := recoveryV12Owner(t)
	m := p.manager
	m.retryRefused(old, admissionAuthority, base)
	next := retryV14Ready(t, p, old, base, true)
	ctx, cancel := context.WithCancel(context.Background())
	next.ctx, next.cancel = ctx, cancel
	cancel()
	err := m.waitRetryStartup(next, time.Now().Add(time.Second))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait=%v", err)
	}
	m.retrySessionEnded(next, base.Add(time.Second))
	m.mu.Lock()
	m.retryRetireLocked(next, base.Add(time.Second))
	r := *m.retryIdentityLocked(old.remoteCall)
	m.mu.Unlock()
	if r.delay != time.Second || r.failed || r.owner != nil || r.ready {
		t.Fatal("ungranted cancellation counted failure or retained candidate")
	}
}

func TestPC92V14RetryDialFailure(t *testing.T) {
	m := retryV14WireManager(t)
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	failed := make(chan struct{}, 1)
	observeConnectionEvents(t, m, func(event ConnectionEvent) {
		if event.Action == "dial_failed" {
			select {
			case failed <- struct{}{}:
			default:
			}
		}
	})
	done := make(chan struct{})
	go func() {
		m.runOutbound(PeerEndpoint{host: "127.0.0.1", port: port, remoteCall: "N1PEER", loginCall: "N0CALL", preferPC9x: true})
		close(done)
	}()
	t.Cleanup(func() {
		m.cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("failed-dial loop did not join")
		}
	})
	select {
	case <-failed:
	case <-time.After(3 * time.Second):
		t.Fatal("actual TCP dial failure not observed")
	}
	m.mu.RLock()
	r := *m.retryIdentityLocked("N1PEER")
	lastGrant := m.retry.lastGrant
	m.mu.RUnlock()
	if r.delay != 4*time.Second || r.dialing || r.owner != nil || !lastGrant.IsZero() {
		t.Fatal("real TCP refusal missed shared backoff advance or consumed startup grant")
	}
}
