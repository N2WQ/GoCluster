package peer

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
)

type timedConnectionEvent struct {
	event ConnectionEvent
	at    time.Time
}

func awaitTCPConnectionEvent(t *testing.T, events <-chan timedConnectionEvent, action string) time.Time {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case got := <-events:
			if got.event.Action == action {
				return got.at
			}
		case <-timer.C:
			t.Fatalf("outbound loop did not report %s", action)
		}
	}
}

func TestOutboundTCPBackoffAfterEstablishedDisconnect(t *testing.T) {
	for _, tc := range []struct {
		name       string
		baseMS     int
		maxMS      int
		initialBad int
		base       time.Duration
		max        time.Duration
		dialFail   bool
	}{
		{"positive_exponential", 100, 800, 3, 100 * time.Millisecond, 800 * time.Millisecond, true},
		{"normalized_nonpositive", 0, 1, 1, time.Second, time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			addr := *listener.Addr().(*net.TCPAddr)
			t.Cleanup(func() { _ = listener.Close() })
			if tc.dialFail {
				_ = listener.Close()
			}
			cfg := completeProtocolTestConfig(config.PeeringConfig{
				Backoff:  config.PeeringBackoff{BaseMS: tc.baseMS, MaxMS: tc.maxMS},
				Timeouts: config.PeeringTimeouts{LoginSeconds: 2, InitSeconds: 2},
			}, "N0CALL")
			m, err := NewManager(cfg, "N0CALL", nil, 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			events := make(chan timedConnectionEvent, 64)
			var dropped atomic.Bool
			m.SetConnectionReporter(func(event ConnectionEvent) {
				select {
				case events <- timedConnectionEvent{event: event, at: time.Now()}:
				default:
					dropped.Store(true)
				}
			})
			if err = m.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			loopDone := make(chan struct{})
			ep := PeerEndpoint{host: "127.0.0.1", port: addr.Port, remoteCall: "N1REM", loginCall: "N0CALL", family: config.PeeringPeerFamilyDXSpider, preferPC9x: true}
			go func() { m.runOutbound(ep); close(loopDone) }()
			t.Cleanup(func() {
				m.cancel()
				_ = listener.Close()
				select {
				case <-loopDone:
				case <-time.After(2 * time.Second):
					t.Error("cancellation did not interrupt real outbound retry")
				}
				m.Stop()
			})
			var previous time.Time
			delay := tc.base
			if tc.dialFail {
				previous = awaitTCPConnectionEvent(t, events, "dial_failed")
				listener, err = net.ListenTCP("tcp", &addr)
				if err != nil {
					t.Fatal(err)
				}
			}
			accept := func(minimum time.Duration, after time.Time) (net.Conn, *bufio.Reader, time.Time) {
				t.Helper()
				if err = listener.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				conn, err := listener.Accept()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = conn.Close() })
				at := awaitTCPConnectionEvent(t, events, "connected")
				if !after.IsZero() && at.Sub(after) < minimum-time.Millisecond {
					t.Fatalf("TCP retry arrived after %s; required delay %s", at.Sub(after), minimum)
				}
				return conn, bufio.NewReader(conn), at
			}
			for range tc.initialBad {
				conn, reader, _ := accept(delay, previous)
				if !previous.IsZero() {
					delay = min(delay*2, tc.max)
				}
				if login := readSessionWire(t, reader, conn); login != "N0CALL" {
					t.Fatalf("login=%q", login)
				}
				_ = conn.Close() // real TCP handshake failure, without establishment
				previous = awaitTCPConnectionEvent(t, events, "rejected")
			}
			conn, reader, _ := accept(delay, previous)
			readOutboundInit(t, conn, reader)
			writeSessionWire(t, conn, "PC22^")
			awaitTCPConnectionEvent(t, events, "established")
			for _, action := range []string{"C", "A"} {
				if line := readSessionWire(t, reader, conn); !pc92TypeLine(action).match(line) {
					t.Fatalf("recovery %s=%q", action, line)
				}
			}
			_ = conn.Close()
			previous = awaitTCPConnectionEvent(t, events, "disconnected")
			delay = tc.base
			for retry := range 3 {
				conn, reader, at := accept(delay, previous)
				if retry == 0 && tc.max > tc.base && at.Sub(previous) >= 600*time.Millisecond {
					t.Fatalf("established disconnect did not reset capped backoff: %s", at.Sub(previous))
				}
				if login := readSessionWire(t, reader, conn); login != "N0CALL" {
					t.Fatalf("retry login=%q", login)
				}
				_ = conn.Close()
				previous = awaitTCPConnectionEvent(t, events, "rejected")
				delay = min(delay*2, tc.max)
			}
			if dropped.Load() {
				t.Fatal("connection evidence channel overflowed")
			}
			// Cancellation occurs during the next nonzero retry wait. The cleanup
			// joins the real loop and manager, including all transport workers.
		})
	}
}
