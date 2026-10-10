package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func serveProbeRecovery(listener net.Listener, accepted func(net.Conn), stages chan<- int) error {
	for session := 0; session < 2; session++ {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		accepted(conn)
		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			return err
		}
		if _, err := io.WriteString(conn, "PC22^\r\nPC51^N2WQ-33^LOOPBACK^1^\r\n"); err != nil {
			return err
		}
		reader := bufio.NewReader(conn)
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("loopback session %d ack %q: %w", session, line, err)
		}
		if line != "PC51^LOOPBACK^N2WQ-33^0^\r\n" {
			return fmt.Errorf("loopback session %d ack = %q", session, line)
		}
		stages <- session
		if session == 1 {
			line, err = reader.ReadString('\n')
			if err != nil {
				return fmt.Errorf("loopback BYE %q: %w", line, err)
			}
			if line != "BYE\r\n" {
				return fmt.Errorf("loopback BYE = %q", line)
			}
		}
		if err := conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

// This fixture admits only two actual TCP sessions, and checks old owners at
// the next real DialContext boundary rather than after the successor connects.
type probeLoopbackFixture struct {
	mu       sync.Mutex
	peers    []net.Conn
	owners   []*probeRetryAttempt
	attempts atomic.Int32
	tickers  *fixtureProbeTickers
}

func (f *probeLoopbackFixture) rememberPeer(conn net.Conn) {
	f.mu.Lock()
	f.peers = append(f.peers, conn)
	f.mu.Unlock()
}

func (f *probeLoopbackFixture) closeConnections() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, conn := range f.peers {
		_ = conn.Close()
	}
	for _, owner := range f.owners {
		_ = owner.conn.Close()
	}
}

func (f *probeLoopbackFixture) runtime(expectedAddress string) probeRuntime {
	var dialer net.Dialer
	return probeRuntime{dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" || address != expectedAddress {
			return nil, probeWireError("non-loopback dial refused")
		}
		attempt := f.attempts.Add(1)
		if attempt == 1 {
			return nil, probeWireError("first local dial")
		}
		if attempt > 3 {
			return nil, probeWireError("unexpected extra loopback dial")
		}
		f.mu.Lock()
		if len(f.owners) > 0 {
			old := f.owners[len(f.owners)-1]
			select {
			case <-old.conn.closed:
			default:
				f.mu.Unlock()
				return nil, probeWireError("loopback old socket still owned")
			}
			if old.ticker == nil {
				f.mu.Unlock()
				return nil, probeWireError("loopback old keeper missing")
			}
			select {
			case <-old.ticker.stopped:
			default:
				f.mu.Unlock()
				return nil, probeWireError("loopback old keeper still owned")
			}
		}
		f.mu.Unlock()
		conn, err := dialer.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		owner := &probeRetryAttempt{conn: newFixtureProbeConn(conn)}
		f.mu.Lock()
		f.owners = append(f.owners, owner)
		f.mu.Unlock()
		return owner.conn, nil
	}, newTicker: func(interval time.Duration) probeTicker {
		ticker := f.tickers.newTicker(interval).(*fixtureProbeTicker)
		f.mu.Lock()
		f.owners[len(f.owners)-1].ticker = ticker
		f.mu.Unlock()
		return ticker
	}}
}

func TestProbeLoopbackRecoveryAndLocalSessionCancellation(t *testing.T) {
	guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
	defer endGuard()
	ctx, cancel := context.WithCancel(guard)
	defer cancel()
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(guard, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	fixture := &probeLoopbackFixture{tickers: newFixtureProbeTickers()}
	stages := make(chan int, 2)
	serverResult, serverDone := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(serverDone)
		serverResult <- serveProbeRecovery(listener, fixture.rememberPeer, stages)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		fixture.closeConnections()
		select {
		case <-serverDone:
		case <-time.After(probeFixtureBound):
			t.Error("loopback server owner did not join")
		}
	})
	runtime := fixture.runtime(listener.Addr().String())
	cfg := fixtureProbeConfig()
	cfg.host = "127.0.0.1"
	cfg.port = listener.Addr().(*net.TCPAddr).Port
	cfg.reconnect = true
	result := launchProbeCommand(ctx, t, cfg, runtime, func() {
		cancel()
		fixture.closeConnections()
	})
	if stage := awaitProbe(guard, t, stages, "first actual loopback establishment"); stage != 0 {
		t.Fatalf("first stage = %d", stage)
	}
	first := awaitProbe(guard, t, fixture.tickers.created, "first loopback ticker")
	if stage := awaitProbe(guard, t, stages, "recovered actual loopback establishment"); stage != 1 {
		t.Fatalf("second stage = %d", stage)
	}
	second := awaitProbe(guard, t, fixture.tickers.created, "second loopback ticker")
	cancel()
	if err := awaitProbe(guard, t, serverResult, "literal loopback BYE/server result"); err != nil {
		t.Fatal(err)
	}
	awaitProbe(guard, t, serverDone, "loopback server completion")
	if err := awaitProbe(guard, t, result, "locally canceled loopback command"); err != nil {
		t.Fatal(err)
	}
	awaitProbe(guard, t, first.stopped, "first loopback ticker retirement")
	awaitProbe(guard, t, second.stopped, "second loopback ticker retirement")
	if fixture.attempts.Load() != 3 || fixture.tickers.count.Load() != 2 {
		t.Fatalf("loopback attempts/tickers = %d/%d", fixture.attempts.Load(), fixture.tickers.count.Load())
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.owners) != 2 {
		t.Fatalf("loopback owners = %d", len(fixture.owners))
	}
	for _, owner := range fixture.owners {
		awaitProbe(guard, t, owner.conn.closed, "loopback socket retirement")
	}
}
