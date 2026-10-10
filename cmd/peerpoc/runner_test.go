package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func launchProbeCommand(ctx context.Context, t *testing.T, cfg probeConfig, runtime probeRuntime, cleanup func()) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- runProbeCommand(ctx, cfg, runtime)
	}()
	t.Cleanup(func() {
		cleanup()
		select {
		case <-done:
		case <-time.After(probeFixtureBound):
			t.Error("command owner did not join during failed-test cleanup")
		}
	})
	return result
}

func TestProbeCommandFailuresDoNotBecomeSuccessfulChildCancellation(t *testing.T) {
	for _, faultKind := range []string{"dial", "read", "write", "callback", "keeper"} {
		t.Run(faultKind, func(t *testing.T) {
			guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
			defer endGuard()
			ctx, cancel := context.WithCancel(guard)
			defer cancel()
			client, remote := net.Pipe()
			defer remote.Close()
			defer client.Close()
			conn := newFixtureProbeConn(client)
			if err := remote.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Fatal(err)
			}
			want := probeWireError(faultKind)
			if faultKind == "read" {
				want = io.EOF
			}
			if faultKind == "write" || faultKind == "callback" || faultKind == "keeper" {
				conn.writeFault = func([]byte) (int, error, bool) { return 0, want, true }
			}
			tickers := newFixtureProbeTickers()
			cfg := fixtureProbeConfig()
			cfg.autoLogin = faultKind == "write"
			runtime := probeRuntime{dial: func(context.Context, string, string) (net.Conn, error) {
				if faultKind == "dial" {
					return nil, want
				}
				return conn, nil
			}, newTicker: tickers.newTicker, timestampNow: time.Now}
			result := launchProbeCommand(ctx, t, cfg, runtime, func() {
				cancel()
				_ = remote.Close()
				_ = conn.Close()
			})
			if faultKind != "dial" && faultKind != "write" {
				awaitProbe(guard, t, conn.readEntered, "failing command reader entry")
				switch faultKind {
				case "read":
					if err := remote.Close(); err != nil {
						t.Fatal(err)
					}
				case "callback":
					if _, err := remote.Write([]byte{255, 251, 1}); err != nil {
						t.Fatal(err)
					}
				case "keeper":
					if _, err := io.WriteString(remote, "PC22^\r\n"); err != nil {
						t.Fatal(err)
					}
					ticker := awaitProbe(guard, t, tickers.created, "failing command keeper owner")
					ticker.ticks <- time.Now()
					awaitProbe(guard, t, ticker.stopped, "failing command ticker retirement")
				}
			}
			if err := awaitProbe(guard, t, result, "unsuccessful command result"); !errors.Is(err, want) {
				t.Fatalf("result = %v, want first cause %v", err, want)
			}
			if ctx.Err() != nil {
				t.Fatal("parent cancellation was credited as failure retirement")
			}
			if faultKind != "dial" {
				awaitProbe(guard, t, conn.closed, "failed command socket retirement")
			}
		})
	}
}

func TestProbeRetryJoinsParkedOwnersBeforeNextDial(t *testing.T) {
	guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
	defer endGuard()
	ctx, cancel := context.WithCancel(guard)
	defer cancel()
	client, remote := net.Pipe()
	defer remote.Close()
	defer client.Close()
	conn := newFixtureProbeConn(client)
	if err := remote.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	interruptEntered, interruptRelease := make(chan struct{}), make(chan struct{})
	stopRelease := make(chan struct{})
	defer func() {
		select {
		case <-interruptRelease:
		default:
			close(interruptRelease)
		}
		select {
		case <-stopRelease:
		default:
			close(stopRelease)
		}
	}()
	conn.interrupt = func(time.Time) error {
		close(interruptEntered)
		<-interruptRelease
		return nil
	}
	tickers := newFixtureProbeTickers()
	tickers.stopRelease = stopRelease
	var attempts atomic.Int32
	nextEntered := make(chan struct{})
	nextOwnerResult := make(chan error, 1)
	var oldTicker atomic.Pointer[fixtureProbeTicker]
	runtime := probeRuntime{dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		if attempts.Add(1) == 1 {
			return conn, nil
		}
		var ownerErr error
		select {
		case <-conn.closed:
		default:
			ownerErr = probeWireError("socket still owned at successor entry")
		}
		previousTicker := oldTicker.Load()
		if previousTicker == nil {
			ownerErr = probeWireError("missing old keeper at successor entry")
		} else {
			select {
			case <-previousTicker.stopped:
			default:
				ownerErr = probeWireError("ticker still owned at successor entry")
			}
		}
		nextOwnerResult <- ownerErr
		close(nextEntered)
		<-ctx.Done()
		return nil, ctx.Err()
	}, newTicker: tickers.newTicker, timestampNow: time.Now}
	cfg := fixtureProbeConfig()
	cfg.reconnect = true
	result := launchProbeCommand(ctx, t, cfg, runtime, func() {
		cancel()
		_ = remote.Close()
		_ = conn.Close()
	})
	if _, err := io.WriteString(remote, "PC22^\r\n"); err != nil {
		t.Fatal(err)
	}
	ticker := awaitProbe(guard, t, tickers.created, "old session keeper owner")
	oldTicker.Store(ticker)
	// EOF is an actual session fault, not fixture cleanup or parent timeout.
	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}
	awaitProbe(guard, t, ticker.stopEntered, "old keeper parked Stop")
	awaitProbe(guard, t, interruptEntered, "old interruptor parked setter")
	requireProbeNotClosed(t, nextEntered, "successor dial")
	close(stopRelease)
	awaitProbe(guard, t, ticker.stopped, "old keeper Stop return")
	requireProbeNotClosed(t, nextEntered, "successor dial while interruptor parked")
	close(interruptRelease)
	awaitProbe(guard, t, nextEntered, "successor dial after both owner releases")
	if err := awaitProbe(guard, t, nextOwnerResult, "successor ownership check"); err != nil {
		t.Fatal(err)
	}
	awaitProbe(guard, t, conn.closed, "old socket retired before successor")
	if attempts.Load() != 2 {
		t.Fatalf("dial attempts = %d", attempts.Load())
	}
	cancel()
	if err := awaitProbe(guard, t, result, "reconnect command local cancellation"); err != nil {
		t.Fatal(err)
	}
}

type probeRetryAttempt struct {
	conn   *fixtureProbeConn
	remote net.Conn
	ticker *fixtureProbeTicker
}

func TestProbeDialFailureRecoveryAndBoundedReconnectChurn(t *testing.T) {
	guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
	defer endGuard()
	ctx, cancel := context.WithCancel(guard)
	defer cancel()
	const sessions = 6
	var mu sync.Mutex
	var owners []*probeRetryAttempt
	var attempts atomic.Int32
	connected := make(chan *probeRetryAttempt, sessions)
	tickers := newFixtureProbeTickers()
	runtime := probeRuntime{dial: func(context.Context, string, string) (net.Conn, error) {
		if attempts.Add(1) == 1 {
			return nil, probeWireError("initial dial")
		}
		mu.Lock()
		defer mu.Unlock()
		if len(owners) > 0 {
			previous := owners[len(owners)-1]
			select {
			case <-previous.conn.closed:
			default:
				return nil, probeWireError("old socket not retired")
			}
			if previous.ticker == nil {
				return nil, probeWireError("old keeper not owned")
			}
			select {
			case <-previous.ticker.stopped:
			default:
				return nil, probeWireError("old ticker not stopped")
			}
		}
		if len(owners) >= sessions {
			return nil, probeWireError("unexpected extra attempt")
		}
		client, remote := net.Pipe()
		owner := &probeRetryAttempt{conn: newFixtureProbeConn(client), remote: remote}
		if len(owners) == sessions-1 {
			// Explicitly cancel after the final underlying socket Close returns.
			// This prevents test scheduling from racing one extra retry admission.
			owner.conn.afterClose = cancel
		}
		owners = append(owners, owner)
		connected <- owner
		return owner.conn, nil
	}, newTicker: func(interval time.Duration) probeTicker {
		ticker := tickers.newTicker(interval).(*fixtureProbeTicker)
		mu.Lock()
		owners[len(owners)-1].ticker = ticker
		mu.Unlock()
		return ticker
	}, timestampNow: time.Now}
	cfg := fixtureProbeConfig()
	cfg.reconnect = true
	cfg.backoffBase, cfg.backoffMax = 20*time.Millisecond, 20*time.Millisecond
	result := launchProbeCommand(ctx, t, cfg, runtime, func() {
		cancel()
		mu.Lock()
		defer mu.Unlock()
		for _, owner := range owners {
			_ = owner.remote.Close()
			_ = owner.conn.Close()
		}
	})
	for i := 0; i < sessions; i++ {
		owner := awaitProbe(guard, t, connected, "connected retry session")
		if err := owner.remote.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(owner.remote, "PC22^\r\nPC51^N2WQ-33^SYNC^1^\r\n"); err != nil {
			t.Fatal(err)
		}
		ticker := awaitProbe(guard, t, tickers.created, "retry keeper creation")
		line, err := bufio.NewReader(owner.remote).ReadString('\n')
		if err != nil || line != "PC51^SYNC^N2WQ-33^0^\r\n" {
			t.Fatalf("retry init barrier = %q / %v", line, err)
		}
		if err := owner.remote.Close(); err != nil {
			t.Fatal(err)
		}
		awaitProbe(guard, t, ticker.stopped, "old retry ticker Stop")
		awaitProbe(guard, t, owner.conn.closed, "old retry socket retirement")
	}
	cancel()
	if err := awaitProbe(guard, t, result, "churn command cancellation"); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != sessions+1 || tickers.count.Load() != sessions {
		t.Fatalf("attempts/tickers = %d/%d, want %d/%d", attempts.Load(), tickers.count.Load(), sessions+1, sessions)
	}
}

type probeBackoffReceipt struct {
	entered chan struct{}
	once    sync.Once
}

func (r *probeBackoffReceipt) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte("reconnecting in 1h0m0s")) {
		r.once.Do(func() { close(r.entered) })
	}
	return len(data), nil
}

func TestProbeCancelDuringBackoffStopsAttempts(t *testing.T) {
	guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
	defer endGuard()
	ctx, cancel := context.WithCancel(guard)
	defer cancel()
	entered := make(chan struct{})
	backoff := &probeBackoffReceipt{entered: make(chan struct{})}
	oldOutput := log.Writer()
	log.SetOutput(backoff)
	defer log.SetOutput(oldOutput)
	var attempts atomic.Int32
	runtime := probeRuntime{dial: func(context.Context, string, string) (net.Conn, error) {
		if attempts.Add(1) == 1 {
			close(entered)
		}
		return nil, probeWireError("dial before backoff")
	}, newTicker: newProbeTicker, timestampNow: time.Now}
	cfg := fixtureProbeConfig()
	cfg.reconnect = true
	cfg.backoffBase, cfg.backoffMax = time.Hour, time.Hour
	result := launchProbeCommand(ctx, t, cfg, runtime, cancel)
	awaitProbe(guard, t, entered, "failed initial dial")
	awaitProbe(guard, t, backoff.entered, "actual command backoff announcement")
	cancel()
	if err := awaitProbe(guard, t, result, "backoff cancellation"); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("canceled backoff admitted %d attempts", attempts.Load())
	}
}
