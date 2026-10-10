package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"dxcluster/peer"
)

func TestProbeDeadlineGateOrdersOrdinarySetterAndInterruption(t *testing.T) {
	for _, first := range []string{"ordinary", "interrupt"} {
		t.Run(first, func(t *testing.T) {
			guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
			defer endGuard()
			ctx, cancel := context.WithCancelCause(guard)
			defer cancel(context.Canceled)
			client, remote := net.Pipe()
			defer remote.Close()
			defer client.Close()
			conn := newFixtureProbeConn(client)
			transport := &probeTransport{conn: conn, ctx: ctx}
			entered, release := make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			if first == "ordinary" {
				conn.readSetter = func(time.Time) error { close(entered); <-release; return nil }
			} else {
				conn.interrupt = func(time.Time) error { close(entered); <-release; return nil }
			}
			readerDone := make(chan error, 1)
			interruptDone := make(chan struct{})
			var workers sync.WaitGroup
			t.Cleanup(func() {
				cancel(context.Canceled)
				select {
				case <-release:
				default:
					close(release)
				}
				_ = remote.Close()
				_ = conn.Close()
				joined := make(chan struct{})
				go func() { workers.Wait(); close(joined) }()
				select {
				case <-joined:
				case <-time.After(probeFixtureBound):
					t.Error("deadline fixture could not join started workers")
				}
			})
			read := func() {
				defer workers.Done()
				reader := peer.NewLineReader(transport, 4096, 0, nil)
				_, err := reader.ReadLine(time.Now().Add(time.Hour))
				readerDone <- err
			}
			interrupt := func() { defer workers.Done(); transport.interrupt(); close(interruptDone) }
			if first == "ordinary" {
				workers.Add(1)
				go read()
				awaitProbe(guard, t, entered, "ordinary setter entered")
				cancel(context.Canceled)
				workers.Add(1)
				go interrupt()
			} else {
				cancel(context.Canceled)
				workers.Add(1)
				go interrupt()
				awaitProbe(guard, t, entered, "interrupt setter entered")
				workers.Add(1)
				go read()
			}
			close(release)
			awaitProbe(guard, t, interruptDone, "deadline interrupt completion")
			if err := awaitProbe(guard, t, readerDone, "real reader return"); err == nil {
				t.Fatal("reader succeeded after cancellation")
			}
			if err := transport.SetReadDeadline(time.Now().Add(time.Hour)); !errors.Is(err, context.Canceled) {
				t.Fatalf("post-cancel ordinary setter = %v", err)
			}
			conn.mu.Lock()
			trace := append([]probeDeadlineEvent(nil), conn.deadlines...)
			conn.mu.Unlock()
			interrupted := false
			for _, event := range trace {
				if event.kind == "interrupt" {
					interrupted = true
					if event.when.After(time.Now()) {
						t.Fatal("interrupt installed future deadline")
					}
				} else if interrupted {
					t.Fatalf("ordinary setter revived I/O after interruption: %+v", trace)
				}
			}
			if !interrupted {
				t.Fatal("missing underlying cancellation deadline")
			}
		})
	}
}

func TestProbePreCanceledStartupDoesNotPublishOrReviveIO(t *testing.T) {
	guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
	defer endGuard()
	ctx, cancel := context.WithCancel(guard)
	cancel()
	client, remote := net.Pipe()
	defer remote.Close()
	defer client.Close()
	conn := newFixtureProbeConn(client)
	cfg := fixtureProbeConfig()
	cfg.autoLogin = true
	session := newProbeSession(ctx, cfg, conn, peer.NewTimestampGenerator(), time.Now, newProbeTicker)
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() { defer close(done); result <- session.run() }()
	t.Cleanup(func() {
		_ = remote.Close()
		_ = conn.Close()
		select {
		case <-done:
		case <-time.After(probeFixtureBound):
			t.Error("pre-canceled session helper did not join")
		}
	})
	if err := awaitProbe(guard, t, result, "pre-canceled session retirement"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cause = %v", err)
	}
	awaitProbe(guard, t, session.interruptDone, "pre-canceled interruptor join")
	awaitProbe(guard, t, done, "pre-canceled session helper completion")
	if len(conn.writes()) != 0 {
		t.Fatal("pre-canceled session wrote credentials")
	}
	for _, event := range conn.deadlines {
		if event.kind != "interrupt" || event.when.After(time.Now()) {
			t.Fatalf("pre-canceled I/O installed ordinary future deadline: %+v", event)
		}
	}
}
