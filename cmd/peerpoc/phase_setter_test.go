package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProbeWriteSetAndClearCannotReviveCanceledIO(t *testing.T) {
	for _, clear := range []bool{false, true} {
		for _, first := range []string{"ordinary", "interrupt"} {
			name := "set/" + first
			if clear {
				name = "clear/" + first
			}
			t.Run(name, func(t *testing.T) {
				guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
				defer endGuard()
				ctx, cancel := context.WithCancelCause(guard)
				defer cancel(context.Canceled)
				client, remote := net.Pipe()
				conn := newFixtureProbeConn(client)
				transport := &probeTransport{conn: conn, ctx: ctx}
				deadline := time.Now().Add(time.Hour)
				if clear {
					deadline = time.Time{}
				}
				entered, release := make(chan struct{}), make(chan struct{})
				defer releaseProbeBarrier(release)
				park := func(time.Time) error { close(entered); <-release; return nil }
				if first == "ordinary" {
					if clear {
						conn.clearSetter = park
					} else {
						conn.phaseSetter = park
					}
				} else {
					conn.interrupt = park
				}
				var workers sync.WaitGroup
				t.Cleanup(func() {
					cancel(context.Canceled)
					releaseProbeBarrier(release)
					if err := remote.Close(); err != nil {
						t.Errorf("close setter fixture peer: %v", err)
					}
					if err := conn.Close(); err != nil {
						t.Errorf("close setter fixture transport: %v", err)
					}
					joined := make(chan struct{})
					go func() { workers.Wait(); close(joined) }()
					select {
					case <-joined:
					case <-time.After(probeFixtureBound):
						t.Error("write setter fixture could not join started workers")
					}
				})
				setDone := make(chan error, 1)
				interruptDone := make(chan struct{})
				set := func() { defer workers.Done(); setDone <- transport.SetWriteDeadline(deadline) }
				interrupt := func() { defer workers.Done(); transport.interrupt(); close(interruptDone) }
				if first == "ordinary" {
					workers.Add(1)
					go set()
					awaitProbe(guard, t, entered, "ordinary write setter entry")
					cancel(context.Canceled)
					workers.Add(1)
					go interrupt()
				} else {
					cancel(context.Canceled)
					workers.Add(1)
					go interrupt()
					awaitProbe(guard, t, entered, "interrupt before ordinary write setter")
					workers.Add(1)
					go set()
				}
				close(release)
				awaitProbe(guard, t, interruptDone, "write deadline interruption completion")
				setErr := awaitProbe(guard, t, setDone, "ordinary write setter completion")
				if first == "ordinary" && setErr != nil {
					t.Fatalf("already-entered ordinary setter = %v", setErr)
				}
				if first == "interrupt" && !errors.Is(setErr, context.Canceled) {
					t.Fatalf("canceled ordinary setter = %v", setErr)
				}
				if err := transport.SetWriteDeadline(deadline); !errors.Is(err, context.Canceled) {
					t.Fatalf("post-interruption set/clear = %v", err)
				}
				interrupted := false
				for _, event := range conn.deadlineTrace() {
					if event.kind == "interrupt" {
						interrupted = true
					} else if interrupted {
						t.Fatalf("ordinary set/clear followed interruption: %+v", conn.deadlineTrace())
					}
				}
				if !interrupted {
					t.Fatal("missing actual underlying interruption")
				}
			})
		}
	}
}

func TestProbePhaseSetterFailurePreventsLaterEffects(t *testing.T) {
	for _, faultAt := range []string{"initial", "init", "read", "clear-PC22", "clear-PC11", "clear-PC61"} {
		t.Run(faultAt, func(t *testing.T) {
			fault := probeWireError(faultAt + " setter")
			var phaseCalls atomic.Int32
			cfg := phaseProbeConfig()
			cfg.autoLogin = faultAt == "initial"
			f := newProbeFixture(t, cfg, func(c *fixtureProbeConn) {
				c.phaseSetter = func(time.Time) error {
					call := phaseCalls.Add(1)
					if faultAt == "initial" || (faultAt == "init" && call == 2) {
						return fault
					}
					return nil
				}
				if faultAt == "read" {
					c.readSetter = func(time.Time) error { return fault }
				}
				if faultAt == "clear-PC22" || faultAt == "clear-PC11" || faultAt == "clear-PC61" {
					c.clearSetter = func(time.Time) error { return fault }
				}
			}, nil)
			wantWrites := 0
			if faultAt != "initial" && faultAt != "read" {
				phaseFirstRead(t, f)
				switch faultAt {
				case "init":
					f.send(t, "PC18^remote^legacy^\r\n")
				case "clear-PC22":
					f.send(t, "PC22^\r\n")
				default:
					f.initialize(t, false)
					wantWrites = 2
					f.send(t, faultAt[len("clear-"):]+"^remote^spot^\r\n")
				}
			}
			f.finish(t, fault)
			if len(f.conn.writes()) != wantWrites || f.tickers.count.Load() != 0 {
				t.Fatalf("failed setter admitted later effects: writes %q / tickers %d", f.conn.writes(), f.tickers.count.Load())
			}
			found := false
			for _, event := range f.conn.deadlineTrace() {
				if errors.Is(event.err, fault) {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing exact failed setter receipt: %+v", f.conn.deadlineTrace())
			}
		})
	}
}
