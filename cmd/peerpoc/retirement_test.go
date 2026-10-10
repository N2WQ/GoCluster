package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestProbeRetirementJoinsEachWorkerBeforeBye(t *testing.T) {
	for _, releaseFirst := range []string{"keeper", "interruptor"} {
		t.Run(releaseFirst, func(t *testing.T) {
			stopRelease := make(chan struct{})
			interruptRelease := make(chan struct{})
			interruptEntered := make(chan struct{})
			var earlyBye atomic.Bool
			var session *probeSession
			defer func() {
				select {
				case <-stopRelease:
				default:
					close(stopRelease)
				}
				select {
				case <-interruptRelease:
				default:
					close(interruptRelease)
				}
			}()
			tickers := newFixtureProbeTickers()
			tickers.stopRelease = stopRelease
			f := newProbeFixture(t, fixtureProbeConfig(), func(c *fixtureProbeConn) {
				c.byeSetter = func(time.Time) error {
					select {
					case <-session.keeperDone:
					default:
						earlyBye.Store(true)
					}
					select {
					case <-session.interruptDone:
					default:
						earlyBye.Store(true)
					}
					return nil
				}
				c.interrupt = func(when time.Time) error {
					// Release actual I/O first, then park the interruptor itself.
					if err := c.Conn.SetDeadline(when); err != nil {
						return err
					}
					close(interruptEntered)
					<-interruptRelease
					return nil
				}
			}, tickers)
			session = f.session
			f.send(t, "PC22^\r\n")
			ticker := awaitProbe(f.guard, t, f.tickers.created, "keeper creation")
			f.syncReader(t)
			f.cancel()
			awaitProbe(f.guard, t, ticker.stopEntered, "parked keeper Stop")
			awaitProbe(f.guard, t, interruptEntered, "parked interruptor")
			if releaseFirst == "keeper" {
				close(stopRelease)
				awaitProbe(f.guard, t, ticker.stopped, "released keeper Stop")
			} else {
				close(interruptRelease)
				awaitProbe(f.guard, t, f.session.interruptDone, "released interruptor")
			}
			requireProbeNotClosed(t, f.done, "session retirement")
			requireProbeNotClosed(t, f.conn.closed, "socket retirement")
			for _, write := range f.conn.writes() {
				if write == "BYE\r\n" {
					t.Fatal("BYE entered before both workers joined")
				}
			}
			if releaseFirst == "keeper" {
				close(interruptRelease)
			} else {
				close(stopRelease)
			}
			if got := f.line(t, "BYE"); got != "BYE\r\n" {
				t.Fatalf("BYE = %q", got)
			}
			f.finish(t, context.Canceled)
			requireProbeStopped(t, f, ticker)
			if earlyBye.Load() {
				t.Fatal("BYE setter entered before actual worker completion")
			}
		})
	}
}

func TestProbeByeIsWritableOrBoundedAfterLocalCancel(t *testing.T) {
	for _, writable := range []bool{false, true} {
		t.Run(map[bool]string{false: "withheld", true: "writable"}[writable], func(t *testing.T) {
			cfg := fixtureProbeConfig()
			cfg.keepalive = 0
			f := newProbeFixture(t, cfg, nil, nil)
			f.send(t, "PC22^\r\n")
			f.syncReader(t)
			// Drain the synchronization publication receipts before observing BYE.
			requireProbeWrite(t, f, "PC51^SYNC^")
			awaitProbe(f.guard, t, f.conn.writeExited, "sync write completion")
			f.cancel()
			start := time.Now()
			requireProbeWrite(t, f, "BYE\r\n")
			if writable {
				f.line(t, "BYE\r\n")
			}
			f.finish(t, context.Canceled)
			if elapsed := time.Since(start); elapsed > 4*time.Second {
				t.Fatalf("BYE exceeded existing two-second bound: %v", elapsed)
			}
			exit := awaitProbe(f.guard, t, f.conn.writeExited, "BYE transport exit")
			if exit.data != "BYE\r\n" || exit.closed {
				t.Fatalf("BYE was rescued by fixture/socket Close: %+v", exit)
			}
		})
	}
}

func TestProbeDeadlineSetterFailuresFallBackToRetirement(t *testing.T) {
	for _, setter := range []string{"read", "interrupt", "bye"} {
		t.Run(setter, func(t *testing.T) {
			fault := probeWireError(setter + " deadline")
			cfg := fixtureProbeConfig()
			cfg.keepalive = 0
			f := newProbeFixture(t, cfg, func(c *fixtureProbeConn) {
				switch setter {
				case "read":
					c.readSetter = func(time.Time) error { return fault }
				case "interrupt":
					c.interrupt = func(time.Time) error { return fault }
				case "bye":
					c.byeSetter = func(time.Time) error { return fault }
				}
			}, nil)
			want := fault
			if setter != "read" {
				f.send(t, "PC22^\r\n")
				f.syncReader(t)
				f.cancel()
				want = context.Canceled
			}
			f.finish(t, want)
			for _, write := range f.conn.writes() {
				if write == "BYE\r\n" {
					t.Fatal("unsafe BYE attempted after setter failure")
				}
			}
		})
	}
}
