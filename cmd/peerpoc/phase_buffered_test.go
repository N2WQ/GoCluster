package main

import (
	"context"
	"errors"
	"io"
	"testing"
)

func releaseProbeBarrier(release chan struct{}) {
	select {
	case <-release:
	default:
		close(release)
	}
}

func TestProbeAcquiredPhaseInputExpiresBeforeDispatch(t *testing.T) {
	for _, marker := range []string{"PC18", "PC22", "PC11", "PC61"} {
		for _, expired := range []bool{false, true} {
			name := marker + "/before"
			if expired {
				name = marker + "/expired"
			}
			t.Run(name, func(t *testing.T) {
				payload := marker + "^remote^legacy^\r\n"
				release := make(chan struct{})
				defer releaseProbeBarrier(release)
				acquired := make(chan probeReadReturn, 1)
				f := newProbeFixture(t, phaseProbeConfig(), func(c *fixtureProbeConn) {
					c.afterRead = func(receipt probeReadReturn) {
						if receipt.data == payload {
							acquired <- receipt
							<-release
						}
					}
				}, nil)
				active := phaseFirstRead(t, f)
				spot := marker == "PC11" || marker == "PC61"
				if spot {
					f.initialize(t, false)
					active = phaseEvent(t, f.conn, "phase", 1)
				}
				beforeWrites := len(f.conn.writes())
				f.send(t, payload)
				receipt := awaitProbe(f.guard, t, acquired, "actual acquired marker bytes")
				if receipt.err != nil || !receipt.at.Before(active.when) {
					t.Fatalf("marker was not acquired before expiry: %+v / %+v", receipt, active)
				}
				if expired {
					awaitPhaseTime(t, f, active.when)
				}
				if f.session.ctx.Err() != nil {
					t.Fatal("socket deadline or fixture canceled acquired-data barrier")
				}
				if expired {
					// The protocol is parked after acquisition, so these are only
					// earlier initialization receipts, never new marker effects.
					for len(f.conn.writeEntered) > 0 {
						<-f.conn.writeEntered
					}
				}
				close(release)
				if expired {
					requireAcquiredExpiryBeforeDispatch(t, f, receipt.entry)
					if len(f.conn.writes()) != beforeWrites || phaseCount(f.conn, "clear") != 0 || f.tickers.count.Load() != 0 {
						t.Fatalf("expired acquired marker dispatched: writes %q, deadlines %+v, tickers %d", f.conn.writes(), f.conn.deadlineTrace(), f.tickers.count.Load())
					}
					return
				}
				if marker == "PC18" {
					f.line(t, "PC19^")
					f.line(t, "PC20^\r\n")
				}
				var ticker *fixtureProbeTicker
				if marker == "PC22" {
					ticker = awaitProbe(f.guard, t, f.tickers.created, "positive acquired PC22 keeper")
				}
				f.syncReader(t)
				f.cancel()
				if ticker != nil {
					f.line(t, "BYE\r\n")
				}
				f.finish(t, context.Canceled)
				if ticker != nil {
					requireProbeStopped(t, f, ticker)
				}
				wantClear := 1
				if marker == "PC18" {
					wantClear = 0
				}
				if phaseCount(f.conn, "clear") != wantClear {
					t.Fatalf("positive marker clear count = %d", phaseCount(f.conn, "clear"))
				}
			})
		}
	}
}

func requireAcquiredExpiryBeforeDispatch(t *testing.T, f *probeFixture, acquired int64) {
	t.Helper()
	for {
		select {
		case <-f.session.ctx.Done():
			if f.guard.Err() != nil || !errors.Is(context.Cause(f.session.ctx), errProbeHandshakeTimeout) {
				t.Fatalf("acquired expiry cause = %v, guard = %v", context.Cause(f.session.ctx), f.guard.Err())
			}
			f.finish(t, errProbeHandshakeTimeout)
			return
		case output := <-f.conn.writeEntered:
			t.Fatalf("expired acquired marker entered output: %q", output)
		case <-f.tickers.created:
			t.Fatal("expired acquired marker acquired keeper")
		case entry := <-f.conn.readEntered:
			if entry > acquired {
				t.Fatal("expired acquired marker admitted the next actual read")
			}
		case <-f.guard.Done():
			t.Fatalf("acquired expiry exceeded fixture guard: %v", f.guard.Err())
		}
	}
}

func TestProbeActualReadErrorWinsLaterPhaseClockCheck(t *testing.T) {
	release := make(chan struct{})
	defer releaseProbeBarrier(release)
	acquired := make(chan probeReadReturn, 1)
	f := newProbeFixture(t, phaseProbeConfig(), func(c *fixtureProbeConn) {
		c.afterRead = func(receipt probeReadReturn) {
			if errors.Is(receipt.err, io.EOF) {
				acquired <- receipt
				<-release
			}
		}
	}, nil)
	phase := phaseFirstRead(t, f)
	// This Close deliberately injects EOF. It is not credited as natural expiry.
	if err := f.peer.Close(); err != nil {
		t.Fatal(err)
	}
	receipt := awaitProbe(f.guard, t, acquired, "underlying EOF acquired before phase deadline")
	if !receipt.at.Before(phase.when) {
		t.Fatal("EOF control missed its pre-expiry acquisition")
	}
	awaitPhaseTime(t, f, phase.when)
	close(release)
	f.finish(t, io.EOF)
}
