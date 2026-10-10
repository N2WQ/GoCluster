package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/peer"
)

func TestProbeTimestampAllocationWaitsForFlush(t *testing.T) {
	for _, first := range []string{"keeper", "init"} {
		t.Run(first, func(t *testing.T) {
			clock := newProbeTimestampClock(probeNoonUTC)
			var owner atomic.Pointer[probeSession]
			var missingClockLock, missingFlushLock atomic.Bool
			now := func() time.Time {
				s := owner.Load()
				if s.wireMu.TryLock() {
					missingClockLock.Store(true)
					s.wireMu.Unlock()
				}
				return clock.now()
			}
			tickers := newFixtureProbeTickers()
			tickers.unbuffered = true
			f := newProbeFixtureWithTimestamp(t, fixtureProbeConfig(), func(c *fixtureProbeConn) {
				c.writeFault = func(data []byte) (int, error, bool) {
					if strings.HasPrefix(string(data), "PC92^") {
						s := owner.Load()
						if s.wireMu.TryLock() {
							missingFlushLock.Store(true)
							s.wireMu.Unlock()
						}
					}
					return 0, nil, false
				}
			}, tickers, peer.NewTimestampGenerator(), now)
			owner.Store(f.session)
			// The first A clock and actual Write have no keeper/raw contender or
			// fixture-held mutex. Their TryLock witnesses cannot be accidental.
			initializeTimestampPeer(t, f, "43200", "43200.01")
			if missingClockLock.Load() || missingFlushLock.Load() {
				t.Fatal("uncontended publication did not own allocation through actual Flush")
			}
			f.send(t, "PC22^\r\n")
			ticker := awaitProbe(f.guard, t, tickers.created, "ordered PC9x keeper")
			f.syncReader(t)
			for len(f.conn.writeEntered) > 0 {
				<-f.conn.writeEntered
			}
			const late = "PC18^remote^pc9x^\r\n"
			if first == "keeper" {
				consumeTimestampTick(f.guard, t, ticker)
			} else {
				f.send(t, late)
			}
			requireProbeWrite(t, f, "PC92^N2WQ-33^43200.02^")
			if first == "keeper" {
				f.send(t, late)
				f.delivered(t, late)
			} else {
				consumeTimestampTick(f.guard, t, ticker)
			}
			if clock.calls.Load() != 3 || f.conn.writers.Load() != 1 {
				t.Fatal("contender allocated before the blocked Flush completed")
			}
			requireTimestampRecord(t, f, "43200.02", first == "init")
			next, completions := 3, 0
			for i := 0; i < 3; i++ {
				line := f.line(t, "PC")
				if line == "PC20^\r\n" {
					completions++
					continue
				}
				add := first == "keeper" && next == 3
				if line != literalProbePC92(noonProbeStamp(next), add) {
					t.Fatalf("post-Flush order/payload = %q at %d", line, next)
				}
				next++
			}
			if next != 5 || completions != 1 || clock.calls.Load() != 5 || f.conn.overlap.Load() {
				t.Fatal("ordered publications lost a record or overlapped")
			}
			f.syncReader(t)
			f.cancel()
			f.line(t, "BYE\r\n")
			f.finish(t, context.Canceled)
			requireProbeStopped(t, f, ticker)
		})
	}
}

func TestProbeTimestampFaultRetiresWaitingProducer(t *testing.T) {
	for _, fault := range []string{"clock", "write"} {
		for _, first := range []string{"keeper", "init"} {
			t.Run(fault+"/"+first, func(t *testing.T) { timestampWaitingFault(t, fault, first) })
		}
	}
}

func timestampWaitingFault(t *testing.T, fault, first string) {
	t.Helper()
	clock := newProbeTimestampClock(probeNoonUTC)
	entered, release := make(chan struct{}), make(chan struct{})
	defer releaseProbeBarrier(release)
	want := probeWireError("PC92 publication")
	if fault == "clock" {
		want = peer.ErrTimestampClock
	}
	now := func() time.Time {
		value := clock.now()
		if fault == "clock" && clock.calls.Load() == 3 {
			close(entered)
			<-release
			return time.Time{}
		}
		return value
	}
	tickers := newFixtureProbeTickers()
	tickers.unbuffered = true
	f := newProbeFixtureWithTimestamp(t, fixtureProbeConfig(), func(c *fixtureProbeConn) {
		c.writeFault = func(data []byte) (int, error, bool) {
			if fault == "write" && strings.Contains(string(data), "^43200.02^") {
				close(entered)
				<-release
				return 0, want, true
			}
			return 0, nil, false
		}
	}, tickers, peer.NewTimestampGenerator(), now)
	initializeTimestampPeer(t, f, "43200", "43200.01")
	f.send(t, "PC22^\r\n")
	ticker := awaitProbe(f.guard, t, tickers.created, "fault PC9x keeper")
	f.syncReader(t)
	f.readerAfterSync(t)
	var missingCancelLock atomic.Bool
	firstCancel := make(chan error, 1)
	trueCancel := f.session.cancel
	f.session.cancel = func(err error) {
		if context.Cause(f.session.ctx) == nil {
			if f.session.wireMu.TryLock() {
				missingCancelLock.Store(true)
				f.session.wireMu.Unlock()
			}
			firstCancel <- err
		}
		trueCancel(err) // Never abort a worker while it owns the gate.
	}
	const late = "PC18^remote^pc9x^\r\n"
	if first == "keeper" {
		consumeTimestampTick(f.guard, t, ticker)
	} else {
		f.send(t, late)
	}
	awaitProbe(f.guard, t, entered, "parked actual allocation/write fault")
	if first == "keeper" {
		f.send(t, late)
		f.delivered(t, late)
	} else {
		consumeTimestampTick(f.guard, t, ticker)
	}
	close(release)
	if got := awaitProbe(f.guard, t, firstCancel, "first causal cancellation under publication gate"); !errors.Is(got, want) {
		t.Fatalf("first cancellation = %v, want %v", got, want)
	}
	if fault == "clock" {
		f.line(t, "BYE\r\n")
	}
	f.finish(t, want)
	requireProbeStopped(t, f, ticker)
	if missingCancelLock.Load() || clock.calls.Load() != 3 {
		t.Fatal("first fault unlocked before cancel or waiter allocated afterward")
	}
	pc92 := 0
	for _, line := range f.conn.writes() {
		if strings.HasPrefix(line, "PC92^") {
			pc92++
		}
	}
	wantRecords := 2
	if fault == "write" {
		wantRecords++
	}
	if pc92 != wantRecords {
		t.Fatalf("fault admitted waiting record: %q", f.conn.writes())
	}
}

func TestProbeTimestampFirstFailureCancelsBeforeUnlock(t *testing.T) {
	for _, fault := range []string{"clock", "write"} {
		t.Run(fault, func(t *testing.T) {
			guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
			defer endGuard()
			conn := newFixtureProbeConn(phaseMemoryConn{})
			want := peer.ErrTimestampClock
			now := func() time.Time { return time.Time{} }
			if fault == "write" {
				want = probeWireError("uncontended write")
				now = func() time.Time { return probeNoonUTC }
				conn.writeFault = func([]byte) (int, error, bool) { return 0, want, true }
			}
			s := newProbeSession(guard, fixtureProbeConfig(), conn, peer.NewTimestampGenerator(), now, newProbeTicker)
			t.Cleanup(func() {
				s.cancel(context.Canceled)
				if err := conn.Close(); err != nil {
					t.Errorf("close uncontended fault transport: %v", err)
				}
			})
			missingLock := false
			trueCancel := s.cancel
			s.cancel = func(err error) {
				if context.Cause(s.ctx) == nil && s.wireMu.TryLock() {
					missingLock = true
					s.wireMu.Unlock()
				}
				trueCancel(err)
			}
			if _, err := s.sendPC92(true); !errors.Is(err, want) || !errors.Is(context.Cause(s.ctx), want) {
				t.Fatalf("uncontended first cause = %v / %v", err, context.Cause(s.ctx))
			}
			if missingLock {
				t.Fatal("uncontended first cancellation occurred after publication unlock")
			}
			if guard.Err() != nil {
				t.Fatal("watchdog substituted for uncontended timestamp failure")
			}
		})
	}
}
