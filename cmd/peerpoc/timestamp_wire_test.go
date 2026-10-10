package main

import (
	"context"
	"testing"
	"time"

	"dxcluster/peer"
)

func TestProbeWireHundredValuesThenRateRefusal(t *testing.T) {
	clock := newProbeTimestampClock(probeNoonUTC)
	r := newTimestampCommand(t, 3, clock.now, nil)
	first := r.next(t)
	initializeTimestampPeer(t, first, "43200", "43200.01")
	first.send(t, "PC22^\r\n")
	ticker := awaitProbe(r.guard, t, r.tickers.created, "hundred-value keeper")
	first.syncReader(t)
	for i := 2; i < 100; i++ {
		consumeTimestampTick(r.guard, t, ticker)
		requireTimestampRecord(t, first, noonProbeStamp(i), false)
	}
	for len(first.conn.writeEntered) > 0 {
		<-first.conn.writeEntered
	}
	consumeTimestampTick(r.guard, t, ticker)
	requireTimestampRefusal(t, r, first, peer.ErrTimestampRate)
	awaitProbe(r.guard, t, ticker.stopped, "rate refusal keeper retirement")
	retry := r.next(t)
	retry.send(t, "PC18^remote^pc9x^\r\n")
	requireTimestampRefusal(t, r, retry, peer.ErrTimestampRate)
	if len(retry.conn.writes()) != 0 {
		t.Fatal("same-second retry published PC92")
	}
	clock.unix.Store(probeNoonUTC.Unix() + 1)
	recovered := r.next(t)
	initializeTimestampPeer(t, recovered, "43201", "43201.01")
	recovered.syncReader(t)
	if clock.calls.Load() != 104 {
		t.Fatalf("bounded publication/refusal clock calls = %d", clock.calls.Load())
	}
	r.stop(t, recovered)
}

func TestProbeWireUTCMidnightWrap(t *testing.T) {
	clock := newProbeTimestampClock(time.Date(2000, 1, 1, 23, 59, 59, 0, time.UTC))
	tickers := newFixtureProbeTickers()
	tickers.unbuffered = true
	f := newProbeFixtureWithTimestamp(t, fixtureProbeConfig(), nil, tickers, peer.NewTimestampGenerator(), clock.now)
	initializeTimestampPeer(t, f, "86399", "86399.01")
	f.send(t, "PC22^\r\n")
	ticker := awaitProbe(f.guard, t, tickers.created, "midnight keeper")
	f.syncReader(t)
	clock.unix.Add(1)
	consumeTimestampTick(f.guard, t, ticker)
	requireTimestampRecord(t, f, "0", false)
	clock.unix.Add(1)
	consumeTimestampTick(f.guard, t, ticker)
	requireTimestampRecord(t, f, "1", false)
	f.cancel()
	f.line(t, "BYE\r\n")
	f.finish(t, context.Canceled)
	requireProbeStopped(t, f, ticker)
	if clock.calls.Load() != 4 {
		t.Fatalf("midnight clock calls = %d", clock.calls.Load())
	}
}

// These direct connected-session controls pair swallowed retry diagnostics with
// errors.Is on the actual session owner, after real successful publications.
func TestProbeTimestampRefusalsHaveExactSessionCause(t *testing.T) {
	for _, kind := range []string{"rate", "clock"} {
		t.Run(kind, func(t *testing.T) {
			clock := newProbeTimestampClock(probeNoonUTC)
			tickers := newFixtureProbeTickers()
			tickers.unbuffered = true
			f := newProbeFixtureWithTimestamp(t, fixtureProbeConfig(), nil, tickers, peer.NewTimestampGenerator(), clock.now)
			initializeTimestampPeer(t, f, "43200", "43200.01")
			f.send(t, "PC22^\r\n")
			ticker := awaitProbe(f.guard, t, tickers.created, "exact-cause keeper")
			f.syncReader(t)
			want := peer.ErrTimestampClock
			if kind == "rate" {
				want = peer.ErrTimestampRate
				for i := 2; i < 100; i++ {
					consumeTimestampTick(f.guard, t, ticker)
					requireTimestampRecord(t, f, noonProbeStamp(i), false)
				}
			} else {
				clock.unix.Add(-1)
			}
			consumeTimestampTick(f.guard, t, ticker)
			if got := f.line(t, "BYE"); got != "BYE\r\n" {
				t.Fatalf("timestamp cause BYE = %q", got)
			}
			f.finish(t, want)
			requireProbeStopped(t, f, ticker)
			if f.guard.Err() != nil {
				t.Fatal("watchdog substituted for exact timestamp cause")
			}
		})
	}
}
