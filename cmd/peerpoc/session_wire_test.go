package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"dxcluster/peer"
)

func TestProbeCancelStalledLineProducers(t *testing.T) {
	for _, producer := range []string{"login", "password", "init", "ack", "keeper"} {
		t.Run(producer, func(t *testing.T) {
			cfg := fixtureProbeConfig()
			cfg.autoLogin = producer == "login" || producer == "password"
			f := newProbeFixture(t, cfg, nil, nil)
			var ticker *fixtureProbeTicker
			var blocked string
			switch producer {
			case "login":
				blocked = requireProbeWrite(t, f, "N2WQ-33\r\n")
			case "password":
				requireProbeWrite(t, f, "N2WQ-33\r\n")
				f.line(t, "N2WQ-33")
				blocked = requireProbeWrite(t, f, "secret\r\n")
			case "init":
				f.send(t, "PC18^remote^legacy^\r\n")
				blocked = requireProbeWrite(t, f, "PC19^")
			case "ack":
				f.send(t, "PC51^N2WQ-33^REMOTE^1^\r\n")
				blocked = requireProbeWrite(t, f, "PC51^REMOTE^N2WQ-33^0^")
			case "keeper":
				f.send(t, "PC22^\r\n")
				ticker = awaitProbe(f.guard, t, f.tickers.created, "keeper ownership")
				ticker.ticks <- time.Now()
				blocked = requireProbeWrite(t, f, "PC51^N2WQ-33^N2WQ-33^1^")
			}
			// Ordinary deadlines are hours away. Cancellation follows actual
			// transport Write entry, and no fixture closes the blocked socket.
			f.cancel()
			f.finish(t, context.Canceled)
			if ticker != nil {
				requireProbeStopped(t, f, ticker)
			}
			found := false
			for len(f.conn.writeExited) > 0 {
				exit := <-f.conn.writeExited
				if exit.data == blocked {
					found = true
					if exit.closed {
						t.Fatal("socket Close rescued stalled producer instead of interruption")
					}
				}
			}
			if !found {
				t.Fatal("blocked transport writer did not exit")
			}
		})
	}
}

func TestProbeCancelStalledReaderAndRawCallback(t *testing.T) {
	for _, raw := range []bool{false, true} {
		t.Run(map[bool]string{false: "reader", true: "raw-reply"}[raw], func(t *testing.T) {
			f := newProbeFixture(t, fixtureProbeConfig(), nil, nil)
			awaitProbe(f.guard, t, f.conn.readEntered, "actual reader entry")
			if raw {
				f.send(t, string([]byte{255, 251, 1})+"PC51^N2WQ-33^AFTER^1^\r\n")
				if got := requireProbeWrite(t, f, string([]byte{255})); got != string([]byte{255, 254, 1}) {
					t.Fatalf("Telnet reply = %v", []byte(got))
				}
			}
			f.cancel()
			f.finish(t, context.Canceled)
			for _, write := range f.conn.writes() {
				if strings.Contains(write, "AFTER") {
					t.Fatalf("buffered line dispatched after failed callback: %q", write)
				}
			}
		})
	}
}

func TestProbeShortAndPartialWritesRetainFirstCause(t *testing.T) {
	for _, producer := range []string{"line", "raw"} {
		for _, fault := range []string{"short-nil", "partial-error"} {
			t.Run(producer+"/"+fault, func(t *testing.T) {
				want := io.ErrShortWrite
				if fault == "partial-error" {
					want = probeWireError(fault)
				}
				cfg := fixtureProbeConfig()
				cfg.autoLogin = producer == "line"
				f := newProbeFixture(t, cfg, func(conn *fixtureProbeConn) {
					conn.writeFault = func(p []byte) (int, error, bool) {
						if fault == "short-nil" {
							return len(p) - 1, nil, true
						}
						return 1, want, true
					}
				}, nil)
				if producer == "raw" {
					f.send(t, string([]byte{255, 251, 1})+"PC51^N2WQ-33^AFTER^1^\r\n")
				}
				awaitProbe(f.guard, t, f.conn.writeEntered, "faulting producer entry")
				f.finish(t, want)
				if len(f.conn.writes()) != 1 {
					t.Fatalf("failed producer continued publication: %q", f.conn.writes())
				}
				if errors.Is(context.Cause(f.session.ctx), context.Canceled) {
					t.Fatal("induced cancellation replaced the causal failure")
				}
			})
		}
	}
}

func TestProbeRawAndBufferedLinesShareWriter(t *testing.T) {
	for _, pc9x := range []bool{false, true} {
		t.Run(modeLabel(pc9x), func(t *testing.T) { probeRawAndBufferedLinesShareWriter(t, pc9x) })
	}
}

func probeRawAndBufferedLinesShareWriter(t *testing.T, pc9x bool) {
	t.Helper()
	clock := newProbeTimestampClock(probeNoonUTC)
	tickers := newFixtureProbeTickers()
	tickers.unbuffered = true
	f := newProbeFixtureWithTimestamp(t, fixtureProbeConfig(), nil, tickers, peer.NewTimestampGenerator(), clock.now)
	if pc9x {
		initializeTimestampPeer(t, f, "43200", "43200.01")
		f.syncReader(t)
	}
	for len(f.conn.writeEntered) > 0 {
		<-f.conn.writeEntered
	}
	f.send(t, "PC22^\r\n")
	ticker := awaitProbe(f.guard, t, f.tickers.created, "keeper creation")
	consumeTimestampTick(f.guard, t, ticker)
	keeperPrefix := "PC51^N2WQ-33^N2WQ-33^1^\r\n"
	if pc9x {
		keeperPrefix = literalProbePC92("43200.02", false)
	}
	keeper := requireProbeWrite(t, f, keeperPrefix)
	// Enter the real reader/parser while the keeper holds the writer. The
	// negotiation reply and buffered ping must follow the complete keeper.
	negotiation := string([]byte{255, 251, 1}) + "PC51^N2WQ-33^AFTER^1^\r\n"
	f.send(t, negotiation)
	f.delivered(t, negotiation)
	if got := f.line(t, keeperPrefix); got != keeper {
		t.Fatalf("keeper frame interleaved: %q", got)
	}
	raw := requireProbeWrite(t, f, string([]byte{255}))
	if raw != string([]byte{255, 254, 1}) {
		t.Fatalf("raw reply = %v", []byte(raw))
	}
	bytes := make([]byte, 3)
	if _, err := io.ReadFull(f.wire, bytes); err != nil || string(bytes) != raw {
		t.Fatalf("raw wire = %v / %v", bytes, err)
	}
	if got := f.line(t, "PC51^AFTER^N2WQ-33^0^"); got != "PC51^AFTER^N2WQ-33^0^\r\n" {
		t.Fatalf("post-negotiation ack = %q", got)
	}
	f.cancel()
	if got := f.line(t, "BYE"); got != "BYE\r\n" {
		t.Fatalf("BYE = %q", got)
	}
	f.finish(t, context.Canceled)
	requireProbeStopped(t, f, ticker)
	if f.conn.overlap.Load() {
		t.Fatal("raw and buffered publishers concurrently entered transport Write")
	}
}

func TestProbeCancelNegotiationBehindBlockedKeeper(t *testing.T) {
	f := newProbeFixture(t, fixtureProbeConfig(), nil, nil)
	f.send(t, "PC22^\r\n")
	ticker := awaitProbe(f.guard, t, f.tickers.created, "keeper creation")
	f.syncReader(t)
	f.readerAfterSync(t)
	requireProbeWrite(t, f, "PC51^SYNC^")
	ticker.ticks <- time.Now()
	keeper := requireProbeWrite(t, f, "PC51^N2WQ-33^N2WQ-33^1^")
	negotiation := string([]byte{255, 251, 1}) + "PC51^N2WQ-33^AFTER^1^\r\n"
	f.send(t, negotiation)
	f.delivered(t, negotiation)
	// Delivery enters the real parser with an already-entered keeper writer.
	// It does not itself identify the scheduler instant of callback entry;
	// the literal wire/retirement checks and missing-mutex negative control
	// own serialization evidence instead of a scheduling-delay assertion.
	f.cancel()
	f.finish(t, context.Canceled)
	requireProbeStopped(t, f, ticker)
	if f.conn.overlap.Load() {
		t.Fatal("callback overlapped the keeper writer")
	}
	for _, write := range f.conn.writes() {
		if strings.Contains(write, "AFTER") {
			t.Fatalf("post-cancel buffered dispatch: %q", write)
		}
	}
	found := false
	for len(f.conn.writeExited) > 0 {
		exit := <-f.conn.writeExited
		if exit.data == keeper {
			found = true
			if exit.closed {
				t.Fatal("fixture Close rescued keeper/callback retirement")
			}
		}
	}
	if !found {
		t.Fatal("blocked keeper transport did not exit")
	}
}
