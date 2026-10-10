package main

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/peer"
)

func TestProbeKeeperFailuresRetireReader(t *testing.T) {
	for _, fault := range []string{"write", "timestamp"} {
		t.Run(fault, func(t *testing.T) {
			want := probeWireError("keeper")
			var enabled atomic.Bool
			f := newProbeFixture(t, fixtureProbeConfig(), func(c *fixtureProbeConn) {
				c.writeFault = func(p []byte) (int, error, bool) {
					if enabled.Load() {
						return 1, want, true
					}
					return 0, nil, false
				}
			}, nil)
			f.initialize(t, fault == "timestamp")
			f.send(t, "PC22^\r\n")
			ticker := awaitProbe(f.guard, t, f.tickers.created, "keeper creation")
			f.syncReader(t)
			f.readerAfter(t, "PC51^N2WQ-33^SYNC^1^\r\n")
			if fault == "timestamp" {
				want = peer.ErrTimestampClock
				if _, err := f.session.timestamps.NextAt(time.Now().Add(24 * time.Hour)); err != nil {
					t.Fatalf("real generator future control: %v", err)
				}
			} else {
				enabled.Store(true)
			}
			ticker.ticks <- time.Now()
			if fault == "timestamp" {
				if got := f.line(t, "BYE"); got != "BYE\r\n" {
					t.Fatalf("timestamp-failure BYE = %q", got)
				}
			}
			f.finish(t, want)
			requireProbeStopped(t, f, ticker)
			if fault == "write" {
				for _, write := range f.conn.writes() {
					if write == "BYE\r\n" {
						t.Fatal("failed buffered writer was replaced to attempt BYE")
					}
				}
			}
			if f.guard.Err() != nil {
				t.Fatal("watchdog cancellation was credited as keeper retirement")
			}
		})
	}
}

func TestProbeSingleKeeperCapturesModeAndRetainsLateInit(t *testing.T) {
	for _, initialPC9x := range []bool{false, true} {
		t.Run(modeLabel(initialPC9x), func(t *testing.T) {
			f := newProbeFixture(t, fixtureProbeConfig(), nil, nil)
			f.initialize(t, initialPC9x)
			f.send(t, "PC22^\r\nPC22^again^\r\nPC22^again^\r\n")
			ticker := awaitProbe(f.guard, t, f.tickers.created, "first PC22 keeper")
			f.syncReader(t)
			f.initialize(t, !initialPC9x)
			f.send(t, "PC22^after-late-init^\r\n")
			f.syncReader(t)
			if f.tickers.count.Load() != 1 {
				t.Fatalf("owned ticker count = %d", f.tickers.count.Load())
			}
			ticker.ticks <- time.Now()
			if initialPC9x {
				line := f.line(t, "PC92^N2WQ-33^")
				if !strings.Contains(line, "^K^5N2WQ-33:5457^1^0^H99^\r\n") {
					t.Fatalf("captured PC9x keeper = %q", line)
				}
			} else if got := f.line(t, "PC51^"); got != "PC51^N2WQ-33^N2WQ-33^1^\r\n" {
				t.Fatalf("captured legacy keeper = %q", got)
			}
			f.cancel()
			f.line(t, "BYE\r\n")
			f.finish(t, context.Canceled)
			requireProbeStopped(t, f, ticker)
			if f.tickers.count.Load() != 1 {
				t.Fatal("duplicate worker survived repeated PC22")
			}
		})
	}
}

func TestProbeSpotEstablishmentAndFirstPC22HaveSeparateOwners(t *testing.T) {
	for _, laterPC22 := range []bool{false, true} {
		t.Run(map[bool]string{false: "spot-only", true: "spot-to-first-PC22"}[laterPC22], func(t *testing.T) {
			f := newProbeFixture(t, fixtureProbeConfig(), nil, nil)
			f.initialize(t, false)
			f.send(t, "PC11^14074^K1ABC^1200^spot^N2WQ^H99^\r\n")
			f.syncReader(t)
			if f.tickers.count.Load() != 0 {
				t.Fatal("spot-only establishment started a keeper")
			}
			var ticker *fixtureProbeTicker
			if laterPC22 {
				f.send(t, "PC22^\r\nPC22^again^\r\n")
				ticker = awaitProbe(f.guard, t, f.tickers.created, "post-spot first-PC22 keeper")
				f.syncReader(t)
				if f.tickers.count.Load() != 1 {
					t.Fatalf("post-spot ticker count = %d", f.tickers.count.Load())
				}
			}
			f.cancel()
			if laterPC22 {
				f.line(t, "BYE\r\n")
			}
			f.finish(t, context.Canceled)
			if laterPC22 {
				requireProbeStopped(t, f, ticker)
			} else {
				for _, write := range f.conn.writes() {
					if write == "BYE\r\n" {
						t.Fatal("spot-only establishment acquired BYE ownership")
					}
				}
			}
		})
	}
}
