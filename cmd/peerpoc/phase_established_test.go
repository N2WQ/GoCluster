package main

import (
	"context"
	"testing"
	"time"
)

func TestProbeEstablishmentClearsPhaseAndNeverReopensForLateInit(t *testing.T) {
	for _, path := range []string{"PC22-first", "init-PC22", "init-PC11", "init-PC61", "spot-PC22"} {
		t.Run(path, func(t *testing.T) {
			f := newProbeFixture(t, phaseProbeConfig(), nil, nil)
			former := phaseFirstRead(t, f)
			phases := 1
			if path != "PC22-first" {
				f.initialize(t, false)
				former = phaseEvent(t, f.conn, "phase", 1)
				phases++
			}
			marker := "PC22^\r\n"
			if path == "init-PC11" || path == "spot-PC22" {
				marker = "PC11^remote^spot^\r\n"
			}
			if path == "init-PC61" {
				marker = "PC61^remote^spot^\r\n"
			}
			f.send(t, marker)
			f.syncReader(t)
			clear := phaseEvent(t, f.conn, "clear", 0)
			if !clear.when.IsZero() || clear.err != nil {
				t.Fatalf("establishment did not clear write deadline: %+v", clear)
			}
			var ticker *fixtureProbeTicker
			if path == "spot-PC22" {
				f.send(t, "PC22^\r\nPC22^again^\r\n")
				f.syncReader(t)
			}
			if path == "PC22-first" || path == "init-PC22" || path == "spot-PC22" {
				ticker = awaitProbe(f.guard, t, f.tickers.created, "established keeper creation")
				if ticker.createdAt.Before(clear.at) {
					t.Fatal("keeper acquired before phase write deadline cleared")
				}
			} else if f.tickers.count.Load() != 0 {
				t.Fatal("spot establishment acquired keeper")
			}
			awaitPhaseTime(t, f, former.when)
			// Literal PC9x init output succeeds after the former phase bound, but
			// an existing legacy keeper retains its first-PC22 captured mode.
			f.initialize(t, true)
			f.syncReader(t)
			if ticker != nil {
				ticker.ticks <- time.Now()
				if got := f.line(t, "PC51^"); got != "PC51^N2WQ-33^N2WQ-33^1^\r\n" {
					t.Fatalf("captured keeper = %q", got)
				}
			}
			if phaseCount(f.conn, "phase") != phases || phaseCount(f.conn, "clear") != 1 {
				t.Fatalf("late init renewed/reopened phase: %+v", f.conn.deadlineTrace())
			}
			f.cancel()
			if ticker != nil {
				f.line(t, "BYE\r\n")
			}
			f.finish(t, context.Canceled)
			if ticker != nil {
				requireProbeStopped(t, f, ticker)
			}
		})
	}
}

func TestProbeEstablishedKeeperDoesNotRefreshInboundIdle(t *testing.T) {
	cfg := phaseProbeConfig()
	cfg.idleTimeout = 600 * time.Millisecond
	f := newProbeFixture(t, cfg, nil, nil)
	phaseFirstRead(t, f)
	f.send(t, "PC22^\r\n")
	ticker := awaitProbe(f.guard, t, f.tickers.created, "idle keeper creation")
	f.syncReader(t)
	f.readerAfterSync(t)
	reads := phaseCount(f.conn, "read")
	for i := 0; i < 2; i++ {
		ticker.ticks <- time.Now()
		if got := f.line(t, "PC51^"); got != "PC51^N2WQ-33^N2WQ-33^1^\r\n" {
			t.Fatalf("idle keeper wire = %q", got)
		}
	}
	if phaseCount(f.conn, "read") != reads {
		t.Fatal("keeper output installed/refreshed read deadline")
	}
	if got := f.line(t, "BYE"); got != "BYE\r\n" {
		t.Fatalf("idle retirement BYE = %q", got)
	}
	requireNaturalPhaseTimeout(t, f, "")
	requireProbeStopped(t, f, ticker)
	if phaseCount(f.conn, "read") != reads || phaseCount(f.conn, "phase") != 1 || phaseCount(f.conn, "clear") != 1 {
		t.Fatalf("established traffic changed budgets: %+v", f.conn.deadlineTrace())
	}
}

func TestProbeEstablishedWritesRemainUnboundedUntilDeliberateCancel(t *testing.T) {
	for _, producer := range []string{"keeper", "late-init"} {
		t.Run(producer, func(t *testing.T) {
			f := newProbeFixture(t, phaseProbeConfig(), nil, nil)
			former := phaseFirstRead(t, f)
			f.send(t, "PC22^\r\n")
			ticker := awaitProbe(f.guard, t, f.tickers.created, "established keeper")
			f.syncReader(t)
			requireProbeWrite(t, f, "PC51^SYNC^")
			prefix := "PC19^"
			if producer == "keeper" {
				ticker.ticks <- time.Now()
				prefix = "PC51^N2WQ-33^N2WQ-33^1^"
			} else {
				f.send(t, "PC18^remote^legacy^\r\n")
			}
			requireProbeWrite(t, f, prefix)
			awaitPhaseTime(t, f, former.when)
			if f.session.ctx.Err() != nil || f.conn.writers.Load() != 1 {
				t.Fatal("established write acquired a phase/idle timeout")
			}
			requireProbeNotClosed(t, f.done, "established blocked writer")
			if phaseCount(f.conn, "phase") != 1 || phaseCount(f.conn, "clear") != 1 {
				t.Fatalf("new established write budget: %+v", f.conn.deadlineTrace())
			}
			f.cancel()
			f.finish(t, context.Canceled)
			requireProbeStopped(t, f, ticker)
		})
	}
}
