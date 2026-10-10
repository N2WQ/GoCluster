package main

import (
	"errors"
	"net"
	"testing"
	"time"
)

// Phase fixtures send at most 16 pipe writes of at most 256 bytes. The reader
// has more than 3 KiB available at every such read (retained partial data is at
// most 16 bytes), so each controlled pipe write is acquired in one Read. Init
// responses add at most 24 writes, two keeper ticks and one BYE. Every entry/
// exit channel therefore has fewer than 40 events, below its existing 128 cap.
// Inputs and responses are finite; a fixture collector never blocks retirement.
func phaseProbeConfig() probeConfig {
	cfg := fixtureProbeConfig()
	cfg.loginTimeout = 200 * time.Millisecond
	cfg.initTimeout = 600 * time.Millisecond
	return cfg
}

func (c *fixtureProbeConn) deadlineTrace() []probeDeadlineEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]probeDeadlineEvent(nil), c.deadlines...)
}

func (c *fixtureProbeConn) entryTrace() []probeWriteEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]probeWriteEntry(nil), c.writeEntries...)
}

func phaseEvent(t *testing.T, c *fixtureProbeConn, kind string, index int) probeDeadlineEvent {
	t.Helper()
	for _, event := range c.deadlineTrace() {
		if event.kind == kind {
			if index == 0 {
				return event
			}
			index--
		}
	}
	t.Fatalf("missing %s deadline receipt: %+v", kind, c.deadlineTrace())
	return probeDeadlineEvent{}
}

func phaseCount(c *fixtureProbeConn, kind string) int {
	count := 0
	for _, event := range c.deadlineTrace() {
		if event.kind == kind {
			count++
		}
	}
	return count
}

func phaseFirstRead(t *testing.T, f *probeFixture) probeDeadlineEvent {
	t.Helper()
	awaitProbe(f.guard, t, f.conn.readEntered, "phase reader entry")
	return phaseEvent(t, f.conn, "phase", 0)
}

func requirePhaseDuration(t *testing.T, event probeDeadlineEvent, duration time.Duration) {
	t.Helper()
	remaining := event.when.Sub(event.at)
	if remaining <= duration-100*time.Millisecond || remaining > duration+10*time.Millisecond {
		t.Fatalf("phase remaining = %v, expected duration %v: %+v", remaining, duration, event)
	}
}

func awaitPhaseTime(t *testing.T, f *probeFixture, deadline time.Time) {
	t.Helper()
	timer := time.NewTimer(time.Until(deadline) + 10*time.Millisecond)
	defer timer.Stop()
	awaitProbe(f.guard, t, timer.C, "observed phase deadline")
}

func isProbeTimeout(err error) bool {
	var timeout net.Error
	return errors.As(err, &timeout) && timeout.Timeout()
}

func requireNaturalPhaseTimeout(t *testing.T, f *probeFixture, writerPrefix string) {
	t.Helper()
	err := awaitProbe(f.guard, t, f.result, "natural phase retirement")
	if !isProbeTimeout(err) || f.guard.Err() != nil {
		t.Fatalf("phase did not fail naturally before guard: %v / %v", err, f.guard.Err())
	}
	awaitProbe(f.guard, t, f.done, "phase session helper completion")
	awaitProbe(f.guard, t, f.session.interruptDone, "phase interruptor join")
	awaitProbe(f.guard, t, f.conn.closed, "phase socket retirement")
	var exitAt time.Time
	var exitOrder int64
	if writerPrefix == "" {
		for len(f.conn.readReturned) > 0 {
			exit := <-f.conn.readReturned
			if isProbeTimeout(exit.err) {
				if exit.closed || exit.interrupted {
					t.Fatalf("read rescued by interruption/Close: %+v", exit)
				}
				exitAt, exitOrder = exit.at, exit.order
			}
		}
	} else {
		for len(f.conn.writeExited) > 0 {
			exit := <-f.conn.writeExited
			if len(exit.data) >= len(writerPrefix) && exit.data[:len(writerPrefix)] == writerPrefix && isProbeTimeout(exit.err) {
				if exit.closed || exit.interrupted {
					t.Fatalf("writer rescued by interruption/Close: %+v", exit)
				}
				exitAt, exitOrder = exit.at, exit.order
			}
		}
	}
	if exitOrder == 0 {
		t.Fatal("missing actual underlying timeout exit receipt")
	}
	boundKind := "read"
	if writerPrefix != "" {
		boundKind = "phase"
	}
	var bound time.Time
	for _, event := range f.conn.deadlineTrace() {
		if event.kind == boundKind {
			bound = event.when
		}
	}
	if bound.IsZero() || exitAt.Before(bound) {
		t.Fatalf("timeout preceded installed %s bound: exit %s, deadline %s", boundKind, exitAt, bound)
	}
	interrupt := phaseEvent(t, f.conn, "interrupt", 0)
	if interrupt.order <= exitOrder || interrupt.at.Before(exitAt) {
		t.Fatalf("interruption preceded causal timeout: exit %d/%s, interrupt %+v", exitOrder, exitAt, interrupt)
	}
}

func requirePhaseBeforeWrite(t *testing.T, f *probeFixture, phase probeDeadlineEvent, prefix string) {
	t.Helper()
	for _, entry := range f.conn.entryTrace() {
		if len(entry.data) >= len(prefix) && entry.data[:len(prefix)] == prefix {
			if phase.order >= entry.order || phase.at.After(entry.at) {
				t.Fatalf("phase installed after producer entry: %+v / %+v", phase, entry)
			}
			return
		}
	}
	t.Fatalf("missing actual %q write entry", prefix)
}
