//go:build qualification

package peer

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestQualificationClockFaultBypassesActorQueue(t *testing.T) {
	p, _, _, base := controllerTestOwner(t)
	p.manager.ctx = t.Context()
	p.wallNow = func() time.Time { return base }
	for range cap(p.lifecycle) {
		p.lifecycle <- protocolRequest{}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	applied, err := p.manager.QualificationApplyClockFault(ctx, -time.Hour, false)
	if err != nil || applied.IsZero() || p.authorityWallNow() != base.Add(-time.Hour) {
		t.Fatalf("fault application waited for actor or failed to change UTC: %v", err)
	}
	if len(p.lifecycle) != cap(p.lifecycle) {
		t.Fatal("fault consumed or queued actor work")
	}
	_, err = p.manager.QualificationApplyClockFault(ctx, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	frozen := p.authorityWallNow()
	p.wallNow = func() time.Time { return base.Add(time.Hour) }
	if p.authorityWallNow() != frozen {
		t.Fatal("frozen authority clock advanced")
	}
	cancel()
	if _, err := p.manager.QualificationApplyClockFault(ctx, 0, false); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled application changed settings")
	}
	if p.authorityWallNow() != frozen {
		t.Fatal("canceled application unfreezes UTC")
	}
}

func TestQ6ObserverTerminalReasons(t *testing.T) {
	for _, test := range []struct {
		name, wire, reason string
		invalid            bool
	}{
		{"remote", "", "remote_closed", false},
		{"overflow", "PC18^one^\nPC18^two^\n", "observer_overflow", true},
		{"oversized", strings.Repeat("X", MaxPeerFrameBytes+3) + "\n", "oversized_output", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := &q6Peer{reader: bufio.NewReaderSize(strings.NewReader(test.wire), MaxPeerFrameBytes+8), events: make(chan q6Wire, 1), done: make(chan struct{})}
			p.monitor()
			select {
			case <-p.done:
			case <-time.After(time.Second):
				t.Fatal("observer did not terminate")
			}
			if p.terminal.reason != test.reason || p.error.Load() != test.invalid || p.terminal.at.IsZero() {
				t.Fatalf("wrong observer terminal evidence: %+v invalid=%v", p.terminal, p.error.Load())
			}
		})
	}
	if q6TerminalReason(net.ErrClosed) != "local_closed" || q6TerminalReason(io.ErrUnexpectedEOF) != "reader_error" {
		t.Fatal("local/reader failure can masquerade as remote closure")
	}
}

func TestQ6DeadlineRejectsLatePredicateAndWire(t *testing.T) {
	deadline := time.Now()
	for _, test := range []struct {
		observed, checked time.Time
		valid             bool
	}{
		{deadline.Add(-time.Millisecond), deadline, true},
		{deadline.Add(time.Nanosecond), deadline.Add(time.Nanosecond), false},
		{deadline.Add(-time.Millisecond), deadline.Add(time.Nanosecond), false},
		{time.Time{}, deadline, false},
	} {
		if got := q6ObservationWithinDeadline(test.observed, test.checked, deadline); got != test.valid {
			t.Fatalf("observation=%s check=%s valid=%v", test.observed, test.checked, got)
		}
	}
}
