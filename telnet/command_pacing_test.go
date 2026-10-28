package telnet

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestCommandPacerBurstAndRefill(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := newCommandPacer(5, 30, start)
	for i := 0; i < 30; i++ {
		if delay := p.take(start); delay != 0 {
			t.Fatalf("initial attempt %d delayed by %v", i+1, delay)
		}
	}
	if delay := p.take(start.Add(199_999_999 * time.Nanosecond)); delay != time.Nanosecond {
		t.Fatalf("31st attempt before refill: delay=%v, want 1ns", delay)
	}
	if delay := p.take(start.Add(200 * time.Millisecond)); delay != 0 {
		t.Fatalf("31st attempt at refill delayed by %v", delay)
	}
	if delay := p.take(start.Add(200 * time.Millisecond)); delay != 200*time.Millisecond {
		t.Fatalf("32nd attempt reused credit: delay=%v", delay)
	}
}

func TestCommandPacerFractionalRefillNeverEarly(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := newCommandPacer(3, 2, start)
	for i := 0; i < 2; i++ {
		if p.take(start) != 0 {
			t.Fatal("initial burst was not available")
		}
	}
	for _, ns := range []int64{333_333_334, 666_666_667, 1_000_000_000} {
		if delay := p.take(start.Add(time.Duration(ns - 1))); delay != time.Nanosecond {
			t.Fatalf("at %dns: delay=%v, want 1ns", ns-1, delay)
		}
		if delay := p.take(start.Add(time.Duration(ns))); delay != 0 {
			t.Fatalf("at %dns: delay=%v, want 0", ns, delay)
		}
	}
}

func TestCommandPacerIdleSaturationAndBackwardClock(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := newCommandPacer(1, 2, start)
	for i := 0; i < 2; i++ {
		if p.take(start) != 0 {
			t.Fatal("initial burst was not available")
		}
	}
	if delay := p.take(start.Add(-time.Hour)); delay != time.Second {
		t.Fatalf("backward clock granted credit: %v", delay)
	}
	idle := start.Add(24 * time.Hour)
	for i := 0; i < 2; i++ {
		if p.take(idle) != 0 {
			t.Fatal("long idle did not restore burst")
		}
	}
	if delay := p.take(idle); delay != time.Second {
		t.Fatalf("idle banked extra credit: %v", delay)
	}
}

func TestCommandPacerMaximumValuesAndLongElapsed(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := newCommandPacer(maxCommandPacingValue, maxCommandPacingValue, start)
	if p.take(start) != 0 {
		t.Fatal("maximum burst did not begin full")
	}
	if p.take(start.Add(time.Duration(math.MaxInt64))) != 0 {
		t.Fatal("long idle failed to refill maximum bucket")
	}
	want := int64(maxCommandPacingValue-1) * commandCreditPerToken
	if p.credit != want {
		t.Fatalf("credit after saturated refill=%d, want %d", p.credit, want)
	}
	if got := newCommandPacer(0, 0, start); got != nil {
		t.Fatal("zero rate did not disable pacing")
	}
}

func TestCommandPacingDirectOptionsValidateBeforeBinding(t *testing.T) {
	for _, tc := range []struct{ rate, burst int }{
		{-1, 30}, {1, 0}, {1, -1}, {maxCommandPacingValue + 1, 30}, {1, maxCommandPacingValue + 1},
	} {
		s := NewServer(ServerOptions{Port: -1, CommandRatePerSecond: tc.rate, CommandBurst: tc.burst}, nil)
		if err := s.Start(); err == nil || !strings.Contains(err.Error(), "invalid telnet command pacing configuration") {
			t.Fatalf("rate=%d burst=%d: Start error=%v", tc.rate, tc.burst, err)
		}
	}
	if err := validateCommandPacingOptions(0, 0); err != nil {
		t.Fatalf("legacy zero direct options must disable pacing: %v", err)
	}
}
