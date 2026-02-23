package ratelimit

import (
	"testing"
	"time"
)

func TestCounterNilSafe(t *testing.T) {
	var counter *Counter
	if total, ok := counter.Inc(); ok || total != 0 {
		t.Fatalf("expected nil counter Inc to return (0,false), got (%d,%t)", total, ok)
	}
	if got := counter.Total(); got != 0 {
		t.Fatalf("expected nil counter total to be 0, got %d", got)
	}
}

func TestCounterAlwaysLogsWhenIntervalDisabled(t *testing.T) {
	counter := NewCounter(0)
	for i := 1; i <= 3; i++ {
		total, ok := counter.Inc()
		if !ok {
			t.Fatalf("expected logging enabled for call %d", i)
		}
		if total != uint64(i) {
			t.Fatalf("expected total %d, got %d", i, total)
		}
	}
	if got := counter.Total(); got != 3 {
		t.Fatalf("expected total 3, got %d", got)
	}
}

func TestCounterThrottlesByInterval(t *testing.T) {
	counter := NewCounter(10 * time.Millisecond)

	if total, ok := counter.Inc(); !ok || total != 1 {
		t.Fatalf("expected first increment to log, got (%d,%t)", total, ok)
	}
	if total, ok := counter.Inc(); ok || total != 2 {
		t.Fatalf("expected second increment to be throttled, got (%d,%t)", total, ok)
	}

	time.Sleep(12 * time.Millisecond)
	if total, ok := counter.Inc(); !ok || total != 3 {
		t.Fatalf("expected increment after interval to log, got (%d,%t)", total, ok)
	}
}

func TestCounterWithRetryMatchesBaseSemantics(t *testing.T) {
	counter := NewCounterWithRetry(10 * time.Millisecond)

	if total, ok := counter.Inc(); !ok || total != 1 {
		t.Fatalf("expected first increment to log, got (%d,%t)", total, ok)
	}
	if total, ok := counter.Inc(); ok || total != 2 {
		t.Fatalf("expected second increment to be throttled, got (%d,%t)", total, ok)
	}
}
