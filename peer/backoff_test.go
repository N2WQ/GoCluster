package peer

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestPeerBackoffResetAndSaturation(t *testing.T) {
	b := newBackoff(2*time.Second, 7*time.Second)
	for cycle := 0; cycle < 3; cycle++ {
		for _, want := range []time.Duration{2, 4, 7, 7} {
			if got := b.Next(); got != want*time.Second {
				t.Fatalf("cycle %d delay=%s want=%s", cycle, got, want*time.Second)
			}
		}
		b.Reset()
	}
	large := newBackoff(time.Duration(math.MaxInt64/2+1), time.Duration(math.MaxInt64))
	if got := large.Next(); got <= 0 {
		t.Fatalf("initial large delay=%s", got)
	}
	if got := large.Next(); got != time.Duration(math.MaxInt64) {
		t.Fatalf("saturating delay overflowed: %s", got)
	}
	for _, tc := range []struct{ base, cap, want time.Duration }{
		{0, 0, time.Second}, {-1, time.Second, time.Second},
		{3 * time.Second, time.Second, 3 * time.Second},
	} {
		b = newBackoff(tc.base, tc.cap)
		b.Reset()
		if got := b.Next(); got != tc.want {
			t.Fatalf("normalized (%s,%s)=%s want %s", tc.base, tc.cap, got, tc.want)
		}
	}
}

func TestPeerBackoffShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- waitPeerRetry(ctx, 300*time.Second) }()
	cancel()
	select {
	case elapsed := <-done:
		if elapsed {
			t.Fatal("cancellation reported elapsed retry")
		}
	case <-time.After(time.Second):
		t.Fatal("retry wait did not join")
	}
}
