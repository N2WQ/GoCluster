package spot

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPeriodicCleanupStartStopIdempotent(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var quit chan struct{}
	var ticks atomic.Uint64

	startPeriodicCleanup(&mu, &quit, 2*time.Millisecond, func() {
		ticks.Add(1)
	})
	if quit == nil {
		t.Fatalf("expected cleanup loop to set quit channel")
	}
	firstQuit := quit

	startPeriodicCleanup(&mu, &quit, 2*time.Millisecond, func() {
		ticks.Add(1)
	})
	if quit != firstQuit {
		t.Fatalf("expected second start call to be ignored")
	}

	deadline := time.Now().Add(200 * time.Millisecond)
	for ticks.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if ticks.Load() == 0 {
		t.Fatalf("expected cleanup callback to run at least once")
	}

	stopPeriodicCleanup(&mu, &quit)
	if quit != nil {
		t.Fatalf("expected stop to clear quit channel")
	}
	stopPeriodicCleanup(&mu, &quit)
}

func TestPeriodicCleanupStopHaltsTicks(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var quit chan struct{}
	var ticks atomic.Uint64

	startPeriodicCleanup(&mu, &quit, 2*time.Millisecond, func() {
		ticks.Add(1)
	})

	deadline := time.Now().Add(200 * time.Millisecond)
	for ticks.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if ticks.Load() == 0 {
		t.Fatalf("expected cleanup callback to run before stop")
	}

	stopPeriodicCleanup(&mu, &quit)
	time.Sleep(8 * time.Millisecond)
	first := ticks.Load()
	time.Sleep(8 * time.Millisecond)
	second := ticks.Load()
	if second > first {
		t.Fatalf("expected ticks to stop increasing after stop (first=%d second=%d)", first, second)
	}
}
