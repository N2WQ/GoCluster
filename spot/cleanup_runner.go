package spot

import (
	"sync"
	"time"
)

// startPeriodicCleanup starts a ticker-driven cleanup loop once.
// It captures the quit channel in the goroutine to avoid races with Stop.
func startPeriodicCleanup(mu *sync.Mutex, quit *chan struct{}, interval time.Duration, tick func()) {
	if mu == nil || quit == nil || interval <= 0 || tick == nil {
		return
	}
	mu.Lock()
	if *quit != nil {
		mu.Unlock()
		return
	}
	done := make(chan struct{})
	*quit = done
	mu.Unlock()

	go func(done <-chan struct{}) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				tick()
			case <-done:
				return
			}
		}
	}(done)
}

// stopPeriodicCleanup stops a previously started cleanup loop.
func stopPeriodicCleanup(mu *sync.Mutex, quit *chan struct{}) {
	if mu == nil || quit == nil {
		return
	}
	mu.Lock()
	if *quit != nil {
		close(*quit)
		*quit = nil
	}
	mu.Unlock()
}
