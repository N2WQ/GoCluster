package main

import (
	"sync/atomic"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// adaptiveRefresher schedules a periodic refresh task (e.g., trust/quality rebuild)
// using the per-band adaptive states. It coalesces to the busiest state across groups
// and gates refreshes on a minimum spot count since the last run.
type adaptiveRefresher struct {
	adaptive  *spot.AdaptiveMinReports
	cfg       config.AdaptiveRefreshByBandConfig
	minutes   map[string]time.Duration
	lastRun   time.Time
	spotCount int64
	runFunc   func()
	quit      chan struct{}
}

// Purpose: Construct an adaptiveRefresher for periodic refresh tasks.
// Key aspects: Returns nil unless enabled, adaptive tracker present, and runFunc provided.
// Upstream: Called by main during startup wiring.
// Downstream: None (just allocates state).
func newAdaptiveRefresher(adaptive *spot.AdaptiveMinReports, cfg config.AdaptiveRefreshByBandConfig, runFunc func()) *adaptiveRefresher {
	if adaptive == nil || !cfg.Enabled || runFunc == nil {
		return nil
	}
	return &adaptiveRefresher{
		adaptive: adaptive,
		cfg:      cfg,
		minutes: map[string]time.Duration{
			"quiet":  time.Duration(cfg.QuietRefreshMinutes) * time.Minute,
			"normal": time.Duration(cfg.NormalRefreshMinutes) * time.Minute,
			"busy":   time.Duration(cfg.BusyRefreshMinutes) * time.Minute,
		},
		runFunc: runFunc,
		quit:    make(chan struct{}),
	}
}

// Purpose: Record an additional spot to gate refresh runs by volume.
// Key aspects: Atomic increment to avoid locks on the hot path.
// Upstream: Called by processOutputSpots per spot.
// Downstream: atomic.AddInt64.
func (r *adaptiveRefresher) IncrementSpots() {
	if r == nil {
		return
	}
	atomic.AddInt64(&r.spotCount, 1)
}

// Purpose: Start the periodic refresh evaluation loop.
// Key aspects: Runs a 1-minute ticker; nil receivers are safe.
// Upstream: Called by main after construction.
// Downstream: time.NewTicker and r.maybeRefresh in the goroutine.
func (r *adaptiveRefresher) Start() {
	if r == nil {
		return
	}
	// Evaluate every minute to avoid long delays when state changes.
	ticker := time.NewTicker(time.Minute)
	// Purpose: Check whether to refresh based on current adaptive state.
	// Key aspects: Owns ticker lifecycle and exits on quit.
	// Upstream: Start.
	// Downstream: ticker.Stop and r.maybeRefresh.
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.maybeRefresh(time.Now().UTC())
			case <-r.quit:
				return
			}
		}
	}()
}

// Purpose: Stop the refresh evaluation loop.
// Key aspects: Closing quit unblocks the goroutine.
// Upstream: Called by main during shutdown.
// Downstream: None (channel close only).
func (r *adaptiveRefresher) Stop() {
	if r == nil {
		return
	}
	close(r.quit)
}

// Purpose: Decide whether to run the refresh callback.
// Key aspects: Uses the busiest adaptive state, interval gating, and min volume.
// Upstream: Start's ticker goroutine.
// Downstream: r.adaptive.HighestState and r.runFunc.
func (r *adaptiveRefresher) maybeRefresh(now time.Time) {
	if r == nil {
		return
	}
	state := r.adaptive.HighestState()
	interval, ok := r.minutes[state]
	if !ok || interval <= 0 {
		interval = time.Duration(r.cfg.NormalRefreshMinutes) * time.Minute
	}
	if r.lastRun.IsZero() {
		r.lastRun = now
		return
	}
	if now.Sub(r.lastRun) < interval {
		return
	}
	if atomic.LoadInt64(&r.spotCount) < int64(r.cfg.MinSpotsSinceLastRefresh) {
		return
	}
	// Run the task and reset counters.
	r.runFunc()
	r.lastRun = now
	atomic.StoreInt64(&r.spotCount, 0)
}

// Purpose: Provide a no-op refresh hook for wiring tests or placeholders.
// Key aspects: Empty body to keep the call site uniform.
// Upstream: Used as default in main when no real refresh exists.
// Downstream: None.
func noopRefresh() {
	// Placeholder for trust/quality refresh; kept separate to allow easy swapping later.
}
