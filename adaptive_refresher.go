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

// IncrementSpots tracks volume since the last refresh so we can gate runs.
func (r *adaptiveRefresher) IncrementSpots() {
	if r == nil {
		return
	}
	atomic.AddInt64(&r.spotCount, 1)
}

func (r *adaptiveRefresher) Start() {
	if r == nil {
		return
	}
	// Evaluate every minute to avoid long delays when state changes.
	ticker := time.NewTicker(time.Minute)
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

func (r *adaptiveRefresher) Stop() {
	if r == nil {
		return
	}
	close(r.quit)
}

// maybeRefresh runs the callback if enough time and spots have accumulated
// based on the busiest current state across all adaptive groups.
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

func noopRefresh() {
	// Placeholder for trust/quality refresh; kept separate to allow easy swapping later.
}
