package main

import (
	"log"
	"sync"
	"time"

	"dxcluster/config"
)

// activityMonitor tracks recent spot rates to choose between "busy" and "quiet" cadences.
// It keeps 1-minute buckets over a sliding window and logs state transitions.
type activityMonitor struct {
	cfg         config.AdaptiveRefreshConfig
	logger      *log.Logger
	mu          sync.Mutex
	buckets     []bucket
	state       string
	quietStreak int
	lastEval    time.Time
	lastRefresh time.Time
	stopCh      chan struct{}
}

type bucket struct {
	start time.Time
	count int
}

var _ = newActivityMonitor

// Purpose: Build an activityMonitor configured for adaptive refresh cadence selection.
// Key aspects: Returns nil when disabled; seeds buckets and a default logger.
// Upstream: None (currently unused; reserved for adaptive refresh wiring).
// Downstream: Uses log.Default and allocates the sliding window buckets.
func newActivityMonitor(cfg config.AdaptiveRefreshConfig, logger *log.Logger) *activityMonitor {
	if cfg.WindowMinutesForRate <= 0 {
		return nil
	}
	if logger == nil {
		logger = log.Default()
	}
	buckets := make([]bucket, cfg.WindowMinutesForRate)
	return &activityMonitor{
		cfg:     cfg,
		logger:  logger,
		state:   "quiet",
		buckets: buckets,
		stopCh:  make(chan struct{}),
	}
}

// Purpose: Start periodic evaluation of recent spot rates.
// Key aspects: Spawns a ticker goroutine; nil receivers are safe no-ops.
// Upstream: Intended for adaptive refresh orchestration (not wired yet).
// Downstream: time.NewTicker and m.evaluate inside the goroutine.
func (m *activityMonitor) Start() {
	if m == nil {
		return
	}
	period := time.Duration(m.cfg.EvaluationPeriodSeconds) * time.Second
	if period <= 0 {
		period = 30 * time.Second
	}
	ticker := time.NewTicker(period)
	// Purpose: Periodically recompute the busy/quiet state until Stop is called.
	// Key aspects: Owns ticker lifecycle and exits on stop channel.
	// Upstream: Start.
	// Downstream: ticker.Stop and m.evaluate.
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.evaluate(time.Now())
			case <-m.stopCh:
				return
			}
		}
	}()
}

// Purpose: Stop the activityMonitor background ticker.
// Key aspects: Closing stopCh is the only shutdown signal.
// Upstream: Intended to be called by owner during shutdown.
// Downstream: None (channel close only).
func (m *activityMonitor) Stop() {
	if m == nil {
		return
	}
	close(m.stopCh)
}

// Purpose: Record a spot arrival for activity rate tracking.
// Key aspects: Minimal lock, rotates stale buckets, bumps the current minute.
// Upstream: Intended for the spot ingest path (not wired yet).
// Downstream: m.rotateBuckets and m.bucketIndex.
func (m *activityMonitor) Increment(now time.Time) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rotateBuckets(now)
	idx := m.bucketIndex(now)
	// When we land in a reused bucket, refresh its start to the current minute so
	// the window math stays accurate as time advances.
	if m.buckets[idx].start.IsZero() || now.Sub(m.buckets[idx].start) >= time.Minute {
		m.buckets[idx].start = now.Truncate(time.Minute)
		m.buckets[idx].count = 0
	}
	m.buckets[idx].count++
}

// Purpose: Map a timestamp to the corresponding bucket index.
// Key aspects: Uses epoch minutes modulo bucket length.
// Upstream: Increment.
// Downstream: None.
func (m *activityMonitor) bucketIndex(t time.Time) int {
	minutes := int(t.Unix()/60) % len(m.buckets)
	return minutes
}

// Purpose: Drop stale buckets outside the sliding window.
// Key aspects: Resets bucket start/count when outside window span.
// Upstream: Increment and evaluate.
// Downstream: None.
func (m *activityMonitor) rotateBuckets(now time.Time) {
	for i := range m.buckets {
		if now.Sub(m.buckets[i].start) >= time.Duration(len(m.buckets))*time.Minute {
			m.buckets[i].start = now.Truncate(time.Minute)
			m.buckets[i].count = 0
		}
	}
}

// Purpose: Update busy/quiet state based on recent spot rate.
// Key aspects: Computes windowed rate and logs state transitions.
// Upstream: Start's ticker goroutine (and tests, if any).
// Downstream: m.rotateBuckets and logger.Printf.
func (m *activityMonitor) evaluate(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rotateBuckets(now)

	var total int
	for _, b := range m.buckets {
		// Only include buckets within window
		if now.Sub(b.start) < time.Duration(len(m.buckets))*time.Minute {
			total += b.count
		}
	}
	windowMinutes := len(m.buckets)
	rate := float64(total) / float64(windowMinutes)

	switch m.state {
	case "quiet":
		if rate > float64(m.cfg.BusyThresholdPerMin) {
			m.state = "busy"
			m.quietStreak = 0
			if m.logger != nil {
				m.logger.Printf("activity: busy (rate=%.1f/min)", rate)
			}
		}
	case "busy":
		if rate < float64(m.cfg.QuietThresholdPerMin) {
			m.quietStreak++
			if m.quietStreak >= m.cfg.QuietConsecutiveWindows {
				m.state = "quiet"
				m.quietStreak = 0
				if m.logger != nil {
					m.logger.Printf("activity: quiet (rate=%.1f/min)", rate)
				}
			}
		} else {
			m.quietStreak = 0
		}
	}
	m.lastEval = now
}
