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

// Start begins periodic evaluation of recent CW/RTTY spot rates using the
// configured evaluation period. A nil monitor is a no-op so callers do not
// need to guard it. Busy/quiet transitions are logged when thresholds are
// crossed; the current state is kept in-memory for future refresh hooks.
func (m *activityMonitor) Start() {
	if m == nil {
		return
	}
	period := time.Duration(m.cfg.EvaluationPeriodSeconds) * time.Second
	if period <= 0 {
		period = 30 * time.Second
	}
	ticker := time.NewTicker(period)
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

func (m *activityMonitor) Stop() {
	if m == nil {
		return
	}
	close(m.stopCh)
}

// Increment records a single CW/RTTY spot observation at the provided time.
// The call is safe to invoke from hot paths; it locks briefly to bump the
// bucket for the current minute and resets stale buckets when the sliding
// window advances.
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

func (m *activityMonitor) bucketIndex(t time.Time) int {
	minutes := int(t.Unix()/60) % len(m.buckets)
	return minutes
}

// rotateBuckets zeros out any buckets that have aged beyond the configured
// window so rate calculations only consider recent activity.
func (m *activityMonitor) rotateBuckets(now time.Time) {
	for i := range m.buckets {
		if now.Sub(m.buckets[i].start) >= time.Duration(len(m.buckets))*time.Minute {
			m.buckets[i].start = now.Truncate(time.Minute)
			m.buckets[i].count = 0
		}
	}
}

// evaluate recomputes the average rate over the sliding window and updates
// the busy/quiet state machine, emitting logs when transitions occur. It
// should be called on the monitor's ticker; callers pass the current time
// to ease testing.
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
			m.logger.Printf("activity: busy (rate=%.1f/min)", rate)
		}
	case "busy":
		if rate < float64(m.cfg.QuietThresholdPerMin) {
			m.quietStreak++
			if m.quietStreak >= m.cfg.QuietConsecutiveWindows {
				m.state = "quiet"
				m.quietStreak = 0
				m.logger.Printf("activity: quiet (rate=%.1f/min)", rate)
			}
		} else {
			m.quietStreak = 0
		}
	}
	m.lastEval = now
}
