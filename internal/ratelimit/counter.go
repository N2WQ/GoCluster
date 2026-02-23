// Package ratelimit provides a lightweight counter for throttling log emission.
package ratelimit

import (
	"sync/atomic"
	"time"
)

// Counter tracks a total count and the last time a log was emitted.
// It is safe for concurrent use.
type Counter struct {
	interval time.Duration
	lastLog  atomic.Int64
	total    atomic.Uint64
	retryCAS bool
}

// NewCounter constructs a Counter that allows a log at most once per interval.
// A zero or negative interval disables throttling (always logs).
func NewCounter(interval time.Duration) Counter {
	return Counter{interval: interval}
}

// NewCounterWithRetry constructs a Counter that retries CAS updates under contention.
// This avoids skipping a log interval when multiple goroutines race to emit.
func NewCounterWithRetry(interval time.Duration) Counter {
	return Counter{
		interval: interval,
		retryCAS: true,
	}
}

// Inc increments the counter and reports whether logging is allowed.
func (c *Counter) Inc() (uint64, bool) {
	if c == nil {
		return 0, false
	}
	total := c.total.Add(1)
	if c.interval <= 0 {
		return total, true
	}
	now := time.Now().UTC().UnixNano()
	if !c.retryCAS {
		last := c.lastLog.Load()
		if now-last < c.interval.Nanoseconds() {
			return total, false
		}
		if c.lastLog.CompareAndSwap(last, now) {
			return total, true
		}
		return total, false
	}
	for {
		last := c.lastLog.Load()
		if now-last < c.interval.Nanoseconds() {
			return total, false
		}
		if c.lastLog.CompareAndSwap(last, now) {
			return total, true
		}
	}
}

// Total reports how many times Inc has been called.
func (c *Counter) Total() uint64 {
	if c == nil {
		return 0
	}
	return c.total.Load()
}
