//go:build !windows

package testsupport

import (
	"testing"
	"time"
)

// All instances use one monotonic epoch so independently owned clocks can
// compare readings. The epoch and clock source are fixed before timed work.
var clockEpoch = time.Now()

// Clock measures test intervals using time.Since or a live benchmark. NewClock
// instances follow Windows' owner-local contract. RunFixedBenchmark supplies
// one immutable shared clock; its benchmark domain must not mix with NewClock.
type Clock struct {
	benchmark *testing.B
}

// NewClock constructs a clock during fixture setup.
func NewClock(t testing.TB) *Clock {
	t.Helper()
	return &Clock{}
}

// NowNS returns elapsed monotonic nanoseconds in the selected shared process or
// live benchmark domain. Equal readings remain equal; no duration is replaced.
func (c *Clock) NowNS() int64 {
	if c.benchmark != nil {
		return int64(c.benchmark.Elapsed())
	}
	ns := time.Since(clockEpoch).Nanoseconds()
	if ns < 0 {
		panic("testsupport.Clock returned a negative monotonic reading")
	}
	return ns
}

// Source identifies the timer used in captured test artifacts.
func (c *Clock) Source() string {
	if c.benchmark != nil {
		return "testing.B.Elapsed(shared live benchmark)"
	}
	return "time.Since(shared monotonic epoch)"
}

// FrequencyHz describes the nominal nanosecond representation, not a measured
// hardware frequency. Source identifies the platform fallback explicitly.
func (c *Clock) FrequencyHz() int64 { return 1_000_000_000 }
