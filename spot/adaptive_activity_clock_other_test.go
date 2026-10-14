//go:build !windows

package spot

import "time"

const activityLatencyClock = "Go monotonic"

var activityLatencyEpoch = time.Now()

func activityLatencyNow() int64 { return time.Since(activityLatencyEpoch).Nanoseconds() }
