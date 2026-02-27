package spot

import "time"

// RecentSupportStore is the shared read contract for recent-evidence stores
// used by resolver gates, stabilizer checks, and stats surfaces.
type RecentSupportStore interface {
	HasRecentSupport(call, band, mode string, minUnique int, now time.Time) bool
	RecentSupportCount(call, band, mode string, now time.Time) int
	ActiveCallCount(now time.Time) int
	ActiveCallCountsByBand(now time.Time) map[string]int
	StartCleanup()
	StopCleanup()
}
