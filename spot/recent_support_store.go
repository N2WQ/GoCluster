// File role: Defines the shared recent-history query contract and the scalar
// Custom SCP requirements selected by each correction consumer.
// Crawler notes: Query requirements carry no store ownership or retained state;
// recording, statistics and lifecycle remain on the canonical store.
// Related docs: docs/decisions/ADR-0286-consumer-specific-historical-requirements.md.
package spot

import "time"

// CustomSCPRequirements identifies a consumer's historical age-score and
// geographic gates. It is a query value, never a mutation of store options.
// Receiver counts remain partial evidence; each consumer applies its own
// receiver qualification minimum separately.
type CustomSCPRequirements struct {
	MinScore   int
	MinH3Cells int
}

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
