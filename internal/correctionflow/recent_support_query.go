// File role: Carries consumer-local history requirements through stabilizer
// family/edit-neighbor probes without replacing the canonical recording owner.
// Query values own no retained state, cleanup or persistence.
package correctionflow

import (
	"time"

	"dxcluster/spot"
)

type customSCPRequirementQueries interface {
	HasRecentSupportWithRequirements(call, band, mode string, minUnique int, requirements spot.CustomSCPRequirements, now time.Time) bool
	RecentSupportCountNormalizedWithRequirements(call, band, mode string, requirements spot.CustomSCPRequirements, now time.Time) int
}

type recentSupportQuery struct {
	store        spot.RecentSupportStore
	custom       bool
	requirements spot.CustomSCPRequirements
}

func (q recentSupportQuery) has(call, band, mode string, minUnique int, now time.Time) bool {
	if q.store == nil {
		return false
	}
	if queries, ok := q.store.(customSCPRequirementQueries); ok && q.custom {
		return queries.HasRecentSupportWithRequirements(call, band, mode, minUnique, q.requirements, now)
	}
	return q.store.HasRecentSupport(call, band, mode, minUnique, now)
}

func (q recentSupportQuery) normalizedCount(call, band, mode string, now time.Time) int {
	if q.store == nil {
		return 0
	}
	if queries, ok := q.store.(customSCPRequirementQueries); ok && q.custom {
		return queries.RecentSupportCountNormalizedWithRequirements(call, band, mode, q.requirements, now)
	}
	if normalized, ok := q.store.(normalizedRecentSupportCounter); ok {
		return normalized.RecentSupportCountNormalized(call, band, mode, now)
	}
	return q.store.RecentSupportCount(call, band, mode, now)
}
