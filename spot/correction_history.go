// File role: Owns one resolver gate's first-read historical counts. This is
// synchronous decision scratch, not a store snapshot or retained cache. Different
// aliases may observe different publications; later gate calls capture afresh.
package spot

import (
	"time"

	"dxcluster/strutil"
)

type customSCPRequirementCounter interface {
	RecentSupportCountWithRequirements(call, band, mode string, requirements CustomSCPRequirements, now time.Time) int
}

type resolverHistoryCount struct {
	call  string
	count int
}

type resolverGateHistory struct {
	store        RecentSupportStore
	custom       customSCPRequirementCounter
	requirements CustomSCPRequirements
	band, mode   string
	now          time.Time
	counts       [8]resolverHistoryCount
	used         int
}

func newResolverGateHistory(cfg CorrectionSettings, band, mode string, now time.Time) resolverGateHistory {
	history := resolverGateHistory{store: cfg.RecentBandStore, requirements: cfg.RecentBandCustomSCPRequirements,
		band: band, mode: mode, now: now}
	if cfg.RecentBandCustomSCPEnabled {
		if custom, ok := cfg.RecentBandStore.(customSCPRequirementCounter); ok {
			history.custom = custom
		}
	}
	return history
}

func (h *resolverGateHistory) validated(identity correctionCallIdentity, displayCall string, minimum int) bool {
	if minimum <= 0 {
		minimum = 2
	}
	return h.support(identity, displayCall) >= minimum
}

func (h *resolverGateHistory) support(identity correctionCallIdentity, displayCall string) int {
	if h.store == nil {
		return 0
	}
	maximum := 0
	for _, alias := range [4]string{identity.Raw, identity.VoteKey, identity.BaseKey, displayCall} {
		maximum = max(maximum, h.count(alias))
	}
	return maximum
}

// Only the gate's two immutable identities call support/validated. Each has
// exactly Raw, VoteKey, BaseKey and display slots, so there can be no ninth
// distinct key. Duplicate and zero results occupy their original slot too.
// Preserve the old helper's normalization and backend methods: further slash
// collapsing or normalized-method dispatch would change alias/store semantics.
func (h *resolverGateHistory) count(alias string) int {
	call := strutil.NormalizeUpper(alias)
	if call == "" {
		return 0
	}
	for i := 0; i < h.used; i++ {
		if h.counts[i].call == call {
			return h.counts[i].count
		}
	}
	var count int
	if h.custom != nil {
		count = h.custom.RecentSupportCountWithRequirements(call, h.band, h.mode, h.requirements, h.now)
	} else {
		count = h.store.RecentSupportCount(call, h.band, h.mode, h.now)
	}
	h.counts[h.used] = resolverHistoryCount{call: call, count: count}
	h.used++
	return count
}
