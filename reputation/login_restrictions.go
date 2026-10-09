// File role: Tightens active station restrictions without resetting spent quota.
// The call-state shard owns these fields. Only ordinary elapsed-window advance
// may replenish counts; reconnects and risk penalties never do so directly.
package reputation

import "time"

func (g *Gate) tightenLoginRestrictions(state *callState, now time.Time, initialWait, extraDelay time.Duration) {
	// Derive the same initial restrictions as a fresh state, but publish only
	// tighter caps/later deadlines. Its window/counts must never replace active
	// state, and an old total-upgrade floor remains until per-band maturity.
	var restrictions callState
	g.resetState(&restrictions, now, initialWait, extraDelay)
	state.perBandLimit = min(state.perBandLimit, restrictions.perBandLimit)
	state.totalCap = min(state.totalCap, restrictions.totalCap)
	if restrictions.nextAllowedAt.After(state.nextAllowedAt) {
		state.nextAllowedAt = restrictions.nextAllowedAt
	}
	if restrictions.nextRampAt.After(state.nextRampAt) {
		state.nextRampAt = restrictions.nextRampAt
	}
	if restrictions.totalCapUpgradeAt.After(state.totalCapUpgradeAt) {
		state.totalCapUpgradeAt = restrictions.totalCapUpgradeAt
	}
}
