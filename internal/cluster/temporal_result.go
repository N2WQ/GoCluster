// File role: Carries request-owned temporal accounting through delayed output.
// No identity registry or public Spot field retains these markers.
package cluster

import (
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
	"sync/atomic"
)

type runtimeTemporalResult struct {
	decoder *correctionflow.TemporalDecoder
	key     spot.ResolverSignalKey
	tracker *stats.Tracker
	handoff atomic.Bool
}

// confirm runs only after the ordinary gates accept an identity mutation.
// Another actual mutation on a later retry counts separately; an unchanged
// retry cannot count again. Confirmation is distinct from downstream delivery.
func (r *runtimeTemporalResult) confirm(before, after string) {
	if r == nil || spot.NormalizeSpotDXCallsign(before) == spot.NormalizeSpotDXCallsign(after) {
		return
	}
	switched := r.decoder.ConfirmCorrection(r.key, after)
	if r.tracker != nil {
		r.tracker.IncrementTemporalCommitted()
		if switched {
			r.tracker.IncrementTemporalPathSwitch()
		}
	}
}

// observeHandoff counts one request's first consumer call, including a refused
// queue publication. It does not claim that a client received the spot.
func (r *runtimeTemporalResult) observeHandoff() {
	if r != nil && r.handoff.CompareAndSwap(false, true) && r.tracker != nil {
		r.tracker.IncrementTemporalHandoffAttempt()
	}
}
