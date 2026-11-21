// File role: Apply the usual neighborhood and exact-slot arbitration to the
// independently settled, release-time CW decay publications.
package correctionflow

import (
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// SelectCWDecaySnapshotForCallAt combines the pinned pre-focal exact slot
// with eligible neighboring publications using ordinary selection semantics.
func SelectCWDecaySnapshotForCallAt(resolver *spot.SignalResolver, prior spot.CWDecayPrior, key spot.ResolverSignalKey, cfg config.CallCorrectionConfig, subject string, now time.Time) ResolverPrimarySelection {
	if resolver == nil || key.Mode != "CW" || now.IsZero() {
		return ResolverPrimarySelection{}
	}
	var unsettled [5]uint64
	selection := selectResolverPrimarySnapshotFromLookup(key, cfg, subject, func(k spot.ResolverSignalKey) (spot.ResolverSnapshot, bool) {
		var s spot.ResolverSnapshot
		var ok bool
		if k == key {
			s, ok = resolver.LookupCWDecayPrior(prior, k, now)
			if !ok || s.ReceiverEvidence().Len() == 0 || s.Generation == 0 {
				// The independent comparison needs a real, current focal
				// population even if adjacent slots could nominate a call.
				s.EvidenceLoss = true
			}
			ok = true
		} else {
			s, ok = resolver.LookupCWDecayEligible(k, now)
		}
		if _, _, pending := s.PendingTransition(); pending || s.State == spot.ResolverStateUncertain || s.State == spot.ResolverStateSplit {
			unsettled[k.Bucket-key.Bucket+2] = s.Generation
		}
		return s, ok
	})
	for slot, generation := range unsettled {
		if generation != 0 && selection.generationWitnesses[slot] == generation {
			return ResolverPrimarySelection{}
		}
	}
	return selection
}
