// File role: Evaluates ambiguity from retained receiver frequency witnesses.
// Work is bounded by the resolver's candidate/receiver caps; no pair cache.
package spot

import "math"

func resolverEvidenceHasFrequencyConflict(view *ResolverEvidenceView) bool {
	if view == nil {
		return false
	}
	for _, candidate := range view.candidates {
		for _, row := range candidate.receivers {
			if row.FrequencyConflict {
				return true
			}
		}
	}
	return false
}

// closestResolverFrequencyPair preserves the original binary64 witnesses.
// At the production/default receiver cap it scans at most 64*64 pairs, allocating
// nothing. Equal minimum separations give the same strict guard verdict.
func closestResolverFrequencyPair(view *ResolverEvidenceView, leftCall, rightCall string) (float64, float64, bool) {
	if view == nil {
		return 0, 0, false
	}
	var left, right []ResolverReceiverObservation
	for _, candidate := range view.candidates {
		if candidate.call == leftCall {
			left = candidate.receivers
		}
		if candidate.call == rightCall {
			right = candidate.receivers
		}
	}
	minimum := math.Inf(1)
	var a, b float64
	found := false
	for _, l := range left {
		for _, r := range right {
			if separation := math.Abs(l.FrequencyKHz - r.FrequencyKHz); separation < minimum {
				minimum, a, b, found = separation, l.FrequencyKHz, r.FrequencyKHz, true
			}
		}
	}
	return a, b, found
}
