// File role: Conserves receiver incidence during bounded neighborhood selection.
// Scratch maps live for one selection (at most five buckets); no history cache.
package correctionflow

import "dxcluster/spot"

type neighborhoodCandidate struct {
	call              string
	support, weighted int
	best              spot.ResolverSnapshot
	receivers         map[string]struct{}
}

func (s ResolverPrimarySelection) allowsReplacement(call string) bool {
	if s.replacementCalls == nil {
		return true
	}
	for _, permitted := range s.replacementCalls {
		if permitted == call {
			return true
		}
	}
	return false
}

// addNeighborhoodEvidence admits every candidate in an already admitted bucket.
// Replacement comparability is separate: ineligible dissent still contributes
// to the distinct receiver denominator and its own candidate tally.
func addNeighborhoodEvidence(groups map[string]*neighborhoodCandidate, receivers map[string]int, snap spot.ResolverSnapshot) {
	snap.ReceiverEvidence().Range(func(call string, observation spot.ResolverReceiverObservation) bool {
		if _, exists := receivers[observation.Receiver]; !exists {
			receivers[observation.Receiver] = observation.WeightMilli
		}
		group := groups[call]
		if group == nil {
			group = &neighborhoodCandidate{call: call, best: snap, receivers: make(map[string]struct{})}
			groups[call] = group
		}
		if _, exists := group.receivers[observation.Receiver]; !exists {
			group.receivers[observation.Receiver] = struct{}{}
			group.support++
			group.weighted += observation.WeightMilli
		}
		if resolverSnapshotBeats(snap, group.best) {
			group.best = snap
		}
		return true
	})
}
