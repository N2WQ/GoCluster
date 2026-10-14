// File role: Conserves receiver incidence during bounded neighborhood selection.
// Scratch maps live for one selection (at most five buckets); no history cache.
package correctionflow

import (
	"math"

	"dxcluster/config"
	"dxcluster/spot"
)

type neighborhoodCandidate struct {
	call              string
	support, weighted int
	best              spot.ResolverSnapshot
	receivers         map[string]struct{}
	nominee           bool // Only an already-admitted settled population may nominate.
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
func addNeighborhoodEvidence(groups map[string]*neighborhoodCandidate, receivers map[string]int, snap spot.ResolverSnapshot, nominate bool) {
	snap.ReceiverEvidence().Range(func(call string, observation spot.ResolverReceiverObservation) bool {
		if _, exists := receivers[observation.Receiver]; !exists {
			receivers[observation.Receiver] = observation.WeightMilli
		}
		group := groups[call]
		if group == nil {
			group = &neighborhoodCandidate{call: call, best: snap, receivers: make(map[string]struct{})}
			groups[call] = group
		}
		group.nominee = group.nominee || nominate
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

// neighborhoodProposalBlocked uses only this selection's five admitted sources.
// Expired transition endpoints no longer compete; observing that fact does not
// settle their bucket. Scratch receiver sets retain dissent after demotion.
func neighborhoodProposalBlocked(call string, snap spot.ResolverSnapshot, groups map[string]*neighborhoodCandidate,
	sources [5]spot.ResolverSnapshot, admitted [5]bool, cfg config.CallCorrectionConfig) (pending, tied bool) {
	for slot := range sources {
		if !admitted[slot] {
			continue
		}
		source := &sources[slot]
		incumbent, challenger, ok := source.PendingTransition()
		if !ok || ResolverSupportForCall(*source, incumbent) == 0 || ResolverSupportForCall(*source, challenger) == 0 {
			continue
		}
		left, _ := resolverNeighborhoodComparable(call, incumbent, snap.Key.Mode, cfg)
		right, _ := resolverNeighborhoodComparable(call, challenger, snap.Key.Mode, cfg)
		if left && right {
			pending = true
			break
		}
	}
	left := groups[call]
	if left == nil {
		return pending, false
	}
	// A differently weighted candidate can sit below the weighted runner. Every
	// equal-raw competitor must still satisfy the same narrow witnesses/exceptions.
	for _, candidate := range snap.CandidateRanks {
		if candidate.Call != call && candidate.Support == left.support &&
			neighborhoodPairAmbiguous(call, candidate.Call, snap.Key.Mode, groups, sources, admitted, cfg) {
			return pending, true
		}
	}
	return pending, false
}

func neighborhoodPairAmbiguous(call, runner, mode string, groups map[string]*neighborhoodCandidate,
	sources [5]spot.ResolverSnapshot, admitted [5]bool, cfg config.CallCorrectionConfig) bool {
	left, right := groups[call], groups[runner]
	if left == nil || right == nil {
		return false
	}
	in := spot.ResolverAmbiguityInput{WinnerSupport: left.support, RunnerSupport: right.support,
		MinSeparationKHz: cfg.FreqGuardMinSeparationKHz, RunnerUpRatio: cfg.FreqGuardRunnerUpRatio,
		MaxEditDistance: cfg.MaxEditDistance}
	// Check cheap support/overlap exceptions before distance or frequency scans.
	if !spot.RejectResolverAmbiguity(in) {
		return false
	}
	for receiver := range left.receivers {
		if _, ok := right.receivers[receiver]; ok {
			in.OverlapCount++
			// Overlap only increases. Once the shared predicate excludes this
			// pair, scanning more repeated receivers cannot restore a veto.
			if !spot.RejectResolverAmbiguity(in) {
				return false
			}
		}
	}
	policy := spot.CorrectionFamilyPolicy{Configured: true,
		TruncationEnabled:          cfg.FamilyPolicy.Truncation.Enabled,
		TruncationMaxLengthDelta:   cfg.FamilyPolicy.Truncation.MaxLengthDelta,
		TruncationMinShorterLength: cfg.FamilyPolicy.Truncation.MinShorterLength,
		TruncationAllowPrefix:      cfg.FamilyPolicy.Truncation.AllowPrefixMatch,
		TruncationAllowSuffix:      cfg.FamilyPolicy.Truncation.AllowSuffixMatch}
	_, in.Related = spot.DetectCorrectionFamilyWithPolicy(call, runner, policy)
	in.Distance = spot.CallDistance(call, runner, mode, cfg.DistanceModelCW, cfg.DistanceModelRTTY)
	// Check cheap support/overlap/family exceptions before frequency scans. Dense
	// capped populations often share all receivers and need no pair scan at all.
	if !spot.RejectResolverAmbiguity(in) {
		return false
	}
	minimum := math.Inf(1)
	for i := range sources {
		if !admitted[i] {
			continue
		}
		source := &sources[i]
		source.ReceiverEvidence().Range(func(leftCall string, a spot.ResolverReceiverObservation) bool {
			if leftCall != call {
				return true
			}
			for j := range sources {
				if !admitted[j] {
					continue
				}
				other := &sources[j]
				other.ReceiverEvidence().Range(func(rightCall string, b spot.ResolverReceiverObservation) bool {
					if rightCall == runner && math.Abs(a.FrequencyKHz-b.FrequencyKHz) < minimum {
						minimum = math.Abs(a.FrequencyKHz - b.FrequencyKHz)
						in.WinnerFrequencyKHz, in.RunnerFrequencyKHz = a.FrequencyKHz, b.FrequencyKHz
					}
					return true
				})
			}
			return true
		})
	}
	return !math.IsInf(minimum, 1) && spot.RejectResolverAmbiguity(in)
}
