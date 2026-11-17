// File role: Conserves receiver incidence during bounded neighborhood selection.
// Scratch maps live for one selection (at most five buckets); no history cache.
package correctionflow

import (
	"dxcluster/config"
	"dxcluster/spot"
)

type neighborhoodCandidate struct {
	call              string
	support, weighted int
	best              spot.ResolverSnapshot
	receivers         map[string]struct{}
	nominee           bool // Only an already-admitted settled population may nominate.
	rank              spot.ResolverCandidateSupport
	evidence          *spot.ResolverEvidenceView
}

func (s ResolverPrimarySelection) allowsReplacement(call string) bool {
	if !s.Snapshot.AllowsReplacement(call) {
		return false
	}
	if s.replacementCalls == nil {
		return true
	}
	for _, permitted := range s.replacementCalls {
		if spot.CorrectionVoteKey(permitted) == spot.CorrectionVoteKey(call) {
			return true
		}
	}
	return false
}

// addNeighborhoodEvidence admits every candidate in an already admitted bucket.
// Replacement comparability is separate: ineligible dissent still contributes
// to the distinct receiver denominator and its own candidate tally.
func addNeighborhoodEvidence(groups map[string]*neighborhoodCandidate, receivers map[string]int, snap spot.ResolverSnapshot, nominate bool) {
	snap.ReceiverEvidence().RangeCanonical(func(identity, call string, observation spot.ResolverReceiverObservation) bool {
		if _, exists := receivers[observation.Receiver]; !exists {
			receivers[observation.Receiver] = observation.WeightMilli
		}
		group := groups[identity]
		if group == nil {
			group = &neighborhoodCandidate{call: call, best: snap, receivers: make(map[string]struct{})}
			groups[identity] = group
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
	call = spot.CorrectionVoteKey(call)
	left := groups[call]
	if left == nil {
		return pending, false
	}
	// A differently weighted candidate can sit below the weighted runner. Every
	// equal-raw competitor must still satisfy the same narrow witnesses/exceptions.
	for _, candidate := range snap.CandidateRanks {
		if candidate.Identity() != call && candidate.Support == left.support &&
			neighborhoodPairAmbiguous(call, candidate.Identity(), snap.Key.Mode, groups, sources, admitted, cfg) {
			return pending, true
		}
	}
	return pending, false
}

func neighborhoodPairAmbiguous(call, runner, mode string, groups map[string]*neighborhoodCandidate,
	sources [5]spot.ResolverSnapshot, admitted [5]bool, cfg config.CallCorrectionConfig) bool {
	call, runner = spot.CorrectionVoteKey(call), spot.CorrectionVoteKey(runner)
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
	var witnessed bool
	in.WinnerFrequencyKHz, in.RunnerFrequencyKHz, witnessed = left.evidence.ClosestFrequencyPair(call, runner)
	return witnessed && spot.RejectResolverAmbiguity(in)
}
