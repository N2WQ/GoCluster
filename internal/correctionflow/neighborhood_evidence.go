// File role: Conserves receiver incidence during bounded neighborhood selection.
// Candidate metadata lives for one selection (at most five buckets); immutable
// sorted receiver rows supply population and overlap without rebuilding sets.
package correctionflow

import (
	"dxcluster/config"
	"dxcluster/spot"
)

type neighborhoodCandidate struct {
	call              string
	support, weighted int
	best              spot.ResolverSnapshot
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

// addNeighborhoodNominees captures only source authority. The merged evidence
// supplies tallies once, after admission; every canonical member of a settled
// source may nominate, including members below that source's winner.
func addNeighborhoodNominees(groups map[string]*neighborhoodCandidate, snap spot.ResolverSnapshot, slashThreshold int) {
	for _, candidate := range snap.ReceiverEvidence().CanonicalSupport(slashThreshold) {
		identity := candidate.Identity()
		group := groups[identity]
		if group == nil {
			group = &neighborhoodCandidate{call: candidate.Call, best: snap}
			groups[identity] = group
		}
		group.nominee = group.nominee || snap.State == spot.ResolverStateConfident || snap.State == spot.ResolverStateProbable
		if resolverSnapshotBeats(snap, group.best) {
			group.best = snap
		}
	}
}

// neighborhoodProposalBlocked uses only this selection's five admitted sources.
// Expired transition endpoints no longer compete; observing that fact does not
// settle their bucket. The immutable receiver union retains demoted dissent.
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
			neighborhoodPairAmbiguous(call, candidate.Identity(), snap.Key.Mode, groups, cfg) {
			return pending, true
		}
	}
	return pending, false
}

func neighborhoodPairAmbiguous(call, runner, mode string, groups map[string]*neighborhoodCandidate,
	cfg config.CallCorrectionConfig) bool {
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
	in.OverlapCount = left.evidence.ReceiverOverlap(call, runner)
	if !spot.RejectResolverAmbiguity(in) {
		return false
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
