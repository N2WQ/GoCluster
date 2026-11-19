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
func addNeighborhoodNomineesDiagnostics(groups map[string]*neighborhoodCandidate, snap spot.ResolverSnapshot, slashThreshold, slot int, diagnostics *ResolverSelectionDiagnostics) {
	for _, candidate := range snap.ReceiverEvidence().CanonicalSupport(slashThreshold) {
		identity := candidate.Identity()
		group := groups[identity]
		created := group == nil
		if group == nil {
			group = &neighborhoodCandidate{call: candidate.Call, best: snap}
			groups[identity] = group
		}
		group.nominee = group.nominee || snap.State == spot.ResolverStateConfident || snap.State == spot.ResolverStateProbable
		updated := resolverSnapshotBeats(snap, group.best)
		if updated {
			group.best = snap
		}
		diagnostics.nomination(slot, candidate, snap.State, group.nominee, created, updated)
	}
}

// neighborhoodProposalBlocked uses only this selection's five admitted sources.
// Expired transition endpoints no longer compete; observing that fact does not
// settle their bucket. The immutable receiver union retains demoted dissent.
func neighborhoodProposalBlockedDiagnostics(call string, snap spot.ResolverSnapshot, groups map[string]*neighborhoodCandidate,
	sources [5]spot.ResolverSnapshot, admitted [5]bool, cfg config.CallCorrectionConfig, diagnostics *ResolverSelectionDiagnostics) (pending, tied bool) {
	for slot := range sources {
		if !admitted[slot] {
			continue
		}
		source := &sources[slot]
		incumbent, challenger, ok := source.PendingTransition()
		diagnostics.rule("pending_transition", slot, incumbent, challenger, ok, "", spot.ResolverAmbiguityInput{})
		if !ok {
			continue
		}
		incumbentSupport := ResolverSupportForCall(*source, incumbent)
		diagnostics.rule("pending_incumbent_support", slot, call, incumbent, incumbentSupport != 0, "", spot.ResolverAmbiguityInput{WinnerSupport: incumbentSupport})
		if incumbentSupport == 0 {
			continue
		}
		challengerSupport := ResolverSupportForCall(*source, challenger)
		diagnostics.rule("pending_challenger_support", slot, call, challenger, challengerSupport != 0, "", spot.ResolverAmbiguityInput{WinnerSupport: challengerSupport})
		if challengerSupport == 0 {
			continue
		}
		left, leftReason := resolverNeighborhoodComparable(call, incumbent, snap.Key.Mode, cfg)
		right, rightReason := resolverNeighborhoodComparable(call, challenger, snap.Key.Mode, cfg)
		diagnostics.rule("pending_incumbent_comparable", slot, call, incumbent, left, leftReason, spot.ResolverAmbiguityInput{})
		diagnostics.rule("pending_challenger_comparable", slot, call, challenger, right, rightReason, spot.ResolverAmbiguityInput{})
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
			neighborhoodPairAmbiguousDiagnostics(call, candidate.Identity(), snap.Key.Mode, groups, cfg, diagnostics) {
			return pending, true
		}
	}
	return pending, false
}

func neighborhoodPairAmbiguousDiagnostics(call, runner, mode string, groups map[string]*neighborhoodCandidate,
	cfg config.CallCorrectionConfig, diagnostics *ResolverSelectionDiagnostics) bool {
	call, runner = spot.CorrectionVoteKey(call), spot.CorrectionVoteKey(runner)
	left, right := groups[call], groups[runner]
	if left == nil || right == nil {
		return false
	}
	in := spot.ResolverAmbiguityInput{WinnerSupport: left.support, RunnerSupport: right.support,
		MinSeparationKHz: cfg.FreqGuardMinSeparationKHz, RunnerUpRatio: cfg.FreqGuardRunnerUpRatio,
		MaxEditDistance: cfg.MaxEditDistance}
	// Check cheap support/overlap exceptions before distance or frequency scans.
	reject := spot.RejectResolverAmbiguity(in)
	diagnostics.rule("ambiguity_support", -1, call, runner, reject, "", in)
	if !reject {
		return false
	}
	in.OverlapCount = left.evidence.ReceiverOverlap(call, runner)
	reject = spot.RejectResolverAmbiguity(in)
	diagnostics.rule("ambiguity_overlap", -1, call, runner, reject, "", in)
	if !reject {
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
	reject = spot.RejectResolverAmbiguity(in)
	diagnostics.rule("ambiguity_family_distance", -1, call, runner, reject, "", in)
	if !reject {
		return false
	}
	var witnessed bool
	in.WinnerFrequencyKHz, in.RunnerFrequencyKHz, witnessed = left.evidence.ClosestFrequencyPair(call, runner)
	diagnostics.rule("ambiguity_frequency_witness", -1, call, runner, witnessed, "", in)
	if !witnessed {
		return false
	}
	reject = spot.RejectResolverAmbiguity(in)
	diagnostics.rule("ambiguity_frequency", -1, call, runner, reject, "", in)
	return reject
}
