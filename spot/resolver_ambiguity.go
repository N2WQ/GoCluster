// File role: Evaluates ambiguity from retained receiver frequency witnesses.
// Work is bounded by the resolver's candidate/receiver caps; no pair cache.
package spot

import "math"

// ResolverAmbiguityInput carries the existing narrow ambiguity witnesses. Raw
// receiver equality is a conservative veto even when weighted ranking differs;
// the frequency, overlap, support, distance and family exceptions still apply.
type ResolverAmbiguityInput struct {
	WinnerSupport, RunnerSupport            int
	WinnerFrequencyKHz, RunnerFrequencyKHz  float64
	MinSeparationKHz, RunnerUpRatio         float64
	OverlapCount, Distance, MaxEditDistance int
	Related                                 bool
}

// RejectResolverAmbiguity shares the owner predicate with bounded synthesized
// evidence. The caller supplies actual eligible frequency/membership witnesses.
func RejectResolverAmbiguity(in ResolverAmbiguityInput) bool {
	return shouldRejectAsAmbiguousMultiSignal(in.WinnerSupport, in.RunnerSupport,
		in.WinnerFrequencyKHz, in.RunnerFrequencyKHz, in.MinSeparationKHz,
		in.RunnerUpRatio, in.OverlapCount, in.Distance, in.MaxEditDistance, in.Related)
}

// ProposalHasAmbiguousRawTie evaluates every eligible equal-count competitor of
// this actual proposal, independently of weighted rank. It reads immutable rows
// under the resolver's constructor policy and does not mutate owner state.
func (r *SignalResolver) ProposalHasAmbiguousRawTie(snapshot ResolverSnapshot, proposedCall string) bool {
	if r == nil || snapshot.ReceiverEvidence() == nil {
		return false
	}
	support := 0
	for _, candidate := range snapshot.CandidateRanks {
		if candidate.Call == proposedCall {
			support = candidate.Support
			break
		}
	}
	if support < ambiguousMultiSignalMinSupport {
		return false
	}
	for _, candidate := range snapshot.CandidateRanks {
		if candidate.Call == proposedCall || candidate.Support != support {
			continue
		}
		var left, right []ResolverReceiverObservation
		for _, group := range snapshot.receiverEvidence.candidates {
			if group.call == proposedCall {
				left = group.receivers
			}
			if group.call == candidate.Call {
				right = group.receivers
			}
		}
		overlap := 0
		for i, j := 0, 0; i < len(left) && j < len(right); {
			switch {
			case left[i].Receiver < right[j].Receiver:
				i++
			case left[i].Receiver > right[j].Receiver:
				j++
			default:
				overlap++
				i++
				j++
			}
		}
		in := ResolverAmbiguityInput{WinnerSupport: support, RunnerSupport: support,
			OverlapCount: overlap, MinSeparationKHz: r.cfg.FreqGuardMinSeparationKHz,
			RunnerUpRatio: r.cfg.FreqGuardRunnerUpRatio, MaxEditDistance: r.cfg.MaxEditDistance}
		// Fully overlapping capped populations avoid distance and frequency work.
		if !RejectResolverAmbiguity(in) {
			continue
		}
		a, b := normalizeCorrectionCallIdentity(proposedCall), normalizeCorrectionCallIdentity(candidate.Call)
		_, in.Related = detectCorrectionFamilyByIdentity(a, b, r.cfg.FamilyPolicy)
		in.Distance = correctionDistance(a, b, snapshot.Key.Mode, r.cfg.DistanceModelCW, r.cfg.DistanceModelRTTY)
		if !RejectResolverAmbiguity(in) {
			continue
		}
		var witnessed bool
		in.WinnerFrequencyKHz, in.RunnerFrequencyKHz, witnessed = closestResolverFrequencyPair(snapshot.ReceiverEvidence(), proposedCall, candidate.Call)
		if witnessed && RejectResolverAmbiguity(in) {
			return true
		}
	}
	return false
}

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
