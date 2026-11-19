// File role: Evaluates ambiguity from retained receiver frequency witnesses.
// Work is bounded by the resolver's candidate/receiver caps; no pair cache.
package spot

import (
	"math"
	"sort"
)

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
	proposal, ok := snapshot.CandidateForCall(proposedCall)
	if !ok || proposal.Support < ambiguousMultiSignalMinSupport {
		return false
	}
	left := resolverCanonicalRows(snapshot.receiverEvidence, proposal.Identity())
	for _, candidate := range snapshot.CandidateRanks {
		if candidate.Identity() == proposal.Identity() || candidate.Support != proposal.Support {
			continue
		}
		right := resolverCanonicalRows(snapshot.receiverEvidence, candidate.Identity())
		in := ResolverAmbiguityInput{WinnerSupport: proposal.Support, RunnerSupport: candidate.Support, OverlapCount: resolverReceiverOverlap(left, right), MinSeparationKHz: r.cfg.FreqGuardMinSeparationKHz, RunnerUpRatio: r.cfg.FreqGuardRunnerUpRatio, MaxEditDistance: r.cfg.MaxEditDistance}
		if !RejectResolverAmbiguity(in) {
			continue
		}
		a, b := normalizeCorrectionCallIdentity(proposal.Identity()), normalizeCorrectionCallIdentity(candidate.Identity())
		_, in.Related = detectCorrectionFamilyByIdentity(a, b, r.cfg.FamilyPolicy)
		in.Distance = correctionDistance(a, b, snapshot.Key.Mode, r.cfg.DistanceModelCW, r.cfg.DistanceModelRTTY)
		if !RejectResolverAmbiguity(in) {
			continue
		}
		var witnessed bool
		in.WinnerFrequencyKHz, in.RunnerFrequencyKHz, witnessed = closestResolverFrequencyPair(snapshot.ReceiverEvidence(), proposal.Identity(), candidate.Identity())
		if witnessed && RejectResolverAmbiguity(in) {
			return true
		}
	}
	return false
}

func resolverEvidenceHasFrequencyConflict(view *ResolverEvidenceView) bool {
	candidates := canonicalResolverCandidates(view)
	for i := range candidates {
		for _, row := range candidates[i].receivers {
			if row.FrequencyConflict {
				return true
			}
		}
	}
	return false
}

// closestResolverFrequencyPair retains exact binary64 witnesses. Small raw
// populations scan directly. Large canonical unions sort bounded local frequency
// copies and walk them once instead of introducing a lifetime pair cache.
func closestResolverFrequencyPair(view *ResolverEvidenceView, leftCall, rightCall string) (float64, float64, bool) {
	if view != nil && len(view.frequencyWitnesses) > 0 {
		// A neighborhood must preserve source-bucket witnesses, but the guard
		// needs only their original frequency bits. Copying complete receiver
		// structs here multiplies temporary storage by timestamps/SNR/strings.
		return closestResolverSortedFrequencies(resolverMergedFrequencyValues(view, leftCall), resolverMergedFrequencyValues(view, rightCall))
	}
	left, right := resolverFrequencyRows(view, leftCall), resolverFrequencyRows(view, rightCall)
	if len(left) == 0 || len(right) == 0 {
		return 0, 0, false
	}
	minimum := math.Inf(1)
	var a, b float64
	if len(left)*len(right) <= 64*64 {
		for _, l := range left {
			for _, r := range right {
				if separation := math.Abs(l.FrequencyKHz - r.FrequencyKHz); separation < minimum {
					minimum, a, b = separation, l.FrequencyKHz, r.FrequencyKHz
				}
			}
		}
	} else {
		ls, rs := make([]float64, len(left)), make([]float64, len(right))
		for i := range left {
			ls[i] = left[i].FrequencyKHz
		}
		for i := range right {
			rs[i] = right[i].FrequencyKHz
		}
		return closestResolverSortedFrequencies(ls, rs)
	}
	return a, b, !math.IsInf(minimum, 1)
}

func resolverMergedFrequencyValues(view *ResolverEvidenceView, call string) []float64 {
	identity := CorrectionVoteKey(call)
	count := 0
	for _, source := range view.frequencyWitnesses {
		if source.call == identity {
			count += len(source.receivers)
		}
	}
	if count == 0 {
		return nil
	}
	values := make([]float64, 0, count)
	for _, source := range view.frequencyWitnesses {
		if source.call != identity {
			continue
		}
		for i := range source.receivers {
			values = append(values, source.receivers[i].FrequencyKHz)
		}
	}
	return values
}

// Both inputs own local backing. Sorting cannot alter a published generation.
func closestResolverSortedFrequencies(left, right []float64) (float64, float64, bool) {
	if len(left) == 0 || len(right) == 0 {
		return 0, 0, false
	}
	sort.Float64s(left)
	sort.Float64s(right)
	minimum := math.Inf(1)
	var a, b float64
	for i, j := 0, 0; i < len(left) && j < len(right); {
		if separation := math.Abs(left[i] - right[j]); separation < minimum {
			minimum, a, b = separation, left[i], right[j]
		}
		if left[i] < right[j] {
			i++
		} else {
			j++
		}
	}
	return a, b, !math.IsInf(minimum, 1)
}
