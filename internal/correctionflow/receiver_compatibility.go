// File role: Pure experimental receiver-compatibility comparison B for replay.
// Scores describe a common observed population; they are not likelihoods, odds,
// calibrated confidence or RF truth. All borrowed evidence is request-local.
package correctionflow

import (
	"math"
	"sort"
	"strings"

	"dxcluster/config"
	"dxcluster/spot"
)

// ReceiverCompatibilityScore is one owned canonical hypothesis. Eligibility is
// C's surviving nomination authority; it never prunes the opposing population.
type ReceiverCompatibilityScore struct {
	Identity       string
	Mean           float64
	Eligible, Keep bool
}

// ReceiverCompatibilityAssessment keeps C diagnostics separate from B's sole
// selection. Proposals are diagnostic C results, never a B fallback or gate beam.
// Unavailable B has no partial Hypotheses or selection. Callers must intersect
// Selected with temporal authority after selection and try at most that one gate.
type ReceiverCompatibilityAssessment struct {
	SelectiveFrequencyAssessment
	Available           bool
	CompatibilityReason string
	Hypotheses          []ReceiverCompatibilityScore // At most nine; owned scalars only.
	Selected            FrequencyProposal
	SelectedOK          bool
	ScoreCalls          int
	Tolerance           float64
	Denominator         float64
}

type receiverCompatibilityMembership struct {
	receiver string
	weight   float64
	identity int
	bin      int
}

// Neumaier compensation also preserves small same-sign score contributions.
// Every accumulator is request-local; invalid intermediate arithmetic refuses
// the whole comparison rather than dropping a term or inventing a neutral score.
type receiverCompatibilitySum struct{ sum, correction float64 }

func (s *receiverCompatibilitySum) add(value float64) bool {
	next := s.sum + value
	if math.Abs(s.sum) >= math.Abs(value) {
		s.correction += (s.sum - next) + value
	} else {
		s.correction += (value - next) + s.sum
	}
	s.sum = next
	return receiverCompatibilityFinite(s.sum) && receiverCompatibilityFinite(s.correction)
}

func (s receiverCompatibilitySum) value() float64 { return s.sum + s.correction }

func receiverCompatibilityFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

// AssessReceiverCompatibilityProposals reuses C's bounded frequency association
// and nomination, but scores every retained identity, including C-disqualified
// and zero-weight opponents, plus canonical KEEP. No gate, publication, cache,
// worker or model fitting is performed. Missing model evidence preserves the
// caller's ordinary result; neither C nor a lower B hypothesis is a fallback.
func AssessReceiverCompatibilityProposals(key spot.ResolverSignalKey, subject string, focalKHz float64,
	ordinary ResolverPrimarySelection, sources [5]FrequencySource, cfg config.CallCorrectionConfig,
	options FrequencyProposalOptions, model *OrdinalModel) ReceiverCompatibilityAssessment {
	assessment, context := prepareSelectiveFrequencyProposals(key, subject, focalKHz, ordinary, sources, cfg, options)
	out := ReceiverCompatibilityAssessment{SelectiveFrequencyAssessment: assessment}
	if context.union == nil {
		out.CompatibilityReason = assessment.Reason
		return out
	}
	hypotheses, reason := receiverCompatibilityHypotheses(subject, context.ranks, assessment.Proposals)
	if reason != "" {
		out.CompatibilityReason = reason
		return out
	}
	memberships, reason := receiverCompatibilityMemberships(context)
	if reason != "" {
		out.CompatibilityReason = reason
		return out
	}
	cells, denominator, reason := receiverCompatibilityCells(memberships)
	if reason != "" {
		out.CompatibilityReason = reason
		return out
	}
	out.Denominator = denominator
	out.ScoreCalls, reason = receiverCompatibilityMeans(model, key.Mode, context.ranks, &cells, denominator, hypotheses)
	if reason != "" {
		out.CompatibilityReason = reason
		return out
	}
	out.Hypotheses, out.Available = hypotheses, true
	selectReceiverCompatibilityWinner(&out)
	return out
}

func receiverCompatibilityHypotheses(subject string, ranks []spot.ResolverCandidateSupport, proposals []FrequencyProposal) ([]ReceiverCompatibilityScore, string) {
	if len(ranks) > 8 {
		return nil, "population_limit"
	}
	keep := spot.CorrectionVoteKey(subject)
	hypotheses := make([]ReceiverCompatibilityScore, 0, len(ranks)+1)
	keepPresent := false
	for _, rank := range ranks {
		identity := rank.Identity()
		if !frequencyModelCall(identity) {
			return nil, "unusable_identity"
		}
		hypothesis := ReceiverCompatibilityScore{Identity: strings.Clone(identity), Keep: identity == keep}
		keepPresent = keepPresent || hypothesis.Keep
		for _, proposal := range proposals {
			if proposal.Identity == identity {
				hypothesis.Eligible = true
				break
			}
		}
		hypotheses = append(hypotheses, hypothesis)
	}
	if !keepPresent {
		hypotheses = append(hypotheses, ReceiverCompatibilityScore{Identity: strings.Clone(keep), Keep: true})
	}
	sort.Slice(hypotheses, func(i, j int) bool { return hypotheses[i].Identity < hypotheses[j].Identity })
	return hypotheses, ""
}

func receiverCompatibilityMemberships(context frequencyProposalContext) ([]receiverCompatibilityMembership, string) {
	weighted := context.population.SupportBasis == spot.ResolverSupportBasisWeighted
	memberships := make([]receiverCompatibilityMembership, 0, context.union.Len())
	reason := ""
	context.union.RangeCanonical(func(identity, _ string, row spot.ResolverReceiverObservation) bool {
		weight := float64(1)
		if weighted {
			weight = float64(row.WeightMilli)
			if weight <= 0 {
				return true
			}
		}
		bin := ordinalSNRBin(context.population.Key.Mode, row.Report, row.HasReport)
		if row.SNRConflict || bin < 0 {
			reason = "snr_unavailable"
			return false
		}
		// The retained rank list and canonical rows come from the same union.
		// Its at-most-eight identities make this small linear lookup bounded.
		for i, rank := range context.ranks {
			if rank.Identity() == identity {
				memberships = append(memberships, receiverCompatibilityMembership{row.Receiver, weight, i, bin})
				break
			}
		}
		return true
	})
	if reason != "" {
		return nil, reason
	}
	// Canonical joining already removed each identity/receiver's source and
	// alias copies. Sorting now places its cross-identity memberships together;
	// no floating-point reduction follows map iteration order.
	sort.Slice(memberships, func(i, j int) bool {
		if memberships[i].receiver != memberships[j].receiver {
			return memberships[i].receiver < memberships[j].receiver
		}
		return memberships[i].identity < memberships[j].identity
	})
	return memberships, ""
}

func receiverCompatibilityCells(memberships []receiverCompatibilityMembership) ([8][6]receiverCompatibilitySum, float64, string) {
	var cells [8][6]receiverCompatibilitySum
	var denominator receiverCompatibilitySum
	for first := 0; first < len(memberships); {
		last, maximum := first, float64(0)
		var total receiverCompatibilitySum
		for last < len(memberships) && memberships[last].receiver == memberships[first].receiver {
			weight := memberships[last].weight
			if !total.add(weight) {
				return cells, 0, "nonfinite_arithmetic"
			}
			maximum = max(maximum, weight)
			last++
		}
		for _, membership := range memberships[first:last] {
			coefficient := maximum * membership.weight / total.value()
			if !cells[membership.identity][membership.bin].add(coefficient) {
				return cells, 0, "nonfinite_arithmetic"
			}
		}
		if !denominator.add(maximum) {
			return cells, 0, "nonfinite_arithmetic"
		}
		first = last
	}
	value := denominator.value()
	if !receiverCompatibilityFinite(value) {
		return cells, 0, "nonfinite_arithmetic"
	}
	if value <= 0 {
		return cells, 0, "zero_positive_weight"
	}
	return cells, value, ""
}

func receiverCompatibilityMeans(model *OrdinalModel, mode string, ranks []spot.ResolverCandidateSupport,
	cells *[8][6]receiverCompatibilitySum, denominator float64, hypotheses []ReceiverCompatibilityScore) (int, string) {
	calls := 0
	// Scores depend on the existing SNR bin, not the report's position inside
	// that bin. Use its inclusive upper endpoint only after validating every
	// positive membership's own measured SNR. Eight by six by nine is 432 calls.
	reports := [6]int{0, 6, 12, 18, 24, 999}
	for h := range hypotheses {
		var numerator receiverCompatibilitySum
		for identity, rank := range ranks {
			for bin, report := range reports {
				coefficient := cells[identity][bin].value()
				if coefficient == 0 {
					continue
				}
				calls++
				score, ok := model.Score(hypotheses[h].Identity, rank.Identity(), mode, report, true)
				if !ok {
					return calls, "ordinal_unavailable"
				}
				if !numerator.add(coefficient * score) {
					return calls, "nonfinite_arithmetic"
				}
			}
		}
		mean := numerator.value() / denominator
		if !receiverCompatibilityFinite(mean) {
			return calls, "nonfinite_arithmetic"
		}
		hypotheses[h].Mean = mean
	}
	return calls, ""
}

func selectReceiverCompatibilityWinner(out *ReceiverCompatibilityAssessment) {
	best, maximum := 0, float64(1)
	for i, hypothesis := range out.Hypotheses {
		maximum = max(maximum, math.Abs(hypothesis.Mean))
		if hypothesis.Mean > out.Hypotheses[best].Mean {
			best = i
		}
	}
	out.Tolerance = 1e-9 * maximum
	winner := out.Hypotheses[best]
	for i, opponent := range out.Hypotheses {
		if i == best {
			continue
		}
		margin := winner.Mean - opponent.Mean
		if !receiverCompatibilityFinite(margin) {
			out.Available, out.Hypotheses = false, nil
			out.CompatibilityReason = "nonfinite_arithmetic"
			return
		}
		if margin <= out.Tolerance {
			out.CompatibilityReason = "numerical_tie"
			return
		}
	}
	if winner.Keep {
		out.CompatibilityReason = "keep"
		return
	}
	if !winner.Eligible {
		out.CompatibilityReason = "ineligible_winner"
		return
	}
	for _, proposal := range out.Proposals {
		if proposal.Identity == winner.Identity {
			out.Selected, out.SelectedOK = proposal, true
			out.CompatibilityReason = "selected"
			return
		}
	}
}
