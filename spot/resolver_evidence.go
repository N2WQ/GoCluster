// File role: Publishes bounded, immutable receiver evidence for resolver selection.
// The resolver owner replaces generations; readers never receive mutable slices.
// Temporal consumers use Compact to retain scoring tallies without these rows.
package spot

import (
	"sort"
	"time"
)

// ResolverSupportBasis names the denominator contract, independently of whether
// a percentage is available. Weighted zero totals remain weighted, not raw.
type ResolverSupportBasis uint8

const (
	ResolverSupportBasisUnspecified ResolverSupportBasis = iota
	ResolverSupportBasisWeighted
	ResolverSupportBasisReceivers
	ResolverSupportBasisUnavailable
)

// EffectiveSupportBasis accepts legacy positive weighted snapshots, but does
// not infer raw evidence from missing or zero weights. Explicit unavailable is
// absorbing so mixed neighborhoods cannot regain authority through that legacy
// compatibility path. Deliberately raw snapshots carry no weighted tallies.
func (s ResolverSnapshot) EffectiveSupportBasis() ResolverSupportBasis {
	switch s.SupportBasis {
	case ResolverSupportBasisUnspecified:
		if s.TotalWeightedSupportMilli > 0 {
			return ResolverSupportBasisWeighted
		}
	case ResolverSupportBasisWeighted:
		if s.TotalWeightedSupportMilli >= 0 {
			return ResolverSupportBasisWeighted
		}
	case ResolverSupportBasisReceivers:
		if s.TotalWeightedSupportMilli != 0 || s.WinnerWeightedSupportMilli != 0 || s.RunnerWeightedSupportMilli != 0 {
			return ResolverSupportBasisUnavailable
		}
		for _, candidate := range s.CandidateRanks {
			if candidate.WeightedSupportMilli != 0 {
				return ResolverSupportBasisUnavailable
			}
		}
		return ResolverSupportBasisReceivers
	}
	return ResolverSupportBasisUnavailable
}

type resolverReporterObservation struct {
	observedAt        time.Time
	frequencyKHz      float64
	frequencyConflict bool
}

// ResolverReceiverObservation is a by-value observation from a published key.
// WeightMilli is the existing receiver reliability weight, not a probability.
type ResolverReceiverObservation struct {
	Receiver          string
	ObservedAt        time.Time
	FrequencyKHz      float64
	WeightMilli       int
	FrequencyConflict bool
}

type resolverEvidenceCandidate struct {
	call      string
	receivers []ResolverReceiverObservation
}

// ResolverEvidenceView is an immutable generation owned by a resolver key.
// Its rows are bounded by MaxCandidatesPerKey * MaxReportersPerCand; deletion
// and eviction are coupled to the owner's existing key/candidate/receiver paths.
type ResolverEvidenceView struct {
	window     time.Duration
	count      int
	candidates []resolverEvidenceCandidate
}

// ReceiverEvidence returns the immutable current observations, or nil for a
// compact/synthetic snapshot. CandidateRanks retain their existing read-only contract.
func (s ResolverSnapshot) ReceiverEvidence() *ResolverEvidenceView { return s.receiverEvidence }

// Compact preserves all scoring tallies without retaining receiver generations.
func (s ResolverSnapshot) Compact() ResolverSnapshot {
	s.receiverEvidence = nil
	return s
}

// Len counts candidate/receiver incidences, including overlapping memberships.
func (v *ResolverEvidenceView) Len() int {
	if v == nil {
		return 0
	}
	return v.count
}

// RecencyWindow is the owner's key-wide eligibility window at publication.
func (v *ResolverEvidenceView) RecencyWindow() time.Duration {
	if v == nil {
		return 0
	}
	return v.window
}

// Range visits deterministically ordered rows by value; false stops iteration.
// Mutating the callback value cannot alter this or any later publication.
func (v *ResolverEvidenceView) Range(visit func(call string, observation ResolverReceiverObservation) bool) {
	if v == nil || visit == nil {
		return
	}
	for _, candidate := range v.candidates {
		for _, observation := range candidate.receivers {
			if !visit(candidate.call, observation) {
				return
			}
		}
	}
}

func publishedResolverEvidence(st *resolverKeyState) *ResolverEvidenceView {
	if st.publishedReceiverEvidence != nil && !st.receiverEvidenceDirty {
		return st.publishedReceiverEvidence
	}
	view := &ResolverEvidenceView{window: st.recencyWindow,
		candidates: make([]resolverEvidenceCandidate, 0, len(st.candidates))}
	for call, candidate := range st.candidates {
		if candidate == nil || len(candidate.reporters) == 0 {
			continue
		}
		rows := make([]ResolverReceiverObservation, 0, len(candidate.reporters))
		for receiver, observation := range candidate.reporters {
			weight := resolverReliabilityScale
			if ref, ok := st.reporterRefs[receiver]; ok {
				weight = ref.weightMilli
			}
			rows = append(rows, ResolverReceiverObservation{Receiver: receiver,
				ObservedAt: observation.observedAt, FrequencyKHz: observation.frequencyKHz,
				FrequencyConflict: observation.frequencyConflict, WeightMilli: weight})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Receiver < rows[j].Receiver })
		view.candidates = append(view.candidates, resolverEvidenceCandidate{call: call, receivers: rows})
		view.count += len(rows)
	}
	sort.Slice(view.candidates, func(i, j int) bool { return view.candidates[i].call < view.candidates[j].call })
	st.publishedReceiverEvidence = view
	st.receiverEvidenceDirty = false
	return view
}
