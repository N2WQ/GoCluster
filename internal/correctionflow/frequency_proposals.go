// File role: Request-local, replay-only frequency association and nomination.
// No view or receiver backing escapes in a proposal. Owner settlement and the
// ordinary/public selection remain separate from the group numerical evidence.
package correctionflow

import (
	"math"
	"math/bits"
	"sort"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// FrequencySource is one actual source read, including ordinary-excluded keys.
// Evidence is separate to permit verified study imports without forging a
// resolver publication. Snapshot ranks and pending endpoints retain owner order.
// Inputs are borrowed only for the duration of AssessFrequencyProposals.
type FrequencySource struct {
	RequestedKey                        spot.ResolverSignalKey
	Read, Present                       bool
	Snapshot                            spot.ResolverSnapshot
	Evidence                            *spot.ResolverEvidenceView
	PendingIncumbent, PendingChallenger string
}

// FrequencySourceFromSnapshot records existing authority; it does not settle it.
func FrequencySourceFromSnapshot(key spot.ResolverSignalKey, snapshot spot.ResolverSnapshot, read, present bool) FrequencySource {
	i, c, _ := snapshot.PendingTransition()
	return FrequencySource{RequestedKey: key, Read: read, Present: present, Snapshot: snapshot,
		Evidence: snapshot.ReceiverEvidence(), PendingIncumbent: i, PendingChallenger: c}
}

type FrequencySourceWitness struct {
	Key           spot.ResolverSignalKey
	Read, Present bool
	Generation    uint64
}

// FrequencyProposal contains gate numbers, never an alternative public snapshot.
type FrequencyProposal struct {
	Call, Identity                              string
	Support, WeightedSupportMilli               int
	SubjectSupport, SubjectWeightedSupportMilli int
	TotalReporters, TotalWeightedSupportMilli   int
	ConfidencePercent                           int
	SupportBasis                                spot.ResolverSupportBasis
	ComparableEditNeighbor                      bool
}

type FrequencyAssessment struct {
	Reason                                              string
	Components, AssociatedComponent, ExcludedComponents int
	Witnesses                                           [5]FrequencySourceWitness
	Proposals                                           []FrequencyProposal // At most 80; rank before selecting eight gate trials.
	BeamLimited                                         bool
}

type FrequencyProposalOptions struct {
	GapHz                int
	MaxWitnessSeparation time.Duration
	MaxDistance          int
}

type frequencyRow struct {
	call, identity string
	observation    spot.ResolverReceiverObservation
	component      int
}

type frequencyComponent struct {
	receivers map[string][]time.Time
}

// AssessFrequencyProposals is pure and CW-only. The caller supplies current
// eligible reads, not an ingest barrier or a saved temporal score. Every raw
// incidence is bounded before growth/alias folding; failure leaves ordinary
// correction untouched. Generation witnesses identify lifecycles, not freshness.
func AssessFrequencyProposals(key spot.ResolverSignalKey, subject string, focalKHz float64,
	ordinary ResolverPrimarySelection, sources [5]FrequencySource, cfg config.CallCorrectionConfig, options FrequencyProposalOptions) FrequencyAssessment {
	out := FrequencyAssessment{Reason: "unavailable", AssociatedComponent: -1}
	if key.Mode != "CW" || !frequencyModelCall(spot.CorrectionVoteKey(subject)) ||
		(options.GapHz != 150 && options.GapHz != 250) ||
		(options.MaxWitnessSeparation != time.Minute && options.MaxWitnessSeparation != 2*time.Minute) ||
		(options.MaxDistance != 1 && options.MaxDistance != 2) || !positiveFrequency(focalKHz) {
		return out
	}
	if ordinary.NeighborhoodSplit || ordinary.generationConflict || ordinary.Snapshot.EvidenceLoss ||
		ordinary.SnapshotOK && (ordinary.Snapshot.State == spot.ResolverStateSplit || ordinary.Snapshot.State == spot.ResolverStateUncertain) {
		out.Reason = "ordinary_protected"
		return out
	}
	rows, basis, reason := frequencySourceRows(key, sources, cfg, &out)
	if reason != "" {
		out.Reason = reason
		return out
	}
	components, masks, reason := partitionFrequencyRows(rows, focalKHz, options.GapHz, &out)
	if reason != "" {
		out.Reason = reason
		return out
	}
	var excluded [16]bool
	for i := range components {
		if i != out.AssociatedComponent && frequencyPairSeparated(components[out.AssociatedComponent], components[i], options.MaxWitnessSeparation) {
			excluded[i] = true
			out.ExcludedComponents++
		}
	}
	views := make([]*spot.ResolverEvidenceView, 0, 5)
	for i := range sources {
		source := &sources[i]
		if !source.Read || !source.Present || source.Evidence == nil {
			continue
		}
		views = append(views, source.Evidence.Filter(func(call string, r spot.ResolverReceiverObservation) bool {
			// Spanning identities remain unresolved dissent, including their far
			// rows. Unknown/conflicted frequencies are never erased by geometry.
			if bits.OnesCount16(masks[spot.CorrectionVoteKey(call)]) > 1 || !knownFrequency(r) {
				return true
			}
			component := frequencyRowComponent(rows, r.FrequencyKHz)
			return component < 0 || !excluded[component]
		}))
	}
	union := spot.MergeResolverEvidenceViews(views...)
	ranks := union.CanonicalSupport(cfg.FamilyPolicy.SlashPrecedenceMinReports)
	total, weighted, conflict := union.ReceiverTotals()
	if conflict {
		out.Reason = "frequency_conflict"
		return out
	}
	population := spot.ResolverSnapshot{Key: key, SupportBasis: basis, CandidateRanks: ranks,
		TotalReporters: total, TotalWeightedSupportMilli: weighted}
	groups := make(map[string]*neighborhoodCandidate, len(ranks))
	for _, rank := range ranks {
		groups[rank.Identity()] = &neighborhoodCandidate{call: rank.Call, support: rank.Support,
			weighted: rank.WeightedSupportMilli, rank: rank, evidence: union}
	}
	for _, rank := range ranks {
		identity := rank.Identity()
		if rank.ReplacementBlocked || identity == spot.CorrectionVoteKey(subject) || !frequencyModelCall(identity) ||
			masks[identity] != uint16(1)<<out.AssociatedComponent || !frequencyNomineeAuthority(identity, sources) {
			continue
		}
		distance := spot.CorrectionCandidateDistance(subject, rank.Call, key.Mode, cfg.DistanceModelCW, cfg.DistanceModelRTTY)
		if distance < 1 || distance > options.MaxDistance || cfg.MaxEditDistance >= 0 && distance > cfg.MaxEditDistance {
			continue
		}
		if frequencyProposalPending(rank.Call, key.Mode, sources, cfg) {
			continue
		}
		tied := false
		for _, other := range ranks {
			if other.Identity() != identity && rank.Support == other.Support &&
				neighborhoodPairAmbiguousDiagnostics(identity, other.Identity(), key.Mode, groups, cfg, nil) {
				tied = true
				break
			}
		}
		confidence, ok := ResolverCallConfidencePercent(population, rank.Call)
		if tied || !ok {
			continue
		}
		out.Proposals = append(out.Proposals, FrequencyProposal{Call: strings.Clone(rank.Call), Identity: strings.Clone(identity),
			Support: rank.Support, WeightedSupportMilli: rank.WeightedSupportMilli,
			SubjectSupport: ResolverSupportForCall(population, subject), SubjectWeightedSupportMilli: ResolverWeightedSupportForCall(population, subject),
			TotalReporters: total, TotalWeightedSupportMilli: weighted, ConfidencePercent: confidence, SupportBasis: basis,
			ComparableEditNeighbor: spot.ResolverSnapshotHasComparableEditNeighbor(population, rank.Call, key.Mode, cfg.DistanceModelCW, cfg.DistanceModelRTTY)})
	}
	sort.Slice(out.Proposals, func(i, j int) bool { return frequencyGeometryAhead(out.Proposals[i], out.Proposals[j]) })
	out.BeamLimited = len(out.Proposals) > 8
	out.Reason = "assessed"
	return out
}

func frequencyModelCall(call string) bool {
	if call == "" || len(call) > 32 {
		return false
	}
	for _, c := range []byte(call) {
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '/' {
			return false
		}
	}
	return true
}

func frequencyGeometryAhead(a, b FrequencyProposal) bool {
	if a.WeightedSupportMilli != b.WeightedSupportMilli {
		return a.WeightedSupportMilli > b.WeightedSupportMilli
	}
	if a.Support != b.Support {
		return a.Support > b.Support
	}
	return a.Identity < b.Identity
}

func positiveFrequency(f float64) bool { return f > 0 && !math.IsNaN(f) && !math.IsInf(f*1000, 0) }
func knownFrequency(r spot.ResolverReceiverObservation) bool {
	return !r.FrequencyConflict && positiveFrequency(r.FrequencyKHz)
}

// frequencySourceRows counts raw slots per key before filtering or alias joins.
func frequencySourceRows(key spot.ResolverSignalKey, sources [5]FrequencySource, cfg config.CallCorrectionConfig,
	out *FrequencyAssessment) ([]frequencyRow, spot.ResolverSupportBasis, string) {
	radius := 0
	if cfg.ResolverNeighborhoodEnabled {
		radius = min(2, max(1, cfg.ResolverNeighborhoodBucketRadius))
	}
	var rows []frequencyRow
	basis := spot.ResolverSupportBasisUnspecified
	slots := 0
	for i := range sources {
		source := &sources[i]
		if len(source.RequestedKey.Band) > 1024 || len(source.RequestedKey.Mode) > 1024 {
			return nil, basis, "label_limit"
		}
		w := FrequencySourceWitness{Key: source.RequestedKey, Read: source.Read, Present: source.Present}
		w.Key.Band, w.Key.Mode = strings.Clone(w.Key.Band), strings.Clone(w.Key.Mode)
		if source.Present {
			w.Generation = source.Snapshot.Generation
		}
		out.Witnesses[i] = w
	}
	for i := range sources {
		source := &sources[i]
		if !source.Read {
			if source.Present {
				return nil, basis, "invalid_source"
			}
			continue
		}
		expected := key
		expected.Bucket += int64(i - 2)
		if i < 2-radius || i > 2+radius || source.RequestedKey != expected {
			return nil, basis, "invalid_source"
		}
		if !source.Present {
			continue
		}
		if source.Snapshot.Key != expected || source.Evidence == nil {
			return nil, basis, "missing_evidence"
		}
		if source.Snapshot.EvidenceLoss {
			return nil, basis, "evidence_loss"
		}
		if source.Evidence.Len() > 5120-len(rows) {
			return nil, basis, "incidence_limit"
		}
		incoming := source.Snapshot.EffectiveSupportBasis()
		if basis == spot.ResolverSupportBasisUnspecified {
			basis = incoming
		} else if basis != incoming {
			basis = spot.ResolverSupportBasisUnavailable
		}
		calls := make(map[string]bool, 16)
		failure := ""
		source.Evidence.Range(func(call string, r spot.ResolverReceiverObservation) bool {
			if len(call) > 1024 || len(r.Receiver) > 1024 {
				failure = "label_limit"
				return false
			}
			if !calls[call] && slots+len(calls) == 80 {
				failure = "spelling_limit"
				return false
			}
			// Check the literal rows: a newer cross-source canonical join must
			// never conceal an earlier source's still-current conflict.
			if r.FrequencyConflict {
				failure = "frequency_conflict"
				return false
			}
			calls[call] = true
			rows = append(rows, frequencyRow{call: call, identity: spot.CorrectionVoteKey(call), observation: r, component: -1})
			return true
		})
		if failure != "" {
			return nil, basis, failure
		}
		slots += len(calls)
		if slots > 80 {
			return nil, basis, "spelling_limit"
		}
	}
	if basis != spot.ResolverSupportBasisWeighted && basis != spot.ResolverSupportBasisReceivers {
		return nil, basis, "support_basis"
	}
	return rows, basis, ""
}

func partitionFrequencyRows(rows []frequencyRow, focal float64, gapHz int, out *FrequencyAssessment) ([]frequencyComponent, map[string]uint16, string) {
	// Unknowns sort last and have no component. Ties need no lexical decision.
	sort.Slice(rows, func(i, j int) bool {
		a, b := knownFrequency(rows[i].observation), knownFrequency(rows[j].observation)
		if a != b {
			return a
		}
		return rows[i].observation.FrequencyKHz < rows[j].observation.FrequencyKHz
	})
	var components []frequencyComponent
	masks := make(map[string]uint16)
	attached := make(map[int]bool)
	for i := range rows {
		r := &rows[i]
		if !knownFrequency(r.observation) {
			continue
		}
		// Compare in the declared Hz unit, converting each measured frequency
		// before subtraction. No epsilon widens the fixed study thresholds.
		if i == 0 || r.observation.FrequencyKHz*1000-rows[i-1].observation.FrequencyKHz*1000 >= float64(gapHz) {
			if len(components) == 16 {
				return nil, nil, "component_limit"
			}
			components = append(components, frequencyComponent{receivers: make(map[string][]time.Time)})
		}
		r.component = len(components) - 1
		c := &components[r.component]
		c.receivers[r.observation.Receiver] = append(c.receivers[r.observation.Receiver], r.observation.ObservedAt)
		masks[r.identity] |= uint16(1) << r.component
		if math.Abs(r.observation.FrequencyKHz*1000-focal*1000) <= 100 {
			attached[r.component] = true
		}
	}
	out.Components = len(components)
	if len(attached) != 1 {
		return nil, nil, "focal_association"
	}
	for c := range attached {
		out.AssociatedComponent = c
	}
	return components, masks, ""
}

func frequencyRowComponent(rows []frequencyRow, f float64) int {
	i := sort.Search(len(rows), func(i int) bool { return !knownFrequency(rows[i].observation) || rows[i].observation.FrequencyKHz >= f })
	if i < len(rows) && knownFrequency(rows[i].observation) && rows[i].observation.FrequencyKHz == f {
		return rows[i].component
	}
	return -1
}

// Exclusion is pairwise relative to the focal component, never transitive.
func frequencyPairSeparated(a, b frequencyComponent, maxTime time.Duration) bool {
	if len(a.receivers) < 2 || len(b.receivers) < 2 {
		return false
	}
	shared := 0
	for receiver, times := range a.receivers {
		witnessed := false
		for _, left := range times {
			for _, right := range b.receivers[receiver] {
				if left.Sub(right).Abs() <= maxTime {
					witnessed = true
					break
				}
			}
			if witnessed {
				break
			}
		}
		if witnessed {
			shared++
			if shared == 2 {
				return true
			}
		}
	}
	return false
}

func frequencyNomineeAuthority(identity string, sources [5]FrequencySource) bool {
	for i := range sources {
		s := &sources[i]
		if !s.Read || !s.Present || (s.Snapshot.State != spot.ResolverStateConfident && s.Snapshot.State != spot.ResolverStateProbable) {
			continue
		}
		if rank, ok := s.Snapshot.CandidateForCall(identity); ok && rank.Support > 0 && !rank.ReplacementBlocked {
			return true
		}
	}
	return false
}

func frequencyProposalPending(call, mode string, sources [5]FrequencySource, cfg config.CallCorrectionConfig) bool {
	for i := range sources {
		s := &sources[i]
		if !s.Read || !s.Present || s.PendingIncumbent == "" || s.PendingChallenger == "" ||
			ResolverSupportForCall(s.Snapshot, s.PendingIncumbent) == 0 || ResolverSupportForCall(s.Snapshot, s.PendingChallenger) == 0 {
			continue
		}
		a, _ := resolverNeighborhoodComparable(call, s.PendingIncumbent, mode, cfg)
		b, _ := resolverNeighborhoodComparable(call, s.PendingChallenger, mode, cfg)
		if a && b {
			return true
		}
	}
	return false
}
