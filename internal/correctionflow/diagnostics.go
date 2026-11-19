// File role: Own bounded, detached diagnostic values for shared correction reads.
// Ordinary entry points pass nil and retain no capture state. Captures contain
// no resolver/view/request pointers; each invocation replaces its destination,
// and cloned strings cannot pin a source publication or CSV backing buffer.
package correctionflow

import (
	"errors"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

const (
	diagnosticMaxLabels          = 1024
	diagnosticMaxCandidates      = 80
	diagnosticMaxIncidences      = 5120
	diagnosticMaxEvents          = 32
	diagnosticMaxEventCandidates = 11
	diagnosticMaxRules           = 80*80*8 + 80*5*4 + 256
	diagnosticMaxBytes           = 8 << 20
	diagnosticMaxRowBytes        = 64 << 10
	diagnosticScaffoldBytes      = 16 << 10
)

type diagnosticBudget struct {
	bytes int
	err   error
}

func (b *diagnosticBudget) fail(message string) {
	if b.err == nil {
		b.err = errors.New(message)
	}
}
func (b *diagnosticBudget) take(bytes int) bool {
	if b.err != nil {
		return false
	}
	if bytes < 0 || bytes > diagnosticMaxBytes-b.bytes {
		b.fail("diagnostic record exceeds bounded encoded size")
		return false
	}
	b.bytes += bytes
	return true
}
func (b *diagnosticBudget) label(value string) bool {
	if len(value) > diagnosticMaxLabels {
		b.fail("diagnostic label exceeds 1024 bytes")
		return false
	}
	return b.err == nil
}
func (b *diagnosticBudget) text(value string) string {
	if !b.label(value) || !b.take(6*len(value)+8) {
		return ""
	}
	return strings.Clone(value)
}

// DiagnosticKey owns its labels; it does not borrow ResolverSignalKey strings.
type DiagnosticKey struct {
	Band, Mode  string
	Bucket      int64
	ToleranceHz int
}

type DiagnosticCandidate struct {
	Call, VoteKey                 string
	ObservedCalls                 []string
	ReplacementBlocked            bool
	Support, WeightedSupportMilli int
}

type DiagnosticReceiver struct {
	Call, Identity, Receiver                  string
	ObservedAt                                time.Time
	FrequencyKHz                              float64
	WeightMilli, Report                       int
	FrequencyConflict, HasReport, SNRConflict bool
	// Resolver publications expose no originating archive ordinal/raw ID.
	RawOrigin string
}

type DiagnosticSnapshot struct {
	Key                                                                               DiagnosticKey
	Generation                                                                        uint64
	EvidenceLoss                                                                      bool
	SupportBasis                                                                      spot.ResolverSupportBasis
	EvaluatedAt                                                                       time.Time
	State                                                                             spot.ResolverState
	Winner, RunnerUp, PendingIncumbent, PendingChallenger                             string
	WinnerSupport, RunnerSupport, Margin, TotalReporters                              int
	WinnerWeightedSupportMilli, RunnerWeightedSupportMilli, TotalWeightedSupportMilli int
	Candidates                                                                        []DiagnosticCandidate
}

type DiagnosticSelection struct {
	Snapshot                                                        DiagnosticSnapshot
	SnapshotOK, UsedNeighborhood, WinnerOverride, NeighborhoodSplit bool
	CandidateCount                                                  int
	ReplacementSetPresent                                           bool
	ReplacementCalls                                                []string
	ExactWinner                                                     string
	GenerationWitnesses                                             [5]uint64
	GenerationConflict                                              bool
	ExcludedUnrelated, ExcludedDistance, ExcludedAnchorMissing      int
}

type DiagnosticLookup struct {
	Read                                                      bool
	Sequence                                                  int
	RequestedKey                                              DiagnosticKey
	Present                                                   bool
	Snapshot                                                  DiagnosticSnapshot
	RawRows                                                   []DiagnosticReceiver
	RawEvidenceAvailable                                      bool
	RecencyWindow                                             time.Duration
	Admission                                                 string
	ComparabilityChecked, Comparable, ExactOverride, Admitted bool
	ComparabilityReason                                       string
}

type DiagnosticNomination struct {
	Slot                 int
	Call, Identity       string
	SourceState          spot.ResolverState
	Nominee              bool
	Created, BestUpdated bool
}

// A rule row exists only for an actual call. Missing later rows mean not
// reached; Exit and the per-input admission/rank statuses identify the stop.
type DiagnosticRule struct {
	Stage               string
	Slot                int
	Call, Other, Reason string
	Result              bool
	Inputs              spot.ResolverAmbiguityInput
}

type ResolverSelectionDiagnostics struct {
	InputKey                                DiagnosticKey
	Anchor                                  string
	EffectiveAnchor                         string
	EffectiveAnchorChecked, AnchorFromExact bool
	RequestedAt                             time.Time
	OwnerClock                              bool
	Exit                                    string
	Reads                                   [5]DiagnosticLookup
	RawSpellings, RawIncidences             int
	Merged                                  bool
	UnionTotalsChecked, UnionConflict       bool
	UnionReceivers, UnionWeighted           int
	RunnerConflictChecked, RunnerConflict   bool
	RunnerRatioThreshold                    float64
	MergedCandidates                        []DiagnosticCandidate
	CanonicalRows                           []DiagnosticReceiver
	Nominations                             []DiagnosticNomination
	Rules                                   []DiagnosticRule
	Output                                  DiagnosticSelection
	budget                                  diagnosticBudget
	candidateSpellings                      int
}

type DiagnosticGenerationCheck struct {
	Expected uint64
	Status   string
	Key      DiagnosticKey
	Present  bool
	Current  uint64
}

type DiagnosticTransform struct {
	Exit                                                                                                                   string
	Display                                                                                                                string
	SnapshotChecked, LossChecked, SplitChecked, WinnerChecked, DisplayChecked, EligibilityChecked, PendingChecked, Applied bool
}

type TemporalReleaseDiagnostics struct {
	Selection      ResolverSelectionDiagnostics
	ProposedAction TemporalDecisionAction
	ProposedWinner string
	SavedSelection DiagnosticSelection
	// Checks are visited in action, snapshot, loss, split, state order.
	OrdinaryGuardChecks                                         int
	OrdinaryGuardStop                                           string
	GenerationChecks                                            [5]DiagnosticGenerationCheck
	ExpectedGenerationWitnesses                                 [5]uint64
	GenerationConflict                                          bool
	GenerationResultChecked, GenerationCurrent                  bool
	WinnerChecked, EligibilityChecked, Eligible, SupportChecked bool
	ProposalSupport                                             int
	OpaqueTieChecked, OpaqueTie                                 bool
	OpaqueTieInternals                                          string
	ExactComparabilityChecked, ExactComparable                  bool
	Transform                                                   DiagnosticTransform
	Exit                                                        string
	Output                                                      DiagnosticSelection
	budget                                                      diagnosticBudget
}

// SelectResolverPrimarySnapshotForCallWithDiagnostics shares the ordinary core.
// A zero at preserves the existing owner-clock Lookup path; nonzero uses the
// existing LookupEligible path. Diagnostic failure does not change selection.
func SelectResolverPrimarySnapshotForCallWithDiagnostics(resolver *spot.SignalResolver, key spot.ResolverSignalKey, cfg config.CallCorrectionConfig,
	anchor string, at time.Time, dst *ResolverSelectionDiagnostics) (ResolverPrimarySelection, error) {
	if dst != nil {
		*dst = ResolverSelectionDiagnostics{}
		dst.begin(key, anchor, at)
	}
	selected := selectResolverPrimarySnapshotForCallDiagnostics(resolver, key, cfg, anchor, at, dst)
	if dst == nil {
		return selected, nil
	}
	return selected, dst.budget.err
}

// SelectTemporalReleaseSnapshotWithDiagnostics captures only checks that the
// existing release executes. A normal witness mismatch is a captured refusal,
// not a diagnostic failure. The opaque spot predicate is never re-evaluated.
func SelectTemporalReleaseSnapshotWithDiagnostics(resolver *spot.SignalResolver, key spot.ResolverSignalKey, cfg config.CallCorrectionConfig,
	subject string, decision TemporalDecision, at time.Time, dst *TemporalReleaseDiagnostics) (ResolverPrimarySelection, error) {
	if dst != nil {
		*dst = TemporalReleaseDiagnostics{}
		dst.Selection.begin(key, subject, at)
		for i := range dst.GenerationChecks {
			dst.GenerationChecks[i].Status = "not_reached"
		}
	}
	selected := selectTemporalReleaseSnapshotDiagnostics(resolver, key, cfg, subject, decision, at, dst)
	if dst == nil {
		return selected, nil
	}
	return selected, dst.budget.err
}

func (d *ResolverSelectionDiagnostics) begin(key spot.ResolverSignalKey, anchor string, at time.Time) {
	d.budget.take(diagnosticScaffoldBytes)
	d.InputKey = cloneDiagnosticKey(&d.budget, key)
	d.Anchor = d.budget.text(anchor)
	d.RequestedAt = at.UTC()
	d.OwnerClock = at.IsZero()
	for i := range d.Reads {
		d.Reads[i].Admission = "not_reached"
	}
}

// EncodedByteUpperBound includes all copied payloads and conservative fixed
// JSON scaffolding. The caller still budgets its own enclosing record fields.
func (d *ResolverSelectionDiagnostics) EncodedByteUpperBound() int { return d.budget.bytes }

// EncodedByteUpperBound includes the nested selector and release capture.
func (d *TemporalReleaseDiagnostics) EncodedByteUpperBound() int { return d.budget.bytes }

func cloneDiagnosticKey(b *diagnosticBudget, key spot.ResolverSignalKey) DiagnosticKey {
	return DiagnosticKey{Band: b.text(key.Band), Mode: b.text(key.Mode), Bucket: key.Bucket, ToleranceHz: key.ToleranceHz}
}

func cloneDiagnosticCandidates(b *diagnosticBudget, ranks []spot.ResolverCandidateSupport) []DiagnosticCandidate {
	if len(ranks) > diagnosticMaxCandidates {
		b.fail("diagnostic candidate bound exceeds 80")
		return nil
	}
	aliases := 0
	for i := range ranks {
		r := &ranks[i]
		aliases += max(1, len(r.ObservedCalls))
		if aliases > diagnosticMaxCandidates {
			b.fail("diagnostic raw spelling bound exceeds 80")
			return nil
		}
		if !b.label(r.Call) || !b.label(r.VoteKey) {
			return nil
		}
		rowBytes := 256 + 6*(len(r.Call)+len(r.VoteKey))
		for _, alias := range r.ObservedCalls {
			if !b.label(alias) {
				return nil
			}
			rowBytes += 6*len(alias) + 8
		}
		if rowBytes > diagnosticMaxRowBytes {
			b.fail("diagnostic candidate row exceeds 64 KiB")
			return nil
		}
		if !b.take(rowBytes) {
			return nil
		}
	}
	if b.err != nil {
		return nil
	}
	out := make([]DiagnosticCandidate, len(ranks))
	for i := range ranks {
		r := &ranks[i]
		out[i] = DiagnosticCandidate{Call: strings.Clone(r.Call), VoteKey: strings.Clone(r.VoteKey), ReplacementBlocked: r.ReplacementBlocked,
			Support: r.Support, WeightedSupportMilli: r.WeightedSupportMilli}
		if r.ObservedCalls != nil {
			out[i].ObservedCalls = make([]string, len(r.ObservedCalls))
			for j := range r.ObservedCalls {
				out[i].ObservedCalls[j] = strings.Clone(r.ObservedCalls[j])
			}
		}
	}
	return out
}

func cloneDiagnosticSnapshot(b *diagnosticBudget, s spot.ResolverSnapshot) DiagnosticSnapshot {
	if !b.take(1024) {
		return DiagnosticSnapshot{}
	}
	incumbent, challenger, _ := s.PendingTransition()
	out := DiagnosticSnapshot{Key: cloneDiagnosticKey(b, s.Key), Generation: s.Generation, EvidenceLoss: s.EvidenceLoss, SupportBasis: s.SupportBasis,
		EvaluatedAt: s.EvaluatedAt.UTC(), State: spot.ResolverState(b.text(string(s.State))), Winner: b.text(s.Winner), RunnerUp: b.text(s.RunnerUp),
		PendingIncumbent: b.text(incumbent), PendingChallenger: b.text(challenger), WinnerSupport: s.WinnerSupport, RunnerSupport: s.RunnerSupport,
		Margin: s.Margin, TotalReporters: s.TotalReporters, WinnerWeightedSupportMilli: s.WinnerWeightedSupportMilli, RunnerWeightedSupportMilli: s.RunnerWeightedSupportMilli,
		TotalWeightedSupportMilli: s.TotalWeightedSupportMilli}
	out.Candidates = cloneDiagnosticCandidates(b, s.CandidateRanks)
	return out
}

func cloneDiagnosticSelection(b *diagnosticBudget, s ResolverPrimarySelection) DiagnosticSelection {
	if !b.take(1024) {
		return DiagnosticSelection{}
	}
	out := DiagnosticSelection{Snapshot: cloneDiagnosticSnapshot(b, s.Snapshot), SnapshotOK: s.SnapshotOK, UsedNeighborhood: s.UsedNeighborhood,
		WinnerOverride: s.WinnerOverride, NeighborhoodSplit: s.NeighborhoodSplit, CandidateCount: s.CandidateCount, ReplacementSetPresent: s.replacementCalls != nil,
		ExactWinner: b.text(s.exactWinner), GenerationWitnesses: s.generationWitnesses, GenerationConflict: s.generationConflict,
		ExcludedUnrelated: s.NeighborhoodExcludedUnrelated, ExcludedDistance: s.NeighborhoodExcludedDistance, ExcludedAnchorMissing: s.NeighborhoodExcludedAnchorMissing}
	if len(s.replacementCalls) > diagnosticMaxCandidates {
		b.fail("diagnostic replacement bound exceeds 80")
		return out
	}
	for _, call := range s.replacementCalls {
		if !b.label(call) || !b.take(6*len(call)+8) {
			return out
		}
	}
	if b.err == nil && s.replacementCalls != nil {
		out.ReplacementCalls = make([]string, len(s.replacementCalls))
		for i := range s.replacementCalls {
			out.ReplacementCalls[i] = strings.Clone(s.replacementCalls[i])
		}
	}
	return out
}
