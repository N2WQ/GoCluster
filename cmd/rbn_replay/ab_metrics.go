package main

import (
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/strutil"
)

// replayConfidenceCounts tracks final confidence glyph frequencies.
// "unknown" corresponds to "?".
type replayConfidenceCounts struct {
	Samples uint64 `json:"samples"`
	Unknown uint64 `json:"unknown"`
	S       uint64 `json:"s"`
	P       uint64 `json:"p"`
	V       uint64 `json:"v"`
	C       uint64 `json:"c"`
	B       uint64 `json:"b"`
	Other   uint64 `json:"other"`
}

func (c *replayConfidenceCounts) Observe(glyph string) {
	if c == nil {
		return
	}
	c.Samples++
	switch normalizeConfidenceGlyph(glyph) {
	case "?":
		c.Unknown++
	case "S":
		c.S++
	case "P":
		c.P++
	case "V":
		c.V++
	case "C":
		c.C++
	case "B":
		c.B++
	default:
		c.Other++
	}
}

type replayConfidenceOutcome struct {
	Final string
}

type replayOutputABMetrics struct {
	ConfidenceCounts replayConfidenceCounts `json:"confidence_counts"`
}

type replayResolverStateCounts struct {
	Samples   uint64 `json:"samples"`
	Confident uint64 `json:"confident"`
	Probable  uint64 `json:"probable"`
	Uncertain uint64 `json:"uncertain"`
	Split     uint64 `json:"split"`
}

func (c *replayResolverStateCounts) Observe(state spot.ResolverState) {
	if c == nil {
		return
	}
	c.Samples++
	switch state {
	case spot.ResolverStateConfident:
		c.Confident++
	case spot.ResolverStateProbable:
		c.Probable++
	case spot.ResolverStateUncertain:
		c.Uncertain++
	case spot.ResolverStateSplit:
		c.Split++
	}
}

type replayResolverABMetrics struct {
	Samples                           uint64                    `json:"samples"`
	MissingSnapshot                   uint64                    `json:"missing_snapshot"`
	StateCounts                       replayResolverStateCounts `json:"state_counts"`
	ProjectedConfidenceCounts         replayConfidenceCounts    `json:"projected_confidence_counts"`
	NeighborhoodUsed                  uint64                    `json:"neighborhood_used"`
	NeighborhoodOverride              uint64                    `json:"neighborhood_winner_override"`
	NeighborhoodSplit                 uint64                    `json:"neighborhood_conflict_split"`
	NeighborhoodExcludedUnrelated     uint64                    `json:"neighborhood_excluded_unrelated"`
	NeighborhoodExcludedDistance      uint64                    `json:"neighborhood_excluded_distance"`
	NeighborhoodExcludedAnchorMissing uint64                    `json:"neighborhood_excluded_anchor_missing"`
	RecentPlus1Applied                uint64                    `json:"recent_plus1_applied"`
	RecentPlus1Rejected               uint64                    `json:"recent_plus1_rejected"`
	RecentPlus1RejectEdit             uint64                    `json:"recent_plus1_reject_edit_neighbor_contested"`
	RecentPlus1RejectDistance         uint64                    `json:"recent_plus1_reject_distance_or_family"`
	RecentPlus1RejectWinner           uint64                    `json:"recent_plus1_reject_winner_recent_insufficient"`
	RecentPlus1RejectSubject          uint64                    `json:"recent_plus1_reject_subject_not_weaker"`
	RecentPlus1RejectOther            uint64                    `json:"recent_plus1_reject_other"`
	BayesReportConsidered             uint64                    `json:"bayes_report_considered"`
	BayesReportApplied                uint64                    `json:"bayes_report_applied"`
	BayesReportRejected               uint64                    `json:"bayes_report_rejected"`
	BayesReportRejectEdit             uint64                    `json:"bayes_report_reject_edit_neighbor_contested"`
	BayesReportRejectWinner           uint64                    `json:"bayes_report_reject_winner_recent_insufficient"`
	BayesReportRejectSubject          uint64                    `json:"bayes_report_reject_subject_not_weaker"`
	BayesReportRejectCandidate        uint64                    `json:"bayes_report_reject_candidate_unvalidated"`
	BayesReportRejectScore            uint64                    `json:"bayes_report_reject_score_below_threshold"`
	BayesReportRejectOther            uint64                    `json:"bayes_report_reject_other"`
	BayesAdvantageConsidered          uint64                    `json:"bayes_advantage_considered"`
	BayesAdvantageApplied             uint64                    `json:"bayes_advantage_applied"`
	BayesAdvantageRejected            uint64                    `json:"bayes_advantage_rejected"`
	BayesAdvantageRejectScore         uint64                    `json:"bayes_advantage_reject_score_below_threshold"`
	BayesAdvantageRejectDelta         uint64                    `json:"bayes_advantage_reject_weighted_delta_insufficient"`
	BayesAdvantageRejectConfidence    uint64                    `json:"bayes_advantage_reject_confidence_insufficient"`
	BayesAdvantageRejectCandidate     uint64                    `json:"bayes_advantage_reject_candidate_unvalidated"`
	BayesAdvantageRejectSubject       uint64                    `json:"bayes_advantage_reject_subject_validated"`
	BayesAdvantageRejectOther         uint64                    `json:"bayes_advantage_reject_other"`
}

type replayStabilizerDelayProxyMetrics struct {
	Eligible                 uint64 `json:"eligible"`
	WouldDelay               uint64 `json:"would_delay"`
	ReasonUnknownOrNonRecent uint64 `json:"reason_unknown_or_nonrecent"`
	ReasonAmbiguousResolver  uint64 `json:"reason_ambiguous_resolver"`
	ReasonPLowConfidence     uint64 `json:"reason_p_low_confidence"`
	ReasonEditNeighbor       uint64 `json:"reason_edit_neighbor_contested"`
}

func (m *replayStabilizerDelayProxyMetrics) Observe(decision correctionflow.StabilizerDelayDecision) {
	if m == nil {
		return
	}
	m.Eligible++
	if !decision.ShouldDelay {
		return
	}
	m.WouldDelay++
	switch decision.Reason {
	case correctionflow.StabilizerDelayReasonUnknownOrNonRecent:
		m.ReasonUnknownOrNonRecent++
	case correctionflow.StabilizerDelayReasonAmbiguous:
		m.ReasonAmbiguousResolver++
	case correctionflow.StabilizerDelayReasonPLowConfidence:
		m.ReasonPLowConfidence++
	case correctionflow.StabilizerDelayReasonEditNeighbor:
		m.ReasonEditNeighbor++
	}
}

type replayABMetrics struct {
	Output               replayOutputABMetrics             `json:"output"`
	Resolver             replayResolverABMetrics           `json:"resolver"`
	StabilizerDelayProxy replayStabilizerDelayProxyMetrics `json:"stabilizer_delay_proxy"`
	Temporal             replayTemporalABMetrics           `json:"temporal"`
}

func (m *replayABMetrics) ObserveAppliedOutput(outcome replayConfidenceOutcome) {
	if m == nil {
		return
	}
	newGlyph := normalizeConfidenceGlyph(outcome.Final)
	m.Output.ConfidenceCounts.Observe(newGlyph)
}

func (m *replayABMetrics) ObserveResolverSnapshot(snap spot.ResolverSnapshot, ok bool) {
	if m == nil {
		return
	}
	m.Resolver.Samples++
	if !ok {
		m.Resolver.MissingSnapshot++
		return
	}
	m.Resolver.StateCounts.Observe(snap.State)
	projected := projectedResolverConfidenceOutcome(snap)
	m.Resolver.ProjectedConfidenceCounts.Observe(projected.Final)
}

func (m *replayABMetrics) ObserveResolverSelection(selection correctionflow.ResolverPrimarySelection) {
	if m == nil {
		return
	}
	if selection.UsedNeighborhood {
		m.Resolver.NeighborhoodUsed++
	}
	if selection.WinnerOverride {
		m.Resolver.NeighborhoodOverride++
	}
	if selection.NeighborhoodSplit {
		m.Resolver.NeighborhoodSplit++
	}
	if selection.NeighborhoodExcludedUnrelated > 0 {
		m.Resolver.NeighborhoodExcludedUnrelated += uint64(selection.NeighborhoodExcludedUnrelated)
	}
	if selection.NeighborhoodExcludedDistance > 0 {
		m.Resolver.NeighborhoodExcludedDistance += uint64(selection.NeighborhoodExcludedDistance)
	}
	if selection.NeighborhoodExcludedAnchorMissing > 0 {
		m.Resolver.NeighborhoodExcludedAnchorMissing += uint64(selection.NeighborhoodExcludedAnchorMissing)
	}
}

func (m *replayABMetrics) ObserveResolverRecentPlus1Gate(gate spot.ResolverPrimaryGateResult, evaluated bool) {
	if m == nil || !evaluated || !gate.RecentPlus1Considered {
		return
	}
	if gate.RecentPlus1Applied {
		m.Resolver.RecentPlus1Applied++
		return
	}
	m.Resolver.RecentPlus1Rejected++
	switch strings.ToLower(strings.TrimSpace(gate.RecentPlus1Reject)) {
	case "edit_neighbor_contested":
		m.Resolver.RecentPlus1RejectEdit++
	case "distance_or_family":
		m.Resolver.RecentPlus1RejectDistance++
	case "winner_recent_insufficient":
		m.Resolver.RecentPlus1RejectWinner++
	case "subject_not_weaker":
		m.Resolver.RecentPlus1RejectSubject++
	default:
		m.Resolver.RecentPlus1RejectOther++
	}
}

func (m *replayABMetrics) ObserveResolverBayesGate(gate spot.ResolverPrimaryGateResult, evaluated bool) {
	if m == nil || !evaluated {
		return
	}
	if gate.BayesReportBonusConsidered {
		m.Resolver.BayesReportConsidered++
		if gate.BayesReportBonusApplied {
			m.Resolver.BayesReportApplied++
		} else {
			m.Resolver.BayesReportRejected++
			switch strings.ToLower(strings.TrimSpace(gate.BayesReportBonusReject)) {
			case "edit_neighbor_contested":
				m.Resolver.BayesReportRejectEdit++
			case "winner_recent_insufficient":
				m.Resolver.BayesReportRejectWinner++
			case "subject_not_weaker":
				m.Resolver.BayesReportRejectSubject++
			case "candidate_unvalidated":
				m.Resolver.BayesReportRejectCandidate++
			case "score_below_threshold":
				m.Resolver.BayesReportRejectScore++
			default:
				m.Resolver.BayesReportRejectOther++
			}
		}
	}
	if gate.BayesAdvantageConsidered {
		m.Resolver.BayesAdvantageConsidered++
		if gate.BayesAdvantageApplied {
			m.Resolver.BayesAdvantageApplied++
		} else {
			m.Resolver.BayesAdvantageRejected++
			switch strings.ToLower(strings.TrimSpace(gate.BayesAdvantageReject)) {
			case "score_below_threshold":
				m.Resolver.BayesAdvantageRejectScore++
			case "weighted_delta_insufficient":
				m.Resolver.BayesAdvantageRejectDelta++
			case "confidence_insufficient":
				m.Resolver.BayesAdvantageRejectConfidence++
			case "candidate_unvalidated":
				m.Resolver.BayesAdvantageRejectCandidate++
			case "subject_validated":
				m.Resolver.BayesAdvantageRejectSubject++
			default:
				m.Resolver.BayesAdvantageRejectOther++
			}
		}
	}
}

type replayTemporalCommitLatencyMetrics struct {
	Samples uint64 `json:"samples"`
	LE500   uint64 `json:"le_500"`
	LE1000  uint64 `json:"le_1000"`
	LE2000  uint64 `json:"le_2000"`
	LE5000  uint64 `json:"le_5000"`
	GT5000  uint64 `json:"gt_5000"`
}

func (m *replayTemporalCommitLatencyMetrics) Observe(latency time.Duration) {
	if m == nil {
		return
	}
	if latency < 0 {
		latency = 0
	}
	ms := latency.Milliseconds()
	m.Samples++
	switch {
	case ms <= 500:
		m.LE500++
	case ms <= 1000:
		m.LE1000++
	case ms <= 2000:
		m.LE2000++
	case ms <= 5000:
		m.LE5000++
	default:
		m.GT5000++
	}
}

type replayTemporalABMetrics struct {
	Pending          uint64                             `json:"pending"`
	Committed        uint64                             `json:"committed"`
	FallbackResolver uint64                             `json:"fallback_resolver"`
	AbstainLowMargin uint64                             `json:"abstain_low_margin"`
	OverflowBypass   uint64                             `json:"overflow_bypass"`
	PathSwitches     uint64                             `json:"path_switches"`
	CommitLatencyMS  replayTemporalCommitLatencyMetrics `json:"commit_latency_ms"`
}

func (m *replayTemporalABMetrics) ObservePending() {
	if m == nil {
		return
	}
	m.Pending++
}

func (m *replayTemporalABMetrics) ObserveDecision(decision correctionflow.TemporalDecision) {
	if m == nil {
		return
	}
	switch decision.Action {
	case correctionflow.TemporalDecisionActionApply:
		m.Committed++
	case correctionflow.TemporalDecisionActionFallbackResolver:
		m.FallbackResolver++
	case correctionflow.TemporalDecisionActionAbstain:
		m.AbstainLowMargin++
	case correctionflow.TemporalDecisionActionBypass:
		m.OverflowBypass++
	}
	if decision.PathSwitched {
		m.PathSwitches++
	}
	if decision.Action != correctionflow.TemporalDecisionActionDefer {
		m.CommitLatencyMS.Observe(decision.CommitLatency)
	}
}

func projectedResolverConfidenceOutcome(snap spot.ResolverSnapshot) replayConfidenceOutcome {
	if snap.State != spot.ResolverStateConfident && snap.State != spot.ResolverStateProbable {
		return replayConfidenceOutcome{Final: "?"}
	}
	winner := spot.NormalizeCallsign(snap.Winner)
	if winner == "" || snap.TotalReporters <= 0 {
		return replayConfidenceOutcome{Final: "?"}
	}
	newGlyph := correctionflow.ResolverConfidenceGlyphForCall(snap, true, winner)
	return replayConfidenceOutcome{
		Final: normalizeConfidenceGlyph(newGlyph),
	}
}

func normalizeConfidenceGlyph(value string) string {
	trimmed := strutil.NormalizeUpper(strings.TrimSpace(value))
	switch trimmed {
	case "S", "P", "V", "C", "B":
		return trimmed
	case "?":
		return "?"
	case "":
		return ""
	default:
		return trimmed
	}
}

// stabilizerDelayProxyEligible reports whether a spot should be considered for
// replay-side stabilizer delay-proxy accounting.
func stabilizerDelayProxyEligible(s *spot.Spot, store spot.RecentSupportStore, cfg config.CallCorrectionConfig) bool {
	if s == nil || store == nil || !cfg.StabilizerEnabled || s.IsBeacon {
		return false
	}
	mode := s.ModeNorm
	if mode == "" {
		mode = s.Mode
	}
	if !spot.IsCallCorrectionCandidate(mode) {
		return false
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	if strings.TrimSpace(call) == "" {
		return false
	}
	band := s.BandNorm
	if strings.TrimSpace(band) == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	band = spot.NormalizeBand(band)
	return band != "" && band != "???"
}
