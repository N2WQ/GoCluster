package main

import (
	"strings"

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
	Final       string
	LegacyFinal string
}

type replayCurrentPathABMetrics struct {
	ConfidenceCounts  replayConfidenceCounts `json:"confidence_counts"`
	LegacyUnknownNowP uint64                 `json:"legacy_unknown_now_p"`
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
	LegacyUnknownNowP                 uint64                    `json:"legacy_unknown_now_p"`
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
	CurrentPath          replayCurrentPathABMetrics        `json:"current_path"`
	Resolver             replayResolverABMetrics           `json:"resolver"`
	StabilizerDelayProxy replayStabilizerDelayProxyMetrics `json:"stabilizer_delay_proxy"`
}

func (m *replayABMetrics) ObserveCurrentPath(outcome replayConfidenceOutcome) {
	if m == nil {
		return
	}
	newGlyph := normalizeConfidenceGlyph(outcome.Final)
	legacyGlyph := normalizeConfidenceGlyph(outcome.LegacyFinal)
	if legacyGlyph == "" {
		legacyGlyph = newGlyph
	}
	m.CurrentPath.ConfidenceCounts.Observe(newGlyph)
	if legacyGlyph == "?" && newGlyph == "P" {
		m.CurrentPath.LegacyUnknownNowP++
	}
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
	if projected.LegacyFinal == "?" && projected.Final == "P" {
		m.Resolver.LegacyUnknownNowP++
	}
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

func projectedResolverConfidenceOutcome(snap spot.ResolverSnapshot) replayConfidenceOutcome {
	if snap.State != spot.ResolverStateConfident && snap.State != spot.ResolverStateProbable {
		return replayConfidenceOutcome{Final: "?", LegacyFinal: "?"}
	}
	winner := spot.NormalizeCallsign(snap.Winner)
	if winner == "" || snap.TotalReporters <= 0 {
		return replayConfidenceOutcome{Final: "?", LegacyFinal: "?"}
	}
	newGlyph := correctionflow.ResolverConfidenceGlyphForCall(snap, true, winner)
	legacyGlyph := formatConfidenceLegacy(correctionflow.ResolverWinnerConfidence(snap), snap.TotalReporters)
	return replayConfidenceOutcome{
		Final:       normalizeConfidenceGlyph(newGlyph),
		LegacyFinal: normalizeConfidenceGlyph(legacyGlyph),
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

// formatConfidenceLegacy mirrors the pre-change confidence bucketing:
// multi-reporter confidence under 25% was '?' instead of 'P'.
func formatConfidenceLegacy(percent int, totalReporters int) string {
	if totalReporters <= 1 {
		return "?"
	}
	value := percent
	if value < 0 {
		value = 0
	}
	if value > 100 {
		value = 100
	}
	switch {
	case value >= 51:
		return "V"
	case value >= 25:
		return "P"
	default:
		return "?"
	}
}

func stabilizerDelayProxyEligible(s *spot.Spot, store *spot.RecentBandStore, cfg config.CallCorrectionConfig) bool {
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
