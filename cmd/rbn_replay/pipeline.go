// File role: Owns replay parity for the runtime correction pipeline.
// Crawler notes: Start here for resolver evidence snapshots, stabilizer delay
// decisions, confidence outcomes, and decision counters used to compare replay
// artifacts against live output behavior.
// Related docs: docs/rbn_replay.md, spot/README.md, docs/decision-log.md.
// Related tests: cmd/rbn_replay/pipeline_test.go.
package main

import (
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/uls"
)

const (
	resolverDecisionPathPrimary                   = "resolver_primary"
	resolverDecisionApplied                       = "resolver_applied"
	resolverDecisionAppliedNeighbor               = "resolver_applied_neighbor_override"
	resolverDecisionAppliedRecentPlus1            = "resolver_applied_recent_plus1"
	resolverDecisionAppliedNeighborRecentPlus1    = "resolver_applied_neighbor_recent_plus1"
	resolverDecisionAppliedBayesReport            = "resolver_applied_bayes_report"
	resolverDecisionAppliedNeighborBayesReport    = "resolver_applied_neighbor_bayes_report"
	resolverDecisionAppliedBayesAdvantage         = "resolver_applied_bayes_advantage"
	resolverDecisionAppliedNeighborBayesAdvantage = "resolver_applied_neighbor_bayes_advantage"
	resolverDecisionNoSnapshot                    = "resolver_no_snapshot"
	resolverDecisionNeighborConflict              = "resolver_neighbor_conflict"
	resolverDecisionStateSplit                    = "resolver_state_split"
	resolverDecisionStateUncertain                = "resolver_state_uncertain"
	resolverDecisionStateUnknown                  = "resolver_state_unknown"
	resolverDecisionPrecallMissing                = "resolver_precall_missing"
	resolverDecisionWinnerMissing                 = "resolver_winner_missing"
	resolverDecisionSameCall                      = "resolver_same_call"
	resolverDecisionInvalidBaseCall               = "resolver_invalid_base"
	resolverDecisionCTYMiss                       = "resolver_cty_miss"
	resolverDecisionGatePrefix                    = "resolver_gate_"
	resolverDecisionRecentPlus1RejectPrefix       = "resolver_recent_plus1_reject_"
	resolverDecisionBayesReportRejectPrefix       = "resolver_bayes_report_reject_"
	resolverDecisionBayesAdvantageRejectPrefix    = "resolver_bayes_advantage_reject_"
	resolverRecentPlus1DisallowEditNeighborGate   = "edit_neighbor_contested"
)

type replayResolverApplyOutcome struct {
	Suppress      bool
	Applied       bool
	Winner        string
	Confidence    replayConfidenceOutcome
	Selection     correctionflow.ResolverPrimarySelection
	Gate          spot.ResolverPrimaryGateResult
	GateEvaluated bool
	// Temporal output ownership lasts through this synchronous outcome only.
	// No request-ID table or retained handoff marker is needed by replay.
	temporal    bool
	temporalKey spot.ResolverSignalKey
}

// normalizedDXCall exists to keep replay code visually aligned with the runtime
// pipeline while sharing correctionflow's canonical normalization.
func normalizedDXCall(s *spot.Spot) string {
	return correctionflow.NormalizedDXCall(s)
}

func buildResolverEvidenceSnapshot(spotEntry *spot.Spot, cfg config.CallCorrectionConfig, adaptive *spot.AdaptiveMinReports, now time.Time) (spot.ResolverEvidence, bool) {
	return correctionflow.BuildResolverEvidenceSnapshot(spotEntry, cfg, adaptive, now)
}

func evaluateStabilizerDelay(
	s *spot.Spot,
	store spot.RecentSupportStore,
	cfg config.CallCorrectionConfig,
	now time.Time,
	snapshot spot.ResolverSnapshot,
	snapshotOK bool,
) correctionflow.StabilizerDelayDecision {
	return correctionflow.EvaluateStabilizerDelay(s, store, cfg, now, snapshot, snapshotOK)
}

// observeResolverPrimaryDecision writes replay decisions into the same tracker
// dimensions used by runtime summaries, making replay output usable in runbooks.
func observeResolverPrimaryDecision(tracker *stats.Tracker, decision, reason string, candidateRank int) {
	if tracker == nil {
		return
	}
	decision = strings.ToLower(strings.TrimSpace(decision))
	if decision == "" {
		decision = "rejected"
	}
	reason = strings.ToLower(strings.TrimSpace(reason))
	if reason == "" {
		reason = "unknown"
	}
	if candidateRank < 0 {
		candidateRank = 0
	}
	tracker.ObserveCallCorrectionDecision(resolverDecisionPathPrimary, decision, reason, candidateRank)
}

func resolverGateDecisionReason(reason string) string {
	reason = strings.ToLower(strings.TrimSpace(reason))
	if reason == "" {
		reason = "unknown"
	}
	return resolverDecisionGatePrefix + reason
}

func resolverRecentPlus1DecisionReason(gate spot.ResolverPrimaryGateResult) (string, bool) {
	if !gate.RecentPlus1Considered || gate.RecentPlus1Applied {
		return "", false
	}
	reject := strings.ToLower(strings.TrimSpace(gate.RecentPlus1Reject))
	if reject == "" {
		return "", false
	}
	return resolverDecisionRecentPlus1RejectPrefix + reject, true
}

func resolverBayesDecisionReason(gate spot.ResolverPrimaryGateResult) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(gate.Reason)) {
	case "min_reports":
		if !gate.BayesReportBonusConsidered || gate.BayesReportBonusApplied {
			return "", false
		}
		reject := strings.ToLower(strings.TrimSpace(gate.BayesReportBonusReject))
		if reject == "" {
			return "", false
		}
		return resolverDecisionBayesReportRejectPrefix + reject, true
	case "advantage":
		if !gate.BayesAdvantageConsidered || gate.BayesAdvantageApplied {
			return "", false
		}
		reject := strings.ToLower(strings.TrimSpace(gate.BayesAdvantageReject))
		if reject == "" {
			return "", false
		}
		return resolverDecisionBayesAdvantageRejectPrefix + reject, true
	default:
		return "", false
	}
}

func resolverAppliedDecisionReason(gate spot.ResolverPrimaryGateResult, selection correctionflow.ResolverPrimarySelection) string {
	switch {
	case gate.BayesAdvantageApplied && selection.WinnerOverride:
		return resolverDecisionAppliedNeighborBayesAdvantage
	case gate.BayesAdvantageApplied:
		return resolverDecisionAppliedBayesAdvantage
	case gate.BayesReportBonusApplied && selection.WinnerOverride:
		return resolverDecisionAppliedNeighborBayesReport
	case gate.BayesReportBonusApplied:
		return resolverDecisionAppliedBayesReport
	case gate.RecentPlus1Applied && selection.WinnerOverride:
		return resolverDecisionAppliedNeighborRecentPlus1
	case gate.RecentPlus1Applied:
		return resolverDecisionAppliedRecentPlus1
	case selection.WinnerOverride:
		return resolverDecisionAppliedNeighbor
	default:
		return resolverDecisionApplied
	}
}

func maybeApplyResolverCorrectionReplay(
	spotEntry *spot.Spot,
	resolver *spot.SignalResolver,
	evidence spot.ResolverEvidence,
	hasEvidence bool,
	cfg config.CallCorrectionConfig,
	ctyDB *cty.CTYDatabase,
	tracker *stats.Tracker,
	adaptive *spot.AdaptiveMinReports,
	recentBandStore spot.RecentSupportStore,
	now time.Time,
) replayResolverApplyOutcome {
	return maybeApplyResolverCorrectionReplayWithSelectionOverride(
		spotEntry,
		resolver,
		evidence,
		hasEvidence,
		cfg,
		ctyDB,
		tracker,
		adaptive,
		recentBandStore,
		now,
		nil,
	)
}

// maybeApplyResolverCorrectionReplayWithSelectionOverride is the replay mirror
// of runtime resolver application. The explicit outcome object lets A/B reports
// explain not only whether a correction applied, but why gates rejected it.
func maybeApplyResolverCorrectionReplayWithSelectionOverride(
	spotEntry *spot.Spot,
	resolver *spot.SignalResolver,
	evidence spot.ResolverEvidence,
	hasEvidence bool,
	cfg config.CallCorrectionConfig,
	ctyDB *cty.CTYDatabase,
	tracker *stats.Tracker,
	adaptive *spot.AdaptiveMinReports,
	recentBandStore spot.RecentSupportStore,
	now time.Time,
	selectionOverride *correctionflow.ResolverPrimarySelection,
) replayResolverApplyOutcome {
	outcome := replayResolverApplyOutcome{}
	if spotEntry == nil {
		return outcome
	}
	outcome.Confidence.Final = normalizeConfidenceGlyph(spotEntry.Confidence)

	if !spot.IsCallCorrectionCandidate(spotEntry.Mode) {
		if strings.TrimSpace(spotEntry.Confidence) == "" {
			spotEntry.Confidence = "?"
		}
		outcome.Confidence.Final = normalizeConfidenceGlyph(spotEntry.Confidence)
		observeResolverPrimaryDecision(tracker, "rejected", "resolver_non_candidate_mode", 0)
		return outcome
	}
	if !cfg.Enabled {
		if strings.TrimSpace(spotEntry.Confidence) == "" {
			spotEntry.Confidence = "?"
		}
		outcome.Confidence.Final = normalizeConfidenceGlyph(spotEntry.Confidence)
		observeResolverPrimaryDecision(tracker, "rejected", "resolver_disabled", 0)
		return outcome
	}

	preCorrectionCall := normalizedDXCall(spotEntry)
	if preCorrectionCall == "" {
		spotEntry.Confidence = "?"
		outcome.Confidence.Final = "?"
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionPrecallMissing, 0)
		return outcome
	}

	if selectionOverride != nil {
		outcome.Selection = *selectionOverride
	} else if hasEvidence && resolver != nil {
		outcome.Selection = correctionflow.SelectResolverPrimarySnapshotForCall(resolver, evidence.Key, cfg, preCorrectionCall)
	}
	snapshot := outcome.Selection.Snapshot
	spotEntry.Confidence = correctionflow.ResolverConfidenceGlyphForCall(snapshot, outcome.Selection.SnapshotOK, preCorrectionCall)
	outcome.Confidence.Final = normalizeConfidenceGlyph(spotEntry.Confidence)

	if !outcome.Selection.SnapshotOK {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionNoSnapshot, 0)
		return outcome
	}
	if outcome.Selection.NeighborhoodSplit {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionNeighborConflict, 0)
		return outcome
	}

	switch snapshot.State {
	case spot.ResolverStateConfident, spot.ResolverStateProbable:
	case spot.ResolverStateSplit:
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionStateSplit, 0)
		return outcome
	case spot.ResolverStateUncertain:
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionStateUncertain, 0)
		return outcome
	default:
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionStateUnknown, 0)
		return outcome
	}

	winnerCall := snapshot.Winner
	if winnerCall == "" {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionWinnerMissing, 1)
		return outcome
	}
	if strings.EqualFold(winnerCall, preCorrectionCall) {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionSameCall, 1)
		return outcome
	}

	gate, gateEvaluated := evaluateResolverPrimaryGateReplay(
		spotEntry,
		preCorrectionCall,
		outcome.Selection,
		cfg,
		recentBandStore,
		adaptive,
		now,
	)
	outcome.Gate = gate
	outcome.GateEvaluated = gateEvaluated
	if !gate.Allow {
		reason := resolverGateDecisionReason(gate.Reason)
		if bayesReason, ok := resolverBayesDecisionReason(gate); ok {
			reason = bayesReason
		} else if plusReason, ok := resolverRecentPlus1DecisionReason(gate); ok {
			reason = plusReason
		}
		observeResolverPrimaryDecision(tracker, "rejected", reason, 1)
		return outcome
	}

	if shouldRejectCTYCall(winnerCall) {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionInvalidBaseCall, 1)
		if strings.EqualFold(cfg.InvalidAction, "suppress") {
			outcome.Suppress = true
			return outcome
		}
		spotEntry.Confidence = "B"
		outcome.Confidence.Final = "B"
		return outcome
	}

	if ctyDB != nil {
		if _, ok := ctyDB.LookupCallsignPortable(winnerCall); !ok {
			observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionCTYMiss, 1)
			if strings.EqualFold(cfg.InvalidAction, "suppress") {
				outcome.Suppress = true
				return outcome
			}
			spotEntry.Confidence = "B"
			outcome.Confidence.Final = "B"
			return outcome
		}
	}

	spotEntry.DXCall = winnerCall
	spotEntry.DXCallNorm = winnerCall
	spotEntry.Confidence = "C"
	outcome.Confidence.Final = "C"
	outcome.Applied = true
	outcome.Winner = winnerCall
	if tracker != nil {
		tracker.IncrementCallCorrections()
	}

	appliedReason := resolverAppliedDecisionReason(gate, outcome.Selection)
	observeResolverPrimaryDecision(tracker, "applied", appliedReason, 1)
	return outcome
}

// evaluateResolverPrimaryGateReplay keeps replay gate decisions on the same
// inputs as runtime while avoiding telnet/archive side effects.
func evaluateResolverPrimaryGateReplay(
	spotEntry *spot.Spot,
	preCorrectionCall string,
	selection correctionflow.ResolverPrimarySelection,
	cfg config.CallCorrectionConfig,
	recentBandStore spot.RecentSupportStore,
	adaptive *spot.AdaptiveMinReports,
	now time.Time,
) (spot.ResolverPrimaryGateResult, bool) {
	if spotEntry == nil {
		return spot.ResolverPrimaryGateResult{}, false
	}
	preCall := spot.NormalizeSpotDXCallsign(preCorrectionCall)
	if preCall == "" {
		return spot.ResolverPrimaryGateResult{}, false
	}
	if !selection.SnapshotOK || selection.NeighborhoodSplit {
		return spot.ResolverPrimaryGateResult{}, false
	}
	snap := selection.Snapshot
	if snap.State != spot.ResolverStateConfident && snap.State != spot.ResolverStateProbable {
		return spot.ResolverPrimaryGateResult{}, false
	}
	winner := snap.Winner
	if winner == "" || winner == preCall {
		return spot.ResolverPrimaryGateResult{}, false
	}

	subjectSupport := correctionflow.ResolverSupportForCall(snap, preCall)
	winnerSupport := correctionflow.ResolverSupportForCall(snap, winner)
	subjectWeightedSupport := correctionflow.ResolverWeightedSupportForCall(snap, preCall)
	winnerWeightedSupport := correctionflow.ResolverWeightedSupportForCall(snap, winner)
	winnerConfidence := correctionflow.ResolverWinnerConfidence(snap)

	subjectMode := spotEntry.ModeNorm
	if subjectMode == "" {
		subjectMode = spotEntry.Mode
	}
	subjectBand := spotEntry.BandNorm
	if subjectBand == "" || subjectBand == "???" {
		subjectBand = spot.FreqToBand(spotEntry.Frequency)
	}

	runtime := correctionflow.ResolveRuntimeSettings(cfg, spotEntry, adaptive, now, false)
	settings := correctionflow.BuildCorrectionSettings(correctionflow.BuildSettingsInput{
		Cfg:             cfg,
		MinReports:      runtime.MinReports,
		Window:          runtime.Window,
		FreqToleranceHz: runtime.FreqToleranceHz,
		RecentBandStore: recentBandStore,
	})

	gateOptions := spot.ResolverPrimaryGateOptions{}
	if cfg.ResolverRecentPlus1Enabled || cfg.BayesBonus.Enabled {
		if spot.ResolverSnapshotHasComparableEditNeighbor(snap, winner, subjectMode, cfg.DistanceModelCW, cfg.DistanceModelRTTY) {
			gateOptions.RecentPlus1DisallowReason = resolverRecentPlus1DisallowEditNeighborGate
		}
	}
	return spot.EvaluateResolverPrimaryGates(
		preCall,
		winner,
		subjectBand,
		subjectMode,
		subjectSupport,
		winnerSupport,
		winnerConfidence,
		subjectWeightedSupport,
		winnerWeightedSupport,
		settings,
		now,
		gateOptions,
	), true
}

// shouldRejectCTYCall applies the same prefilter used by ingest validation so
// replayed corrections fail for the same malformed-base reasons as production.
func shouldRejectCTYCall(call string) bool {
	normalized := spot.NormalizeCallsign(call)
	base := strings.TrimSpace(uls.NormalizeForLicense(normalized))
	if uls.AllowlistMatchAny(base) {
		return false
	}
	if base != "" {
		return !spot.IsValidNormalizedCallsign(base)
	}
	return normalized != "" && !spot.IsValidNormalizedCallsign(normalized)
}

// recordRecentBandObservation updates replay support memory only after a spot
// would have survived output. That keeps stabilizer and recent-band metrics from
// learning from suppressed corrections.
func recordRecentBandObservation(s *spot.Spot, store spot.RecentSupportStore, cfg config.CallCorrectionConfig) {
	if s == nil || store == nil || s.IsBeacon {
		return
	}
	if customStore, ok := store.(*spot.CustomSCPStore); ok && cfg.CustomSCP.Enabled {
		customStore.RecordSpot(s)
		return
	}
	legacyStore, ok := store.(*spot.RecentBandStore)
	if !ok || legacyStore == nil {
		return
	}
	if !cfg.RecentBandBonusEnabled && !cfg.StabilizerEnabled {
		return
	}
	mode := s.ModeNorm
	if strings.TrimSpace(mode) == "" {
		mode = s.Mode
	}
	if !spot.IsCallCorrectionCandidate(mode) {
		return
	}
	call := normalizedDXCall(s)
	if call == "" {
		return
	}
	band := s.BandNorm
	if band == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	band = spot.NormalizeBand(band)
	if band == "" || band == "???" {
		return
	}
	reporter := s.DECallNorm
	if reporter == "" {
		reporter = s.DECall
	}
	legacyStore.Record(call, band, mode, reporter, s.Time)
}
