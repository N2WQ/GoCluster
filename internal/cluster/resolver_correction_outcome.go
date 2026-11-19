// File role: Exposes the ordinary correction's executed outcome to the live
// additional-rescue caller. Stabilizer and legacy callers retain suppression-only
// wrappers; no glyph inference or repeated gate/validation evaluation is needed.
package cluster

import (
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/ui"
)

type resolverCorrectionOutcome struct {
	selection                              correctionflow.ResolverPrimarySelection
	gate                                   spot.ResolverPrimaryGateResult
	gateEvaluated                          bool
	applied, suppress, replacementRejected bool
}

func (r resolverCorrectionOutcome) terminal() bool {
	return r.applied || r.suppress || r.replacementRejected
}

// Keep the ordinary linear guard/gate/validation order together: its rejection
// and mutation branches own existing confidence, telemetry and confirmation.
func applyResolverCorrection(spotEntry *spot.Spot, resolver *spot.SignalResolver,
	evidence spot.ResolverEvidence, hasEvidence bool, cfg config.CallCorrectionConfig,
	ctyDB *cty.CTYDatabase, metaCache *callMetaCache, tracker *stats.Tracker, dash ui.Surface,
	recentBandStore spot.RecentSupportStore, adaptive *spot.AdaptiveMinReports,
	selectionOverride *correctionflow.ResolverPrimarySelection, now time.Time,
	temporalResult *runtimeTemporalResult) (result resolverCorrectionOutcome) {
	if spotEntry == nil {
		return result
	}
	if spot.IsLocalSelfSpot(spotEntry) {
		spotEntry.Confidence = "V"
		return result
	}
	if !spot.IsCallCorrectionCandidate(spotEntry.Mode) {
		if strings.TrimSpace(spotEntry.Confidence) == "" {
			spotEntry.Confidence = "?"
		}
		observeResolverPrimaryDecision(tracker, "rejected", "resolver_non_candidate_mode", 0)
		return result
	}
	if !cfg.Enabled {
		if strings.TrimSpace(spotEntry.Confidence) == "" {
			spotEntry.Confidence = "?"
		}
		observeResolverPrimaryDecision(tracker, "rejected", "resolver_disabled", 0)
		return result
	}

	preCorrectionCall := normalizedDXCall(spotEntry)
	if preCorrectionCall == "" {
		spotEntry.Confidence = "?"
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionPrecallMissing, 0)
		return result
	}

	selection := correctionflow.ResolverPrimarySelection{}
	snapshot := spot.ResolverSnapshot{}
	snapshotOK := false
	if selectionOverride != nil {
		selection = *selectionOverride
		snapshot = selection.Snapshot
		snapshotOK = selection.SnapshotOK
	} else if hasEvidence && resolver != nil {
		selection = correctionflow.SelectResolverPrimarySnapshotForCall(resolver, evidence.Key, cfg, preCorrectionCall)
		snapshot = selection.Snapshot
		snapshotOK = selection.SnapshotOK
	}
	// Rejection retains this identity's percentage; only an accepted identity
	// change below replaces it with the winner's support share.
	result.selection = selection
	setResolverConfidence(spotEntry, snapshot, snapshotOK, preCorrectionCall)
	if !snapshotOK {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionNoSnapshot, 0)
		return result
	}
	// This event describes this evaluation, before later recognition/delivery.
	// Keep ordinary traffic quiet and format only a loss or alternate decision.
	decisionReason, proposedCall := "", ""
	decisionSuppressed := false
	defer func() {
		emitResolverDecision(dash, spotEntry, snapshot, preCorrectionCall, proposedCall, decisionReason, decisionSuppressed)
	}()
	if snapshot.EvidenceLoss {
		decisionReason = "resolver_evidence_loss"
		observeResolverPrimaryDecision(tracker, "rejected", "resolver_evidence_loss", 0)
		return result
	}
	if selection.NeighborhoodSplit {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionNeighborConflict, 0)
		return result
	}

	switch snapshot.State {
	case spot.ResolverStateConfident, spot.ResolverStateProbable:
	case spot.ResolverStateSplit:
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionStateSplit, 0)
		return result
	case spot.ResolverStateUncertain:
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionStateUncertain, 0)
		return result
	default:
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionStateUnknown, 0)
		return result
	}

	winnerCall := spot.NormalizeSpotDXCallsign(snapshot.Winner)
	if winnerCall == "" {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionWinnerMissing, 1)
		return result
	}
	if spot.CorrectionVoteKey(winnerCall) == spot.CorrectionVoteKey(preCorrectionCall) {
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionSameCall, 1)
		return result
	}
	proposedCall = winnerCall

	subjectSupport := correctionflow.ResolverSupportForCall(snapshot, preCorrectionCall)
	winnerSupport := correctionflow.ResolverSupportForCall(snapshot, winnerCall)
	subjectWeightedSupport := correctionflow.ResolverWeightedSupportForCall(snapshot, preCorrectionCall)
	winnerWeightedSupport := correctionflow.ResolverWeightedSupportForCall(snapshot, winnerCall)
	winnerConfidence := correctionflow.ResolverWinnerConfidence(snapshot)
	subjectMode := spotEntry.ModeNorm
	if subjectMode == "" {
		subjectMode = spotEntry.Mode
	}
	subjectBand := spotEntry.BandNorm
	if subjectBand == "" || subjectBand == "???" {
		subjectBand = spot.FreqToBand(spotEntry.Frequency)
	}

	if now.IsZero() {
		now = time.Now().UTC()
	}
	// Activity belongs to successful primary admission, never a gate read/retry.
	runtime := correctionflow.ResolveRuntimeSettings(cfg, spotEntry, adaptive, now, false)
	settings := correctionflow.BuildCorrectionSettings(correctionflow.BuildSettingsInput{
		Cfg:             cfg,
		MinReports:      runtime.MinReports,
		Window:          runtime.Window,
		FreqToleranceHz: runtime.FreqToleranceHz,
		RecentBandStore: recentBandStore,
	})
	gateOptions := spot.ResolverPrimaryGateOptions{}
	if cfg.ResolverRecentPlus1Enabled || config.BayesBonusModeEnabled(cfg.BayesBonus.Enabled, cfg.BayesBonus.EnabledDistancesByMode, subjectMode, cfg.MaxEditDistance) {
		if spot.ResolverSnapshotHasComparableEditNeighbor(snapshot, winnerCall, subjectMode, cfg.DistanceModelCW, cfg.DistanceModelRTTY) {
			gateOptions.RecentPlus1DisallowReason = resolverRecentPlus1DisallowEditNeighborGate
		}
	}
	gate := spot.EvaluateResolverPrimaryGates(
		preCorrectionCall,
		winnerCall,
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
	)
	result.gate, result.gateEvaluated = gate, true
	if !gate.Allow {
		reason := resolverGateDecisionReason(gate.Reason)
		if bayesReason, ok := resolverBayesDecisionReason(gate); ok {
			reason = bayesReason
		} else if plusReason, ok := resolverRecentPlus1DecisionReason(gate); ok {
			reason = plusReason
		}
		decisionReason = reason
		observeResolverPrimaryDecision(tracker, "rejected", reason, 1)
		return result
	}

	if shouldRejectCTYCall(winnerCall) {
		result.replacementRejected = true
		decisionReason = resolverDecisionInvalidBaseCall
		observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionInvalidBaseCall, 1)
		if strings.EqualFold(cfg.InvalidAction, "suppress") {
			decisionSuppressed = true
			result.suppress = true
			return result
		}
		spotEntry.Confidence = "B"
		return result
	}

	if ctyDB != nil {
		if info := effectivePrefixInfo(ctyDB, metaCache, winnerCall); info != nil {
			if replacementLicenseRejected(spotEntry, winnerCall, ctyDB, metaCache) {
				result.replacementRejected = true
				decisionReason = resolverDecisionLicenseMissing
				observeResolverPrimaryDecision(tracker, "rejected", decisionReason, 1)
				return result
			}
			spotEntry.DXCall = winnerCall
			spotEntry.DXCallNorm = winnerCall
			result.applied = true
			resetCorrectedDXClassification(spotEntry)
			temporalResult.confirm(preCorrectionCall, winnerCall)
			spotEntry.Confidence = "C"
			spotEntry.ConfidencePercent = winnerConfidence
			spotEntry.ConfidencePercentOK = true
			if tracker != nil {
				tracker.IncrementCallCorrections()
			}
			appliedReason := resolverAppliedDecisionReason(gate, selection)
			decisionReason = appliedReason
			observeResolverPrimaryDecision(tracker, "applied", appliedReason, 1)
		} else {
			result.replacementRejected = true
			decisionReason = resolverDecisionCTYMiss
			observeResolverPrimaryDecision(tracker, "rejected", resolverDecisionCTYMiss, 1)
			if strings.EqualFold(cfg.InvalidAction, "suppress") {
				decisionSuppressed = true
				result.suppress = true
				return result
			}
			spotEntry.Confidence = "B"
		}
		return result
	}

	spotEntry.DXCall = winnerCall
	spotEntry.DXCallNorm = winnerCall
	result.applied = true
	resetCorrectedDXClassification(spotEntry)
	temporalResult.confirm(preCorrectionCall, winnerCall)
	spotEntry.Confidence = "C"
	spotEntry.ConfidencePercent = winnerConfidence
	spotEntry.ConfidencePercentOK = true
	if tracker != nil {
		tracker.IncrementCallCorrections()
	}
	appliedReason := resolverAppliedDecisionReason(gate, selection)
	decisionReason = appliedReason
	observeResolverPrimaryDecision(tracker, "applied", appliedReason, 1)
	return result
}
