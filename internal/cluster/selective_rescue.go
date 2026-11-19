// File role: Applies the shared C assessment only after an ordinary nonterminal
// outcome. Authority is request-local: an Observe overflow or stabilizer retry
// cannot acquire it from an action name, confidence glyph or saved envelope.
package cluster

import (
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

type selectiveRescueAuthority uint8

const (
	selectiveRescueNone selectiveRescueAuthority = iota
	selectiveRescueImmediate
	selectiveRescueHeldApply
	selectiveRescueHeldFallback
)

type selectiveRescueRequest struct {
	authority selectiveRescueAuthority
	permitted string
	saved     *correctionflow.FrequencyLifecycleVector
}

// applySelectiveRescue borrows current publications only during this call. No
// receiver view, proposal, score or authority marker enters a retained owner.
func (p *outputPipeline) applySelectiveRescue(ctx *outputSpotContext, evidence spot.ResolverEvidence, hasEvidence bool,
	ordinary resolverCorrectionOutcome, request selectiveRescueRequest, now time.Time) resolverCorrectionOutcome {
	s := ctx.spot
	if !p.correctionCfg.SelectiveRescueEnabled || !p.correctionCfg.Enabled || ordinary.terminal() ||
		request.authority < selectiveRescueImmediate || request.authority > selectiveRescueHeldFallback ||
		s == nil || s.IsBeacon || spot.IsLocalSelfSpot(s) ||
		!hasEvidence || evidence.Key.Mode != "CW" || p.signalResolver == nil {
		return ordinary
	}
	sources := correctionflow.ReadFrequencySources(p.signalResolver, evidence.Key, p.correctionCfg, now)
	if request.authority == selectiveRescueHeldApply && (request.saved == nil ||
		*request.saved != correctionflow.FrequencySourceLifecycles(sources)) {
		return ordinary
	}
	subject := normalizedDXCall(s)
	assessment := correctionflow.AssessSelectiveFrequencyProposals(evidence.Key, subject, s.Frequency,
		ordinary.selection, sources, p.correctionCfg,
		correctionflow.FrequencyProposalOptions{GapHz: 150, MaxWitnessSeparation: time.Minute, MaxDistance: 3})
	trials := 0
	for _, proposal := range assessment.Proposals {
		if request.authority == selectiveRescueHeldApply && proposal.Identity != spot.CorrectionVoteKey(request.permitted) {
			continue
		}
		if trials == 8 {
			break
		}
		trials++
		gate := p.evaluateSelectiveRescueGate(s, subject, proposal, now)
		ordinary.gate, ordinary.gateEvaluated = gate, true
		if !gate.Allow {
			continue
		}
		// The first allowance owns final validation, including failure. A rejected
		// target never authorizes another candidate or another C assessment.
		return p.applySelectiveRescueReplacement(ctx, subject, proposal, ordinary)
	}
	return ordinary
}

func (p *outputPipeline) evaluateSelectiveRescueGate(s *spot.Spot, subject string, proposal correctionflow.FrequencyProposal, now time.Time) spot.ResolverPrimaryGateResult {
	cfg := p.correctionCfg
	runtime := correctionflow.ResolveRuntimeSettings(cfg, s, p.adaptiveMinReports, now, false)
	settings := correctionflow.BuildCorrectionSettings(correctionflow.BuildSettingsInput{Cfg: cfg,
		MinReports: runtime.MinReports, Window: runtime.Window, FreqToleranceHz: runtime.FreqToleranceHz,
		RecentBandStore: p.recentBandStore})
	mode, band := s.ModeNorm, s.BandNorm
	if mode == "" {
		mode = s.Mode
	}
	if band == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	options := spot.ResolverPrimaryGateOptions{}
	if proposal.ComparableEditNeighbor && (cfg.ResolverRecentPlus1Enabled ||
		config.BayesBonusModeEnabled(cfg.BayesBonus.Enabled, cfg.BayesBonus.EnabledDistancesByMode, mode, cfg.MaxEditDistance)) {
		options.RecentPlus1DisallowReason = resolverRecentPlus1DisallowEditNeighborGate
	}
	return spot.EvaluateResolverPrimaryGates(subject, proposal.Call, band, mode, proposal.SubjectSupport, proposal.Support,
		proposal.ConfidencePercent, proposal.SubjectWeightedSupportMilli, proposal.WeightedSupportMilli, settings, now, options)
}

func (p *outputPipeline) applySelectiveRescueReplacement(ctx *outputSpotContext, subject string,
	proposal correctionflow.FrequencyProposal, result resolverCorrectionOutcome) resolverCorrectionOutcome {
	s, cfg := ctx.spot, p.correctionCfg
	reason := "resolver_selective_applied"
	defer func() {
		emitResolverDecision(p.dash, s, result.selection.Snapshot, subject, proposal.Call, reason, result.suppress)
	}()
	if shouldRejectCTYCall(proposal.Call) {
		reason = resolverDecisionInvalidBaseCall
		result.replacementRejected = true
		if strings.EqualFold(cfg.InvalidAction, "suppress") {
			result.suppress = true
		} else {
			s.Confidence = "B"
		}
	} else if ctx.ctyDB != nil && effectivePrefixInfo(ctx.ctyDB, p.metaCache, proposal.Call) == nil {
		reason = resolverDecisionCTYMiss
		result.replacementRejected = true
		if strings.EqualFold(cfg.InvalidAction, "suppress") {
			result.suppress = true
		} else {
			s.Confidence = "B"
		}
	} else if replacementLicenseRejected(s, proposal.Call, ctx.ctyDB, p.metaCache) {
		// License rejection intentionally keeps the ordinary retained-call glyph.
		// It is still terminal even when no B marker is stamped.
		reason = resolverDecisionLicenseMissing
		result.replacementRejected = true
	}
	if result.replacementRejected {
		observeResolverPrimaryDecision(p.tracker, "rejected", reason, 1)
		return result
	}
	s.DXCall, s.DXCallNorm = proposal.Call, proposal.Call
	resetCorrectedDXClassification(s)
	ctx.temporalResult.confirm(subject, proposal.Call)
	setResolverConfidence(s, result.selection.Snapshot, result.selection.SnapshotOK, proposal.Call)
	s.Confidence = "C"
	result.applied = true
	if p.tracker != nil {
		p.tracker.IncrementCallCorrections()
	}
	observeResolverPrimaryDecision(p.tracker, "applied", reason, 1)
	return result
}
