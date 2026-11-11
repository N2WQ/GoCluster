// File role: Output-pipeline delivery decisions, delayed release handling, and
// caller-owned archive snapshots for telnet/peer/archive fanout.
package cluster

import (
	"strings"
	"time"

	"dxcluster/internal/correctionflow"
	"dxcluster/internal/qualificationstage"
	"dxcluster/spot"
)

type outputDeliveryPlan struct {
	allowFast         bool
	allowMed          bool
	allowSlow         bool
	telnetDeliverNow  bool
	telnetDeliverSelf bool
}

func (p outputDeliveryPlan) normalFanoutAllowed() bool {
	return p.allowFast || p.allowMed || p.allowSlow
}

// startStabilizerReleaseLoop routes delayed spots back through the output
// pipeline instead of broadcasting from the stabilizer. That preserves one
// fanout owner and keeps delayed-output support evidence in the same path as
// immediate output.
func (p *outputPipeline) startStabilizerReleaseLoop() {
	p.stabilizerReleaseDone = make(chan struct{})
	go func() {
		defer close(p.stabilizerReleaseDone)
		releaseCh := p.telnetStabilizer.ReleaseChan()
		for envelope := range releaseCh {
			p.handleStabilizerRelease(envelope)
		}
	}()
}

// handleStabilizerRelease rechecks resolver, support-floor, license, and the
// final shared secondary gates after a delay because the evidence that justified
// holding the spot may have changed while it was queued.
func (p *outputPipeline) handleStabilizerRelease(envelope *telnetStabilizerEnvelope) {
	if envelope == nil || envelope.spot == nil {
		return
	}
	delayed := envelope.spot
	releaseCtx := outputSpotContext{spot: delayed, peerOriginal: envelope.peerOriginal,
		temporalResult: envelope.temporalResult, stabilizerHeld: true}
	if p.rejectExpiredOutput(&releaseCtx) {
		return
	}
	heldCall := normalizedDXCall(delayed)
	priorConfidence, _ := ordinaryStabilizerConfidence(delayed.Confidence)
	bestConfidence := max(envelope.bestConfidence, priorConfidence)
	checksCompleted := envelope.checksCompleted + 1
	ctyDB := p.ctyLookup()
	now := time.Now().UTC()
	resolverEvidence := spot.ResolverEvidence{Key: envelope.resolverKey}
	hasResolverEvidence := envelope.hasResolverKey
	if !hasResolverEvidence {
		resolverEvidence, hasResolverEvidence = buildResolverEvidenceSnapshot(delayed, p.correctionCfg, p.adaptiveMinReports, now)
	}
	resolverEvidenceEnqueued := envelope.evidenceEnqueued
	selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(p.signalResolver, resolverEvidence.Key, p.correctionCfg, normalizedDXCall(delayed), now)
	if maybeApplyResolverCorrectionWithSelectionOverride(
		delayed,
		p.signalResolver,
		resolverEvidence,
		hasResolverEvidence,
		p.correctionCfg,
		ctyDB,
		p.metaCache,
		p.tracker,
		p.dash,
		p.recentBandStore,
		p.adaptiveMinReports,
		p.spotterReliability,
		p.spotterReliabilityCW,
		p.spotterReliabilityRTTY,
		p.confusionModel,
		&selection, now, envelope.temporalResult,
	) {
		return
	}
	applySupportFloor(delayed, p.recentBandStore, p.customSCPStore, nil, p.correctionCfg)
	delayed.RefreshBeaconFlag()
	delayed.EnsureNormalized()
	if applyLicenseGate(delayed, ctyDB, p.metaCache, p.unlicensedReporter) {
		return
	}
	if normalizedDXCall(delayed) != heldCall {
		// Accepted replacement owns its confidence; the original's maximum
		// cannot cross identity. C/B remain outside the ordinary ordering.
		bestConfidence = stabilizerConfidenceUnknown
	} else if fresh, ordinary := ordinaryStabilizerConfidence(delayed.Confidence); ordinary {
		bestConfidence = max(bestConfidence, fresh)
	}
	delaySnapshot, delaySnapshotOK := spot.ResolverSnapshot{}, false
	if hasResolverEvidence && p.signalResolver != nil {
		delaySnapshot, delaySnapshotOK = p.signalResolver.LookupEligible(resolverEvidence.Key, now)
	}
	if selection.SnapshotOK && selection.Snapshot.EvidenceLoss {
		// Only loss crosses the selected-neighborhood boundary. Other delay
		// inputs retain exact-bucket meaning; no saved envelope flag can outlive
		// the fresh eligibility decision above.
		delaySnapshot.EvidenceLoss, delaySnapshotOK = true, true
	}
	delayDecision := evaluateTelnetStabilizerDelay(delayed, p.recentBandStore, p.correctionCfg, time.Now().UTC(), delaySnapshot, delaySnapshotOK)
	// Fresh support evaluation can contend on store locks. Sample again before
	// either a retry reservation or a timeout disposition can consume the spot.
	if p.rejectExpiredOutput(&releaseCtx) {
		return
	}
	if shouldRetryTelnetByStabilizer(delayDecision, checksCompleted) {
		if p.telnetStabilizer.enqueueWithTemporalResult(
			delayed,
			checksCompleted,
			delayDecision.Reason.String(),
			resolverEvidence.Key,
			hasResolverEvidence,
			resolverEvidenceEnqueued,
			envelope.temporalResult,
			envelope.peerOriginal,
			envelope.entryGlyph,
			bestConfidence,
		) {
			if p.tracker != nil {
				p.tracker.IncrementStabilizerHeld()
				p.tracker.IncrementStabilizerHeldReason(delayDecision.Reason.String())
			}
			return
		}
		if p.tracker != nil {
			p.tracker.IncrementStabilizerOverflowRelease()
		}
		delayDecision.ShouldDelay = false
	}
	outputConfidence, retained := terminalStabilizerConfidence(delayed.Confidence, bestConfidence)
	if !shouldRecordRecentBandAfterStabilizerDelay(p.correctionCfg.StabilizerTimeoutAction, delayDecision.ShouldDelay) {
		delayed.Confidence = outputConfidence
		if p.tracker != nil {
			if retained {
				p.tracker.IncrementStabilizerRetainedAboveFresh()
			}
			p.tracker.IncrementStabilizerSuppressedTimeout()
			p.tracker.IncrementStabilizerSuppressedTimeoutReason(delayDecision.Reason.String())
			p.tracker.ObserveStabilizerGlyphTurns(delayed.Confidence, checksCompleted)
			p.tracker.ObserveStabilizerTransition(envelope.entryGlyph, delayed.Confidence, checksCompleted)
		}
		return
	}
	releaseCtx.stabilizerResolverKey, releaseCtx.hasStabilizerResolverKey = resolverEvidence.Key, hasResolverEvidence
	releaseCtx.dxIdentityChanged = normalizedDXCall(delayed) != heldCall
	p.prepareOutputLocation(&releaseCtx, true)
	if p.rejectExpiredOutput(&releaseCtx) {
		return
	}
	// A terminal path counts before secondary thinning, even without fanout.
	// Earlier correction/license aborts and shutdown drops never reach this point;
	// re-holds return above. Initial admission overflow never enters this handler.
	if p.tracker != nil {
		if retained {
			p.tracker.IncrementStabilizerRetainedAboveFresh()
		}
		p.tracker.ObserveStabilizerTransition(envelope.entryGlyph, outputConfidence, checksCompleted)
	}
	p.recordAcceptedObservation(delayed)
	delayed.Confidence = outputConfidence
	plan := p.buildFinalDeliveryPlan(delayed)
	if !plan.normalFanoutAllowed() {
		p.emitSpot(&releaseCtx, plan)
		return
	}
	p.updateGridCache(&releaseCtx)
	if p.emitSpot(&releaseCtx, plan) && p.tracker != nil {
		p.tracker.IncrementStabilizerReleasedDelayed()
		p.tracker.IncrementStabilizerReleasedDelayedReason(stabilizerReleaseReason(delayDecision, envelope.delayReason))
		p.tracker.ObserveStabilizerGlyphTurns(delayed.Confidence, checksCompleted)
	}
}

// computeSecondaryAllows translates optional secondary dedup rails into
// fast/med/slow output-policy decisions. Telnet clients consume all three
// policy lanes; archive and peer consume the final shared MED lane.
func (p *outputPipeline) computeSecondaryAllows(s *spot.Spot) (bool, bool, bool) {
	allowFast := true
	var evidence [3]spot.SecondaryDedupeEvidence
	if p.secondaryFast != nil {
		allowFast, evidence[0] = p.secondaryFast.Evaluate(s)
	}
	allowMed := true
	if p.secondaryMed != nil {
		allowMed, evidence[1] = p.secondaryMed.Evaluate(s)
	}
	allowSlow := true
	if p.secondarySlow != nil {
		allowSlow, evidence[2] = p.secondarySlow.Evaluate(s)
	}
	fallbackAllowed := allowFast
	fallbackEvidence := evidence[0]
	if p.secondaryFast == nil {
		if p.secondaryMed != nil {
			fallbackAllowed = allowMed
			fallbackEvidence = evidence[1]
		} else if p.secondarySlow != nil {
			fallbackAllowed = allowSlow
			fallbackEvidence = evidence[2]
		}
	}
	if p.secondaryFast == nil {
		allowFast = fallbackAllowed
		evidence[0] = fallbackEvidence
	}
	if p.secondaryMed == nil {
		allowMed = fallbackAllowed
		evidence[1] = fallbackEvidence
	}
	if p.secondarySlow == nil {
		allowSlow = fallbackAllowed
		evidence[2] = fallbackEvidence
	}
	if s != nil {
		s.SecondaryDedupe = evidence
	}
	return allowFast, allowMed, allowSlow
}

func (p *outputPipeline) buildFinalDeliveryPlan(s *spot.Spot) outputDeliveryPlan {
	allowFast, allowMed, allowSlow := p.computeSecondaryAllows(s)
	plan := outputDeliveryPlan{
		allowFast:        allowFast,
		allowMed:         allowMed,
		allowSlow:        allowSlow,
		telnetDeliverNow: p.telnet != nil,
	}
	if !plan.normalFanoutAllowed() {
		plan.telnetDeliverNow = false
		plan.telnetDeliverSelf = p.telnet != nil
		return plan
	}
	return plan
}

// deliverSpot is the final synchronous fanout stage after all mutation and
// suppression decisions are complete.
func (p *outputPipeline) deliverSpot(ctx *outputSpotContext) {
	if ctx != nil && ctx.spot != nil {
		qualificationstage.Observe(qualificationstage.DeliveryStart, ctx.spot.Comment, -1)
	}
	if p.secondaryStage != nil {
		p.secondaryStage.Add(1)
	}
	plan, ok := p.resolveDeliveryPlan(ctx)
	if !ok {
		return
	}
	if plan.normalFanoutAllowed() {
		p.updateGridCache(ctx)
	}
	p.emitSpot(ctx, plan)
}

// recordAcceptedObservation is called once by the path that resolves core
// acceptance, before secondary/output gates. Pending, reheld, suppressed and
// discarded work returns before this point. Each store retains its own admission
// rules, including CustomSCP V-only; family display witnesses remain lane-owned.
func (p *outputPipeline) recordAcceptedObservation(s *spot.Spot) {
	now := time.Now().UTC()
	recordRecentBandObservationAt(s, p.recentBandStore, p.customSCPStore, p.correctionCfg, now)
	recordWhoSpotsMeObservation(s, p.whoSpotsMeStore, now)
	p.recordFTRecentBandObservationAt(s, now)
}

// resolveDeliveryPlan decides whether a spot reaches final fanout now, waits in
// the stabilizer, or is reduced to a telnet self-echo. Secondary dedupe is the
// final shared thinning gate after stabilizer processing.
func (p *outputPipeline) resolveDeliveryPlan(ctx *outputSpotContext) (outputDeliveryPlan, bool) {
	if p.rejectExpiredOutput(ctx) {
		return outputDeliveryPlan{}, false
	}
	s := ctx.spot
	plan := outputDeliveryPlan{
		allowFast:        true,
		allowMed:         true,
		allowSlow:        true,
		telnetDeliverNow: p.telnet != nil,
	}
	if !p.stabilizerEnabled {
		p.recordAcceptedObservation(s)
		plan = p.buildFinalDeliveryPlan(s)
		return plan, true
	}

	plan.telnetDeliverNow = false
	delaySnapshot, delaySnapshotOK := spot.ResolverSnapshot{}, false
	if ctx.hasStabilizerResolverKey && p.signalResolver != nil {
		delaySnapshot, delaySnapshotOK = p.signalResolver.Lookup(ctx.stabilizerResolverKey)
	}
	if ctx.stabilizerEvidenceLoss {
		// Selection may protect a lost neighbor even with no exact snapshot.
		delaySnapshot.EvidenceLoss, delaySnapshotOK = true, true
	}
	delayDecision := evaluateTelnetStabilizerDelay(s, p.recentBandStore, p.correctionCfg, time.Now().UTC(), delaySnapshot, delaySnapshotOK)
	if p.rejectExpiredOutput(ctx) {
		return outputDeliveryPlan{}, false
	}
	if delayDecision.ShouldDelay {
		delayed := cloneSpotForTelnetStabilizer(s)
		bestConfidence, _ := ordinaryStabilizerConfidence(s.Confidence)
		if delayed != nil && p.telnetStabilizer.enqueueWithTemporalResult(
			delayed,
			0,
			delayDecision.Reason.String(),
			ctx.stabilizerResolverKey,
			ctx.hasStabilizerResolverKey,
			ctx.stabilizerEvidenceEnqueued,
			ctx.temporalResult,
			ctx.peerOriginal,
			delayed.Confidence,
			bestConfidence,
		) {
			if p.tracker != nil {
				p.tracker.IncrementStabilizerHeld()
				p.tracker.IncrementStabilizerHeldReason(delayDecision.Reason.String())
			}
			return plan, false
		}
		// A refused admission can wait on the scheduler lifecycle lock. It never
		// acquired held ownership, but must still be fresh before core learning.
		if p.rejectExpiredOutput(ctx) {
			return outputDeliveryPlan{}, false
		}
		p.recordAcceptedObservation(s)
		plan = p.buildFinalDeliveryPlan(s)
		if p.tracker != nil {
			p.tracker.IncrementStabilizerOverflowRelease()
			if stabilizerImmediateCountEligible(s) {
				p.tracker.IncrementStabilizerReleasedImmediate()
				p.tracker.IncrementStabilizerReleasedImmediateReason(delayDecision.Reason.String())
			}
		}
	} else {
		p.recordAcceptedObservation(s)
		plan = p.buildFinalDeliveryPlan(s)
		if p.tracker != nil && stabilizerImmediateCountEligible(s) {
			p.tracker.IncrementStabilizerReleasedImmediate()
			p.tracker.IncrementStabilizerReleasedImmediateReason(stabilizerDelayReasonNone.String())
		}
	}
	return plan, true
}

func (p *outputPipeline) updateGridCache(ctx *outputSpotContext) {
	if p.gridUpdate == nil {
		return
	}
	s := ctx.spot
	// Target lookups own their persistence. Retained old values, including a
	// same-grid reported flag, cannot promote target cache provenance.
	if dxGrid := strings.TrimSpace(s.DXMetadata.Grid); !ctx.dxIdentityChanged && dxGrid != "" && !s.DXMetadata.GridDerived {
		dxCall := s.DXCallNorm
		if dxCall == "" {
			dxCall = s.DXCall
		}
		p.gridUpdate(dxCall, dxGrid)
	}
	if deGrid := strings.TrimSpace(s.DEMetadata.Grid); deGrid != "" && !s.DEMetadata.GridDerived {
		deCall := s.DECallNorm
		if deCall == "" {
			deCall = s.DECall
		}
		p.gridUpdate(deCall, deGrid)
	}
}

// emitSpot seals local output for ring/archive/telnet. Peer publication uses the
// separately owned original tuple, under the same final MED and timing gates.
// The result reports final age admission, not queue success or client receipt.
func (p *outputPipeline) emitSpot(ctx *outputSpotContext, plan outputDeliveryPlan) bool {
	if ctx == nil || ctx.spot == nil {
		return false
	}
	if p.rejectExpiredOutput(ctx) {
		return false
	}
	shared := ctx.spot.SealForAsync()
	if shared == nil {
		return false
	}
	if p.buf != nil && plan.normalFanoutAllowed() && shouldBufferSpot(shared) {
		ctx.temporalResult.observeHandoff()
		p.buf.AddOwned(shared)
	}
	emittedNow := false
	if p.archiveWriter != nil && plan.allowMed && shouldArchiveSpot(shared) {
		ctx.temporalResult.observeHandoff()
		p.archiveWriter.EnqueueOwned(archiveSnapshotForSpot(shared))
		emittedNow = true
	}
	if plan.telnetDeliverNow && plan.normalFanoutAllowed() {
		lanes := familyLanes{plan.allowFast, plan.allowMed, plan.allowSlow}
		publish := func(offered familyLanes) bool {
			ctx.temporalResult.observeHandoff()
			emittedNow = true // Preserve attempted-broadcast health/accounting.
			return p.telnet.TryBroadcastSpotOwned(shared, offered.fast, offered.med, offered.slow)
		}
		if p.familySuppressor != nil {
			lanes = p.familySuppressor.handoff(shared, p.correctionCfg, time.Time{}, lanes,
				func(now time.Time) correctionflow.ResolverPrimarySelection {
					return p.selectFamilyEvidenceAt(ctx, shared, now)
				}, publish)
		} else {
			publish(lanes)
		}
		// Queue refusal has an attempted ordinary lane and must not gain a
		// direct SELF retry. Partial masks already deliver SELF once in broadcast.
		if !lanes.any() {
			plan.telnetDeliverSelf = true
		}
	}
	if plan.telnetDeliverSelf {
		ctx.temporalResult.observeHandoff()
		p.telnet.DeliverSelfSpotOwned(normalizedDXCall(shared), shared)
	}
	if p.peerManager != nil && plan.allowMed && ctx.peerOriginal != nil && p.peerManager.ShouldPublishLocalSpot(shared) {
		original := ctx.peerOriginal.publicationSpot()
		ctx.temporalResult.observeHandoff()
		if p.peerManager.PublishDX(&original) {
			emittedNow = true
		}
	}
	if emittedNow && p.lastOutput != nil {
		p.lastOutput.Store(time.Now().UTC().UnixNano())
	}
	return true
}

// selectFamilyEvidenceAt reads one existing eligible selection for the final
// literal identity. Called inside family arbitration's mutex after its decision
// clock is sampled; this is neither an ingest barrier nor another observation.
func (p *outputPipeline) selectFamilyEvidenceAt(ctx *outputSpotContext, shared *spot.Spot, now time.Time) correctionflow.ResolverPrimarySelection {
	key, hasKey := ctx.stabilizerResolverKey, ctx.hasStabilizerResolverKey
	if !hasKey {
		// Authoritative local SELF bypasses resolver mutation but can still use
		// the existing key derivation for a read-only family comparison.
		evidence, ok := buildResolverEvidenceSnapshot(shared, p.correctionCfg, p.adaptiveMinReports, now)
		key, hasKey = evidence.Key, ok
	}
	if !hasKey {
		return correctionflow.ResolverPrimarySelection{}
	}
	return correctionflow.SelectResolverPrimarySnapshotForCallAt(p.signalResolver, key, p.correctionCfg, normalizedDXCall(shared), now)
}

func archiveSnapshotForSpot(shared *spot.Spot) *spot.Spot {
	if shared == nil {
		return nil
	}
	if !shared.IsBeacon || strings.TrimSpace(shared.Comment) != "" {
		return shared
	}
	snapshot := shared.Clone()
	snapshot.EnsureBlankBeaconComment()
	return snapshot
}
