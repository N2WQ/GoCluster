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
	delaySnapshot, delaySnapshotOK := spot.ResolverSnapshot{}, false
	if hasResolverEvidence && p.signalResolver != nil {
		delaySnapshot, delaySnapshotOK = p.signalResolver.LookupEligible(resolverEvidence.Key, now)
	}
	delayDecision := evaluateTelnetStabilizerDelay(delayed, p.recentBandStore, p.correctionCfg, time.Now().UTC(), delaySnapshot, delaySnapshotOK)
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
	if !shouldRecordRecentBandAfterStabilizerDelay(p.correctionCfg.StabilizerTimeoutAction, delayDecision.ShouldDelay) {
		if p.tracker != nil {
			p.tracker.IncrementStabilizerSuppressedTimeout()
			p.tracker.IncrementStabilizerSuppressedTimeoutReason(delayDecision.Reason.String())
			p.tracker.ObserveStabilizerGlyphTurns(delayed.Confidence, checksCompleted)
		}
		return
	}
	recordRecentBandObservation(delayed, p.recentBandStore, p.customSCPStore, p.correctionCfg)
	recordWhoSpotsMeObservation(delayed, p.whoSpotsMeStore, time.Now().UTC())
	plan := p.buildFinalDeliveryPlan(delayed)
	releaseCtx := outputSpotContext{spot: delayed, peerOriginal: envelope.peerOriginal, temporalResult: envelope.temporalResult,
		stabilizerResolverKey: resolverEvidence.Key, hasStabilizerResolverKey: hasResolverEvidence}
	if !plan.normalFanoutAllowed() {
		p.emitSpot(&releaseCtx, plan)
		return
	}
	if p.tracker != nil {
		p.tracker.IncrementStabilizerReleasedDelayed()
		p.tracker.IncrementStabilizerReleasedDelayedReason(stabilizerReleaseReason(delayDecision, envelope.delayReason))
		p.tracker.ObserveStabilizerGlyphTurns(delayed.Confidence, checksCompleted)
	}
	p.updateGridCache(delayed)
	p.emitSpot(&releaseCtx, plan)
}

// computeSecondaryAllows translates optional secondary dedup rails into
// fast/med/slow output-policy decisions. Telnet clients consume all three
// policy lanes; archive and peer consume the final shared MED lane.
func (p *outputPipeline) computeSecondaryAllows(s *spot.Spot) (bool, bool, bool) {
	allowFast := true
	if p.secondaryFast != nil {
		allowFast = p.secondaryFast.ShouldForward(s)
	}
	allowMed := true
	if p.secondaryMed != nil {
		allowMed = p.secondaryMed.ShouldForward(s)
	}
	allowSlow := true
	if p.secondarySlow != nil {
		allowSlow = p.secondarySlow.ShouldForward(s)
	}
	fallbackAllowed := allowFast
	if p.secondaryFast == nil {
		if p.secondaryMed != nil {
			fallbackAllowed = allowMed
		} else if p.secondarySlow != nil {
			fallbackAllowed = allowSlow
		}
	}
	if p.secondaryFast == nil {
		allowFast = fallbackAllowed
	}
	if p.secondaryMed == nil {
		allowMed = fallbackAllowed
	}
	if p.secondarySlow == nil {
		allowSlow = fallbackAllowed
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
		p.updateGridCache(ctx.spot)
	}
	p.emitSpot(ctx, plan)
}

// resolveDeliveryPlan decides whether a spot reaches final fanout now, waits in
// the stabilizer, or is reduced to a telnet self-echo. Secondary dedupe is the
// final shared thinning gate after stabilizer processing.
func (p *outputPipeline) resolveDeliveryPlan(ctx *outputSpotContext) (outputDeliveryPlan, bool) {
	s := ctx.spot
	plan := outputDeliveryPlan{
		allowFast:        true,
		allowMed:         true,
		allowSlow:        true,
		telnetDeliverNow: p.telnet != nil,
	}
	if !p.stabilizerEnabled {
		recordRecentBandObservation(s, p.recentBandStore, p.customSCPStore, p.correctionCfg)
		recordWhoSpotsMeObservation(s, p.whoSpotsMeStore, time.Now().UTC())
		p.recordFTRecentBandObservation(s)
		plan = p.buildFinalDeliveryPlan(s)
		return plan, true
	}

	plan.telnetDeliverNow = false
	delaySnapshot, delaySnapshotOK := spot.ResolverSnapshot{}, false
	if ctx.hasStabilizerResolverKey && p.signalResolver != nil {
		delaySnapshot, delaySnapshotOK = p.signalResolver.Lookup(ctx.stabilizerResolverKey)
	}
	delayDecision := evaluateTelnetStabilizerDelay(s, p.recentBandStore, p.correctionCfg, time.Now().UTC(), delaySnapshot, delaySnapshotOK)
	if delayDecision.ShouldDelay {
		delayed := cloneSpotForTelnetStabilizer(s)
		if delayed != nil && p.telnetStabilizer.enqueueWithTemporalResult(
			delayed,
			0,
			delayDecision.Reason.String(),
			ctx.stabilizerResolverKey,
			ctx.hasStabilizerResolverKey,
			ctx.stabilizerEvidenceEnqueued,
			ctx.temporalResult,
			ctx.peerOriginal,
		) {
			if p.tracker != nil {
				p.tracker.IncrementStabilizerHeld()
				p.tracker.IncrementStabilizerHeldReason(delayDecision.Reason.String())
			}
			return plan, false
		}
		plan = p.buildFinalDeliveryPlan(s)
		if p.tracker != nil {
			p.tracker.IncrementStabilizerOverflowRelease()
			if stabilizerImmediateCountEligible(s) {
				p.tracker.IncrementStabilizerReleasedImmediate()
				p.tracker.IncrementStabilizerReleasedImmediateReason(delayDecision.Reason.String())
			}
		}
	} else {
		plan = p.buildFinalDeliveryPlan(s)
		if p.tracker != nil && stabilizerImmediateCountEligible(s) {
			p.tracker.IncrementStabilizerReleasedImmediate()
			p.tracker.IncrementStabilizerReleasedImmediateReason(stabilizerDelayReasonNone.String())
		}
	}
	// Family display hiding is not pending stabilizer ownership. Once release
	// or overflow resolves that ownership, shared gates govern history admission.
	if shouldRecordRecentBandInMainLoop(p.stabilizerEnabled, !plan.normalFanoutAllowed()) {
		recordRecentBandObservation(s, p.recentBandStore, p.customSCPStore, p.correctionCfg)
		recordWhoSpotsMeObservation(s, p.whoSpotsMeStore, time.Now().UTC())
		p.recordFTRecentBandObservation(s)
	}
	return plan, true
}

func (p *outputPipeline) updateGridCache(s *spot.Spot) {
	if p.gridUpdate == nil {
		return
	}
	if dxGrid := strings.TrimSpace(s.DXMetadata.Grid); dxGrid != "" && !s.DXMetadata.GridDerived {
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
func (p *outputPipeline) emitSpot(ctx *outputSpotContext, plan outputDeliveryPlan) {
	if ctx == nil || ctx.spot == nil {
		return
	}
	shared := ctx.spot.SealForAsync()
	if shared == nil {
		return
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
