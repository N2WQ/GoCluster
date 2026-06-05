// File role: Hosts the ordered live output pipeline stages after primary
// dedupe and before archive, peer, and telnet delivery.
// Crawler notes: Start here when a spot is classified, mode-hydrated,
// resolver-mutated, aged out, path-tagged, confidence-stamped, or prepared for
// fan-out after it leaves primary dedupe.
// Related docs: README.md, spot/README.md, pathreliability/README.md.
// Related tests: internal/cluster/*output_pipeline*_test.go.
package cluster

import (
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"dxcluster/filter"
	"dxcluster/internal/correctionflow"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"dxcluster/strutil"
)

// prepareSpotContext normalizes and hydrates metadata before any output-stage
// decision. Keeping mode inference and beacon stamping up front makes later
// support traces easier: downstream stages can assume canonical fields.
func (p *outputPipeline) prepareSpotContext(s *spot.Spot) (outputSpotContext, bool) {
	if s == nil {
		return outputSpotContext{}, false
	}
	s.EnsureNormalized()
	spot.ApplySourceHumanFlag(s)
	ctyDB := p.ctyLookup()
	explicitMode := strings.TrimSpace(s.Mode) != ""
	if !explicitMode && ctyDB != nil {
		hydrateDXMetadataForModeInference(s, ctyDB, p.metaCache)
	}
	if p.modeAssigner != nil {
		p.modeAssigner.Assign(s, explicitMode)
	}
	s.EnsureNormalized()
	if p.refresher != nil {
		p.refresher.IncrementSpots()
	}
	s.RefreshBeaconFlag()
	if s.IsBeacon {
		s.Confidence = "V"
	}
	if !s.IsBeacon && p.spotPolicy.MaxAgeSeconds > 0 {
		if time.Since(s.Time) > time.Duration(p.spotPolicy.MaxAgeSeconds)*time.Second {
			return outputSpotContext{}, false
		}
	}
	return outputSpotContext{
		spot:      s,
		ctyDB:     ctyDB,
		modeUpper: s.ModeNorm,
	}, true
}

// applyToxicityStage classifies human comments before any fanout-visible work.
// AI misses are queued to bounded workers; the output loop keeps processing
// unrelated non-human spots and resumes this spot when the result returns.
func (p *outputPipeline) applyToxicityStage(s *spot.Spot) bool {
	if p == nil || p.toxicityClassifier == nil || s == nil {
		return true
	}
	s.EnsureNormalized()
	spot.ApplySourceHumanFlag(s)
	return p.toxicityClassifier.ClassifyOrEnqueue(s, time.Now().UTC())
}

// applyResolverStage owns resolver and temporal-decoder mutation for non-FT
// telnet spots. It leaves FT modes alone because their support glyphs come from
// burst corroboration rather than resolver family edits.
func (p *outputPipeline) applyResolverStage(ctx *outputSpotContext, temporalRelease *runtimeTemporalRelease) bool {
	s := ctx.spot
	if spot.IsLocalSelfSpot(s) {
		// Local self-spots are operator-authoritative input. Keep the existing
		// pipeline ownership, but bypass resolver/temporal mutation and stamp V
		// so telnet timing and custom_scp admission remain immediate.
		s.Confidence = "V"
		ctx.dirty = true
		return true
	}
	if modeSupportsFTConfidenceGlyph(ctx.modeUpper) {
		// FT confidence is handled by the dedicated corroboration rail. Do not
		// let resolver/temporal fallback paths stamp a placeholder glyph first,
		// or the FT stage will skip the spot entirely.
		return true
	}
	if p.telnet == nil || s.IsBeacon {
		return true
	}
	skipResolverApply := false
	var resolverEvidence spot.ResolverEvidence
	var hasResolverEvidence bool
	if temporalRelease != nil {
		pending := temporalRelease.pending
		resolverEvidence = pending.evidence
		hasResolverEvidence = pending.hasEvidence
		ctx.stabilizerResolverKey = pending.stabilizerResolverKey
		ctx.hasStabilizerResolverKey = pending.hasStabilizerResolverKey
		ctx.stabilizerEvidenceEnqueued = pending.stabilizerEvidenceEnqueued
		p.recordRuntimeTemporalDecision(temporalRelease.decision)
		switch temporalRelease.decision.Action {
		case correctionflow.TemporalDecisionActionApply:
			selection := temporalRelease.decision.Selection
			if maybeApplyResolverCorrectionWithSelectionOverride(
				s,
				p.signalResolver,
				resolverEvidence,
				hasResolverEvidence,
				p.correctionCfg,
				ctx.ctyDB,
				p.metaCache,
				p.tracker,
				p.dash,
				p.recentBandStore,
				p.adaptiveMinReports,
				p.spotterReliability,
				p.spotterReliabilityCW,
				p.spotterReliabilityRTTY,
				p.confusionModel,
				&selection,
			) {
				return false
			}
		case correctionflow.TemporalDecisionActionAbstain:
			skipResolverApply = true
			setResolverConfidence(
				s,
				temporalRelease.pending.selection.Snapshot,
				temporalRelease.pending.selection.SnapshotOK,
				normalizedDXCall(s),
			)
		}
	} else {
		now := time.Now().UTC()
		resolverEvidence, hasResolverEvidence = buildResolverEvidenceSnapshot(s, p.correctionCfg, p.adaptiveMinReports, now)
		if hasResolverEvidence && p.signalResolver != nil && p.signalResolver.Enqueue(resolverEvidence) {
			ctx.stabilizerEvidenceEnqueued = true
		}
		if hasResolverEvidence {
			ctx.stabilizerResolverKey = resolverEvidence.Key
			ctx.hasStabilizerResolverKey = true
		}
		held, skip := p.maybeHoldTemporalSpot(ctx, now, resolverEvidence, hasResolverEvidence)
		if held {
			return false
		}
		skipResolverApply = skip
	}
	if !skipResolverApply && temporalRelease == nil {
		if maybeApplyResolverCorrection(
			s,
			p.signalResolver,
			resolverEvidence,
			hasResolverEvidence,
			p.correctionCfg,
			ctx.ctyDB,
			p.metaCache,
			p.tracker,
			p.dash,
			p.recentBandStore,
			p.adaptiveMinReports,
			p.spotterReliability,
			p.spotterReliabilityCW,
			p.spotterReliabilityRTTY,
			p.confusionModel,
		) {
			return false
		}
	}
	if !skipResolverApply && temporalRelease != nil && temporalRelease.decision.Action != correctionflow.TemporalDecisionActionApply {
		if maybeApplyResolverCorrection(
			s,
			p.signalResolver,
			resolverEvidence,
			hasResolverEvidence,
			p.correctionCfg,
			ctx.ctyDB,
			p.metaCache,
			p.tracker,
			p.dash,
			p.recentBandStore,
			p.adaptiveMinReports,
			p.spotterReliability,
			p.spotterReliabilityCW,
			p.spotterReliabilityRTTY,
			p.confusionModel,
		) {
			return false
		}
	}
	s.RefreshBeaconFlag()
	ctx.dirty = true
	return true
}

// maybeHoldTemporalSpot gives contested resolver selections a short bounded
// window to settle. Overflow actions are explicit so operators can distinguish
// "held for evidence" from "released because the hold queue was full."
func (p *outputPipeline) maybeHoldTemporalSpot(
	ctx *outputSpotContext,
	now time.Time,
	resolverEvidence spot.ResolverEvidence,
	hasResolverEvidence bool,
) (bool, bool) {
	if !hasResolverEvidence || p.temporal == nil || !p.temporal.Enabled() {
		return false, false
	}
	subject := normalizedDXCall(ctx.spot)
	selection := correctionflow.ResolverPrimarySelection{}
	if subject != "" {
		selection = correctionflow.SelectResolverPrimarySnapshotForCall(
			p.signalResolver,
			resolverEvidence.Key,
			p.correctionCfg,
			subject,
		)
	}
	if !selection.SnapshotOK || !p.temporal.decoder.ShouldHoldSelection(selection) {
		return false, false
	}
	accepted, id, reason := p.temporal.Observe(
		now,
		resolverEvidence.Key,
		subject,
		runtimeTemporalPending{
			spot:                       ctx.spot,
			evidence:                   resolverEvidence,
			hasEvidence:                hasResolverEvidence,
			stabilizerResolverKey:      ctx.stabilizerResolverKey,
			hasStabilizerResolverKey:   ctx.hasStabilizerResolverKey,
			stabilizerEvidenceEnqueued: ctx.stabilizerEvidenceEnqueued,
			selection:                  selection,
		},
	)
	if accepted {
		if p.tracker != nil {
			p.tracker.IncrementTemporalPending()
		}
		return true, false
	}
	overflowDecision := correctionflow.TemporalDecision{
		ID:            id,
		Reason:        reason,
		CommitLatency: 0,
	}
	switch p.correctionCfg.TemporalDecoder.OverflowAction {
	case "abstain":
		overflowDecision.Action = correctionflow.TemporalDecisionActionAbstain
		p.recordRuntimeTemporalDecision(overflowDecision)
		setResolverConfidence(ctx.spot, selection.Snapshot, selection.SnapshotOK, subject)
		return false, true
	case "bypass":
		overflowDecision.Action = correctionflow.TemporalDecisionActionBypass
	default:
		overflowDecision.Action = correctionflow.TemporalDecisionActionFallbackResolver
	}
	p.recordRuntimeTemporalDecision(overflowDecision)
	return false, false
}

// applyPostResolverAdjustments handles policy corrections that depend on the
// resolver result but still precede fanout, including harmonics, averaged
// frequency, placeholder glyphs, and support floors.
func (p *outputPipeline) applyPostResolverAdjustments(ctx *outputSpotContext) bool {
	s := ctx.spot
	if !s.IsBeacon && p.harmonicDetector != nil && p.harmonicCfg.Enabled {
		if drop, fundamental, corroborators, deltaDB := p.harmonicDetector.ShouldDrop(s, time.Now().UTC()); drop {
			harmonicMsg := formatHarmonicSuppressedMessage(s.DXCall, s.Frequency, fundamental, corroborators, deltaDB)
			if p.tracker != nil {
				p.tracker.IncrementHarmonicSuppressions()
			}
			if p.droppedCallLogger != nil {
				detail := fmt.Sprintf("corroborators=%d_delta_db=%d", corroborators, deltaDB)
				p.droppedCallLogger.LogHarmonic(
					droppedCallSourceFromSpot(s),
					droppedCallDXFromSpot(s),
					droppedCallDEFromSpot(s),
					droppedCallDXFromSpot(s),
					droppedCallModeFromSpot(s),
					detail,
				)
			}
			if p.dash != nil {
				p.dash.AppendHarmonic(harmonicMsg)
			} else {
				log.Println(harmonicMsg)
			}
			return false
		}
	}
	if !s.IsBeacon && p.freqAvg != nil && shouldAverageFrequency(s) {
		window := frequencyAverageWindow(p.spotPolicy)
		tolerance := frequencyAverageTolerance(p.spotPolicy)
		dxCall := s.DXCallNorm
		if dxCall == "" {
			dxCall = s.DXCall
		}
		avg, corroborators, _ := p.freqAvg.Average(dxCall, s.Frequency, time.Now().UTC(), window, tolerance)
		rounded := math.Floor(avg*100+0.5) / 100
		delta := math.Abs(rounded - s.Frequency)
		if corroborators >= p.spotPolicy.FrequencyAveragingMinReports && delta >= 0.005 {
			s.Frequency = rounded
			if p.tracker != nil {
				p.tracker.IncrementFrequencyCorrections()
			}
			ctx.dirty = true
		}
	}
	if !s.IsBeacon {
		if modeSupportsResolverConfidenceGlyph(ctx.modeUpper) && strings.TrimSpace(s.Confidence) == "" {
			s.Confidence = "?"
			ctx.dirty = true
		}
		if applySupportFloor(s, p.recentBandStore, p.customSCPStore, nil, p.correctionCfg) {
			ctx.dirty = true
		}
	}
	return true
}

// finalizeSpotForMetrics performs the last validation and metrics accounting
// after mutation. Keeping license drops here means corrected metadata and calls
// are what get evaluated and reported.
func (p *outputPipeline) finalizeSpotForMetrics(ctx *outputSpotContext) bool {
	s := ctx.spot
	if ctx.dirty {
		s.EnsureNormalized()
	}
	if applyLicenseGate(s, ctx.ctyDB, p.metaCache, p.unlicensedReporter) {
		return false
	}
	if p.tracker != nil {
		modeKey := ctx.modeUpper
		if modeKey == "" {
			modeKey = filter.UnknownModeToken
		}
		p.tracker.IncrementMode(modeKey)

		sourceLabel := sourceStatsLabel(s)
		p.tracker.IncrementSource(sourceLabel)
		p.tracker.IncrementSourceMode(sourceLabel, modeKey)
		if sourceLabel == "PSKREPORTER" {
			switch strutil.NormalizeUpper(s.Mode) {
			case "PSK31", "PSK63":
				p.tracker.IncrementSourceMode(sourceLabel, s.Mode)
			}
		}
	}
	if !p.broadcastKeepSSID {
		base := s.DECallNorm
		if base == "" {
			base = s.DECall
		}
		stripped := collapseSSIDForBroadcast(base)
		s.DECallStripped = stripped
		s.DECallNormStripped = stripped
	}
	return true
}

func (p *outputPipeline) prepareFanoutSpot(ctx *outputSpotContext) bool {
	s := ctx.spot
	if p.secondaryActive && (s.DEMetadata.ADIF <= 0 || s.DEMetadata.CQZone <= 0) && ctx.ctyDB != nil {
		call := s.DECallNorm
		if call == "" {
			call = s.DECall
		}
		call = normalizeCallForMetadata(call)
		if info := effectivePrefixInfo(ctx.ctyDB, p.metaCache, call); info != nil {
			deGrid := strings.TrimSpace(s.DEMetadata.Grid)
			deGridDerived := s.DEMetadata.GridDerived
			s.DEMetadata = metadataFromPrefix(info)
			if deGrid != "" {
				s.DEMetadata.Grid = deGrid
				s.DEMetadata.GridDerived = deGridDerived
			}
			s.InvalidateMetadataCache()
			s.EnsureNormalized()
		}
	}
	if isStale(s, p.spotPolicy) {
		return false
	}
	if p.gridLookup != nil || p.gridLookupSync != nil {
		gridBackfilled := false
		dxCall := s.DXCallNorm
		if dxCall == "" {
			dxCall = s.DXCall
		}
		if strings.TrimSpace(s.DXMetadata.Grid) == "" {
			dxLookupCall := normalizeCallForMetadata(dxCall)
			if dxLookupCall == "" {
				dxLookupCall = dxCall
			}
			if grid, derived, ok := lookupGridUnified(dxLookupCall, p.gridLookupSync, p.gridLookup); ok {
				s.DXMetadata.Grid = grid
				s.DXMetadata.GridDerived = derived
				gridBackfilled = true
			}
		}
		deCall := s.DECallNorm
		if deCall == "" {
			deCall = s.DECall
		}
		if strings.TrimSpace(s.DEMetadata.Grid) == "" {
			deLookupCall := normalizeCallForMetadata(deCall)
			if deLookupCall == "" {
				deLookupCall = deCall
			}
			if grid, derived, ok := lookupGridUnified(deLookupCall, p.gridLookupSync, p.gridLookup); ok {
				s.DEMetadata.Grid = grid
				s.DEMetadata.GridDerived = derived
				gridBackfilled = true
			}
		}
		if gridBackfilled {
			s.InvalidateMetadataCache()
			s.EnsureNormalized()
		}
	}
	if p.pathPredictor != nil && p.pathPredictor.Config().Enabled {
		hydrateSpotFinePathCells(s)
		if s.HasReport {
			mode := s.ModeNorm
			if strings.TrimSpace(mode) == "" {
				mode = s.Mode
			}
			if ft8, ok := pathreliability.FT8Equivalent(mode, s.Report, p.pathPredictor.Config()); ok {
				cells := hydrateSpotPathCells(s)
				band := s.BandNorm
				if strings.TrimSpace(band) == "" {
					band = s.Band
				}
				band = strings.TrimSpace(spot.NormalizeBand(band))
				if band == "" || band == "???" {
					band = strings.TrimSpace(spot.NormalizeBand(spot.FreqToBand(s.Frequency)))
				}
				if band != "" && band != "???" && allowedBand(p.allowedBands, band) {
					spotTime := s.Time.UTC()
					if spotTime.IsZero() {
						spotTime = time.Now().UTC()
					}
					bucket := pathreliability.BucketForIngest(mode)
					if bucket != pathreliability.BucketNone {
						if p.pathReport != nil {
							p.pathReport.ObserveWithCells(s, spotTime, cells.deCoarse, cells.dxCoarse)
						}
						p.pathPredictor.UpdateWithReceiverHash(bucket, cells.deFine, cells.dxFine, cells.deCoarse, cells.dxCoarse, band, ft8, 1.0, spotTime, s.IsBeacon, pathReceiverHash(s))
					}
				}
			}
		}
	}
	return true
}

// pathReceiverHash intentionally hashes only receiver identity, not mutable
// metadata, so receiver caps remain stable across grid backfills and CTY
// refreshes.
func pathReceiverHash(s *spot.Spot) uint64 {
	if s == nil {
		return 0
	}
	receiver := s.DECallNorm
	if strings.TrimSpace(receiver) == "" {
		receiver = s.DECall
	}
	return pathreliability.ReceiverIdentityHash(receiver)
}

// releaseDueTemporal re-enters held spots through processSpot so released work
// follows the same mutation, validation, and fanout path as fresh spots.
func (p *outputPipeline) releaseDueTemporal(now time.Time, force bool) {
	if p.temporal == nil || !p.temporal.Enabled() {
		return
	}
	releases := p.temporal.Drain(now, force)
	if p.tracker != nil {
		for range releases {
			p.tracker.DecrementTemporalPending()
		}
	}
	for idx := range releases {
		release := releases[idx]
		p.processSpot(release.pending.spot, &release)
	}
}

// recordRuntimeTemporalDecision maps decoder actions into tracker counters that
// operators use to tell normal settling from overflow or low-margin abstention.
func (p *outputPipeline) recordRuntimeTemporalDecision(decision correctionflow.TemporalDecision) {
	if p.tracker == nil {
		return
	}
	p.tracker.ObserveTemporalCommitLatency(decision.CommitLatency)
	switch decision.Action {
	case correctionflow.TemporalDecisionActionApply:
		p.tracker.IncrementTemporalCommitted()
	case correctionflow.TemporalDecisionActionFallbackResolver:
		p.tracker.IncrementTemporalFallbackResolver()
	case correctionflow.TemporalDecisionActionAbstain:
		p.tracker.IncrementTemporalAbstainLowMargin()
	case correctionflow.TemporalDecisionActionBypass:
		p.tracker.IncrementTemporalOverflowBypass()
	}
	if decision.PathSwitched {
		p.tracker.IncrementTemporalPathSwitch()
	}
}
