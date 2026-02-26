package main

import (
	"fmt"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/strutil"
	"dxcluster/uls"
)

func formatConfidence(percent int, totalReporters int) string {
	return correctionflow.FormatConfidence(percent, totalReporters)
}

func normalizedDXCall(s *spot.Spot) string {
	return correctionflow.NormalizedDXCall(s)
}

func buildResolverEvidenceSnapshot(spotEntry *spot.Spot, cfg config.CallCorrectionConfig, adaptive *spot.AdaptiveMinReports, now time.Time) (spot.ResolverEvidence, bool) {
	return correctionflow.BuildResolverEvidenceSnapshot(spotEntry, cfg, adaptive, now)
}

func observeResolverCurrentDecision(resolver *spot.SignalResolver, key spot.ResolverSignalKey, spotEntry *spot.Spot, preCorrectionCall string) {
	correctionflow.ObserveResolverCurrentDecision(resolver, key, spotEntry, preCorrectionCall)
}

func evaluateStabilizerDelay(
	s *spot.Spot,
	store *spot.RecentBandStore,
	cfg config.CallCorrectionConfig,
	now time.Time,
	snapshot spot.ResolverSnapshot,
	snapshotOK bool,
) correctionflow.StabilizerDelayDecision {
	return correctionflow.EvaluateStabilizerDelay(s, store, cfg, now, snapshot, snapshotOK)
}

type resolverMethodDecision struct {
	Winner     string
	Comparable bool
	Applied    bool
}

func classifyResolverMethodDecision(selection correctionflow.ResolverPrimarySelection, preCorrectionCall string, gate spot.ResolverPrimaryGateResult, gateEvaluated bool) (resolverMethodDecision, bool) {
	preCall := spot.NormalizeCallsign(preCorrectionCall)
	if preCall == "" {
		return resolverMethodDecision{}, false
	}
	if !selection.SnapshotOK {
		return resolverMethodDecision{}, false
	}
	snap := selection.Snapshot
	if snap.State != spot.ResolverStateConfident && snap.State != spot.ResolverStateProbable {
		return resolverMethodDecision{}, true
	}

	winner := spot.NormalizeCallsign(snap.Winner)
	if winner == "" {
		return resolverMethodDecision{}, false
	}
	return resolverMethodDecision{
		Winner:     winner,
		Comparable: true,
		Applied:    !strings.EqualFold(winner, preCall) && (!gateEvaluated || gate.Allow),
	}, true
}

func evaluateResolverPrimaryGateReplay(
	spotEntry *spot.Spot,
	preCorrectionCall string,
	selection correctionflow.ResolverPrimarySelection,
	cfg config.CallCorrectionConfig,
	recentBandStore *spot.RecentBandStore,
	knownCallset *spot.KnownCallsigns,
	adaptive *spot.AdaptiveMinReports,
	now time.Time,
) (spot.ResolverPrimaryGateResult, bool) {
	if spotEntry == nil {
		return spot.ResolverPrimaryGateResult{}, false
	}
	preCall := spot.NormalizeCallsign(preCorrectionCall)
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
	winner := spot.NormalizeCallsign(snap.Winner)
	if winner == "" || strings.EqualFold(winner, preCall) {
		return spot.ResolverPrimaryGateResult{}, false
	}

	subjectSupport := correctionflow.ResolverSupportForCall(snap, preCall)
	winnerSupport := correctionflow.ResolverSupportForCall(snap, winner)
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
		Cfg:                cfg,
		MinReports:         runtime.MinReports,
		CooldownMinReports: runtime.CooldownMinReports,
		Window:             runtime.Window,
		FreqToleranceHz:    runtime.FreqToleranceHz,
		QualityBinHz:       runtime.QualityBinHz,
		RecentBandStore:    recentBandStore,
		KnownCallset:       knownCallset,
	})

	gateOptions := spot.ResolverPrimaryGateOptions{}
	if cfg.ResolverRecentPlus1Enabled {
		if spot.ResolverSnapshotHasComparableEditNeighbor(snap, winner, subjectMode, cfg.DistanceModelCW, cfg.DistanceModelRTTY) {
			gateOptions.RecentPlus1DisallowReason = "edit_neighbor_contested"
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
		settings,
		now,
		gateOptions,
	), true
}

func maybeApplyCallCorrectionReplay(
	spotEntry *spot.Spot,
	idx *spot.CorrectionIndex,
	cfg config.CallCorrectionConfig,
	ctyDB *cty.CTYDatabase,
	tracker *stats.Tracker,
	cooldown *spot.CallCooldown,
	adaptive *spot.AdaptiveMinReports,
	spotterReliability spot.SpotterReliability,
	spotterReliabilityCW spot.SpotterReliability,
	spotterReliabilityRTTY spot.SpotterReliability,
	confusionModel *spot.ConfusionModel,
	recentBandStore *spot.RecentBandStore,
	knownCallset *spot.KnownCallsigns,
	now time.Time,
) (bool, replayConfidenceOutcome) {
	outcome := replayConfidenceOutcome{}
	if spotEntry == nil {
		return false, outcome
	}
	if !spot.IsCallCorrectionCandidate(spotEntry.Mode) {
		return false, outcome
	}
	if idx == nil || !cfg.Enabled {
		if strings.TrimSpace(spotEntry.Confidence) == "" {
			spotEntry.Confidence = "?"
		}
		outcome.Final = normalizeConfidenceGlyph(spotEntry.Confidence)
		outcome.LegacyFinal = outcome.Final
		return false, outcome
	}

	runtime := correctionflow.ResolveRuntimeSettings(cfg, spotEntry, adaptive, now, true)
	settings := correctionflow.BuildCorrectionSettings(correctionflow.BuildSettingsInput{
		Cfg:                    cfg,
		MinReports:             runtime.MinReports,
		CooldownMinReports:     runtime.CooldownMinReports,
		Window:                 runtime.Window,
		FreqToleranceHz:        runtime.FreqToleranceHz,
		QualityBinHz:           runtime.QualityBinHz,
		DebugLog:               cfg.DebugLog,
		TraceLogger:            nil,
		Cooldown:               cooldown,
		SpotterReliability:     spotterReliability,
		SpotterReliabilityCW:   spotterReliabilityCW,
		SpotterReliabilityRTTY: spotterReliabilityRTTY,
		ConfusionModel:         confusionModel,
		RecentBandStore:        recentBandStore,
		KnownCallset:           knownCallset,
		DecisionObserver: func(tr spot.CorrectionTrace) {
			if tracker == nil {
				return
			}
			tracker.ObserveCallCorrectionDecision(tr.DecisionPath, tr.Decision, tr.Reason, tr.CandidateRank, tr.PriorBonusApplied)
		},
	})
	ctyValid := func(string) bool { return true }
	if ctyDB != nil {
		ctyValid = func(call string) bool {
			_, ok := ctyDB.LookupCallsignPortable(call)
			return ok
		}
	}
	result := correctionflow.ApplyConsensusCorrection(correctionflow.ApplyInput{
		SpotEntry:          spotEntry,
		Index:              idx,
		Settings:           settings,
		Window:             runtime.Window,
		CandidateWindowKHz: runtime.CandidateWindowKHz,
		Now:                now,
		InvalidAction:      cfg.InvalidAction,
		RejectInvalidBase:  shouldRejectCTYCall,
		CTYValidCall:       ctyValid,
	})
	outcome.Final = normalizeConfidenceGlyph(spotEntry.Confidence)
	outcome.LegacyFinal = normalizeConfidenceGlyph(formatConfidenceLegacy(result.SubjectConfidence, result.TotalReporters))
	if outcome.Final == "C" || outcome.Final == "B" {
		outcome.LegacyFinal = outcome.Final
	}
	if result.Applied && tracker != nil {
		tracker.IncrementCallCorrections()
	}
	return result.Suppress, outcome
}

func shouldRejectCTYCall(call string) bool {
	base := strings.TrimSpace(uls.NormalizeForLicense(call))
	if base == "" {
		return false
	}
	if uls.AllowlistMatchAny(base) {
		return false
	}
	return hasLeadingLettersBeforeDigit(base, 3)
}

func hasLeadingLettersBeforeDigit(call string, threshold int) bool {
	if threshold <= 0 {
		return false
	}
	call = strutil.NormalizeUpper(call)
	if call == "" {
		return false
	}
	count := 0
	for i := 0; i < len(call); i++ {
		ch := call[i]
		if ch >= '0' && ch <= '9' {
			return count >= threshold
		}
		if ch >= 'A' && ch <= 'Z' {
			count++
			if count >= threshold {
				return true
			}
			continue
		}
		break
	}
	return count >= threshold
}

func recordRecentBandObservation(s *spot.Spot, store *spot.RecentBandStore, cfg config.CallCorrectionConfig) {
	if s == nil || store == nil || s.IsBeacon {
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
	store.Record(call, band, mode, reporter, s.Time)
}

type disagreementSampleRow struct {
	TS time.Time

	Band    string
	Mode    string
	FreqKHz float64
	Spotter string

	PreCall   string
	FinalCall string
	Corrected bool

	SnapshotState  spot.ResolverState
	SnapshotWinner string
	SnapshotRunner string
	WinnerSupport  int
	RunnerSupport  int
	TotalReporters int

	Class string
}

func classifyDisagreementSample(snapshot spot.ResolverSnapshot, snapshotOK bool, spotEntry *spot.Spot, preCorrectionCall string) *disagreementSampleRow {
	if !snapshotOK || spotEntry == nil {
		return nil
	}
	finalCall := normalizedDXCall(spotEntry)
	if finalCall == "" {
		return nil
	}
	preCall := spot.NormalizeCallsign(preCorrectionCall)
	corrected := preCall != "" && !strings.EqualFold(preCall, finalCall) && strings.EqualFold(strings.TrimSpace(spotEntry.Confidence), "C")

	class := ""
	if corrected {
		switch snapshot.State {
		case spot.ResolverStateSplit:
			class = "SP"
		case spot.ResolverStateUncertain:
			class = "UC"
		case spot.ResolverStateConfident:
			if snapshot.Winner != "" && !strings.EqualFold(snapshot.Winner, finalCall) {
				class = "DW"
			}
		}
	}
	if class == "" {
		return nil
	}

	band := spotEntry.BandNorm
	if band == "" || band == "???" {
		band = spot.FreqToBand(spotEntry.Frequency)
	}
	band = spot.NormalizeBand(band)

	mode := spotEntry.ModeNorm
	if mode == "" {
		mode = spotEntry.Mode
	}
	mode = strutil.NormalizeUpper(mode)

	spotter := spotEntry.DECallNorm
	if spotter == "" {
		spotter = spotEntry.DECall
	}
	spotter = strutil.NormalizeUpper(spotter)

	return &disagreementSampleRow{
		TS: spotEntry.Time,

		Band:    band,
		Mode:    mode,
		FreqKHz: spotEntry.Frequency,
		Spotter: spotter,

		PreCall:   preCall,
		FinalCall: finalCall,
		Corrected: corrected,

		SnapshotState:  snapshot.State,
		SnapshotWinner: snapshot.Winner,
		SnapshotRunner: snapshot.RunnerUp,
		WinnerSupport:  snapshot.WinnerSupport,
		RunnerSupport:  snapshot.RunnerSupport,
		TotalReporters: snapshot.TotalReporters,

		Class: class,
	}
}

func disagreementCSVRow(row *disagreementSampleRow) []string {
	if row == nil {
		return nil
	}
	return []string{
		row.TS.UTC().Format("2006-01-02 15:04:05"),
		row.Band,
		row.Mode,
		fmt.Sprintf("%.1f", row.FreqKHz),
		row.Spotter,
		row.PreCall,
		row.FinalCall,
		fmt.Sprintf("%v", row.Corrected),
		string(row.SnapshotState),
		row.SnapshotWinner,
		row.SnapshotRunner,
		fmt.Sprintf("%d", row.WinnerSupport),
		fmt.Sprintf("%d", row.RunnerSupport),
		fmt.Sprintf("%d", row.TotalReporters),
		row.Class,
	}
}
