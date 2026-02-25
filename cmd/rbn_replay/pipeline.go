package main

import (
	"fmt"
	"strings"
	"time"

	"dxcluster/bandmap"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/strutil"
	"dxcluster/uls"
)

func spotsToEntries(spots []*spot.Spot) []bandmap.SpotEntry {
	if len(spots) == 0 {
		return nil
	}
	entries := make([]bandmap.SpotEntry, 0, len(spots))
	for _, s := range spots {
		if s == nil {
			continue
		}
		entries = append(entries, bandmap.SpotEntry{
			Call:    s.DXCall,
			Spotter: s.DECall,
			Mode:    s.Mode,
			FreqHz:  uint32(s.Frequency*1000 + 0.5),
			Time:    s.Time.Unix(),
			SNR:     s.Report,
		})
	}
	return entries
}

func formatConfidence(percent int, totalReporters int) string {
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

func callCorrectionWindowForMode(cfg config.CallCorrectionConfig, mode string) time.Duration {
	baseSeconds := cfg.RecencySeconds
	if baseSeconds <= 0 {
		baseSeconds = 45
	}
	switch strutil.NormalizeUpper(mode) {
	case "CW":
		if cfg.RecencySecondsCW > 0 {
			baseSeconds = cfg.RecencySecondsCW
		}
	case "RTTY":
		if cfg.RecencySecondsRTTY > 0 {
			baseSeconds = cfg.RecencySecondsRTTY
		}
	}
	return time.Duration(baseSeconds) * time.Second
}

type correctionRuntimeSettings struct {
	window             time.Duration
	minReports         int
	cooldownMinReports int
	qualityBinHz       int
	freqToleranceHz    float64
	candidateWindowKHz float64
}

func resolveCorrectionRuntimeSettings(cfg config.CallCorrectionConfig, spotEntry *spot.Spot, adaptive *spot.AdaptiveMinReports, now time.Time, observeAdaptive bool) correctionRuntimeSettings {
	mode := ""
	band := ""
	if spotEntry != nil {
		mode = spotEntry.Mode
		band = spotEntry.Band
	}
	modeUpper := strutil.NormalizeUpper(mode)
	if band == "" && spotEntry != nil {
		band = spot.FreqToBand(spotEntry.Frequency)
	}
	isVoice := modeUpper == "USB" || modeUpper == "LSB"
	window := callCorrectionWindowForMode(cfg, modeUpper)

	if observeAdaptive && adaptive != nil && (modeUpper == "CW" || modeUpper == "RTTY") && spotEntry != nil {
		reporter := spotEntry.DECallNorm
		if reporter == "" {
			reporter = spotEntry.DECall
		}
		adaptive.Observe(band, reporter, now)
	}

	minReports := cfg.MinConsensusReports
	cooldownMinReports := cfg.CooldownMinReporters
	if adaptive != nil && (modeUpper == "CW" || modeUpper == "RTTY") {
		if dyn := adaptive.MinReportsForBand(band, now); dyn > 0 {
			minReports = dyn
			cooldownMinReports = dyn
		}
	}

	state := "normal"
	if adaptive != nil {
		state = adaptive.StateForBand(band, now)
	}

	qualityBinHz := cfg.QualityBinHz
	freqToleranceHz := cfg.FrequencyToleranceHz
	if isVoice {
		freqToleranceHz = cfg.VoiceFrequencyToleranceHz
	} else if params, ok := resolveBandStateParams(cfg.BandStateOverrides, band, state); ok {
		if params.QualityBinHz > 0 {
			qualityBinHz = params.QualityBinHz
		}
		if params.FrequencyToleranceHz > 0 {
			freqToleranceHz = params.FrequencyToleranceHz
		}
	}

	candidateWindowKHz := freqToleranceHz / 1000.0
	if candidateWindowKHz <= 0 {
		candidateWindowKHz = 0.5
	}
	if isVoice && cfg.VoiceCandidateWindowKHz > 0 {
		candidateWindowKHz = cfg.VoiceCandidateWindowKHz
	}

	return correctionRuntimeSettings{
		window:             window,
		minReports:         minReports,
		cooldownMinReports: cooldownMinReports,
		qualityBinHz:       qualityBinHz,
		freqToleranceHz:    freqToleranceHz,
		candidateWindowKHz: candidateWindowKHz,
	}
}

type bandStateParams struct {
	QualityBinHz         int
	FrequencyToleranceHz float64
}

func resolveBandStateParams(overrides []config.BandStateOverride, band, state string) (bandStateParams, bool) {
	b := strings.ToLower(strings.TrimSpace(band))
	if b == "" || len(overrides) == 0 {
		return bandStateParams{}, false
	}
	stateKey := strings.ToLower(strings.TrimSpace(state))
	for _, o := range overrides {
		for _, candidate := range o.Bands {
			if strings.ToLower(strings.TrimSpace(candidate)) != b {
				continue
			}
			switch stateKey {
			case "quiet":
				return bandStateParams{
					QualityBinHz:         o.Quiet.QualityBinHz,
					FrequencyToleranceHz: o.Quiet.FrequencyToleranceHz,
				}, true
			case "busy":
				return bandStateParams{
					QualityBinHz:         o.Busy.QualityBinHz,
					FrequencyToleranceHz: o.Busy.FrequencyToleranceHz,
				}, true
			default:
				return bandStateParams{
					QualityBinHz:         o.Normal.QualityBinHz,
					FrequencyToleranceHz: o.Normal.FrequencyToleranceHz,
				}, true
			}
		}
	}
	return bandStateParams{}, false
}

func normalizedDXCall(s *spot.Spot) string {
	if s == nil {
		return ""
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	return spot.NormalizeCallsign(call)
}

func buildResolverEvidenceSnapshot(spotEntry *spot.Spot, cfg config.CallCorrectionConfig, adaptive *spot.AdaptiveMinReports, now time.Time) (spot.ResolverEvidence, bool) {
	if spotEntry == nil || !cfg.Enabled {
		return spot.ResolverEvidence{}, false
	}
	mode := spotEntry.ModeNorm
	if mode == "" {
		mode = spotEntry.Mode
	}
	mode = strutil.NormalizeUpper(mode)
	if !spot.IsCallCorrectionCandidate(mode) {
		return spot.ResolverEvidence{}, false
	}

	call := normalizedDXCall(spotEntry)
	reporter := spotEntry.DECallNorm
	if reporter == "" {
		reporter = spotEntry.DECall
	}
	reporter = strutil.NormalizeUpper(reporter)
	if call == "" || reporter == "" {
		return spot.ResolverEvidence{}, false
	}

	band := spotEntry.BandNorm
	if band == "" || band == "???" {
		band = spot.FreqToBand(spotEntry.Frequency)
	}
	band = spot.NormalizeBand(band)
	if band == "" || band == "???" {
		return spot.ResolverEvidence{}, false
	}

	runtime := resolveCorrectionRuntimeSettings(cfg, spotEntry, adaptive, now, false)
	key := spot.NewResolverSignalKey(spotEntry.Frequency, band, mode, runtime.freqToleranceHz)

	return spot.ResolverEvidence{
		ObservedAt:    now,
		Key:           key,
		DXCall:        call,
		Spotter:       reporter,
		FrequencyKHz:  spotEntry.Frequency,
		RecencyWindow: runtime.window,
	}, true
}

func observeResolverCurrentDecision(resolver *spot.SignalResolver, key spot.ResolverSignalKey, spotEntry *spot.Spot, preCorrectionCall string) {
	if resolver == nil || spotEntry == nil {
		return
	}
	finalCall := normalizedDXCall(spotEntry)
	if finalCall == "" {
		return
	}
	preCall := spot.NormalizeCallsign(preCorrectionCall)
	corrected := preCall != "" && !strings.EqualFold(preCall, finalCall) && strings.EqualFold(strings.TrimSpace(spotEntry.Confidence), "C")
	resolver.ObserveCurrentDecision(key, finalCall, corrected)
}

type resolverMethodDecision struct {
	Winner     string
	Comparable bool
	Applied    bool
}

func classifyResolverMethodDecision(resolver *spot.SignalResolver, key spot.ResolverSignalKey, preCorrectionCall string) (resolverMethodDecision, bool) {
	if resolver == nil {
		return resolverMethodDecision{}, false
	}
	preCall := spot.NormalizeCallsign(preCorrectionCall)
	if preCall == "" {
		return resolverMethodDecision{}, false
	}

	snap, ok := resolver.Lookup(key)
	if !ok {
		return resolverMethodDecision{}, false
	}
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
		Applied:    !strings.EqualFold(winner, preCall),
	}, true
}

func buildCorrectionSettings(
	cfg config.CallCorrectionConfig,
	minReports int,
	cooldownMinReports int,
	window time.Duration,
	freqToleranceHz float64,
	qualityBinHz int,
	cooldown *spot.CallCooldown,
	spotterReliability spot.SpotterReliability,
	spotterReliabilityCW spot.SpotterReliability,
	spotterReliabilityRTTY spot.SpotterReliability,
	confusionModel *spot.ConfusionModel,
	recentBandStore *spot.RecentBandStore,
	knownCallset *spot.KnownCallsigns,
	decisionObserver spot.CorrectionDecisionObserver,
) spot.CorrectionSettings {
	return spot.CorrectionSettings{
		MinConsensusReports: minReports,
		FamilyPolicy: spot.CorrectionFamilyPolicy{
			Configured:                 true,
			TruncationEnabled:          cfg.FamilyPolicy.Truncation.Enabled,
			TruncationMaxLengthDelta:   cfg.FamilyPolicy.Truncation.MaxLengthDelta,
			TruncationMinShorterLength: cfg.FamilyPolicy.Truncation.MinShorterLength,
			TruncationAllowPrefix:      cfg.FamilyPolicy.Truncation.AllowPrefixMatch,
			TruncationAllowSuffix:      cfg.FamilyPolicy.Truncation.AllowSuffixMatch,
		},
		SlashPrecedenceMinReports: cfg.FamilyPolicy.SlashPrecedenceMinReports,
		CandidateEvalTopK:         cfg.CandidateEvalTopK,
		MinAdvantage:              cfg.MinAdvantage,
		TruncationAdvantagePolicy: spot.CorrectionTruncationAdvantagePolicy{
			Configured:                true,
			Enabled:                   cfg.FamilyPolicy.Truncation.RelaxAdvantage.Enabled,
			MinAdvantage:              cfg.FamilyPolicy.Truncation.RelaxAdvantage.MinAdvantage,
			RequireCandidateValidated: cfg.FamilyPolicy.Truncation.RelaxAdvantage.RequireCandidateValidated,
			RequireSubjectUnvalidated: cfg.FamilyPolicy.Truncation.RelaxAdvantage.RequireSubjectUnvalidated,
		},
		MinConfidencePercent:              cfg.MinConfidencePercent,
		MaxEditDistance:                   cfg.MaxEditDistance,
		RecencyWindow:                     window,
		Strategy:                          cfg.Strategy,
		MinSNRCW:                          cfg.MinSNRCW,
		MinSNRRTTY:                        cfg.MinSNRRTTY,
		MinSNRVoice:                       cfg.MinSNRVoice,
		DistanceModelCW:                   cfg.DistanceModelCW,
		DistanceModelRTTY:                 cfg.DistanceModelRTTY,
		Distance3ExtraReports:             cfg.Distance3ExtraReports,
		Distance3ExtraAdvantage:           cfg.Distance3ExtraAdvantage,
		Distance3ExtraConfidence:          cfg.Distance3ExtraConfidence,
		DebugLog:                          false,
		TraceLogger:                       nil,
		FreqGuardMinSeparationKHz:         cfg.FreqGuardMinSeparationKHz,
		FreqGuardRunnerUpRatio:            cfg.FreqGuardRunnerUpRatio,
		FrequencyToleranceHz:              freqToleranceHz,
		QualityBinHz:                      qualityBinHz,
		QualityGoodThreshold:              cfg.QualityGoodThreshold,
		QualityNewCallIncrement:           cfg.QualityNewCallIncrement,
		QualityBustedDecrement:            cfg.QualityBustedDecrement,
		SpotterReliability:                spotterReliability,
		SpotterReliabilityCW:              spotterReliabilityCW,
		SpotterReliabilityRTTY:            spotterReliabilityRTTY,
		MinSpotterReliability:             cfg.MinSpotterReliability,
		ConfusionModel:                    confusionModel,
		ConfusionWeight:                   cfg.ConfusionModelWeight,
		RecentBandBonusEnabled:            cfg.RecentBandBonusEnabled,
		RecentBandWindow:                  time.Duration(cfg.RecentBandWindowSeconds) * time.Second,
		RecentBandBonusMax:                cfg.RecentBandBonusMax,
		RecentBandRecordMinUniqueSpotters: cfg.RecentBandRecordMinUniqueSpotters,
		RecentBandStore:                   recentBandStore,
		TruncationLengthBonusEnabled:      cfg.FamilyPolicy.Truncation.LengthBonus.Enabled,
		TruncationLengthBonusMax:          cfg.FamilyPolicy.Truncation.LengthBonus.Max,
		TruncationLengthBonusRequireCandidateValidated: cfg.FamilyPolicy.Truncation.LengthBonus.RequireCandidateValidated,
		TruncationLengthBonusRequireSubjectUnvalidated: cfg.FamilyPolicy.Truncation.LengthBonus.RequireSubjectUnvalidated,
		TruncationDelta2RailsEnabled:                   cfg.FamilyPolicy.Truncation.Delta2Rails.Enabled,
		TruncationDelta2ExtraConfidence:                cfg.FamilyPolicy.Truncation.Delta2Rails.ExtraConfidencePercent,
		TruncationDelta2RequireCandidateValidated:      cfg.FamilyPolicy.Truncation.Delta2Rails.RequireCandidateValidated,
		TruncationDelta2RequireSubjectUnvalidated:      cfg.FamilyPolicy.Truncation.Delta2Rails.RequireSubjectUnvalidated,
		PriorBonusEnabled:                              cfg.PriorBonusEnabled,
		PriorBonusMax:                                  cfg.PriorBonusMax,
		PriorBonusDistanceMax:                          cfg.PriorBonusDistanceMax,
		PriorBonusRequiresSCP:                          cfg.PriorBonusRequiresSCP,
		PriorBonusApplyTo:                              cfg.PriorBonusApplyTo,
		PriorBonusKnownCallset:                         knownCallset,
		Cooldown:                                       cooldown,
		CooldownMinReporters:                           cooldownMinReports,
		DecisionObserver:                               decisionObserver,
	}
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
) bool {
	if spotEntry == nil {
		return false
	}
	if !spot.IsCallCorrectionCandidate(spotEntry.Mode) {
		return false
	}
	if idx == nil || !cfg.Enabled {
		if strings.TrimSpace(spotEntry.Confidence) == "" {
			spotEntry.Confidence = "?"
		}
		return false
	}

	runtime := resolveCorrectionRuntimeSettings(cfg, spotEntry, adaptive, now, true)
	window := runtime.window
	defer idx.Add(spotEntry, now, window)

	settings := buildCorrectionSettings(
		cfg,
		runtime.minReports,
		runtime.cooldownMinReports,
		window,
		runtime.freqToleranceHz,
		runtime.qualityBinHz,
		cooldown,
		spotterReliability,
		spotterReliabilityCW,
		spotterReliabilityRTTY,
		confusionModel,
		recentBandStore,
		knownCallset,
		func(tr spot.CorrectionTrace) {
			if tracker == nil {
				return
			}
			tracker.ObserveCallCorrectionDecision(tr.DecisionPath, tr.Decision, tr.Reason, tr.CandidateRank, tr.PriorBonusApplied)
		},
	)

	others := idx.Candidates(spotEntry, now, window, runtime.candidateWindowKHz)
	entries := spotsToEntries(others)
	corrected, _, _, subjectConfidence, totalReporters, ok := spot.SuggestCallCorrection(spotEntry, entries, settings, now)

	spotEntry.Confidence = formatConfidence(subjectConfidence, totalReporters)
	if !ok {
		return false
	}

	correctedNorm := spot.NormalizeCallsign(corrected)
	if shouldRejectCTYCall(correctedNorm) {
		if strings.EqualFold(cfg.InvalidAction, "suppress") {
			return true
		}
		spotEntry.Confidence = "B"
		return false
	}

	if ctyDB != nil {
		if _, ok := ctyDB.LookupCallsignPortable(correctedNorm); ok {
			spotEntry.DXCall = correctedNorm
			spotEntry.DXCallNorm = correctedNorm
			spotEntry.Confidence = "C"
			if tracker != nil {
				tracker.IncrementCallCorrections()
			}
		} else {
			if strings.EqualFold(cfg.InvalidAction, "suppress") {
				return true
			}
			spotEntry.Confidence = "B"
		}
		return false
	}

	spotEntry.DXCall = correctedNorm
	spotEntry.DXCallNorm = correctedNorm
	spotEntry.Confidence = "C"
	if tracker != nil {
		tracker.IncrementCallCorrections()
	}

	return false
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

func classifyDisagreementSample(resolver *spot.SignalResolver, key spot.ResolverSignalKey, spotEntry *spot.Spot, preCorrectionCall string) *disagreementSampleRow {
	if resolver == nil || spotEntry == nil {
		return nil
	}
	finalCall := normalizedDXCall(spotEntry)
	if finalCall == "" {
		return nil
	}
	preCall := spot.NormalizeCallsign(preCorrectionCall)
	corrected := preCall != "" && !strings.EqualFold(preCall, finalCall) && strings.EqualFold(strings.TrimSpace(spotEntry.Confidence), "C")

	snap, ok := resolver.Lookup(key)
	if !ok {
		return nil
	}

	class := ""
	if corrected {
		switch snap.State {
		case spot.ResolverStateSplit:
			class = "SP"
		case spot.ResolverStateUncertain:
			class = "UC"
		case spot.ResolverStateConfident:
			if snap.Winner != "" && !strings.EqualFold(snap.Winner, finalCall) {
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

		SnapshotState:  snap.State,
		SnapshotWinner: snap.Winner,
		SnapshotRunner: snap.RunnerUp,
		WinnerSupport:  snap.WinnerSupport,
		RunnerSupport:  snap.RunnerSupport,
		TotalReporters: snap.TotalReporters,

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
