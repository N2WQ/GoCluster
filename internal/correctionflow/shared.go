package correctionflow

import (
	"sort"
	"strings"
	"time"

	"dxcluster/bandmap"
	"dxcluster/config"
	"dxcluster/spot"
	"dxcluster/strutil"
)

type RuntimeSettings struct {
	Window             time.Duration
	MinReports         int
	CooldownMinReports int
	QualityBinHz       int
	FreqToleranceHz    float64
	CandidateWindowKHz float64
}

type bandStateParams struct {
	QualityBinHz         int
	FrequencyToleranceHz float64
}

type BuildSettingsInput struct {
	Cfg                    config.CallCorrectionConfig
	MinReports             int
	CooldownMinReports     int
	Window                 time.Duration
	FreqToleranceHz        float64
	QualityBinHz           int
	DebugLog               bool
	TraceLogger            spot.CorrectionTraceLogger
	Cooldown               *spot.CallCooldown
	SpotterReliability     spot.SpotterReliability
	SpotterReliabilityCW   spot.SpotterReliability
	SpotterReliabilityRTTY spot.SpotterReliability
	ConfusionModel         *spot.ConfusionModel
	RecentBandStore        *spot.RecentBandStore
	KnownCallset           *spot.KnownCallsigns
	DecisionObserver       spot.CorrectionDecisionObserver
}

type ApplyInput struct {
	SpotEntry          *spot.Spot
	Index              *spot.CorrectionIndex
	Settings           spot.CorrectionSettings
	Window             time.Duration
	CandidateWindowKHz float64
	Now                time.Time
	InvalidAction      string
	RejectInvalidBase  func(string) bool
	CTYValidCall       func(string) bool
}

type ApplyResult struct {
	Suppress            bool
	Applied             bool
	CorrectedCall       string
	Supporters          int
	CorrectedConfidence int
	SubjectConfidence   int
	TotalReporters      int
	RejectReason        string // "invalid_base" | "cty_miss"
}

type ResolverPrimarySelection struct {
	Snapshot          spot.ResolverSnapshot
	SnapshotOK        bool
	UsedNeighborhood  bool
	WinnerOverride    bool
	NeighborhoodSplit bool
	CandidateCount    int
	// NeighborhoodExcluded* count neighborhood winner candidates excluded from
	// competition due to comparability rails.
	NeighborhoodExcludedUnrelated int
	NeighborhoodExcludedDistance  int
	// NeighborhoodExcludedAnchorMissing is set when neighborhood arbitration is
	// enabled but no anchor call is available; selection fails closed to exact.
	NeighborhoodExcludedAnchorMissing int
}

// FormatConfidence converts subject confidence percent into the public glyph.
// Contract: callers provide subject confidence (not corrected-candidate confidence).
// Mapping is intentionally coarse and stable: unknown ("?") for <=1 reporter,
// then P (<=50) or V (>=51). C/B are set by apply/validation rails, not here.
func FormatConfidence(percent int, totalReporters int) string {
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
	default:
		return "P"
	}
}

// ResolverSupportForCall returns reporter support for one call in snapshot.
// It prefers candidate rank support and falls back to winner/runner fields when
// rank lists are unavailable in synthetic snapshots.
func ResolverSupportForCall(snapshot spot.ResolverSnapshot, call string) int {
	call = spot.NormalizeCallsign(call)
	if call == "" {
		return 0
	}
	for _, candidate := range snapshot.CandidateRanks {
		if strings.EqualFold(candidate.Call, call) {
			if candidate.Support > 0 {
				return candidate.Support
			}
			return 0
		}
	}
	if snapshot.WinnerSupport > 0 && strings.EqualFold(snapshot.Winner, call) {
		return snapshot.WinnerSupport
	}
	if snapshot.RunnerSupport > 0 && strings.EqualFold(snapshot.RunnerUp, call) {
		return snapshot.RunnerSupport
	}
	return 0
}

// ResolverWeightedSupportForCall returns weighted support (milli-units) for one
// call in snapshot. It mirrors ResolverSupportForCall fallback behavior.
func ResolverWeightedSupportForCall(snapshot spot.ResolverSnapshot, call string) int {
	call = spot.NormalizeCallsign(call)
	if call == "" {
		return 0
	}
	for _, candidate := range snapshot.CandidateRanks {
		if strings.EqualFold(candidate.Call, call) {
			if candidate.WeightedSupportMilli > 0 {
				return candidate.WeightedSupportMilli
			}
			return 0
		}
	}
	if snapshot.WinnerWeightedSupportMilli > 0 && strings.EqualFold(snapshot.Winner, call) {
		return snapshot.WinnerWeightedSupportMilli
	}
	if snapshot.RunnerWeightedSupportMilli > 0 && strings.EqualFold(snapshot.RunnerUp, call) {
		return snapshot.RunnerWeightedSupportMilli
	}
	return 0
}

// ResolverCallConfidencePercent computes call-specific confidence percentage.
// It prefers weighted support when available and falls back to reporter support.
func ResolverCallConfidencePercent(snapshot spot.ResolverSnapshot, call string) (int, bool) {
	if snapshot.TotalWeightedSupportMilli > 0 {
		support := ResolverWeightedSupportForCall(snapshot, call)
		if support > 0 {
			return clampPercent(support * 100 / snapshot.TotalWeightedSupportMilli), true
		}
	}
	if snapshot.TotalReporters > 0 {
		support := ResolverSupportForCall(snapshot, call)
		if support > 0 {
			return clampPercent(support * 100 / snapshot.TotalReporters), true
		}
	}
	return 0, false
}

// ResolverWinnerConfidence computes winner confidence percentage from snapshot.
func ResolverWinnerConfidence(snapshot spot.ResolverSnapshot) int {
	percent, ok := ResolverCallConfidencePercent(snapshot, snapshot.Winner)
	if !ok {
		return 0
	}
	return percent
}

// ResolverConfidenceGlyphForCall maps resolver snapshot evidence to output glyph
// semantics for the call that will actually be emitted.
func ResolverConfidenceGlyphForCall(snapshot spot.ResolverSnapshot, snapshotOK bool, emittedCall string) string {
	if !snapshotOK {
		return "?"
	}
	if snapshot.State == spot.ResolverStateSplit || snapshot.State == spot.ResolverStateUncertain {
		if snapshot.TotalReporters <= 1 {
			return "?"
		}
		// Keep contested resolver outcomes conservative so conflicting variants
		// do not both present as strong-verified ("V").
		return "P"
	}

	percent, ok := ResolverCallConfidencePercent(snapshot, emittedCall)
	if !ok {
		return "?"
	}
	return FormatConfidence(percent, snapshot.TotalReporters)
}

// SelectResolverPrimarySnapshot returns the resolver snapshot used by
// resolver-primary correction decisions. When neighborhood mode is enabled, it
// competes winners across adjacent resolver buckets to reduce boundary forks.
func SelectResolverPrimarySnapshot(resolver *spot.SignalResolver, key spot.ResolverSignalKey, cfg config.CallCorrectionConfig) ResolverPrimarySelection {
	return SelectResolverPrimarySnapshotForCall(resolver, key, cfg, "")
}

// SelectResolverPrimarySnapshotForCall is the call-anchored variant of
// SelectResolverPrimarySnapshot.
//
// Anchor contract:
//   - When anchorCall is provided (typically pre-correction DX call), only
//     neighborhood winners comparable to that anchor are admitted.
//   - When anchorCall is blank, exact snapshot winner is used as fallback
//     anchor when available.
//   - If no anchor can be established, selection fails closed to exact snapshot.
func SelectResolverPrimarySnapshotForCall(resolver *spot.SignalResolver, key spot.ResolverSignalKey, cfg config.CallCorrectionConfig, anchorCall string) ResolverPrimarySelection {
	selection := ResolverPrimarySelection{}
	if resolver == nil {
		return selection
	}
	exact, exactOK := resolver.Lookup(key)
	selection.Snapshot = exact
	selection.SnapshotOK = exactOK
	if !cfg.ResolverNeighborhoodEnabled {
		return selection
	}
	anchor := spot.NormalizeCallsign(anchorCall)
	if anchor == "" && exactOK {
		anchor = spot.NormalizeCallsign(exact.Winner)
	}
	if anchor == "" {
		selection.NeighborhoodExcludedAnchorMissing = 1
		return selection
	}

	radius := cfg.ResolverNeighborhoodBucketRadius
	if radius <= 0 {
		radius = 1
	}
	if radius > 2 {
		radius = 2
	}

	type aggregate struct {
		call     string
		support  int
		weighted int
		best     spot.ResolverSnapshot
	}

	aggregates := make(map[string]*aggregate, (radius*2)+1)
	totalSupport := 0
	totalWeighted := 0
	for offset := -radius; offset <= radius; offset++ {
		neighborKey := key
		neighborKey.Bucket += int64(offset)
		snap, ok := resolver.Lookup(neighborKey)
		if !ok {
			continue
		}
		if snap.State != spot.ResolverStateConfident && snap.State != spot.ResolverStateProbable {
			continue
		}
		winner := spot.NormalizeCallsign(snap.Winner)
		if winner == "" {
			continue
		}
		support := snap.WinnerSupport
		if support < 0 {
			support = 0
		}
		weighted := snap.WinnerWeightedSupportMilli
		if weighted < 0 {
			weighted = 0
		}
		if support == 0 && weighted == 0 {
			continue
		}

		if offset != 0 {
			selection.UsedNeighborhood = true
		}
		comparable, rejectReason := resolverNeighborhoodComparable(anchor, winner, key.Mode, cfg)
		if !comparable {
			switch rejectReason {
			case "distance":
				selection.NeighborhoodExcludedDistance++
			default:
				selection.NeighborhoodExcludedUnrelated++
			}
			continue
		}
		totalSupport += support
		totalWeighted += weighted

		group := aggregates[winner]
		if group == nil {
			group = &aggregate{call: winner, best: snap}
			aggregates[winner] = group
		}
		group.support += support
		group.weighted += weighted
		if resolverSnapshotBeats(snap, group.best) {
			group.best = snap
		}
	}

	if !selection.UsedNeighborhood && exactOK {
		return selection
	}
	if len(aggregates) == 0 {
		if exactOK {
			return selection
		}
		return ResolverPrimarySelection{}
	}

	ranked := make([]aggregate, 0, len(aggregates))
	for _, group := range aggregates {
		if group == nil {
			continue
		}
		ranked = append(ranked, *group)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].weighted != ranked[j].weighted {
			return ranked[i].weighted > ranked[j].weighted
		}
		if ranked[i].support != ranked[j].support {
			return ranked[i].support > ranked[j].support
		}
		leftRank := resolverStateRank(ranked[i].best.State)
		rightRank := resolverStateRank(ranked[j].best.State)
		if leftRank != rightRank {
			return leftRank > rightRank
		}
		return ranked[i].call < ranked[j].call
	})
	selection.CandidateCount = len(ranked)
	top := ranked[0]

	synth := top.best
	synth.Key = key
	synth.Winner = top.call
	synth.WinnerSupport = top.support
	synth.WinnerWeightedSupportMilli = top.weighted
	if totalSupport > 0 {
		synth.TotalReporters = totalSupport
	}
	if totalWeighted > 0 {
		synth.TotalWeightedSupportMilli = totalWeighted
	}

	candidateRanks := make([]spot.ResolverCandidateSupport, 0, len(ranked))
	for _, candidate := range ranked {
		candidateRanks = append(candidateRanks, spot.ResolverCandidateSupport{
			Call:                 candidate.call,
			Support:              candidate.support,
			WeightedSupportMilli: candidate.weighted,
		})
	}
	synth.CandidateRanks = candidateRanks

	if len(ranked) > 1 {
		runner := ranked[1]
		synth.RunnerUp = runner.call
		synth.RunnerSupport = runner.support
		synth.RunnerWeightedSupportMilli = runner.weighted
		synth.Margin = synth.WinnerSupport - synth.RunnerSupport

		comparable, rejectReason := resolverNeighborhoodComparable(top.call, runner.call, key.Mode, cfg)
		if !comparable {
			switch rejectReason {
			case "distance":
				selection.NeighborhoodExcludedDistance++
			default:
				selection.NeighborhoodExcludedUnrelated++
			}
		} else {
			ratioThreshold := cfg.FreqGuardRunnerUpRatio
			if ratioThreshold <= 0 {
				ratioThreshold = 0.5
			}
			conflict := false
			switch {
			case synth.WinnerWeightedSupportMilli > 0 && synth.RunnerWeightedSupportMilli > 0:
				conflict = float64(synth.RunnerWeightedSupportMilli) >= ratioThreshold*float64(synth.WinnerWeightedSupportMilli)
			case synth.WinnerSupport > 0 && synth.RunnerSupport > 0:
				conflict = float64(synth.RunnerSupport) >= ratioThreshold*float64(synth.WinnerSupport)
			}
			if conflict {
				synth.State = spot.ResolverStateSplit
				synth.Winner = ""
				synth.Margin = 0
				selection.NeighborhoodSplit = true
			}
		}
	} else {
		synth.RunnerUp = ""
		synth.RunnerSupport = 0
		synth.RunnerWeightedSupportMilli = 0
		synth.Margin = synth.WinnerSupport
	}

	selection.Snapshot = synth
	selection.SnapshotOK = true
	if !selection.NeighborhoodSplit {
		exactWinner := ""
		if exactOK {
			exactWinner = spot.NormalizeCallsign(exact.Winner)
		}
		if exactWinner != "" && !strings.EqualFold(exactWinner, synth.Winner) {
			comparable, rejectReason := resolverNeighborhoodComparable(exactWinner, synth.Winner, key.Mode, cfg)
			if !comparable {
				switch rejectReason {
				case "distance":
					selection.NeighborhoodExcludedDistance++
				default:
					selection.NeighborhoodExcludedUnrelated++
				}
				selection.Snapshot = exact
				selection.SnapshotOK = exactOK
				selection.WinnerOverride = false
				return selection
			}
			selection.WinnerOverride = true
		}
	}
	return selection
}

func resolverNeighborhoodComparable(anchorCall, candidateCall, mode string, cfg config.CallCorrectionConfig) (bool, string) {
	anchor := spot.CorrectionVoteKey(anchorCall)
	candidate := spot.CorrectionVoteKey(candidateCall)
	if anchor == "" || candidate == "" {
		return false, "unrelated"
	}
	if strings.EqualFold(anchor, candidate) {
		return true, ""
	}
	familyPolicy := spot.CorrectionFamilyPolicy{
		Configured:                 true,
		TruncationEnabled:          cfg.FamilyPolicy.Truncation.Enabled,
		TruncationMaxLengthDelta:   cfg.FamilyPolicy.Truncation.MaxLengthDelta,
		TruncationMinShorterLength: cfg.FamilyPolicy.Truncation.MinShorterLength,
		TruncationAllowPrefix:      cfg.FamilyPolicy.Truncation.AllowPrefixMatch,
		TruncationAllowSuffix:      cfg.FamilyPolicy.Truncation.AllowSuffixMatch,
	}
	if relation, ok := spot.DetectCorrectionFamilyWithPolicy(anchor, candidate, familyPolicy); ok {
		switch relation.Kind {
		case spot.CorrectionFamilySlash:
			return true, ""
		case spot.CorrectionFamilyTruncation:
			if cfg.ResolverNeighborhoodAllowTruncation {
				return true, ""
			}
			return false, "distance"
		default:
			return false, "unrelated"
		}
	}

	maxDistance := cfg.ResolverNeighborhoodMaxDistance
	if maxDistance <= 0 {
		maxDistance = 1
	}
	distance := spot.CallDistance(anchor, candidate, mode, cfg.DistanceModelCW, cfg.DistanceModelRTTY)
	if distance <= maxDistance {
		return true, ""
	}
	if distance <= maxDistance+1 {
		return false, "distance"
	}
	return false, "unrelated"
}

const editNeighborAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// EditDistanceOneSubstitutionVariants returns deterministic distance-1
// substitution variants for a correction vote key.
func EditDistanceOneSubstitutionVariants(call string) []string {
	key := spot.CorrectionVoteKey(call)
	if key == "" {
		return nil
	}
	if strings.Contains(key, "/") {
		return nil
	}
	if len(key) < 3 || len(key) > 12 {
		return nil
	}
	seed := []byte(key)
	out := make([]string, 0, len(seed)*(len(editNeighborAlphabet)-1))
	for idx := range seed {
		original := seed[idx]
		for i := 0; i < len(editNeighborAlphabet); i++ {
			replacement := editNeighborAlphabet[i]
			if replacement == original {
				continue
			}
			seed[idx] = replacement
			out = append(out, string(seed))
		}
		seed[idx] = original
	}
	return out
}

func resolverSnapshotBeats(left, right spot.ResolverSnapshot) bool {
	if left.WinnerWeightedSupportMilli != right.WinnerWeightedSupportMilli {
		return left.WinnerWeightedSupportMilli > right.WinnerWeightedSupportMilli
	}
	if left.WinnerSupport != right.WinnerSupport {
		return left.WinnerSupport > right.WinnerSupport
	}
	leftRank := resolverStateRank(left.State)
	rightRank := resolverStateRank(right.State)
	if leftRank != rightRank {
		return leftRank > rightRank
	}
	return left.EvaluatedAt.After(right.EvaluatedAt)
}

func resolverStateRank(state spot.ResolverState) int {
	switch state {
	case spot.ResolverStateConfident:
		return 2
	case spot.ResolverStateProbable:
		return 1
	default:
		return 0
	}
}

// CallCorrectionWindowForMode returns the recency window used by correction.
// Mode-specific overrides are only for CW/RTTY; other modes use base recency.
func CallCorrectionWindowForMode(cfg config.CallCorrectionConfig, mode string) time.Duration {
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

// ResolveRuntimeSettings computes per-spot correction runtime parameters.
// Invariants:
//   - voice (USB/LSB) uses voice tolerance and optional voice search window override.
//   - adaptive min-reports can override both correction and cooldown min-report gates.
//   - candidate search window always has a positive fallback (0.5 kHz).
//
// Side effect: when observeAdaptive is true, CW/RTTY observations are recorded before
// querying adaptive state so the current spot can influence near-term thresholds.
func ResolveRuntimeSettings(cfg config.CallCorrectionConfig, spotEntry *spot.Spot, adaptive *spot.AdaptiveMinReports, now time.Time, observeAdaptive bool) RuntimeSettings {
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
	window := CallCorrectionWindowForMode(cfg, modeUpper)

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

	return RuntimeSettings{
		Window:             window,
		MinReports:         minReports,
		CooldownMinReports: cooldownMinReports,
		QualityBinHz:       qualityBinHz,
		FreqToleranceHz:    freqToleranceHz,
		CandidateWindowKHz: candidateWindowKHz,
	}
}

// BuildCorrectionSettings maps config/runtime inputs into SuggestCallCorrection settings.
// Ownership: this is a pure mapper; it does not mutate shared state and keeps all
// side-effect callbacks (trace logger/decision observer) passed through from callers.
func BuildCorrectionSettings(in BuildSettingsInput) spot.CorrectionSettings {
	cfg := in.Cfg
	return spot.CorrectionSettings{
		MinConsensusReports: in.MinReports,
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
		MinConfidencePercent:                           cfg.MinConfidencePercent,
		MaxEditDistance:                                cfg.MaxEditDistance,
		RecencyWindow:                                  in.Window,
		Strategy:                                       cfg.Strategy,
		MinSNRCW:                                       cfg.MinSNRCW,
		MinSNRRTTY:                                     cfg.MinSNRRTTY,
		MinSNRVoice:                                    cfg.MinSNRVoice,
		DistanceModelCW:                                cfg.DistanceModelCW,
		DistanceModelRTTY:                              cfg.DistanceModelRTTY,
		Distance3ExtraReports:                          cfg.Distance3ExtraReports,
		Distance3ExtraAdvantage:                        cfg.Distance3ExtraAdvantage,
		Distance3ExtraConfidence:                       cfg.Distance3ExtraConfidence,
		DebugLog:                                       in.DebugLog,
		TraceLogger:                                    in.TraceLogger,
		FreqGuardMinSeparationKHz:                      cfg.FreqGuardMinSeparationKHz,
		FreqGuardRunnerUpRatio:                         cfg.FreqGuardRunnerUpRatio,
		FrequencyToleranceHz:                           in.FreqToleranceHz,
		QualityBinHz:                                   in.QualityBinHz,
		QualityGoodThreshold:                           cfg.QualityGoodThreshold,
		QualityNewCallIncrement:                        cfg.QualityNewCallIncrement,
		QualityBustedDecrement:                         cfg.QualityBustedDecrement,
		SpotterReliability:                             in.SpotterReliability,
		SpotterReliabilityCW:                           in.SpotterReliabilityCW,
		SpotterReliabilityRTTY:                         in.SpotterReliabilityRTTY,
		MinSpotterReliability:                          cfg.MinSpotterReliability,
		ConfusionModel:                                 in.ConfusionModel,
		ConfusionWeight:                                cfg.ConfusionModelWeight,
		RecentBandBonusEnabled:                         cfg.RecentBandBonusEnabled,
		RecentBandWindow:                               time.Duration(cfg.RecentBandWindowSeconds) * time.Second,
		RecentBandBonusMax:                             cfg.RecentBandBonusMax,
		RecentBandRecordMinUniqueSpotters:              cfg.RecentBandRecordMinUniqueSpotters,
		RecentBandStore:                                in.RecentBandStore,
		ResolverRecentPlus1Enabled:                     cfg.ResolverRecentPlus1Enabled,
		ResolverRecentPlus1MinUniqueWinner:             cfg.ResolverRecentPlus1MinUniqueWinner,
		ResolverRecentPlus1RequireSubjectWeaker:        cfg.ResolverRecentPlus1RequireSubjectWeaker,
		ResolverRecentPlus1MaxDistance:                 cfg.ResolverRecentPlus1MaxDistance,
		ResolverRecentPlus1AllowTruncation:             cfg.ResolverRecentPlus1AllowTruncation,
		TruncationLengthBonusEnabled:                   cfg.FamilyPolicy.Truncation.LengthBonus.Enabled,
		TruncationLengthBonusMax:                       cfg.FamilyPolicy.Truncation.LengthBonus.Max,
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
		PriorBonusKnownCallset:                         in.KnownCallset,
		Cooldown:                                       in.Cooldown,
		CooldownMinReporters:                           in.CooldownMinReports,
		DecisionObserver:                               in.DecisionObserver,
	}
}

// ApplyConsensusCorrection runs one correction decision and applies rails.
// Order contract:
//   - candidate lookup is performed against current index contents
//   - the current spot is added to the index on return (deferred), regardless of outcome
//
// Rail contract:
//   - no suggestion -> keep subject call, confidence is ?/P/V
//   - invalid-base or CTY miss -> reject reason set, either suppress or mark B
//   - accepted correction -> DX call updated and confidence set to C
func ApplyConsensusCorrection(in ApplyInput) ApplyResult {
	result := ApplyResult{}
	if in.SpotEntry == nil || in.Index == nil {
		return result
	}
	if in.Now.IsZero() {
		in.Now = time.Now().UTC()
	} else {
		in.Now = in.Now.UTC()
	}
	defer in.Index.Add(in.SpotEntry, in.Now, in.Window)

	others := in.Index.Candidates(in.SpotEntry, in.Now, in.Window, in.CandidateWindowKHz)
	entries := spotsToEntries(others)
	corrected, supporters, correctedConfidence, subjectConfidence, totalReporters, ok := spot.SuggestCallCorrection(in.SpotEntry, entries, in.Settings, in.Now)
	result.Supporters = supporters
	result.CorrectedConfidence = correctedConfidence
	result.SubjectConfidence = subjectConfidence
	result.TotalReporters = totalReporters

	in.SpotEntry.Confidence = FormatConfidence(subjectConfidence, totalReporters)
	if !ok {
		return result
	}

	correctedNorm := spot.NormalizeCallsign(corrected)
	result.CorrectedCall = correctedNorm

	if in.RejectInvalidBase != nil && in.RejectInvalidBase(correctedNorm) {
		result.RejectReason = "invalid_base"
		if strings.EqualFold(in.InvalidAction, "suppress") {
			result.Suppress = true
			return result
		}
		in.SpotEntry.Confidence = "B"
		return result
	}

	if in.CTYValidCall != nil && !in.CTYValidCall(correctedNorm) {
		result.RejectReason = "cty_miss"
		if strings.EqualFold(in.InvalidAction, "suppress") {
			result.Suppress = true
			return result
		}
		in.SpotEntry.Confidence = "B"
		return result
	}

	in.SpotEntry.DXCall = correctedNorm
	in.SpotEntry.DXCallNorm = correctedNorm
	in.SpotEntry.Confidence = "C"
	result.Applied = true
	return result
}

// NormalizedDXCall returns normalized DX call from DXCallNorm or DXCall fallback.
func NormalizedDXCall(s *spot.Spot) string {
	if s == nil {
		return ""
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	return spot.NormalizeCallsign(call)
}

// BuildResolverEvidenceSnapshot creates immutable resolver evidence from one spot.
// It only emits evidence for correction-candidate modes with valid normalized call,
// reporter, and band identity.
func BuildResolverEvidenceSnapshot(spotEntry *spot.Spot, cfg config.CallCorrectionConfig, adaptive *spot.AdaptiveMinReports, now time.Time) (spot.ResolverEvidence, bool) {
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

	call := NormalizedDXCall(spotEntry)
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

	runtime := ResolveRuntimeSettings(cfg, spotEntry, adaptive, now, false)
	key := spot.NewResolverSignalKey(spotEntry.Frequency, band, mode, runtime.FreqToleranceHz)

	return spot.ResolverEvidence{
		ObservedAt:    now,
		Key:           key,
		DXCall:        call,
		Spotter:       reporter,
		FrequencyKHz:  spotEntry.Frequency,
		RecencyWindow: runtime.Window,
	}, true
}

// ObserveResolverCurrentDecision records final decision outcome for resolver telemetry.
// A decision is marked corrected only when final call differs from pre-call and
// the confidence rail indicates an applied correction ("C").
func ObserveResolverCurrentDecision(resolver *spot.SignalResolver, key spot.ResolverSignalKey, spotEntry *spot.Spot, preCorrectionCall string) {
	if resolver == nil || spotEntry == nil {
		return
	}
	finalCall := NormalizedDXCall(spotEntry)
	if finalCall == "" {
		return
	}
	preCall := spot.NormalizeCallsign(preCorrectionCall)
	corrected := preCall != "" && !strings.EqualFold(preCall, finalCall) && strings.EqualFold(strings.TrimSpace(spotEntry.Confidence), "C")
	resolver.ObserveCurrentDecision(key, finalCall, corrected)
}

// resolveBandStateParams selects per-band state override values.
// Matching is case-insensitive on band labels and returns false when no override matches.
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

func clampPercent(value int) int {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

// spotsToEntries converts correction-index candidates into SuggestCallCorrection inputs.
// The conversion is read-only and allocates a compact slice sized to candidate count.
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
