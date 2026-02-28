package spot

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"dxcluster/strutil"

	lev "github.com/agnivade/levenshtein"
)

// CorrectionFamilyPolicy controls relationship detection between candidate calls.
// Configured distinguishes zero-value tests from explicit runtime wiring.
type CorrectionFamilyPolicy struct {
	Configured bool

	TruncationEnabled          bool
	TruncationMaxLengthDelta   int
	TruncationMinShorterLength int
	TruncationAllowPrefix      bool
	TruncationAllowSuffix      bool
}

// CorrectionTruncationAdvantagePolicy controls truncation-family min_advantage relaxation.
// Configured distinguishes zero-value tests from explicit runtime wiring.
type CorrectionTruncationAdvantagePolicy struct {
	Configured bool
	Enabled    bool

	MinAdvantage              int
	RequireCandidateValidated bool
	RequireSubjectUnvalidated bool
}

// CorrectionBayesBonusPolicy controls a conservative Bayesian-style rail used
// only for resolver-primary distance-1/2 near-threshold admission.
type CorrectionBayesBonusPolicy struct {
	// Configured distinguishes explicit runtime wiring from zero-value tests.
	Configured bool
	Enabled    bool

	WeightDistance1Milli int
	WeightDistance2Milli int

	WeightedSmoothingMilli int
	RecentSmoothing        int

	ObsLogCapMilli   int
	PriorLogMinMilli int
	PriorLogMaxMilli int

	ReportThresholdDistance1Milli int
	ReportThresholdDistance2Milli int

	AdvantageThresholdDistance1Milli int
	AdvantageThresholdDistance2Milli int

	AdvantageMinWeightedDeltaDistance1Milli int
	AdvantageMinWeightedDeltaDistance2Milli int

	AdvantageExtraConfidenceDistance1 int
	AdvantageExtraConfidenceDistance2 int

	RequireCandidateValidated          bool
	RequireSubjectUnvalidatedDistance2 bool
}

// CorrectionSettings contains resolver-primary correction rails.
// This is intentionally independent from config package types to keep spot logic
// testable without import cycles.
type CorrectionSettings struct {
	FreqGuardMinSeparationKHz float64
	FreqGuardRunnerUpRatio    float64
	FrequencyToleranceHz      float64

	MinConsensusReports       int
	FamilyPolicy              CorrectionFamilyPolicy
	SlashPrecedenceMinReports int
	MinAdvantage              int
	TruncationAdvantagePolicy CorrectionTruncationAdvantagePolicy
	MinConfidencePercent      int
	RecencyWindow             time.Duration
	MaxEditDistance           int

	DistanceModelCW   string
	DistanceModelRTTY string

	Distance3ExtraReports    int
	Distance3ExtraAdvantage  int
	Distance3ExtraConfidence int

	KnownCallset                      *KnownCallsigns
	RecentBandRecordMinUniqueSpotters int
	RecentBandStore                   RecentSupportStore

	ResolverRecentPlus1Enabled              bool
	ResolverRecentPlus1MinUniqueWinner      int
	ResolverRecentPlus1RequireSubjectWeaker bool
	ResolverRecentPlus1MaxDistance          int
	ResolverRecentPlus1AllowTruncation      bool

	TruncationLengthBonusEnabled                   bool
	TruncationLengthBonusMax                       int
	TruncationLengthBonusRequireCandidateValidated bool
	TruncationLengthBonusRequireSubjectUnvalidated bool

	TruncationDelta2RailsEnabled              bool
	TruncationDelta2ExtraConfidence           int
	TruncationDelta2RequireCandidateValidated bool
	TruncationDelta2RequireSubjectUnvalidated bool

	BayesBonusPolicy CorrectionBayesBonusPolicy
}

// ResolverPrimaryGateResult reports resolver-primary gate evaluation outcome.
// It mirrors key thresholds so callers can emit deterministic observability.
type ResolverPrimaryGateResult struct {
	Allow                       bool
	Reason                      string
	Distance                    int
	MinReports                  int
	MinAdvantage                int
	MinConfidence               int
	WinnerSupport               int
	EffectiveSupport            int
	SubjectSupport              int
	WinnerConfidence            int
	SubjectWeightedSupportMilli int
	WinnerWeightedSupportMilli  int
	LengthBonus                 int
	RecentPlus1Considered       bool
	RecentPlus1Applied          bool
	RecentPlus1Reject           string
	RecentPlus1Winner           int
	RecentPlus1Subject          int
	BayesReportBonusConsidered  bool
	BayesReportBonusApplied     bool
	BayesReportBonusReject      string
	BayesAdvantageConsidered    bool
	BayesAdvantageApplied       bool
	BayesAdvantageReject        string
	BayesWinnerRecent           int
	BayesSubjectRecent          int
	BayesScoreMilli             int
	BayesDistanceWeightMilli    int
	EffectiveAdvantageSupport   int
}

// ResolverPrimaryGateOptions carries resolver-primary context that cannot be
// derived from static call/support inputs alone.
type ResolverPrimaryGateOptions struct {
	RecentPlus1DisallowReason string
}

var correctionEligibleModes = map[string]struct{}{
	"CW":   {},
	"RTTY": {},
	"USB":  {},
	"LSB":  {},
}

// IsCallCorrectionCandidate reports whether mode is eligible for call correction rails.
func IsCallCorrectionCandidate(mode string) bool {
	_, ok := correctionEligibleModes[strutil.NormalizeUpper(mode)]
	return ok
}

// EvaluateResolverPrimaryGates applies correction-family-sensitive threshold
// rails for resolver-primary winner admission.
//
// Invariants:
//   - Pure function: no shared mutable state updates.
//   - Applies max-edit rails before confidence/support gates.
//   - Truncation-family relaxations never bypass confidence and advantage rails.
func EvaluateResolverPrimaryGates(
	subjectCall, winnerCall, subjectBand, subjectMode string,
	subjectSupport, winnerSupport, winnerConfidence int,
	subjectWeightedSupportMilli, winnerWeightedSupportMilli int,
	settings CorrectionSettings,
	now time.Time,
	options ResolverPrimaryGateOptions,
) ResolverPrimaryGateResult {
	cfg := settings
	options.RecentPlus1DisallowReason = strings.ToLower(strings.TrimSpace(options.RecentPlus1DisallowReason))
	result := ResolverPrimaryGateResult{
		Allow:                       false,
		WinnerSupport:               winnerSupport,
		EffectiveSupport:            winnerSupport,
		SubjectSupport:              subjectSupport,
		WinnerConfidence:            winnerConfidence,
		SubjectWeightedSupportMilli: subjectWeightedSupportMilli,
		WinnerWeightedSupportMilli:  winnerWeightedSupportMilli,
	}

	subjectIdentity := normalizeCorrectionCallIdentity(subjectCall)
	winnerIdentity := normalizeCorrectionCallIdentity(winnerCall)
	if subjectIdentity.VoteKey == "" || winnerIdentity.VoteKey == "" {
		result.Reason = "invalid_identity"
		return result
	}
	if strings.EqualFold(subjectIdentity.VoteKey, winnerIdentity.VoteKey) {
		result.Reason = "same_call"
		return result
	}

	distance := correctionDistance(subjectIdentity, winnerIdentity, subjectMode, cfg.DistanceModelCW, cfg.DistanceModelRTTY)
	result.Distance = distance
	if cfg.MaxEditDistance >= 0 && distance > cfg.MaxEditDistance {
		result.Reason = "max_edit_distance"
		return result
	}

	minReports := cfg.MinConsensusReports
	minAdvantage := cfg.MinAdvantage
	minConf := cfg.MinConfidencePercent
	if distance >= 3 {
		minReports += cfg.Distance3ExtraReports
		minAdvantage += cfg.Distance3ExtraAdvantage
		minConf += cfg.Distance3ExtraConfidence
	}
	familyPolicy := normalizeCorrectionFamilyPolicy(cfg.FamilyPolicy)
	truncationAdvantagePolicy := normalizeCorrectionTruncationAdvantagePolicy(cfg.TruncationAdvantagePolicy)

	truncationRelation := false
	candidateMoreSpecific := false
	lengthDelta := 0
	candidateValidated := false
	candidateValidatedKnown := false
	subjectValidated := false
	subjectValidatedKnown := false
	if relation, ok := detectCorrectionFamilyByIdentity(subjectIdentity, winnerIdentity, familyPolicy); ok && relation.Kind == CorrectionFamilyTruncation {
		truncationRelation = true
		candidateMoreSpecific = len(winnerIdentity.VoteKey) > len(subjectIdentity.VoteKey)
		if candidateMoreSpecific {
			lengthDelta = len(winnerIdentity.VoteKey) - len(subjectIdentity.VoteKey)
			candidateValidated = resolverCallValidated(winnerIdentity, winnerCall, subjectBand, subjectMode, cfg, now)
			subjectValidated = resolverCallValidated(subjectIdentity, subjectCall, subjectBand, subjectMode, cfg, now)
			candidateValidatedKnown = true
			subjectValidatedKnown = true
		}
	}
	resolveCandidateValidated := func() bool {
		if candidateValidatedKnown {
			return candidateValidated
		}
		candidateValidated = resolverCallValidated(winnerIdentity, winnerCall, subjectBand, subjectMode, cfg, now)
		candidateValidatedKnown = true
		return candidateValidated
	}
	resolveSubjectValidated := func() bool {
		if subjectValidatedKnown {
			return subjectValidated
		}
		subjectValidated = resolverCallValidated(subjectIdentity, subjectCall, subjectBand, subjectMode, cfg, now)
		subjectValidatedKnown = true
		return subjectValidated
	}

	if truncationRelation && candidateMoreSpecific && truncationAdvantagePolicy.Enabled {
		eligible := true
		if truncationAdvantagePolicy.RequireCandidateValidated && !resolveCandidateValidated() {
			eligible = false
		}
		if truncationAdvantagePolicy.RequireSubjectUnvalidated && resolveSubjectValidated() {
			eligible = false
		}
		if eligible {
			minAdvantage = truncationAdvantagePolicy.MinAdvantage
		}
	}

	if truncationRelation && candidateMoreSpecific && cfg.TruncationDelta2RailsEnabled && lengthDelta >= 2 {
		if cfg.TruncationDelta2RequireCandidateValidated && !resolveCandidateValidated() {
			result.Reason = "truncation_delta2_candidate_unvalidated"
			result.MinReports = minReports
			result.MinAdvantage = minAdvantage
			result.MinConfidence = minConf
			return result
		}
		if cfg.TruncationDelta2RequireSubjectUnvalidated && resolveSubjectValidated() {
			result.Reason = "truncation_delta2_subject_validated"
			result.MinReports = minReports
			result.MinAdvantage = minAdvantage
			result.MinConfidence = minConf
			return result
		}
		if cfg.TruncationDelta2ExtraConfidence > 0 {
			minConf += cfg.TruncationDelta2ExtraConfidence
		}
	}

	effectiveSupport := winnerSupport
	lengthBonus := 0
	if cfg.TruncationLengthBonusEnabled && cfg.TruncationLengthBonusMax > 0 && effectiveSupport < minReports && truncationRelation && candidateMoreSpecific {
		eligible := true
		if cfg.TruncationLengthBonusRequireCandidateValidated && !resolveCandidateValidated() {
			eligible = false
		}
		if cfg.TruncationLengthBonusRequireSubjectUnvalidated && resolveSubjectValidated() {
			eligible = false
		}
		if eligible && lengthDelta > 0 {
			lengthBonus = lengthDelta
			if lengthBonus > cfg.TruncationLengthBonusMax {
				lengthBonus = cfg.TruncationLengthBonusMax
			}
			effectiveSupport += lengthBonus
		}
	}

	result.MinReports = minReports
	result.MinAdvantage = minAdvantage
	result.MinConfidence = minConf
	result.LengthBonus = lengthBonus
	effectiveWinnerSupportForAdvantage := winnerSupport

	if cfg.ResolverRecentPlus1Enabled && effectiveSupport == minReports-1 {
		result.RecentPlus1Considered = true
		rejectReason := ""
		if options.RecentPlus1DisallowReason != "" {
			rejectReason = options.RecentPlus1DisallowReason
		}
		distanceAllowed := distance <= cfg.ResolverRecentPlus1MaxDistance
		familyAllowed := cfg.ResolverRecentPlus1AllowTruncation && truncationRelation
		if rejectReason == "" && !distanceAllowed && !familyAllowed {
			rejectReason = "distance_or_family"
		}
		winnerRecent := resolverCallRecentSupport(winnerIdentity, winnerCall, subjectBand, subjectMode, cfg, now)
		subjectRecent := resolverCallRecentSupport(subjectIdentity, subjectCall, subjectBand, subjectMode, cfg, now)
		result.RecentPlus1Winner = winnerRecent
		result.RecentPlus1Subject = subjectRecent
		if rejectReason == "" && winnerRecent < cfg.ResolverRecentPlus1MinUniqueWinner {
			rejectReason = "winner_recent_insufficient"
		}
		if rejectReason == "" && cfg.ResolverRecentPlus1RequireSubjectWeaker && winnerRecent <= subjectRecent {
			rejectReason = "subject_not_weaker"
		}
		if rejectReason == "" {
			effectiveSupport++
			result.RecentPlus1Applied = true
		} else {
			result.RecentPlus1Reject = rejectReason
		}
	}

	bayes := normalizeCorrectionBayesBonusPolicy(cfg.BayesBonusPolicy)
	if bayes.Enabled && distance >= 1 && distance <= 2 {
		weightDistance := bayes.WeightDistance1Milli
		reportThreshold := bayes.ReportThresholdDistance1Milli
		advantageThreshold := bayes.AdvantageThresholdDistance1Milli
		minWeightedDelta := bayes.AdvantageMinWeightedDeltaDistance1Milli
		extraConfidence := bayes.AdvantageExtraConfidenceDistance1
		if distance == 2 {
			weightDistance = bayes.WeightDistance2Milli
			reportThreshold = bayes.ReportThresholdDistance2Milli
			advantageThreshold = bayes.AdvantageThresholdDistance2Milli
			minWeightedDelta = bayes.AdvantageMinWeightedDeltaDistance2Milli
			extraConfidence = bayes.AdvantageExtraConfidenceDistance2
		}

		winnerRecent := resolverCallRecentSupport(winnerIdentity, winnerCall, subjectBand, subjectMode, cfg, now)
		subjectRecent := resolverCallRecentSupport(subjectIdentity, subjectCall, subjectBand, subjectMode, cfg, now)
		result.BayesWinnerRecent = winnerRecent
		result.BayesSubjectRecent = subjectRecent
		result.BayesDistanceWeightMilli = weightDistance

		weightedNumerator := winnerWeightedSupportMilli + bayes.WeightedSmoothingMilli
		weightedDenominator := subjectWeightedSupportMilli + bayes.WeightedSmoothingMilli
		obsTermMilli := logRatioMilli(weightedNumerator, weightedDenominator)
		obsTermMilli = clampInt(obsTermMilli, -bayes.ObsLogCapMilli, bayes.ObsLogCapMilli)

		recentNumerator := winnerRecent + bayes.RecentSmoothing
		recentDenominator := subjectRecent + bayes.RecentSmoothing
		priorTermMilli := logRatioMilli(recentNumerator, recentDenominator)
		priorTermMilli = clampInt(priorTermMilli, bayes.PriorLogMinMilli, bayes.PriorLogMaxMilli)

		bayesScoreMilli := obsTermMilli + int(math.Round(float64(weightDistance*priorTermMilli)/1000.0))
		result.BayesScoreMilli = bayesScoreMilli

		if effectiveSupport == minReports-1 {
			result.BayesReportBonusConsidered = true
			rejectReason := ""
			if options.RecentPlus1DisallowReason != "" {
				rejectReason = options.RecentPlus1DisallowReason
			}
			if rejectReason == "" && winnerRecent < cfg.ResolverRecentPlus1MinUniqueWinner {
				rejectReason = "winner_recent_insufficient"
			}
			if rejectReason == "" && winnerRecent <= subjectRecent {
				rejectReason = "subject_not_weaker"
			}
			if rejectReason == "" && bayes.RequireCandidateValidated && !resolveCandidateValidated() {
				rejectReason = "candidate_unvalidated"
			}
			if rejectReason == "" && bayesScoreMilli < reportThreshold {
				rejectReason = "score_below_threshold"
			}
			if rejectReason == "" {
				effectiveSupport++
				result.BayesReportBonusApplied = true
			} else {
				result.BayesReportBonusReject = rejectReason
			}
		}

		if result.BayesReportBonusApplied && winnerSupport == subjectSupport {
			result.BayesAdvantageConsidered = true
			rejectReason := ""
			if bayesScoreMilli < advantageThreshold {
				rejectReason = "score_below_threshold"
			}
			if rejectReason == "" && (winnerWeightedSupportMilli-subjectWeightedSupportMilli) < minWeightedDelta {
				rejectReason = "weighted_delta_insufficient"
			}
			if rejectReason == "" && winnerConfidence < minConf+extraConfidence {
				rejectReason = "confidence_insufficient"
			}
			if rejectReason == "" && bayes.RequireCandidateValidated && !resolveCandidateValidated() {
				rejectReason = "candidate_unvalidated"
			}
			if rejectReason == "" && distance == 2 && bayes.RequireSubjectUnvalidatedDistance2 && resolveSubjectValidated() {
				rejectReason = "subject_validated"
			}
			if rejectReason == "" {
				result.BayesAdvantageApplied = true
				effectiveWinnerSupportForAdvantage++
			} else {
				result.BayesAdvantageReject = rejectReason
			}
		}
	}
	result.EffectiveSupport = effectiveSupport
	result.EffectiveAdvantageSupport = effectiveWinnerSupportForAdvantage

	if effectiveSupport < minReports {
		result.Reason = "min_reports"
		return result
	}
	if effectiveWinnerSupportForAdvantage < subjectSupport+minAdvantage {
		result.Reason = "advantage"
		return result
	}
	if winnerConfidence < minConf {
		result.Reason = "confidence"
		return result
	}

	result.Allow = true
	return result
}

func resolverCallValidated(identity correctionCallIdentity, displayCall, subjectBand, subjectMode string, cfg CorrectionSettings, now time.Time) bool {
	if cfg.KnownCallset != nil {
		candidates := []string{identity.Raw, identity.VoteKey, identity.BaseKey, displayCall}
		for _, call := range candidates {
			call = strings.TrimSpace(call)
			if call == "" {
				continue
			}
			if cfg.KnownCallset.Contains(call) {
				return true
			}
		}
	}
	minUnique := cfg.RecentBandRecordMinUniqueSpotters
	if minUnique <= 0 {
		minUnique = 2
	}
	return resolverCallRecentSupport(identity, displayCall, subjectBand, subjectMode, cfg, now) >= minUnique
}

func resolverCallRecentSupport(identity correctionCallIdentity, displayCall, subjectBand, subjectMode string, cfg CorrectionSettings, now time.Time) int {
	if cfg.RecentBandStore == nil {
		return 0
	}
	candidates := []string{identity.Raw, identity.VoteKey, identity.BaseKey, displayCall}
	seenCalls := make(map[string]struct{}, len(candidates))
	maxSupport := 0
	for _, candidateCall := range candidates {
		candidateCall = strings.TrimSpace(candidateCall)
		if candidateCall == "" {
			continue
		}
		upper := strutil.NormalizeUpper(candidateCall)
		if _, exists := seenCalls[upper]; exists {
			continue
		}
		seenCalls[upper] = struct{}{}
		support := cfg.RecentBandStore.RecentSupportCount(upper, subjectBand, subjectMode, now)
		if support > maxSupport {
			maxSupport = support
		}
	}
	return maxSupport
}

type correctionCallIdentity struct {
	Raw      string
	VoteKey  string
	BaseKey  string
	HasSlash bool
	SlashKey string
}

// CorrectionFamilyKind classifies call-pair relations used by resolver rails.
type CorrectionFamilyKind string

const (
	CorrectionFamilySlash      CorrectionFamilyKind = "slash"
	CorrectionFamilyTruncation CorrectionFamilyKind = "truncation"
)

// CorrectionFamilyRelation captures a directed relation where MoreSpecific can
// suppress LessSpecific in a family.
type CorrectionFamilyRelation struct {
	Kind         CorrectionFamilyKind
	LessSpecific string
	MoreSpecific string
}

// CorrectionVoteKey returns the normalized correction vote key for a callsign.
func CorrectionVoteKey(call string) string {
	return normalizeCorrectionCallIdentity(call).VoteKey
}

// CorrectionFamilyKeys returns deterministic keys for family-aware lookups.
func CorrectionFamilyKeys(call string) []string {
	identity := normalizeCorrectionCallIdentity(call)
	if identity.VoteKey == "" {
		return nil
	}
	keys := make([]string, 0, 2)
	keys = append(keys, identity.VoteKey)
	if identity.BaseKey != "" && identity.BaseKey != identity.VoteKey {
		keys = append(keys, identity.BaseKey)
	}
	return keys
}

// DetectCorrectionFamily reports whether two calls are in the same correction family.
func DetectCorrectionFamily(callA, callB string) (CorrectionFamilyRelation, bool) {
	return DetectCorrectionFamilyWithPolicy(callA, callB, CorrectionFamilyPolicy{})
}

// DetectCorrectionFamilyWithPolicy reports whether two calls are in the same correction family
// under the provided policy.
func DetectCorrectionFamilyWithPolicy(callA, callB string, policy CorrectionFamilyPolicy) (CorrectionFamilyRelation, bool) {
	a := normalizeCorrectionCallIdentity(callA)
	b := normalizeCorrectionCallIdentity(callB)
	return detectCorrectionFamilyByIdentity(a, b, normalizeCorrectionFamilyPolicy(policy))
}

func detectCorrectionFamilyByIdentity(a, b correctionCallIdentity, policy CorrectionFamilyPolicy) (CorrectionFamilyRelation, bool) {
	keyA := a.VoteKey
	if keyA == "" {
		keyA = a.Raw
	}
	keyB := b.VoteKey
	if keyB == "" {
		keyB = b.Raw
	}
	if keyA == "" || keyB == "" || keyA == keyB {
		return CorrectionFamilyRelation{}, false
	}
	if a.BaseKey != "" && a.BaseKey == b.BaseKey && a.HasSlash != b.HasSlash {
		if a.HasSlash {
			return CorrectionFamilyRelation{
				Kind:         CorrectionFamilySlash,
				LessSpecific: keyB,
				MoreSpecific: keyA,
			}, true
		}
		return CorrectionFamilyRelation{
			Kind:         CorrectionFamilySlash,
			LessSpecific: keyA,
			MoreSpecific: keyB,
		}, true
	}
	if a.HasSlash || b.HasSlash {
		return CorrectionFamilyRelation{}, false
	}
	if !policy.TruncationEnabled {
		return CorrectionFamilyRelation{}, false
	}
	shorter, longer := keyA, keyB
	if len(shorter) > len(longer) {
		shorter, longer = longer, shorter
	}
	if len(shorter) < policy.TruncationMinShorterLength {
		return CorrectionFamilyRelation{}, false
	}
	if len(longer)-len(shorter) > policy.TruncationMaxLengthDelta {
		return CorrectionFamilyRelation{}, false
	}
	prefixMatch := policy.TruncationAllowPrefix && strings.HasPrefix(longer, shorter)
	suffixMatch := policy.TruncationAllowSuffix && strings.HasSuffix(longer, shorter)
	if prefixMatch || suffixMatch {
		return CorrectionFamilyRelation{
			Kind:         CorrectionFamilyTruncation,
			LessSpecific: shorter,
			MoreSpecific: longer,
		}, true
	}
	return CorrectionFamilyRelation{}, false
}

// normalizeCorrectionCallIdentity derives correction identity keys.
// VoteKey groups semantically equivalent slash variants (e.g. KH6/W1AW == W1AW/KH6).
func normalizeCorrectionCallIdentity(call string) correctionCallIdentity {
	raw := strutil.NormalizeUpper(call)
	if raw == "" {
		return correctionCallIdentity{}
	}
	segments := strings.Split(raw, "/")
	parts := make([]string, 0, len(segments))
	for _, seg := range segments {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		parts = append(parts, seg)
	}
	if len(parts) == 0 {
		return correctionCallIdentity{}
	}
	if len(parts) == 1 {
		return correctionCallIdentity{
			Raw:      raw,
			VoteKey:  parts[0],
			BaseKey:  parts[0],
			HasSlash: false,
		}
	}
	baseIdx := selectCorrectionBaseSegment(parts)
	base := parts[baseIdx]
	regionParts := make([]string, 0, len(parts)-1)
	for i, seg := range parts {
		if i == baseIdx {
			continue
		}
		regionParts = append(regionParts, seg)
	}
	if len(regionParts) == 0 {
		return correctionCallIdentity{
			Raw:      raw,
			VoteKey:  base,
			BaseKey:  base,
			HasSlash: false,
		}
	}
	sort.Strings(regionParts)
	regional := strings.Join(regionParts, "/")
	voteKey := base + "/" + regional
	return correctionCallIdentity{
		Raw:      raw,
		VoteKey:  voteKey,
		BaseKey:  base,
		HasSlash: true,
		SlashKey: voteKey,
	}
}

func selectCorrectionBaseSegment(parts []string) int {
	bestIdx := 0
	bestScore := correctionSegmentScore(parts[0])
	for i := 1; i < len(parts); i++ {
		score := correctionSegmentScore(parts[i])
		if score > bestScore {
			bestIdx = i
			bestScore = score
			continue
		}
		if score < bestScore {
			continue
		}
		if len(parts[i]) > len(parts[bestIdx]) {
			bestIdx = i
			continue
		}
		if len(parts[i]) < len(parts[bestIdx]) {
			continue
		}
		if parts[i] < parts[bestIdx] {
			bestIdx = i
		}
	}
	return bestIdx
}

func correctionSegmentScore(seg string) int {
	if seg == "" {
		return -1000
	}
	score := 0
	if validateNormalizedCallsign(seg) {
		score += 100
	}
	hasAlpha := false
	hasDigit := false
	onlyDigits := true
	for i := 0; i < len(seg); i++ {
		ch := seg[i]
		if ch >= 'A' && ch <= 'Z' {
			hasAlpha = true
			onlyDigits = false
			continue
		}
		if ch >= '0' && ch <= '9' {
			hasDigit = true
			continue
		}
		onlyDigits = false
	}
	if hasAlpha && hasDigit {
		score += 40
	}
	if len(seg) >= 4 {
		score += 20
	}
	if onlyDigits {
		score -= 50
	}
	score += len(seg)
	return score
}

func correctionDistance(subject, candidate correctionCallIdentity, mode, cwModel, rttyModel string) int {
	if subject.BaseKey != "" && subject.BaseKey == candidate.BaseKey {
		if !subject.HasSlash || !candidate.HasSlash {
			return 0
		}
		if subject.SlashKey != "" && subject.SlashKey == candidate.SlashKey {
			return 0
		}
	}
	subjectKey := subject.VoteKey
	if subjectKey == "" {
		subjectKey = subject.Raw
	}
	candidateKey := candidate.VoteKey
	if candidateKey == "" {
		candidateKey = candidate.Raw
	}
	return callDistance(subjectKey, candidateKey, mode, cwModel, rttyModel)
}

func normalizeCorrectionFamilyPolicy(policy CorrectionFamilyPolicy) CorrectionFamilyPolicy {
	cfg := policy
	if !cfg.Configured {
		cfg.TruncationEnabled = true
		cfg.TruncationMaxLengthDelta = 1
		cfg.TruncationMinShorterLength = 3
		cfg.TruncationAllowPrefix = true
		cfg.TruncationAllowSuffix = true
		return cfg
	}
	if cfg.TruncationMaxLengthDelta <= 0 {
		cfg.TruncationMaxLengthDelta = 1
	}
	if cfg.TruncationMinShorterLength <= 0 {
		cfg.TruncationMinShorterLength = 3
	}
	if !cfg.TruncationAllowPrefix && !cfg.TruncationAllowSuffix {
		cfg.TruncationEnabled = false
	}
	return cfg
}

func normalizeCorrectionTruncationAdvantagePolicy(policy CorrectionTruncationAdvantagePolicy) CorrectionTruncationAdvantagePolicy {
	cfg := policy
	if !cfg.Configured {
		cfg.Enabled = true
		cfg.MinAdvantage = 0
		cfg.RequireCandidateValidated = true
		cfg.RequireSubjectUnvalidated = true
		return cfg
	}
	if cfg.MinAdvantage < 0 {
		cfg.MinAdvantage = 0
	}
	return cfg
}

func normalizeCorrectionBayesBonusPolicy(policy CorrectionBayesBonusPolicy) CorrectionBayesBonusPolicy {
	cfg := policy
	if !cfg.Configured {
		cfg.WeightDistance1Milli = 350
		cfg.WeightDistance2Milli = 200
		cfg.WeightedSmoothingMilli = 1000
		cfg.RecentSmoothing = 2
		cfg.ObsLogCapMilli = 350
		cfg.PriorLogMinMilli = -200
		cfg.PriorLogMaxMilli = 600
		cfg.ReportThresholdDistance1Milli = 450
		cfg.ReportThresholdDistance2Milli = 650
		cfg.AdvantageThresholdDistance1Milli = 700
		cfg.AdvantageThresholdDistance2Milli = 950
		cfg.AdvantageMinWeightedDeltaDistance1Milli = 200
		cfg.AdvantageMinWeightedDeltaDistance2Milli = 300
		cfg.AdvantageExtraConfidenceDistance1 = 3
		cfg.AdvantageExtraConfidenceDistance2 = 5
		cfg.RequireCandidateValidated = true
		cfg.RequireSubjectUnvalidatedDistance2 = true
		return cfg
	}
	if cfg.WeightDistance1Milli < 0 {
		cfg.WeightDistance1Milli = 0
	}
	if cfg.WeightDistance2Milli < 0 {
		cfg.WeightDistance2Milli = 0
	}
	if cfg.WeightDistance1Milli == 0 {
		cfg.WeightDistance1Milli = 350
	}
	if cfg.WeightDistance2Milli == 0 {
		cfg.WeightDistance2Milli = 200
	}
	if cfg.WeightDistance1Milli > 1000 {
		cfg.WeightDistance1Milli = 1000
	}
	if cfg.WeightDistance2Milli > 1000 {
		cfg.WeightDistance2Milli = 1000
	}
	if cfg.WeightedSmoothingMilli <= 0 {
		cfg.WeightedSmoothingMilli = 1000
	}
	if cfg.RecentSmoothing <= 0 {
		cfg.RecentSmoothing = 2
	}
	if cfg.ObsLogCapMilli <= 0 {
		cfg.ObsLogCapMilli = 350
	}
	if cfg.PriorLogMinMilli == 0 {
		cfg.PriorLogMinMilli = -200
	}
	if cfg.PriorLogMaxMilli == 0 {
		cfg.PriorLogMaxMilli = 600
	}
	if cfg.PriorLogMinMilli >= cfg.PriorLogMaxMilli {
		cfg.PriorLogMinMilli = -200
		cfg.PriorLogMaxMilli = 600
	}
	if cfg.ReportThresholdDistance1Milli <= 0 {
		cfg.ReportThresholdDistance1Milli = 450
	}
	if cfg.ReportThresholdDistance2Milli <= 0 {
		cfg.ReportThresholdDistance2Milli = 650
	}
	if cfg.ReportThresholdDistance2Milli < cfg.ReportThresholdDistance1Milli {
		cfg.ReportThresholdDistance2Milli = cfg.ReportThresholdDistance1Milli
	}
	if cfg.AdvantageThresholdDistance1Milli <= 0 {
		cfg.AdvantageThresholdDistance1Milli = 700
	}
	if cfg.AdvantageThresholdDistance2Milli <= 0 {
		cfg.AdvantageThresholdDistance2Milli = 950
	}
	if cfg.AdvantageThresholdDistance2Milli < cfg.AdvantageThresholdDistance1Milli {
		cfg.AdvantageThresholdDistance2Milli = cfg.AdvantageThresholdDistance1Milli
	}
	if cfg.AdvantageMinWeightedDeltaDistance1Milli <= 0 {
		cfg.AdvantageMinWeightedDeltaDistance1Milli = 200
	}
	if cfg.AdvantageMinWeightedDeltaDistance2Milli <= 0 {
		cfg.AdvantageMinWeightedDeltaDistance2Milli = 300
	}
	if cfg.AdvantageMinWeightedDeltaDistance2Milli < cfg.AdvantageMinWeightedDeltaDistance1Milli {
		cfg.AdvantageMinWeightedDeltaDistance2Milli = cfg.AdvantageMinWeightedDeltaDistance1Milli
	}
	if cfg.AdvantageExtraConfidenceDistance1 < 0 {
		cfg.AdvantageExtraConfidenceDistance1 = 0
	}
	if cfg.AdvantageExtraConfidenceDistance2 < 0 {
		cfg.AdvantageExtraConfidenceDistance2 = 0
	}
	if cfg.AdvantageExtraConfidenceDistance1 == 0 {
		cfg.AdvantageExtraConfidenceDistance1 = 3
	}
	if cfg.AdvantageExtraConfidenceDistance2 == 0 {
		cfg.AdvantageExtraConfidenceDistance2 = 5
	}
	if cfg.AdvantageExtraConfidenceDistance2 < cfg.AdvantageExtraConfidenceDistance1 {
		cfg.AdvantageExtraConfidenceDistance2 = cfg.AdvantageExtraConfidenceDistance1
	}
	return cfg
}

const (
	distanceModelPlain  = "plain"
	distanceModelMorse  = "morse"
	distanceModelBaudot = "baudot"

	ambiguousMultiSignalMinSupport      = 2
	ambiguousMultiSignalMaxSupportGap   = 1
	ambiguousMultiSignalMaxOverlapRatio = 0.25

	defaultDistanceInsertCost = 1
	defaultDistanceDeleteCost = 1
	defaultDistanceSubCost    = 2
	defaultDistanceScale      = 2
)

func normalizeCWDistanceModel(model string) string {
	switch strings.ToLower(strings.TrimSpace(model)) {
	case distanceModelMorse:
		return distanceModelMorse
	default:
		return distanceModelPlain
	}
}

func normalizeRTTYDistanceModel(model string) string {
	switch strings.ToLower(strings.TrimSpace(model)) {
	case distanceModelBaudot:
		return distanceModelBaudot
	default:
		return distanceModelPlain
	}
}

func callDistanceCore(subject, candidate, mode, cwModel, rttyModel string) int {
	switch mode {
	case "CW":
		if cwModel == distanceModelMorse {
			return cwCallDistance(subject, candidate)
		}
	case "RTTY":
		if rttyModel == distanceModelBaudot {
			return rttyCallDistance(subject, candidate)
		}
	}
	return lev.ComputeDistance(subject, candidate)
}

func callDistance(subject, candidate, mode, cwModel, rttyModel string) int {
	modeKey := strutil.NormalizeUpper(mode)
	return callDistanceCore(
		subject,
		candidate,
		modeKey,
		normalizeCWDistanceModel(cwModel),
		normalizeRTTYDistanceModel(rttyModel),
	)
}

// CallDistance computes mode-aware call distance with the same semantics used
// by resolver primary gating.
func CallDistance(subject, candidate, mode, cwModel, rttyModel string) int {
	return callDistance(subject, candidate, mode, cwModel, rttyModel)
}

// IsEditNeighborPair reports whether calls are distance-1 neighbors under the
// mode-aware distance model. Slash variants are excluded.
func IsEditNeighborPair(left, right, mode, cwModel, rttyModel string) bool {
	left = CorrectionVoteKey(left)
	right = CorrectionVoteKey(right)
	if left == "" || right == "" || strings.EqualFold(left, right) {
		return false
	}
	if strings.Contains(left, "/") || strings.Contains(right, "/") {
		return false
	}
	return CallDistance(left, right, mode, cwModel, rttyModel) == 1
}

// ResolverSnapshotHasComparableEditNeighbor reports whether snapshot evidence
// contains an edit-neighbor candidate with support comparable to call.
func ResolverSnapshotHasComparableEditNeighbor(snapshot ResolverSnapshot, call, mode, cwModel, rttyModel string) bool {
	call = CorrectionVoteKey(call)
	if call == "" || len(snapshot.CandidateRanks) == 0 {
		return false
	}
	callSupport := 0
	for _, candidate := range snapshot.CandidateRanks {
		candidateCall := CorrectionVoteKey(candidate.Call)
		if strings.EqualFold(candidateCall, call) {
			callSupport = candidate.Support
			break
		}
	}
	for _, candidate := range snapshot.CandidateRanks {
		candidateCall := CorrectionVoteKey(candidate.Call)
		if candidateCall == "" || strings.EqualFold(candidateCall, call) {
			continue
		}
		if candidate.Support < callSupport {
			continue
		}
		if IsEditNeighborPair(call, candidateCall, mode, cwModel, rttyModel) {
			return true
		}
	}
	return false
}

// shouldRejectAsAmbiguousMultiSignal applies split-signal guard rails to two
// competing candidates in the same frequency neighborhood.
func shouldRejectAsAmbiguousMultiSignal(
	winnerSupport int,
	runnerSupport int,
	winnerFreqKHz float64,
	runnerFreqKHz float64,
	minSeparationKHz float64,
	runnerUpRatio float64,
	overlapCount int,
	distance int,
	maxEditDistance int,
	related bool,
) bool {
	if winnerSupport <= 0 || runnerSupport <= 0 {
		return false
	}
	if winnerSupport <= runnerSupport {
		return false
	}
	if winnerSupport < ambiguousMultiSignalMinSupport || runnerSupport < ambiguousMultiSignalMinSupport {
		return false
	}
	if winnerSupport-runnerSupport > ambiguousMultiSignalMaxSupportGap {
		return false
	}
	if math.Abs(winnerFreqKHz-runnerFreqKHz) >= minSeparationKHz {
		return false
	}
	if maxEditDistance >= 0 && distance > maxEditDistance {
		return false
	}
	if related {
		return false
	}
	minRunnerSupport := int(math.Ceil(runnerUpRatio * float64(winnerSupport)))
	if minRunnerSupport < ambiguousMultiSignalMinSupport {
		minRunnerSupport = ambiguousMultiSignalMinSupport
	}
	if runnerSupport < minRunnerSupport {
		return false
	}
	minSupport := winnerSupport
	if runnerSupport < minSupport {
		minSupport = runnerSupport
	}
	if minSupport <= 0 {
		return false
	}
	overlapRatio := float64(overlapCount) / float64(minSupport)
	return overlapRatio <= ambiguousMultiSignalMaxOverlapRatio
}

// ConfigureMorseWeights applies Morse distance weights and rebuilds lookup tables.
func ConfigureMorseWeights(insert, delete, sub, scale int) {
	morseInsertCost, morseDeleteCost, morseSubCost, morseScale = normalizeDistanceWeights(insert, delete, sub, scale)
	morseRuneIndex, morseCostTable = buildRuneCostTable(morseCodes, morsePatternCost)
}

// ConfigureBaudotWeights applies Baudot distance weights and rebuilds lookup tables.
func ConfigureBaudotWeights(insert, delete, sub, scale int) {
	baudotInsertCost, baudotDeleteCost, baudotSubCost, baudotScale = normalizeDistanceWeights(insert, delete, sub, scale)
	baudotRuneIndex, baudotCostTable = buildRuneCostTable(baudotCodes, baudotPatternCost)
}

func normalizeDistanceWeights(insert, delete, sub, scale int) (int, int, int, int) {
	if insert <= 0 {
		insert = defaultDistanceInsertCost
	}
	if delete <= 0 {
		delete = defaultDistanceDeleteCost
	}
	if sub <= 0 {
		sub = defaultDistanceSubCost
	}
	if scale <= 0 {
		scale = defaultDistanceScale
	}
	return insert, delete, sub, scale
}

func cwCallDistance(a, b string) int {
	return weightedCallDistance(a, b, morseRuneIndex, morseCostTable)
}

func rttyCallDistance(a, b string) int {
	return weightedCallDistance(a, b, baudotRuneIndex, baudotCostTable)
}

func weightedCallDistance(a, b string, runeIndex map[rune]int, costTable [][]int) int {
	ra := []rune(strings.ToUpper(a))
	rb := []rune(strings.ToUpper(b))
	la := len(ra)
	lb := len(rb)

	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	prev, prevPool := borrowIntSlice(lb + 1)
	cur, curPool := borrowIntSlice(lb + 1)
	defer returnIntSlice(prev, prevPool)
	defer returnIntSlice(cur, curPool)

	for j := 0; j <= lb; j++ {
		prev[j] = j
	}

	for i := 1; i <= la; i++ {
		cur[0] = i
		for j := 1; j <= lb; j++ {
			insert := cur[j-1] + 1
			delete := prev[j] + 1
			replace := prev[j-1] + weightedRuneDist(ra[i-1], rb[j-1], runeIndex, costTable)
			cur[j] = min3(insert, delete, replace)
		}
		prev, cur = cur, prev
	}

	return prev[lb]
}

func weightedRuneDist(a, b rune, runeIndex map[rune]int, costTable [][]int) int {
	if a == b {
		return 0
	}
	if i, ok := runeIndex[a]; ok {
		if j, ok := runeIndex[b]; ok {
			return costTable[i][j]
		}
	}
	return defaultDistanceSubCost
}

func min3(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

func clampInt(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func logRatioMilli(numerator, denominator int) int {
	if numerator <= 0 || denominator <= 0 {
		return 0
	}
	return int(math.Round(1000.0 * math.Log(float64(numerator)/float64(denominator))))
}

func borrowIntSlice(n int) ([]int, bool) {
	if n <= 0 {
		return nil, false
	}
	if n <= 64 {
		if pooled, ok := levBufPool.Get().(*[]int); ok {
			buf := *pooled
			if cap(buf) >= 64 {
				return buf[:n], true
			}
		}
		return make([]int, n), false
	}
	return make([]int, n), false
}

func returnIntSlice(buf []int, fromPool bool) {
	if !fromPool || buf == nil {
		return
	}
	if cap(buf) >= 64 {
		resized := buf[:64]
		levBufPool.Put(&resized)
	}
}

var morseCodes = map[rune]string{
	'A': ".-",
	'B': "-...",
	'C': "-.-.",
	'D': "-..",
	'E': ".",
	'F': "..-.",
	'G': "--.",
	'H': "....",
	'I': "..",
	'J': ".---",
	'K': "-.-",
	'L': ".-..",
	'M': "--",
	'N': "-.",
	'O': "---",
	'P': ".--.",
	'Q': "--.-",
	'R': ".-.",
	'S': "...",
	'T': "-",
	'U': "..-",
	'V': "...-",
	'W': ".--",
	'X': "-..-",
	'Y': "-.--",
	'Z': "--..",
	'0': "-----",
	'1': ".----",
	'2': "..---",
	'3': "...--",
	'4': "....-",
	'5': ".....",
	'6': "-....",
	'7': "--...",
	'8': "---..",
	'9': "----.",
	'/': "-..-.",
}

var (
	morseRuneIndex map[rune]int
	morseCostTable [][]int

	morseInsertCost = defaultDistanceInsertCost
	morseDeleteCost = defaultDistanceDeleteCost
	morseSubCost    = defaultDistanceSubCost
	morseScale      = defaultDistanceScale

	baudotInsertCost = defaultDistanceInsertCost
	baudotDeleteCost = defaultDistanceDeleteCost
	baudotSubCost    = defaultDistanceSubCost
	baudotScale      = defaultDistanceScale

	levBufPool = sync.Pool{
		New: func() interface{} {
			buf := make([]int, 64)
			return &buf
		},
	}
)

var baudotCodes = map[rune]string{
	'A': "L00011",
	'B': "L11001",
	'C': "L01110",
	'D': "L01001",
	'E': "L00001",
	'F': "L01101",
	'G': "L11010",
	'H': "L10100",
	'I': "L00110",
	'J': "L01011",
	'K': "L01111",
	'L': "L10010",
	'M': "L11100",
	'N': "L01100",
	'O': "L11000",
	'P': "L10110",
	'Q': "L10111",
	'R': "L01010",
	'S': "L00101",
	'T': "L10000",
	'U': "L00111",
	'V': "L11110",
	'W': "L10011",
	'X': "L11101",
	'Y': "L10101",
	'Z': "L10001",
	'0': "F10110",
	'1': "F10111",
	'2': "F10011",
	'3': "F00001",
	'4': "F01010",
	'5': "F10000",
	'6': "F10101",
	'7': "F00111",
	'8': "F00110",
	'9': "F11000",
	'/': "F11101",
}

var (
	baudotRuneIndex map[rune]int
	baudotCostTable [][]int
)

func init() {
	morseRuneIndex, morseCostTable = buildRuneCostTable(morseCodes, morsePatternCost)
	baudotRuneIndex, baudotCostTable = buildRuneCostTable(baudotCodes, baudotPatternCost)
}

func buildRuneCostTable(codebook map[rune]string, cost func(a, b string) int) (map[rune]int, [][]int) {
	index := make(map[rune]int, len(codebook))
	keys := make([]rune, 0, len(codebook))
	for r := range codebook {
		index[r] = len(keys)
		keys = append(keys, r)
	}
	size := len(keys)
	table := make([][]int, size)
	for i := range table {
		table[i] = make([]int, size)
	}
	for i, ra := range keys {
		for j, rb := range keys {
			if ra == rb {
				table[i][j] = 0
				continue
			}
			table[i][j] = cost(codebook[ra], codebook[rb])
		}
	}
	return index, table
}

func morsePatternCost(a, b string) int {
	return weightedPatternCost(a, b, getMorseWeights().distanceWeightSet)
}

type distanceWeightSet struct {
	ins   int
	del   int
	sub   int
	scale int
}

type morseWeightSet struct {
	distanceWeightSet
}

func getMorseWeights() morseWeightSet {
	return morseWeightSet{
		distanceWeightSet: distanceWeightSet{
			ins:   morseInsertCost,
			del:   morseDeleteCost,
			sub:   morseSubCost,
			scale: morseScale,
		},
	}
}

func baudotPatternCost(a, b string) int {
	return weightedPatternCost(a, b, distanceWeightSet{
		ins:   baudotInsertCost,
		del:   baudotDeleteCost,
		sub:   baudotSubCost,
		scale: baudotScale,
	})
}

func weightedPatternCost(a, b string, cfg distanceWeightSet) int {
	if a == b {
		return 0
	}
	ra := []rune(a)
	rb := []rune(b)
	la := len(ra)
	lb := len(rb)
	if la == 0 {
		return cfg.ins
	}
	if lb == 0 {
		return cfg.ins
	}
	prev := make([]int, lb+1)
	cur := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j * cfg.ins
	}
	for i := 1; i <= la; i++ {
		cur[0] = i * cfg.del
		for j := 1; j <= lb; j++ {
			subCost := 0
			if ra[i-1] != rb[j-1] {
				subCost = cfg.sub
			}
			insert := cur[j-1] + cfg.ins
			delete := prev[j] + cfg.del
			replace := prev[j-1] + subCost
			cur[j] = min3(insert, delete, replace)
		}
		prev, cur = cur, prev
	}

	raw := prev[lb]
	maxLen := la
	if lb > maxLen {
		maxLen = lb
	}
	scale := cfg.scale
	if scale <= 0 {
		scale = defaultDistanceScale
	}
	normalized := float64(raw) / float64(maxLen+1)
	scaled := int(math.Ceil(normalized * float64(scale)))
	if scaled < 1 && raw > 0 {
		scaled = 1
	}
	return scaled
}
