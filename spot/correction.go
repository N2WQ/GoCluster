package spot

import (
	"dxcluster/bandmap"
	"dxcluster/strutil"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	lev "github.com/agnivade/levenshtein"
)

// CorrectionSettings captures the knobs that govern whether a consensus-based
// call correction should happen. The values ultimately come from data/config/pipeline.yaml,
// but the struct is deliberately defined here so the algorithm can be unit-tested
// without importing the config package (which would create a cycle).
type CorrectionSettings struct {
	// Strategy controls how consensus is computed:
	//   - "center": pick a cluster center (median-like) and compare to subject
	//   - "classic": subject vs. alternate comparisons (legacy behavior)
	//   - "majority": most-reported call on-frequency (unique spotters), distance as safety cap only
	Strategy string
	// DebugLog enables per-subject diagnostic logging of decisions.
	DebugLog bool
	// TraceLogger records decision traces asynchronously when DebugLog is true.
	TraceLogger CorrectionTraceLogger
	// Frequency guard to avoid merging nearby strong signals.
	FreqGuardMinSeparationKHz float64
	FreqGuardRunnerUpRatio    float64
	// FrequencyToleranceHz defines how close two spots must be to be considered the same signal.
	FrequencyToleranceHz float64
	// Quality-based anchors (frequency-binned call scores).
	QualityBinHz            int
	QualityGoodThreshold    int
	QualityNewCallIncrement int
	QualityBustedDecrement  int
	// MinConsensusReports is the number of *other* unique spotters that must
	// agree on the same DX call before we consider overriding the subject spot.
	MinConsensusReports int
	// MinAdvantage is the minimum delta (candidate supporters - subject supporters)
	// required before a correction can happen.
	MinAdvantage int
	// MinConfidencePercent enforces that the alternate call must represent at least
	// this percentage of all unique spotters currently reporting that frequency.
	MinConfidencePercent int
	// RecencyWindow bounds how old the supporting spots can be. Anything older
	// than this duration is ignored so stale data never drives a correction.
	RecencyWindow time.Duration
	// MaxEditDistance bounds how different the alternate call can be compared to
	// the subject call. A value of 2 typically allows single-character typos.
	MaxEditDistance int

	// MinSNRCW/MinSNRRTTY/MinSNRVoice let callers ignore corroborators below a
	// minimum signal-to-noise ratio. FT8/FT4 aren't run through call correction.
	MinSNRCW   int
	MinSNRRTTY int
	// MinSNRVoice lets callers ignore low-SNR USB/LSB reports when present.
	MinSNRVoice int

	// DistanceModelCW/DistanceModelRTTY control mode-specific distance behavior.
	// Supported values:
	//   - "plain": rune-based Levenshtein
	//   - "morse": Morse-aware (CW only)
	//   - "baudot": Baudot-aware (RTTY only)
	DistanceModelCW   string
	DistanceModelRTTY string

	// Distance3Extra* tighten consensus requirements when the candidate callsign
	// is at edit distance 3 from the subject. They are additive to the base
	// thresholds above. Set them to zero to disable the stricter bar.
	Distance3ExtraReports    int
	Distance3ExtraAdvantage  int
	Distance3ExtraConfidence int
	// Spotter reliability weights (0..1). Reporters below MinSpotterReliability are ignored
	// when counting corroborators.
	SpotterReliability    SpotterReliability
	MinSpotterReliability float64
	// Cooldown protects a subject call from being flipped away when it already has
	// recent diverse support on this frequency bin.
	Cooldown *CallCooldown
	// CooldownMinReporters allows adaptive thresholds per band/state when provided.
	CooldownMinReporters int
}

var correctionEligibleModes = map[string]struct{}{
	"CW":   {},
	"RTTY": {},
	"USB":  {},
	"LSB":  {},
}

// CorrectionTrace captures the inputs and outcome of a correction decision for audit/comparison.
type CorrectionTrace struct {
	Timestamp                 time.Time `json:"ts"`
	Strategy                  string    `json:"strategy"`
	DecisionPath              string    `json:"decision_path,omitempty"`
	FrequencyKHz              float64   `json:"freq_khz"`
	SubjectCall               string    `json:"subject"`
	WinnerCall                string    `json:"winner"`
	Mode                      string    `json:"mode"`
	Source                    string    `json:"source"`
	TotalReporters            int       `json:"total_reporters"`
	SubjectSupport            int       `json:"subject_support"`
	WinnerSupport             int       `json:"winner_support"`
	RunnerUpSupport           int       `json:"runner_up_support"`
	SubjectConfidence         int       `json:"subject_confidence"`
	WinnerConfidence          int       `json:"winner_confidence"`
	Distance                  int       `json:"distance"`
	DistanceModel             string    `json:"distance_model"`
	MaxEditDistance           int       `json:"max_edit_distance"`
	MinReports                int       `json:"min_reports"`
	MinAdvantage              int       `json:"min_advantage"`
	MinConfidence             int       `json:"min_confidence"`
	Distance3ExtraReports     int       `json:"d3_extra_reports"`
	Distance3ExtraAdvantage   int       `json:"d3_extra_advantage"`
	Distance3ExtraConfidence  int       `json:"d3_extra_confidence"`
	FreqGuardMinSeparationKHz float64   `json:"freq_guard_min_separation_khz"`
	FreqGuardRunnerUpRatio    float64   `json:"freq_guard_runner_ratio"`
	Decision                  string    `json:"decision"`
	Reason                    string    `json:"reason,omitempty"`
}

// IsCallCorrectionCandidate reports whether a mode is eligible for call correction.
// Key aspects: Limits to CW/RTTY and USB/LSB voice modes to avoid digital-mode conflicts.
// Upstream: call correction pipeline and harmonic detection.
// Downstream: correctionEligibleModes lookup.
// IsCallCorrectionCandidate returns true if the given mode is eligible for
// consensus-based call correction. Only CW/RTTY and USB/LSB voice modes
// are considered because other digital modes already embed their own error correction.
func IsCallCorrectionCandidate(mode string) bool {
	_, ok := correctionEligibleModes[strutil.NormalizeUpper(mode)]
	return ok
}

// frequencyToleranceKHz defines how close two frequencies must be to be considered
// the "same" signal. We only care about agreement on essentially identical frequencies,
// so we allow a half-kilohertz wiggle room to absorb rounding differences between
// data sources.
var frequencyToleranceKHz = 0.5

// SetFrequencyToleranceHz sets the global frequency tolerance for correction clustering.
// Key aspects: Normalizes non-positive values to default.
// Upstream: main startup configuration.
// Downstream: frequencyToleranceKHz global.
// SetFrequencyToleranceHz updates the frequency similarity window in Hz.
func SetFrequencyToleranceHz(hz float64) {
	if hz <= 0 {
		frequencyToleranceKHz = 0.5
		return
	}
	frequencyToleranceKHz = hz / 1000.0
}

// ConfigureMorseWeights configures Morse edit distance weights.
// Key aspects: Applies defaults when non-positive and rebuilds cost table.
// Upstream: main startup configuration.
// Downstream: buildRuneCostTable and morse weight globals.
// ConfigureMorseWeights allows callers to tune dot/dash edit costs. Non-positive
// inputs fall back to defaults (ins=1, del=1, sub=2, scale=2).
func ConfigureMorseWeights(insert, delete, sub, scale int) {
	if insert > 0 {
		morseInsertCost = insert
	} else {
		morseInsertCost = 1
	}
	if delete > 0 {
		morseDeleteCost = delete
	} else {
		morseDeleteCost = 1
	}
	if sub > 0 {
		morseSubCost = sub
	} else {
		morseSubCost = 2
	}
	if scale > 0 {
		morseScale = scale
	} else {
		morseScale = 2
	}
	// Rebuild the Morse cost table with the new weights.
	morseRuneIndex, morseCostTable = buildRuneCostTable(morseCodes, morsePatternCost)
}

// ConfigureBaudotWeights configures Baudot edit distance weights for RTTY.
// Key aspects: Applies defaults when non-positive and rebuilds cost table.
// Upstream: main startup configuration.
// Downstream: buildRuneCostTable and baudot weight globals.
// ConfigureBaudotWeights allows callers to tune ITA2 edit costs for RTTY distance.
// Non-positive inputs fall back to defaults (ins=1, del=1, sub=2, scale=2).
func ConfigureBaudotWeights(insert, delete, sub, scale int) {
	if insert > 0 {
		baudotInsertCost = insert
	} else {
		baudotInsertCost = 1
	}
	if delete > 0 {
		baudotDeleteCost = delete
	} else {
		baudotDeleteCost = 1
	}
	if sub > 0 {
		baudotSubCost = sub
	} else {
		baudotSubCost = 2
	}
	if scale > 0 {
		baudotScale = scale
	} else {
		baudotScale = 2
	}
	baudotRuneIndex, baudotCostTable = buildRuneCostTable(baudotCodes, baudotPatternCost)
}

// SuggestCallCorrection analyzes recent spots on the same frequency and determines
// whether there is overwhelming evidence that the subject spot's DX call should
// be corrected. IMPORTANT: This function only suggests a correction. The caller
// (e.g., the main pipeline when call correction is enabled) decides whether to
// apply it and is responsible for updating any caches or deduplication structures.
//
// Parameters:
//   - subject: the spot we are evaluating.
//   - others: a slice of other recent spots (e.g., from a spatial index). Frequencies are in Hz.
//   - settings: consensus thresholds (min reporters, freshness).
//   - now: the time reference used to evaluate recency. Passing it as an argument
//     rather than calling time.Now() simplifies deterministic testing.
//
// Returns:
//   - correctedCall: the most likely callsign if consensus is met.
//   - supporters: how many unique spotters contributed to the correction.
//   - ok: true if a correction is recommended, false otherwise.
//
// Purpose: Suggest a corrected callsign based on nearby corroborators.
// Key aspects: Applies consensus strategy, distance limits, and confidence gates.
// Upstream: main call correction pipeline.
// Downstream: distance calculations, quality anchors, and logging.
func SuggestCallCorrection(subject *Spot, others []bandmap.SpotEntry, settings CorrectionSettings, now time.Time) (correctedCall string, supporters int, correctedConfidence int, subjectConfidence int, totalReporters int, ok bool) {
	if subject == nil {
		return "", 0, 0, 0, 0, false
	}

	cfg := normalizeCorrectionSettings(settings)
	subjectIdentity := normalizeCorrectionCallIdentity(subject.DXCall)
	if subjectIdentity.VoteKey == "" {
		return "", 0, 0, 0, 0, false
	}
	subjectReporter := strings.TrimSpace(subject.DECall)

	trace := CorrectionTrace{
		Timestamp:                 now,
		Strategy:                  strings.ToLower(strings.TrimSpace(cfg.Strategy)),
		FrequencyKHz:              subject.Frequency,
		SubjectCall:               subjectIdentity.Raw,
		Mode:                      strutil.NormalizeUpper(subject.Mode),
		Source:                    strutil.NormalizeUpper(subject.SourceNode),
		MaxEditDistance:           cfg.MaxEditDistance,
		MinReports:                cfg.MinConsensusReports,
		MinAdvantage:              cfg.MinAdvantage,
		MinConfidence:             cfg.MinConfidencePercent,
		Distance3ExtraReports:     cfg.Distance3ExtraReports,
		Distance3ExtraAdvantage:   cfg.Distance3ExtraAdvantage,
		Distance3ExtraConfidence:  cfg.Distance3ExtraConfidence,
		FreqGuardMinSeparationKHz: cfg.FreqGuardMinSeparationKHz,
		FreqGuardRunnerUpRatio:    cfg.FreqGuardRunnerUpRatio,
		DistanceModel:             distanceModelPlain,
	}
	switch trace.Mode {
	case "CW":
		trace.DistanceModel = normalizeCWDistanceModel(cfg.DistanceModelCW)
	case "RTTY":
		trace.DistanceModel = normalizeRTTYDistanceModel(cfg.DistanceModelRTTY)
	}
	allReporters := make(map[string]struct{}, len(others)+1)
	clusterSpots := make([]bandmap.SpotEntry, 0, len(others)+1)
	clusterSpots = append(clusterSpots, bandmap.SpotEntry{
		Call:    subjectIdentity.Raw,
		Spotter: subject.DECall,
		Mode:    subject.Mode,
		FreqHz:  uint32(subject.Frequency*1000 + 0.5),
		Time:    subject.Time.Unix(),
		SNR:     subject.Report,
	})

	type callAggregate struct {
		identity  correctionCallIdentity
		reporters map[string]struct{}
		// Keep per-variant reporter sets so display selection can preserve the
		// most credible slash form after canonical grouping.
		variantReporters map[string]map[string]struct{}
		variantLastSeen  map[string]time.Time
		lastSeen         time.Time
		lastFreq         float64
	}

	// Pre-size call stats map to avoid resize churn; expect up to len(others)+1 unique calls.
	callStats := make(map[string]*callAggregate, len(others)+1)
	ensureCallEntry := func(identity correctionCallIdentity) *callAggregate {
		entry, ok := callStats[identity.VoteKey]
		if !ok {
			entry = &callAggregate{
				identity:         identity,
				reporters:        make(map[string]struct{}, 4),
				variantReporters: make(map[string]map[string]struct{}, 2),
				variantLastSeen:  make(map[string]time.Time, 2),
			}
			callStats[identity.VoteKey] = entry
		}
		return entry
	}
	addReporter := func(identity correctionCallIdentity, reporter string, seenAt time.Time, freqKHz float64) {
		// Ignore reporters that fall below the configured reliability floor.
		if reliabilityFor(cfg.SpotterReliability, reporter) < cfg.MinSpotterReliability {
			return
		}
		entry := ensureCallEntry(identity)
		entry.reporters[reporter] = struct{}{}
		if seenAt.After(entry.lastSeen) {
			entry.lastSeen = seenAt
		}
		entry.lastFreq = freqKHz

		variant := identity.Raw
		if variant == "" {
			variant = identity.VoteKey
		}
		reporters := entry.variantReporters[variant]
		if reporters == nil {
			reporters = make(map[string]struct{}, 2)
			entry.variantReporters[variant] = reporters
		}
		reporters[reporter] = struct{}{}
		if seenAt.After(entry.variantLastSeen[variant]) {
			entry.variantLastSeen[variant] = seenAt
		}
	}
	displayForKey := func(key string) string {
		entry := callStats[key]
		if entry == nil {
			return key
		}
		best := ""
		bestSupport := -1
		var bestSeen time.Time
		for variant, reporters := range entry.variantReporters {
			support := len(reporters)
			seenAt := entry.variantLastSeen[variant]
			if support > bestSupport {
				best = variant
				bestSupport = support
				bestSeen = seenAt
				continue
			}
			if support < bestSupport {
				continue
			}
			if seenAt.After(bestSeen) {
				best = variant
				bestSeen = seenAt
				continue
			}
			if seenAt.Equal(bestSeen) && (best == "" || variant < best) {
				best = variant
			}
		}
		if best == "" {
			return key
		}
		return best
	}

	subjectAgg := ensureCallEntry(subjectIdentity)
	if subjectReporter != "" && passesSNRThreshold(subject, cfg) {
		addReporter(subjectIdentity, subjectReporter, subject.Time, subject.Frequency)
		allReporters[subjectReporter] = struct{}{}
	}
	if subjectAgg.lastSeen.IsZero() {
		subjectAgg.lastSeen = subject.Time
		subjectAgg.lastFreq = subject.Frequency
	}

	toleranceHz := cfg.FrequencyToleranceHz
	if toleranceHz <= 0 {
		toleranceHz = 500 // fallback to a half-kHz window
	}
	toleranceKHz := toleranceHz / 1000.0

	for _, entry := range others {
		otherIdentity := normalizeCorrectionCallIdentity(entry.Call)
		if otherIdentity.VoteKey == "" {
			continue
		}
		reporter := strings.TrimSpace(entry.Spotter)
		if reporter == "" {
			continue
		}
		if !passesSNREntry(entry, cfg) {
			continue
		}
		entryFreqKHz := float64(entry.FreqHz) / 1000.0
		if math.Abs(entryFreqKHz-subject.Frequency) > toleranceKHz {
			continue
		}
		seenAt := time.Unix(entry.Time, 0)
		if now.Sub(seenAt) > cfg.RecencyWindow {
			continue
		}
		if reporter == subjectReporter {
			if otherIdentity.VoteKey == subjectIdentity.VoteKey {
				allReporters[reporter] = struct{}{}
				addReporter(otherIdentity, reporter, seenAt, entryFreqKHz)
			}
			continue
		}
		if !strings.EqualFold(entry.Mode, subject.Mode) {
			continue
		}
		allReporters[reporter] = struct{}{}
		addReporter(otherIdentity, reporter, seenAt, entryFreqKHz)
		clusterSpots = append(clusterSpots, entry)
	}

	// Slash precedence: when a base call has both bare and slash-explicit
	// variants in the same bucket, drop the bare variant if at least one slash
	// variant meets existing credibility requirements.
	excludedCalls := make(map[string]struct{})
	slashPrecedenceActive := false
	type baseGroup struct {
		bareKey   string
		slashKeys []string
	}
	baseGroups := make(map[string]*baseGroup, len(callStats))
	for key, agg := range callStats {
		if agg == nil || agg.identity.BaseKey == "" {
			continue
		}
		group := baseGroups[agg.identity.BaseKey]
		if group == nil {
			group = &baseGroup{}
			baseGroups[agg.identity.BaseKey] = group
		}
		if agg.identity.HasSlash {
			group.slashKeys = append(group.slashKeys, key)
			continue
		}
		if group.bareKey == "" || key < group.bareKey {
			group.bareKey = key
		}
	}
	for _, group := range baseGroups {
		if group == nil || group.bareKey == "" || len(group.slashKeys) == 0 {
			continue
		}
		credibleSlash := false
		for _, slashKey := range group.slashKeys {
			if agg := callStats[slashKey]; agg != nil && len(agg.reporters) >= cfg.MinConsensusReports {
				credibleSlash = true
				break
			}
		}
		if !credibleSlash {
			continue
		}
		excludedCalls[group.bareKey] = struct{}{}
		slashPrecedenceActive = true
	}

	// Confidence denominator excludes calls filtered by slash precedence.
	includedReporters := make(map[string]struct{}, len(allReporters))
	for key, agg := range callStats {
		if agg == nil {
			continue
		}
		if _, excluded := excludedCalls[key]; excluded {
			continue
		}
		for reporter := range agg.reporters {
			includedReporters[reporter] = struct{}{}
		}
	}
	totalReporters = len(includedReporters)
	trace.TotalReporters = totalReporters
	if totalReporters == 0 {
		if slashPrecedenceActive {
			trace.DecisionPath = "slash_precedence"
			trace.Reason = "slash_precedence_no_reporters"
		} else {
			trace.Reason = "no_reporters"
		}
		trace.Decision = "rejected"
		logCorrectionTrace(cfg, trace, clusterSpots)
		return "", 0, 0, 0, 0, false
	}

	freqHz := subject.Frequency * 1000.0

	// Majority-of-unique-spotters: pick the call with the most unique reporters
	// on-frequency (within recency/SNR gates). Distance is only a safety cap.
	callKeys := make([]string, 0, len(callStats))
	for call := range callStats {
		if _, excluded := excludedCalls[call]; excluded {
			continue
		}
		callKeys = append(callKeys, call)
	}
	if len(callKeys) == 0 {
		trace.Decision = "rejected"
		trace.DecisionPath = "slash_precedence"
		trace.Reason = "slash_precedence_no_winner"
		logCorrectionTrace(cfg, trace, clusterSpots)
		return "", 0, 0, 0, 0, false
	}

	subjectCount := 0
	if _, excluded := excludedCalls[subjectIdentity.VoteKey]; !excluded {
		subjectCount = len(subjectAgg.reporters)
	}
	trace.SubjectSupport = subjectCount
	if totalReporters > 0 {
		subjectConfidence = subjectCount * 100 / totalReporters
	}
	trace.SubjectConfidence = subjectConfidence

	if cfg.Cooldown != nil && subjectAgg != nil && subjectCount > 0 {
		cfg.Cooldown.Record(subjectIdentity.VoteKey, freqHz, subjectAgg.reporters, cfg.CooldownMinReporters, cfg.RecencyWindow, now)
	}

	type rankedCall struct {
		key        string
		display    string
		support    int
		confidence int
		lastSeen   time.Time
		lastFreq   float64
	}

	rankedCalls := make([]rankedCall, 0, len(callKeys))
	for call, agg := range callStats {
		if agg == nil {
			continue
		}
		if _, excluded := excludedCalls[call]; excluded {
			continue
		}
		support := len(agg.reporters)
		confidence := 0
		if totalReporters > 0 {
			confidence = support * 100 / totalReporters
		}
		rankedCalls = append(rankedCalls, rankedCall{
			key:        call,
			display:    displayForKey(call),
			support:    support,
			confidence: confidence,
			lastSeen:   agg.lastSeen,
			lastFreq:   agg.lastFreq,
		})
	}
	sort.Slice(rankedCalls, func(i, j int) bool {
		if rankedCalls[i].support != rankedCalls[j].support {
			return rankedCalls[i].support > rankedCalls[j].support
		}
		if !rankedCalls[i].lastSeen.Equal(rankedCalls[j].lastSeen) {
			return rankedCalls[i].lastSeen.After(rankedCalls[j].lastSeen)
		}
		return rankedCalls[i].key < rankedCalls[j].key
	})

	majorityKey := ""
	if len(rankedCalls) > 0 {
		majorityKey = rankedCalls[0].key
	}

	// Anchor path: if a call in this cluster is already considered good, prefer
	// its closest good neighbor candidate, but still require full gate checks.
	allCalls := make([]string, 0, len(rankedCalls)+1)
	allCalls = append(allCalls, subjectIdentity.VoteKey)
	seen := map[string]struct{}{subjectIdentity.VoteKey: {}}
	for _, rc := range rankedCalls {
		if _, ok := seen[rc.key]; !ok {
			allCalls = append(allCalls, rc.key)
			seen[rc.key] = struct{}{}
		}
	}
	anchorKey := ""
	hasGoodAnchor := false
	if store := currentCallQuality(); store != nil {
		for _, c := range allCalls {
			if store.IsGood(c, freqHz, &cfg) {
				hasGoodAnchor = true
				break
			}
		}
	}
	if hasGoodAnchor {
		if anchor, okAnchor := findAnchorForCall(subjectIdentity.VoteKey, freqHz, subject.Mode, allCalls, &cfg); okAnchor {
			anchorKey = anchor
		}
	}

	type candidateEval struct {
		support       int
		confidence    int
		distance      int
		runnerUp      int
		runnerUpFreq  float64
		minReports    int
		minAdvantage  int
		minConfidence int
		reason        string
	}

	evaluateCandidate := func(candidateKey string) candidateEval {
		agg := callStats[candidateKey]
		if agg == nil {
			return candidateEval{reason: "no_winner"}
		}
		support := len(agg.reporters)
		confidence := 0
		if totalReporters > 0 {
			confidence = support * 100 / totalReporters
		}

		runner := rankedCall{}
		hasRunner := false
		for _, rc := range rankedCalls {
			if rc.key == candidateKey {
				continue
			}
			runner = rc
			hasRunner = true
			break
		}
		runnerSupport := 0
		runnerFreq := 0.0
		if hasRunner {
			runnerSupport = runner.support
			runnerFreq = runner.lastFreq
		}

		distance := correctionDistance(subjectIdentity, agg.identity, subject.Mode, cfg.DistanceModelCW, cfg.DistanceModelRTTY)
		if cfg.MaxEditDistance >= 0 && distance > cfg.MaxEditDistance {
			return candidateEval{
				support:      support,
				confidence:   confidence,
				distance:     distance,
				runnerUp:     runnerSupport,
				runnerUpFreq: runnerFreq,
				reason:       "max_edit_distance",
			}
		}

		minReports := cfg.MinConsensusReports
		minAdvantage := cfg.MinAdvantage
		minConf := cfg.MinConfidencePercent
		if distance >= 3 {
			minReports += cfg.Distance3ExtraReports
			minAdvantage += cfg.Distance3ExtraAdvantage
			minConf += cfg.Distance3ExtraConfidence
		}
		if support < minReports {
			return candidateEval{
				support:       support,
				confidence:    confidence,
				distance:      distance,
				runnerUp:      runnerSupport,
				runnerUpFreq:  runnerFreq,
				minReports:    minReports,
				minAdvantage:  minAdvantage,
				minConfidence: minConf,
				reason:        "min_reports",
			}
		}
		if support < subjectCount+minAdvantage {
			return candidateEval{
				support:       support,
				confidence:    confidence,
				distance:      distance,
				runnerUp:      runnerSupport,
				runnerUpFreq:  runnerFreq,
				minReports:    minReports,
				minAdvantage:  minAdvantage,
				minConfidence: minConf,
				reason:        "advantage",
			}
		}
		if confidence < minConf {
			return candidateEval{
				support:       support,
				confidence:    confidence,
				distance:      distance,
				runnerUp:      runnerSupport,
				runnerUpFreq:  runnerFreq,
				minReports:    minReports,
				minAdvantage:  minAdvantage,
				minConfidence: minConf,
				reason:        "confidence",
			}
		}

		if runnerSupport > 0 {
			freqSeparation := math.Abs(agg.lastFreq - runnerFreq)
			if freqSeparation >= cfg.FreqGuardMinSeparationKHz && float64(runnerSupport) >= cfg.FreqGuardRunnerUpRatio*float64(support) {
				return candidateEval{
					support:       support,
					confidence:    confidence,
					distance:      distance,
					runnerUp:      runnerSupport,
					runnerUpFreq:  runnerFreq,
					minReports:    minReports,
					minAdvantage:  minAdvantage,
					minConfidence: minConf,
					reason:        "freq_guard",
				}
			}
		}

		if cfg.Cooldown != nil {
			if block, _ := cfg.Cooldown.ShouldBlock(subjectIdentity.VoteKey, freqHz, cfg.CooldownMinReporters, cfg.RecencyWindow, subjectCount, subjectConfidence, support, confidence, now); block {
				return candidateEval{
					support:       support,
					confidence:    confidence,
					distance:      distance,
					runnerUp:      runnerSupport,
					runnerUpFreq:  runnerFreq,
					minReports:    minReports,
					minAdvantage:  minAdvantage,
					minConfidence: minConf,
					reason:        "cooldown",
				}
			}
		}

		return candidateEval{
			support:       support,
			confidence:    confidence,
			distance:      distance,
			runnerUp:      runnerSupport,
			runnerUpFreq:  runnerFreq,
			minReports:    minReports,
			minAdvantage:  minAdvantage,
			minConfidence: minConf,
		}
	}

	type decisionAttempt struct {
		key  string
		path string
	}
	attempts := make([]decisionAttempt, 0, 2)
	if anchorKey != "" && anchorKey != subjectIdentity.VoteKey {
		attempts = append(attempts, decisionAttempt{key: anchorKey, path: "anchor"})
	}
	if majorityKey != "" && majorityKey != subjectIdentity.VoteKey && majorityKey != anchorKey {
		attempts = append(attempts, decisionAttempt{key: majorityKey, path: "consensus"})
	}

	if len(attempts) == 0 {
		if slashPrecedenceActive {
			trace.DecisionPath = "slash_precedence"
			trace.Reason = "slash_precedence_no_winner"
		} else {
			trace.Reason = "no_winner"
		}
		trace.Decision = "rejected"
		logCorrectionTrace(cfg, trace, clusterSpots)
		return "", 0, 0, subjectConfidence, totalReporters, false
	}

	lastEval := candidateEval{}
	lastPath := ""
	lastKey := ""
	for _, attempt := range attempts {
		lastPath = attempt.path
		lastKey = attempt.key
		lastEval = evaluateCandidate(attempt.key)
		if lastEval.reason != "" {
			continue
		}

		updateCallQualityForCluster(attempt.key, freqHz, &cfg, clusterSpots)
		trace.DecisionPath = decorateDecisionPath(attempt.path, slashPrecedenceActive)
		trace.WinnerCall = displayForKey(attempt.key)
		trace.WinnerSupport = lastEval.support
		trace.RunnerUpSupport = lastEval.runnerUp
		trace.WinnerConfidence = lastEval.confidence
		trace.Distance = lastEval.distance
		trace.MinReports = lastEval.minReports
		trace.MinAdvantage = lastEval.minAdvantage
		trace.MinConfidence = lastEval.minConfidence
		trace.Decision = "applied"
		trace.Reason = ""
		logCorrectionTrace(cfg, trace, clusterSpots)
		return displayForKey(attempt.key), lastEval.support, lastEval.confidence, subjectConfidence, totalReporters, true
	}

	trace.DecisionPath = decorateDecisionPath(lastPath, slashPrecedenceActive)
	trace.WinnerCall = displayForKey(lastKey)
	trace.WinnerSupport = lastEval.support
	trace.RunnerUpSupport = lastEval.runnerUp
	trace.WinnerConfidence = lastEval.confidence
	trace.Distance = lastEval.distance
	trace.MinReports = lastEval.minReports
	trace.MinAdvantage = lastEval.minAdvantage
	trace.MinConfidence = lastEval.minConfidence
	trace.Decision = "rejected"
	trace.Reason = lastEval.reason
	if trace.Reason == "" {
		if slashPrecedenceActive {
			trace.Reason = "slash_precedence_no_winner"
		} else {
			trace.Reason = "no_winner"
		}
	}
	logCorrectionTrace(cfg, trace, clusterSpots)
	return "", 0, 0, subjectConfidence, totalReporters, false
}

type correctionCallIdentity struct {
	Raw      string
	VoteKey  string
	BaseKey  string
	HasSlash bool
	SlashKey string
}

// normalizeCorrectionCallIdentity derives correction-only identity keys.
// VoteKey groups semantically equivalent variants (e.g., KH6/W1AW == W1AW/KH6).
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

func decorateDecisionPath(path string, slashPrecedence bool) string {
	if !slashPrecedence {
		return path
	}
	if path == "" {
		return "slash_precedence"
	}
	return path + "+slash_precedence"
}

// Purpose: Select the closest good anchor call for a busted call.
// Key aspects: Uses mode-aware distance and quality anchors.
// Upstream: SuggestCallCorrection.
// Downstream: callQuality.IsGood and correctionDistance.
// findAnchorForCall selects the closest good anchor (by mode-aware distance) for a busted call.
func findAnchorForCall(bustedCall string, freqHz float64, mode string, candidates []string, cfg *CorrectionSettings) (string, bool) {
	bustedIdentity := normalizeCorrectionCallIdentity(bustedCall)
	if bustedIdentity.VoteKey == "" || cfg == nil {
		return "", false
	}
	store := currentCallQuality()
	if store == nil {
		return "", false
	}
	modeKey := strings.TrimSpace(mode)
	best := ""
	bestDist := math.MaxInt
	for _, c := range candidates {
		candidateIdentity := normalizeCorrectionCallIdentity(c)
		candidateKey := candidateIdentity.VoteKey
		if candidateKey == "" || candidateKey == bustedIdentity.VoteKey {
			continue
		}
		if !store.IsGood(candidateKey, freqHz, cfg) {
			continue
		}
		dist := correctionDistance(bustedIdentity, candidateIdentity, modeKey, cfg.DistanceModelCW, cfg.DistanceModelRTTY)
		if cfg.MaxEditDistance >= 0 && dist > cfg.MaxEditDistance {
			continue
		}
		if dist < bestDist {
			bestDist = dist
			best = candidateKey
		}
	}
	if best == "" {
		return "", false
	}
	return best, true
}

// Purpose: Update call quality scores after a cluster decision.
// Key aspects: Rewards winner and penalizes busted calls.
// Upstream: SuggestCallCorrection.
// Downstream: callQuality.Add.
// updateCallQualityForCluster updates the quality store after a resolved cluster.
func updateCallQualityForCluster(winnerCall string, freqHz float64, cfg *CorrectionSettings, clusterSpots []bandmap.SpotEntry) {
	if cfg == nil || winnerCall == "" || len(clusterSpots) == 0 {
		return
	}
	store := currentCallQuality()
	if store == nil {
		return
	}
	winnerIdentity := normalizeCorrectionCallIdentity(winnerCall)
	winnerKey := winnerIdentity.VoteKey
	if winnerKey == "" {
		return
	}
	store.Add(winnerKey, freqHz, cfg.QualityBinHz, cfg.QualityNewCallIncrement)

	distinct := make(map[string]struct{})
	for _, s := range clusterSpots {
		callIdentity := normalizeCorrectionCallIdentity(s.Call)
		call := callIdentity.VoteKey
		if call == "" {
			continue
		}
		distinct[call] = struct{}{}
	}
	for call := range distinct {
		if call == winnerKey {
			continue
		}
		store.Add(call, freqHz, cfg.QualityBinHz, -cfg.QualityBustedDecrement)
	}
}

// Purpose: Emit a correction trace to the configured logger.
// Key aspects: No-op when debug logging is disabled.
// Upstream: SuggestCallCorrection.
// Downstream: cfg.TraceLogger.Enqueue.
func logCorrectionTrace(cfg CorrectionSettings, tr CorrectionTrace, votes []bandmap.SpotEntry) {
	if !cfg.DebugLog || cfg.TraceLogger == nil {
		return
	}
	cfg.TraceLogger.Enqueue(CorrectionLogEntry{
		Trace: tr,
		Votes: votes,
	})
}

// Purpose: Normalize correction settings with defaults.
// Key aspects: Applies minimums and standardizes strategy names.
// Upstream: SuggestCallCorrection.
// Downstream: string normalization.
// normalizeCorrectionSettings fills in safe defaults so callers can omit config
// while unit tests can deliberately pass tiny values.
func normalizeCorrectionSettings(settings CorrectionSettings) CorrectionSettings {
	cfg := settings
	// Honor configured strategy; fallback to majority when blank/invalid.
	switch strings.ToLower(strings.TrimSpace(cfg.Strategy)) {
	case "center", "classic", "majority":
		cfg.Strategy = strings.ToLower(strings.TrimSpace(cfg.Strategy))
	default:
		cfg.Strategy = "majority"
	}
	if cfg.FreqGuardMinSeparationKHz <= 0 {
		cfg.FreqGuardMinSeparationKHz = 0.1
	}
	if cfg.FreqGuardRunnerUpRatio <= 0 {
		cfg.FreqGuardRunnerUpRatio = 0.5
	}
	if cfg.FrequencyToleranceHz <= 0 {
		cfg.FrequencyToleranceHz = 500
	}
	if cfg.QualityBinHz <= 0 {
		cfg.QualityBinHz = 1000
	}
	if cfg.QualityGoodThreshold <= 0 {
		cfg.QualityGoodThreshold = 2
	}
	if cfg.QualityNewCallIncrement == 0 {
		cfg.QualityNewCallIncrement = 1
	}
	if cfg.QualityBustedDecrement == 0 {
		cfg.QualityBustedDecrement = 1
	}
	if cfg.MinConsensusReports <= 0 {
		cfg.MinConsensusReports = 4
	}
	if cfg.MinAdvantage <= 0 {
		cfg.MinAdvantage = 1
	}
	if cfg.MinConfidencePercent <= 0 {
		cfg.MinConfidencePercent = 70
	}
	if cfg.RecencyWindow <= 0 {
		cfg.RecencyWindow = 45 * time.Second
	}
	if cfg.MaxEditDistance <= 0 {
		cfg.MaxEditDistance = 2
	}
	if cfg.MinSNRCW < 0 {
		cfg.MinSNRCW = 0
	}
	if cfg.MinSNRRTTY < 0 {
		cfg.MinSNRRTTY = 0
	}
	if cfg.Distance3ExtraReports < 0 {
		cfg.Distance3ExtraReports = 0
	}
	if cfg.Distance3ExtraAdvantage < 0 {
		cfg.Distance3ExtraAdvantage = 0
	}
	if cfg.Distance3ExtraConfidence < 0 {
		cfg.Distance3ExtraConfidence = 0
	}
	cfg.DistanceModelCW = normalizeCWDistanceModel(cfg.DistanceModelCW)
	cfg.DistanceModelRTTY = normalizeRTTYDistanceModel(cfg.DistanceModelRTTY)
	if cfg.MinSpotterReliability < 0 {
		cfg.MinSpotterReliability = 0
	}
	return cfg
}

// Purpose: Return the minimum SNR threshold for a mode.
// Key aspects: Uses per-mode thresholds from config.
// Upstream: passesSNRThreshold and passesSNREntry.
// Downstream: strings.ToUpper.
func minSNRThresholdForMode(mode string, cfg CorrectionSettings) int {
	switch strutil.NormalizeUpper(mode) {
	case "CW":
		return cfg.MinSNRCW
	case "RTTY":
		return cfg.MinSNRRTTY
	case "USB", "LSB":
		return cfg.MinSNRVoice
	default:
		return 0
	}
}

// Purpose: Check whether a spot meets the SNR threshold for its mode.
// Key aspects: Requires HasReport to enforce thresholds.
// Upstream: SuggestCallCorrection.
// Downstream: minSNRThresholdForMode.
func passesSNRThreshold(s *Spot, cfg CorrectionSettings) bool {
	if s == nil {
		return false
	}
	required := minSNRThresholdForMode(s.Mode, cfg)
	if required <= 0 {
		return true
	}
	return s.Report >= required
}

// Purpose: Check whether a bandmap entry meets SNR threshold.
// Key aspects: Uses entry.Mode and entry.SNR.
// Upstream: SuggestCallCorrection.
// Downstream: minSNRThresholdForMode.
func passesSNREntry(e bandmap.SpotEntry, cfg CorrectionSettings) bool {
	required := minSNRThresholdForMode(e.Mode, cfg)
	if required <= 0 {
		return true
	}
	return e.SNR >= required
}

// CorrectionIndex maintains a time-bounded, frequency-bucketed view of recent
// spots so consensus checks can run without scanning the entire ring buffer.
type CorrectionIndex struct {
	mu       sync.Mutex
	buckets  map[int]*correctionBucket
	lastSeen map[int]time.Time

	sweepQuit chan struct{}
}

type correctionBucket struct {
	spots []*Spot
}

// NewCorrectionIndex constructs an empty correction index.
// Key aspects: Initializes bucket and lastSeen maps.
// Upstream: main startup.
// Downstream: map allocation.
// NewCorrectionIndex constructs an empty index.
func NewCorrectionIndex() *CorrectionIndex {
	return &CorrectionIndex{
		buckets:  make(map[int]*correctionBucket),
		lastSeen: make(map[int]time.Time),
	}
}

// Add inserts a spot into the frequency bucket index.
// Key aspects: Prunes stale entries and tracks lastSeen per bucket.
// Upstream: processOutputSpots call correction path.
// Downstream: bucketKey and pruneAndAppend.
// Add inserts a spot into the appropriate bucket and prunes stale entries.
func (ci *CorrectionIndex) Add(s *Spot, now time.Time, window time.Duration) {
	if ci == nil || s == nil {
		return
	}
	if window <= 0 {
		window = 45 * time.Second
	}

	key := bucketKey(s.Frequency)

	ci.mu.Lock()
	defer ci.mu.Unlock()

	ci.cleanup(now, window)

	bucket := ci.buckets[key]
	if bucket == nil {
		bucket = &correctionBucket{}
		ci.buckets[key] = bucket
	}

	bucket.spots = pruneAndAppend(bucket.spots, s, now, window)
	if len(bucket.spots) == 0 {
		delete(ci.buckets, key)
		delete(ci.lastSeen, key)
	} else {
		ci.lastSeen[key] = now
	}
}

// Candidates retrieve nearby spots for call correction.
// Key aspects: Scans adjacent buckets and prunes stale entries.
// Upstream: SuggestCallCorrection.
// Downstream: bucketKey and prune.
// Candidates retrieves nearby spots within the specified +/- window (kHz).
func (ci *CorrectionIndex) Candidates(subject *Spot, now time.Time, window time.Duration, searchKHz float64) []*Spot {
	if ci == nil || subject == nil {
		return nil
	}
	if window <= 0 {
		window = 45 * time.Second
	}
	if searchKHz <= 0 {
		searchKHz = 0.5
	}

	key := bucketKey(subject.Frequency)
	rangeBuckets := int(math.Ceil(searchKHz * 10))
	minKey := key - rangeBuckets
	maxKey := key + rangeBuckets

	ci.mu.Lock()
	defer ci.mu.Unlock()

	var results []*Spot
	for k := minKey; k <= maxKey; k++ {
		bucket := ci.buckets[k]
		if bucket == nil {
			continue
		}
		bucket.spots = prune(bucket.spots, now, window)
		if len(bucket.spots) == 0 {
			// Drop empty buckets to prevent map growth as frequencies churn.
			delete(ci.buckets, k)
			delete(ci.lastSeen, k)
			continue
		}
		ci.lastSeen[k] = now
		results = append(results, bucket.spots...)
	}
	return results
}

// Purpose: Compute the bucket key for a frequency.
// Key aspects: Half-up rounding to 0.1 kHz.
// Upstream: CorrectionIndex.Add and Candidates.
// Downstream: math.Floor.
func bucketKey(freq float64) int {
	// Half-up rounding to 0.1 kHz keeps bucket boundaries stable at .x5 points.
	return int(math.Floor(freq*10 + 0.5))
}

// Purpose: Drop stale spots outside the recency window.
// Key aspects: Returns a compacted slice of active spots.
// Upstream: Candidates.
// Downstream: time comparisons.
func prune(spots []*Spot, now time.Time, window time.Duration) []*Spot {
	if len(spots) == 0 {
		return spots
	}
	cutoff := now.Add(-window)
	dst := spots[:0]
	for _, s := range spots {
		if s == nil {
			continue
		}
		if s.Time.Before(cutoff) {
			continue
		}
		dst = append(dst, s)
	}
	return dst
}

// Purpose: Prune stale spots and append the new spot.
// Key aspects: Reuses prune() to keep slice compact.
// Upstream: CorrectionIndex.Add.
// Downstream: prune.
func pruneAndAppend(spots []*Spot, s *Spot, now time.Time, window time.Duration) []*Spot {
	spots = prune(spots, now, window)
	return append(spots, s)
}

// Purpose: Remove inactive buckets to bound memory.
// Key aspects: Deletes buckets when lastSeen is outside window.
// Upstream: CorrectionIndex.Add and StartCleanup.
// Downstream: map deletes.
// cleanup removes buckets that have been inactive longer than the window to keep
// the map bounded even when frequencies churn across the spectrum.
func (ci *CorrectionIndex) cleanup(now time.Time, window time.Duration) {
	if len(ci.lastSeen) == 0 {
		return
	}
	cutoff := now.Add(-window)
	for key, last := range ci.lastSeen {
		if last.Before(cutoff) {
			delete(ci.lastSeen, key)
			delete(ci.buckets, key)
		}
	}
}

// StartCleanup starts a periodic cleanup goroutine for the index.
// Key aspects: Uses ticker and quit channel; guards against double start.
// Upstream: main startup.
// Downstream: cleanup and time.NewTicker.
// StartCleanup launches a periodic sweep to evict inactive buckets even when Candidates/Add
// are not called frequently, keeping memory bounded.
func (ci *CorrectionIndex) StartCleanup(interval, window time.Duration) {
	if ci == nil {
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	ci.mu.Lock()
	if ci.sweepQuit != nil {
		ci.mu.Unlock()
		return
	}
	ci.sweepQuit = make(chan struct{})
	ci.mu.Unlock()

	// Purpose: Periodically invoke cleanup until StopCleanup is called.
	// Key aspects: Ticker-driven loop with quit channel.
	// Upstream: StartCleanup.
	// Downstream: cleanup and ticker.Stop.
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ci.mu.Lock()
				ci.cleanup(time.Now().UTC(), window)
				ci.mu.Unlock()
			case <-ci.sweepQuit:
				return
			}
		}
	}()
}

// StopCleanup stops the periodic cleanup goroutine.
// Key aspects: Closes quit channel and clears it.
// Upstream: main shutdown.
// Downstream: channel close only.
// StopCleanup stops the periodic cleanup goroutine.
func (ci *CorrectionIndex) StopCleanup() {
	if ci == nil {
		return
	}
	ci.mu.Lock()
	defer ci.mu.Unlock()
	if ci.sweepQuit != nil {
		close(ci.sweepQuit)
		ci.sweepQuit = nil
	}
}

const (
	distanceModelPlain  = "plain"
	distanceModelMorse  = "morse"
	distanceModelBaudot = "baudot"
)

// Purpose: Normalize CW distance model selection.
// Key aspects: Defaults to plain when unknown.
// Upstream: normalizeCorrectionSettings and callDistance.
// Downstream: strings.ToLower.
func normalizeCWDistanceModel(model string) string {
	switch strings.ToLower(strings.TrimSpace(model)) {
	case distanceModelMorse:
		return distanceModelMorse
	default:
		return distanceModelPlain
	}
}

// Purpose: Normalize RTTY distance model selection.
// Key aspects: Defaults to plain when unknown.
// Upstream: normalizeCorrectionSettings and callDistance.
// Downstream: strings.ToLower.
func normalizeRTTYDistanceModel(model string) string {
	switch strings.ToLower(strings.TrimSpace(model)) {
	case distanceModelBaudot:
		return distanceModelBaudot
	default:
		return distanceModelPlain
	}
}

// callDistanceCore picks the distance function based on mode/model without caching.
// Purpose: Compute mode-aware call distance without caching.
// Key aspects: Routes to CW/RTTY-specific models or plain Levenshtein.
// Upstream: callDistance.
// Downstream: cwCallDistance, rttyCallDistance, lev.Distance.
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

// callDistance normalizes mode/model inputs before routing to the core distance function.
// Purpose: Compute mode-aware call distance with normalized models.
// Key aspects: Normalizes model strings before calling core.
// Upstream: SuggestCallCorrection and findAnchorForCall.
// Downstream: callDistanceCore and normalizeCW/RTTYDistanceModel.
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

// Purpose: Compute CW-aware edit distance between two callsigns.
// Key aspects: Uses Levenshtein with Morse-weighted substitutions; insert/delete
// cost 1; pooled DP buffers limit allocations; substitutions use prebuilt costs.
// Upstream: callDistanceCore (CW mode distance path).
// Downstream: morseCharDist, min3, borrowIntSlice, returnIntSlice.
func cwCallDistance(a, b string) int {
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
		prev[j] = j // j inserts
	}

	for i := 1; i <= la; i++ {
		cur[0] = i // i deletes
		for j := 1; j <= lb; j++ {
			insert := cur[j-1] + 1
			delete := prev[j] + 1
			replace := prev[j-1] + morseCharDist(ra[i-1], rb[j-1])
			cur[j] = min3(insert, delete, replace)
		}
		prev, cur = cur, prev
	}

	return prev[lb]
}

// Purpose: Return substitution cost between two runes using Morse code weights.
// Key aspects: Looks up precomputed table; falls back to a fixed penalty.
// Upstream: cwCallDistance.
// Downstream: morseRuneIndex, morseCostTable.
func morseCharDist(a, b rune) int {
	if a == b {
		return 0
	}
	if i, ok := morseRuneIndex[a]; ok {
		if j, ok := morseRuneIndex[b]; ok {
			return morseCostTable[i][j]
		}
	}
	// Fallback cost when the rune is not in the Morse table.
	return 2
}

// Purpose: Compute RTTY-aware edit distance between two callsigns.
// Key aspects: Same DP structure as CW but uses Baudot substitution costs.
// Upstream: callDistanceCore (RTTY mode distance path).
// Downstream: baudotCharDist, min3, borrowIntSlice, returnIntSlice.
func rttyCallDistance(a, b string) int {
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
			replace := prev[j-1] + baudotCharDist(ra[i-1], rb[j-1])
			cur[j] = min3(insert, delete, replace)
		}
		prev, cur = cur, prev
	}

	return prev[lb]
}

// Purpose: Return substitution cost between two runes using Baudot weights.
// Key aspects: Looks up precomputed table; falls back to a fixed penalty.
// Upstream: rttyCallDistance.
// Downstream: baudotRuneIndex, baudotCostTable.
func baudotCharDist(a, b rune) int {
	if a == b {
		return 0
	}
	if i, ok := baudotRuneIndex[a]; ok {
		if j, ok := baudotRuneIndex[b]; ok {
			return baudotCostTable[i][j]
		}
	}
	// Fallback cost when the rune is not in the Baudot table.
	return 2
}

// Purpose: Return the minimum of three integers.
// Key aspects: Branches to avoid allocations or slices.
// Upstream: cwCallDistance, rttyCallDistance, morsePatternCost, baudotPatternCost.
// Downstream: None.
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

// Purpose: Provide an int buffer for edit-distance DP with optional pooling.
// Key aspects: Reuses a pooled 64-cap slice for small buffers; returns a flag to
// drive proper pool return.
// Upstream: cwCallDistance, rttyCallDistance.
// Downstream: levBufPool.
func borrowIntSlice(n int) ([]int, bool) {
	if n <= 0 {
		return nil, false
	}
	if n <= 64 {
		buf := levBufPool.Get().([]int)
		return buf[:n], true
	}
	return make([]int, n), false
}

// Purpose: Return a pooled DP buffer to the pool.
// Key aspects: Re-slices to the original pool cap to keep buffers bounded.
// Upstream: cwCallDistance, rttyCallDistance.
// Downstream: levBufPool.
func returnIntSlice(buf []int, fromPool bool) {
	if !fromPool || buf == nil {
		return
	}
	// Keep pooled buffers bounded to the original cap.
	if cap(buf) >= 64 {
		levBufPool.Put(buf[:64])
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

	morseInsertCost = 1
	morseDeleteCost = 1
	morseSubCost    = 2
	morseScale      = 2

	baudotInsertCost = 1
	baudotDeleteCost = 1
	baudotSubCost    = 2
	baudotScale      = 2

	levBufPool = sync.Pool{
		New: func() interface{} {
			// Typical callsigns are short; a small buffer covers common cases.
			return make([]int, 64)
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

// Purpose: Build Morse and Baudot cost tables at package init.
// Key aspects: Precomputes rune indexes and dense cost matrices for fast lookup.
// Upstream: Go runtime init for the spot package.
// Downstream: buildRuneCostTable, morsePatternCost, baudotPatternCost.
func init() {
	morseRuneIndex, morseCostTable = buildRuneCostTable(morseCodes, morsePatternCost)
	baudotRuneIndex, baudotCostTable = buildRuneCostTable(baudotCodes, baudotPatternCost)
}

// Purpose: Build rune indexes and dense substitution-cost tables for a codebook.
// Key aspects: Computes pairwise costs once; small tables avoid per-call DP.
// Upstream: init.
// Downstream: cost function (morsePatternCost or baudotPatternCost).
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
			a := codebook[ra]
			b := codebook[rb]
			table[i][j] = cost(a, b)
		}
	}
	return index, table
}

// Purpose: Compute weighted, normalized edit cost between two Morse patterns.
// Key aspects: Runs Levenshtein on dot/dash strings with weights, then normalizes
// by length and scales to a small integer for substitution costs.
// Upstream: buildRuneCostTable (Morse table build).
// Downstream: getMorseWeights, min3.
func morsePatternCost(a, b string) int {
	cfg := getMorseWeights()
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
		prev[j] = j * cfg.ins // j inserts
	}

	for i := 1; i <= la; i++ {
		cur[0] = i * cfg.del // i deletes
		for j := 1; j <= lb; j++ {
			subCost := 0
			if ra[i-1] != rb[j-1] {
				subCost = cfg.sub // dot<->dash heavier than insert/delete
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
	normalized := float64(raw) / float64(maxLen+1)
	scale := cfg.scale
	if scale <= 0 {
		scale = 2
	}
	scaled := int(math.Ceil(normalized * float64(scale)))
	if scaled < 1 && raw > 0 {
		scaled = 1
	}
	return scaled
}

type morseWeightSet struct {
	ins   int
	del   int
	sub   int
	scale int
}

// Purpose: Snapshot the current Morse weighting settings.
// Key aspects: Reads global cost parameters once per call.
// Upstream: morsePatternCost.
// Downstream: None.
func getMorseWeights() morseWeightSet {
	return morseWeightSet{
		ins:   morseInsertCost,
		del:   morseDeleteCost,
		sub:   morseSubCost,
		scale: morseScale,
	}
}

// Purpose: Compute weighted, normalized edit cost between two Baudot patterns.
// Key aspects: Runs weighted Levenshtein and scales the result for substitutions.
// Upstream: buildRuneCostTable (Baudot table build).
// Downstream: min3.
func baudotPatternCost(a, b string) int {
	if a == b {
		return 0
	}
	ra := []rune(a)
	rb := []rune(b)
	la := len(ra)
	lb := len(rb)
	if la == 0 {
		return baudotInsertCost
	}
	if lb == 0 {
		return baudotInsertCost
	}
	prev := make([]int, lb+1)
	cur := make([]int, lb+1)

	for j := 0; j <= lb; j++ {
		prev[j] = j * baudotInsertCost
	}

	for i := 1; i <= la; i++ {
		cur[0] = i * baudotDeleteCost
		for j := 1; j <= lb; j++ {
			subCost := 0
			if ra[i-1] != rb[j-1] {
				subCost = baudotSubCost
			}
			insert := cur[j-1] + baudotInsertCost
			delete := prev[j] + baudotDeleteCost
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
	scale := baudotScale
	if scale <= 0 {
		scale = 2
	}
	normalized := float64(raw) / float64(maxLen+1)
	scaled := int(math.Ceil(normalized * float64(scale)))
	if scaled < 1 && raw > 0 {
		scaled = 1
	}
	return scaled
}
