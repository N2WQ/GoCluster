package spot

import (
	"dxcluster/bandmap"
	"math"
	"strings"
	"sync"
	"time"

	lev "github.com/agnivade/levenshtein"
)

// CorrectionSettings captures the knobs that govern whether a consensus-based
// call correction should happen. The values ultimately come from config.yaml,
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

	// MinSNRCW/MinSNRRTTY let callers ignore corroborators below a minimum
	// signal-to-noise ratio. FT8/FT4 aren't run through call correction so we
	// only need CW/RTTY thresholds.
	MinSNRCW   int
	MinSNRRTTY int

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
	// DistanceCache* control memoization of string distance calculations. This
	// lowers CPU when the same candidate set is evaluated repeatedly during
	// bursts. Disable by setting size<=0 or ttl<=0.
	DistanceCacheSize int
	DistanceCacheTTL  time.Duration
	// Spotter reliability weights (0..1). Reporters below MinSpotterReliability are ignored
	// when counting corroborators.
	SpotterReliability    SpotterReliability
	MinSpotterReliability float64
}

var correctionEligibleModes = map[string]struct{}{
	"CW":   {},
	"RTTY": {},
	"SSB":  {},
}

// CorrectionTrace captures the inputs and outcome of a correction decision for audit/comparison.
type CorrectionTrace struct {
	Timestamp                 time.Time `json:"ts"`
	Strategy                  string    `json:"strategy"`
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

// IsCallCorrectionCandidate returns true if the given mode is eligible for
// consensus-based call correction. Only CW, RTTY, and SSB are considered
// because other digital modes already embed their own error correction.
func IsCallCorrectionCandidate(mode string) bool {
	_, ok := correctionEligibleModes[strings.ToUpper(strings.TrimSpace(mode))]
	return ok
}

// frequencyToleranceKHz defines how close two frequencies must be to be considered
// the "same" signal. We only care about agreement on essentially identical frequencies,
// so we allow a half-kilohertz wiggle room to absorb rounding differences between
// data sources.
var frequencyToleranceKHz = 0.5

// distanceCacheEntry stores a computed distance with expiry.
type distanceCacheEntry struct {
	distance int
	expires  time.Time
}

// distanceCache is a size- and TTL-bound memoization map for call distances.
type distanceCache struct {
	mu      sync.Mutex
	entries map[string]distanceCacheEntry
	order   []string // insertion order for eviction
	max     int
	ttl     time.Duration
}

func (dc *distanceCache) configure(max int, ttl time.Duration) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if max <= 0 || ttl <= 0 {
		dc.max = 0
		dc.entries = nil
		dc.order = nil
		dc.ttl = 0
		return
	}
	if dc.max != max || dc.ttl != ttl || dc.entries == nil {
		dc.max = max
		dc.ttl = ttl
		dc.entries = make(map[string]distanceCacheEntry, max)
		dc.order = dc.order[:0]
	}
}

func (dc *distanceCache) get(key string, now time.Time) (int, bool) {
	if dc.max <= 0 || dc.ttl <= 0 {
		return 0, false
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	entry, ok := dc.entries[key]
	if !ok {
		return 0, false
	}
	if now.After(entry.expires) {
		delete(dc.entries, key)
		return 0, false
	}
	return entry.distance, true
}

func (dc *distanceCache) put(key string, distance int, now time.Time) {
	if dc.max <= 0 || dc.ttl <= 0 {
		return
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if dc.entries == nil {
		dc.entries = make(map[string]distanceCacheEntry, dc.max)
	}
	dc.entries[key] = distanceCacheEntry{
		distance: distance,
		expires:  now.Add(dc.ttl),
	}
	dc.order = append(dc.order, key)
	if len(dc.entries) > dc.max {
		dc.evictOldest()
	}
}

func (dc *distanceCache) evictOldest() {
	// Drop oldest entries until we're within the max bound.
	for len(dc.entries) > dc.max && len(dc.order) > 0 {
		k := dc.order[0]
		dc.order = dc.order[1:]
		if _, ok := dc.entries[k]; ok {
			delete(dc.entries, k)
		}
	}
	// Trim runaway order slice if it has grown much larger than the cache.
	if cap(dc.order) > 0 && len(dc.order) > dc.max*2 {
		dc.order = append([]string(nil), dc.order...)
	}
}

// SetFrequencyToleranceHz updates the frequency similarity window in Hz.
func SetFrequencyToleranceHz(hz float64) {
	if hz <= 0 {
		frequencyToleranceKHz = 0.5
		return
	}
	frequencyToleranceKHz = hz / 1000.0
}

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
func SuggestCallCorrection(subject *Spot, others []bandmap.SpotEntry, settings CorrectionSettings, now time.Time) (correctedCall string, supporters int, correctedConfidence int, subjectConfidence int, totalReporters int, ok bool) {
	if subject == nil {
		return "", 0, 0, 0, 0, false
	}

	cfg := normalizeCorrectionSettings(settings)
	subjectCall := strings.TrimSpace(subject.DXCall)
	if subjectCall == "" {
		return "", 0, 0, 0, 0, false
	}
	subjectReporter := strings.TrimSpace(subject.DECall)

	trace := CorrectionTrace{
		Timestamp:                 now,
		Strategy:                  strings.ToLower(strings.TrimSpace(cfg.Strategy)),
		FrequencyKHz:              subject.Frequency,
		SubjectCall:               strings.ToUpper(subjectCall),
		Mode:                      strings.ToUpper(strings.TrimSpace(subject.Mode)),
		Source:                    strings.ToUpper(strings.TrimSpace(subject.SourceNode)),
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
	if trace.Mode == "CW" {
		trace.DistanceModel = normalizeCWDistanceModel(cfg.DistanceModelCW)
	} else if trace.Mode == "RTTY" {
		trace.DistanceModel = normalizeRTTYDistanceModel(cfg.DistanceModelRTTY)
	}
	allReporters := map[string]struct{}{}
	clusterSpots := make([]bandmap.SpotEntry, 0, len(others)+1)
	clusterSpots = append(clusterSpots, bandmap.SpotEntry{
		Call:    subject.DXCall,
		Spotter: subject.DECall,
		Mode:    subject.Mode,
		FreqHz:  uint32(subject.Frequency*1000 + 0.5),
		Time:    subject.Time.Unix(),
		SNR:     subject.Report,
	})

	type callAggregate struct {
		reporters map[string]struct{}
		lastSeen  time.Time
		lastFreq  float64
	}

	callStats := make(map[string]*callAggregate)
	ensureCallEntry := func(call string) *callAggregate {
		entry, ok := callStats[call]
		if !ok {
			entry = &callAggregate{reporters: make(map[string]struct{})}
			callStats[call] = entry
		}
		return entry
	}
	addReporter := func(call, reporter string, seenAt time.Time, freqKHz float64) {
		// Ignore reporters that fall below the configured reliability floor.
		if reliabilityFor(cfg.SpotterReliability, reporter) < cfg.MinSpotterReliability {
			return
		}
		entry := ensureCallEntry(call)
		entry.reporters[reporter] = struct{}{}
		if seenAt.After(entry.lastSeen) {
			entry.lastSeen = seenAt
		}
		entry.lastFreq = freqKHz
	}

	subjectAgg := ensureCallEntry(subjectCall)
	if subjectReporter != "" && passesSNRThreshold(subject, cfg) {
		addReporter(subjectCall, subjectReporter, subject.Time, subject.Frequency)
		allReporters[subjectReporter] = struct{}{}
	}
	if subjectAgg.lastSeen.IsZero() {
		subjectAgg.lastSeen = subject.Time
		subjectAgg.lastFreq = subject.Frequency
	}

	for _, entry := range others {
		otherCall := strings.TrimSpace(entry.Call)
		if otherCall == "" {
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
		if math.Abs(entryFreqKHz-subject.Frequency) > frequencyToleranceKHz {
			continue
		}
		seenAt := time.Unix(entry.Time, 0)
		if now.Sub(seenAt) > cfg.RecencyWindow {
			continue
		}
		if reporter == subjectReporter {
			if otherCall == subjectCall {
				allReporters[reporter] = struct{}{}
				addReporter(otherCall, reporter, seenAt, entryFreqKHz)
			}
			continue
		}
		if !strings.EqualFold(entry.Mode, subject.Mode) {
			continue
		}
		allReporters[reporter] = struct{}{}
		addReporter(otherCall, reporter, seenAt, entryFreqKHz)
		clusterSpots = append(clusterSpots, entry)
	}

	totalReporters = len(allReporters)
	trace.TotalReporters = totalReporters
	if totalReporters == 0 {
		trace.Decision = "rejected"
		trace.Reason = "no_reporters"
		logCorrectionTrace(cfg, trace, clusterSpots)
		return "", 0, 0, 0, 0, false
	}

	cacheCfg := distanceCacheConfig{
		size: cfg.DistanceCacheSize,
		ttl:  cfg.DistanceCacheTTL,
	}
	localCache := &distanceCache{}
	localCache.configure(cacheCfg.size, cacheCfg.ttl)
	freqHz := subject.Frequency * 1000.0

	// Majority-of-unique-spotters: pick the call with the most unique reporters
	// on-frequency (within recency/SNR gates). Distance is only a safety cap.
	callKeys := make([]string, 0, len(callStats))
	for call := range callStats {
		callKeys = append(callKeys, call)
	}
	if len(callKeys) == 0 {
		return "", 0, 0, 0, 0, false
	}

	subjectCount := len(subjectAgg.reporters)
	trace.SubjectSupport = subjectCount
	if totalReporters > 0 {
		subjectConfidence = subjectCount * 100 / totalReporters
	}
	trace.SubjectConfidence = subjectConfidence

	// Anchor path: use existing good calls in this bin to snap busted calls quickly.
	allCalls := make([]string, 0, len(callKeys)+1)
	allCalls = append(allCalls, subjectCall)
	seen := map[string]struct{}{subjectCall: {}}
	for _, c := range callKeys {
		if _, ok := seen[c]; !ok {
			allCalls = append(allCalls, c)
			seen[c] = struct{}{}
		}
	}
	hasGoodAnchor := false
	for _, c := range allCalls {
		if callQuality.IsGood(c, freqHz, &cfg) {
			hasGoodAnchor = true
			break
		}
	}
	if hasGoodAnchor {
		if anchor, okAnchor := findAnchorForCall(subjectCall, freqHz, subject.Mode, allCalls, &cfg); okAnchor {
			anchorSupport := 0
			if agg := callStats[anchor]; agg != nil {
				anchorSupport = len(agg.reporters)
			}
			anchorConfidence := 0
			if totalReporters > 0 {
				anchorConfidence = anchorSupport * 100 / totalReporters
			}
			anchorDistance := cachedCallDistance(subjectCall, anchor, subject.Mode, cfg.DistanceModelCW, cfg.DistanceModelRTTY, cacheCfg, localCache, now)
			anchorRunner := 0
			for call, agg := range callStats {
				if call == anchor || agg == nil {
					continue
				}
				if len(agg.reporters) > anchorRunner {
					anchorRunner = len(agg.reporters)
				}
			}
			updateCallQualityForCluster(anchor, freqHz, &cfg, clusterSpots)
			trace.WinnerCall = strings.ToUpper(anchor)
			trace.WinnerSupport = anchorSupport
			trace.RunnerUpSupport = anchorRunner
			trace.WinnerConfidence = anchorConfidence
			trace.Distance = anchorDistance
			trace.Decision = "applied"
			logCorrectionTrace(cfg, trace, clusterSpots)
			return anchor, anchorSupport, anchorConfidence, subjectConfidence, totalReporters, true
		}
	}

	bestCall := ""
	bestCount := 0
	bestConfidence := 0
	var bestTime time.Time
	bestFreq := 0.0
	runnerUp := 0
	runnerUpFreq := 0.0
	for _, candidateCall := range callKeys {
		agg := callStats[candidateCall]
		if agg == nil {
			continue
		}
		count := len(agg.reporters)
		confidence := count * 100 / totalReporters
		if count > bestCount || (count == bestCount && agg.lastSeen.After(bestTime)) {
			runnerUp = bestCount
			runnerUpFreq = bestFreq
			bestCall = candidateCall
			bestCount = count
			bestConfidence = confidence
			bestTime = agg.lastSeen
			bestFreq = agg.lastFreq
		}
	}

	if bestCall == "" || bestCall == subjectCall {
		trace.Decision = "rejected"
		trace.Reason = "no_winner"
		logCorrectionTrace(cfg, trace, clusterSpots)
		return "", 0, 0, subjectConfidence, totalReporters, false
	}

	centerDistance := cachedCallDistance(subjectCall, bestCall, subject.Mode, cfg.DistanceModelCW, cfg.DistanceModelRTTY, cacheCfg, localCache, now)
	trace.Distance = centerDistance
	trace.WinnerCall = strings.ToUpper(bestCall)
	trace.WinnerSupport = bestCount
	trace.WinnerConfidence = bestConfidence
	trace.RunnerUpSupport = runnerUp

	if cfg.MaxEditDistance >= 0 && centerDistance > cfg.MaxEditDistance {
		trace.Decision = "rejected"
		trace.Reason = "max_edit_distance"
		logCorrectionTrace(cfg, trace, clusterSpots)
		return "", 0, 0, subjectConfidence, totalReporters, false
	}

	// Apply consensus thresholds anchored on the majority winner.
	minReports := cfg.MinConsensusReports
	minAdvantage := cfg.MinAdvantage
	minConf := cfg.MinConfidencePercent
	if centerDistance >= 3 {
		minReports += cfg.Distance3ExtraReports
		minAdvantage += cfg.Distance3ExtraAdvantage
		minConf += cfg.Distance3ExtraConfidence
	}
	if bestCount < minReports {
		trace.Decision = "rejected"
		trace.Reason = "min_reports"
		trace.MinReports = minReports
		logCorrectionTrace(cfg, trace, clusterSpots)
		return "", 0, 0, subjectConfidence, totalReporters, false
	}
	if bestCount < subjectCount+minAdvantage {
		trace.Decision = "rejected"
		trace.Reason = "advantage"
		trace.MinAdvantage = minAdvantage
		logCorrectionTrace(cfg, trace, clusterSpots)
		return "", 0, 0, subjectConfidence, totalReporters, false
	}
	if bestConfidence < minConf {
		trace.Decision = "rejected"
		trace.Reason = "confidence"
		trace.MinConfidence = minConf
		logCorrectionTrace(cfg, trace, clusterSpots)
		return "", 0, 0, subjectConfidence, totalReporters, false
	}

	// Frequency-aware guard: if a nearby runner-up has comparable support, skip correction to avoid merging distinct signals.
	if runnerUp > 0 {
		freqSeparation := math.Abs(bestFreq - runnerUpFreq)
		if freqSeparation >= cfg.FreqGuardMinSeparationKHz && float64(runnerUp) >= cfg.FreqGuardRunnerUpRatio*float64(bestCount) {
			trace.Decision = "rejected"
			trace.Reason = "freq_guard"
			logCorrectionTrace(cfg, trace, clusterSpots)
			return "", 0, 0, subjectConfidence, totalReporters, false
		}
	}

	updateCallQualityForCluster(bestCall, freqHz, &cfg, clusterSpots)
	trace.Decision = "applied"
	logCorrectionTrace(cfg, trace, clusterSpots)

	return bestCall, bestCount, bestConfidence, subjectConfidence, totalReporters, true
}

// findAnchorForCall selects the closest good anchor (by mode-aware distance) for a busted call.
func findAnchorForCall(bustedCall string, freqHz float64, mode string, candidates []string, cfg *CorrectionSettings) (string, bool) {
	busted := strings.TrimSpace(bustedCall)
	if busted == "" || cfg == nil {
		return "", false
	}
	modeKey := strings.TrimSpace(mode)
	best := ""
	bestDist := math.MaxInt
	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if c == "" || c == busted {
			continue
		}
		if !callQuality.IsGood(c, freqHz, cfg) {
			continue
		}
		dist := callDistance(busted, c, modeKey, cfg.DistanceModelCW, cfg.DistanceModelRTTY)
		if cfg.MaxEditDistance >= 0 && dist > cfg.MaxEditDistance {
			continue
		}
		if dist < bestDist {
			bestDist = dist
			best = c
		}
	}
	if best == "" {
		return "", false
	}
	return best, true
}

// updateCallQualityForCluster updates the quality store after a resolved cluster.
func updateCallQualityForCluster(winnerCall string, freqHz float64, cfg *CorrectionSettings, clusterSpots []bandmap.SpotEntry) {
	if cfg == nil || winnerCall == "" || len(clusterSpots) == 0 {
		return
	}
	callQuality.Add(winnerCall, freqHz, cfg.QualityBinHz, cfg.QualityNewCallIncrement)

	distinct := make(map[string]struct{})
	for _, s := range clusterSpots {
		call := strings.TrimSpace(s.Call)
		if call == "" {
			continue
		}
		distinct[call] = struct{}{}
	}
	for call := range distinct {
		if call == strings.ToUpper(strings.TrimSpace(winnerCall)) {
			continue
		}
		callQuality.Add(call, freqHz, cfg.QualityBinHz, -cfg.QualityBustedDecrement)
	}
}

func logCorrectionTrace(cfg CorrectionSettings, tr CorrectionTrace, votes []bandmap.SpotEntry) {
	if !cfg.DebugLog || cfg.TraceLogger == nil {
		return
	}
	cfg.TraceLogger.Enqueue(CorrectionLogEntry{
		Trace: tr,
		Votes: votes,
	})
}

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
	if cfg.DistanceCacheSize <= 0 {
		cfg.DistanceCacheSize = 5000
	}
	cfg.DistanceModelCW = normalizeCWDistanceModel(cfg.DistanceModelCW)
	cfg.DistanceModelRTTY = normalizeRTTYDistanceModel(cfg.DistanceModelRTTY)
	if cfg.DistanceCacheTTL <= 0 {
		// Default TTL tracks the recency window so cached distances expire alongside supporting data.
		cfg.DistanceCacheTTL = cfg.RecencyWindow
		if cfg.DistanceCacheTTL <= 0 {
			cfg.DistanceCacheTTL = 2 * time.Minute
		}
	}
	if cfg.MinSpotterReliability < 0 {
		cfg.MinSpotterReliability = 0
	}
	return cfg
}

func minSNRThresholdForMode(mode string, cfg CorrectionSettings) int {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "CW":
		return cfg.MinSNRCW
	case "RTTY":
		return cfg.MinSNRRTTY
	default:
		return 0
	}
}

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
	mu      sync.Mutex
	buckets map[int]*correctionBucket
}

type correctionBucket struct {
	spots []*Spot
}

// NewCorrectionIndex constructs an empty index.
func NewCorrectionIndex() *CorrectionIndex {
	return &CorrectionIndex{
		buckets: make(map[int]*correctionBucket),
	}
}

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

	bucket := ci.buckets[key]
	if bucket == nil {
		bucket = &correctionBucket{}
		ci.buckets[key] = bucket
	}

	bucket.spots = pruneAndAppend(bucket.spots, s, now, window)
}

// Candidates retrieves nearby spots (within +/- 0.5 kHz) for the given subject.
func (ci *CorrectionIndex) Candidates(subject *Spot, now time.Time, window time.Duration) []*Spot {
	if ci == nil || subject == nil {
		return nil
	}
	if window <= 0 {
		window = 45 * time.Second
	}

	key := bucketKey(subject.Frequency)
	minKey := key - 5
	maxKey := key + 5

	ci.mu.Lock()
	defer ci.mu.Unlock()

	var results []*Spot
	for k := minKey; k <= maxKey; k++ {
		bucket := ci.buckets[k]
		if bucket == nil {
			continue
		}
		bucket.spots = prune(bucket.spots, now, window)
		results = append(results, bucket.spots...)
	}
	return results
}

func bucketKey(freq float64) int {
	return int(math.Round(freq * 10))
}

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

func pruneAndAppend(spots []*Spot, s *Spot, now time.Time, window time.Duration) []*Spot {
	spots = prune(spots, now, window)
	return append(spots, s)
}

// legacyLevenshtein remains for Morse/Baudot helpers; general path uses agnivade/levenshtein.
func legacyLevenshtein(a, b string) int {
	if a == b {
		return 0
	}
	r1 := []rune(a)
	r2 := []rune(b)
	len1 := len(r1)
	len2 := len(r2)
	if len1 == 0 {
		return len2
	}
	if len2 == 0 {
		return len1
	}
	prev := make([]int, len2+1)
	cur := make([]int, len2+1)
	for j := 0; j <= len2; j++ {
		prev[j] = j
	}
	for i := 1; i <= len1; i++ {
		cur[0] = i
		for j := 1; j <= len2; j++ {
			cost := 0
			if r1[i-1] != r2[j-1] {
				cost = 1
			}
			insert := cur[j-1] + 1
			delete := prev[j] + 1
			replace := prev[j-1] + cost
			cur[j] = min(insert, min(delete, replace))
		}
		prev, cur = cur, prev
	}
	return prev[len2]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

const (
	distanceModelPlain  = "plain"
	distanceModelMorse  = "morse"
	distanceModelBaudot = "baudot"
)

type distanceCacheConfig struct {
	size int
	ttl  time.Duration
}

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

// cachedCallDistance wraps callDistanceCore with an optional memoization layer.
// A preconfigured cache is supplied by the caller so we avoid reconfiguring
// shared state on every distance call.
func cachedCallDistance(subject, candidate, mode, cwModel, rttyModel string, cacheCfg distanceCacheConfig, cache *distanceCache, now time.Time) int {
	modeKey := strings.ToUpper(strings.TrimSpace(mode))
	cwModelNorm := normalizeCWDistanceModel(cwModel)
	rttyModelNorm := normalizeRTTYDistanceModel(rttyModel)

	if cacheCfg.size > 0 && cacheCfg.ttl > 0 && cache != nil {
		key := distanceCacheKey(subject, candidate, modeKey, cwModelNorm, rttyModelNorm)
		if dist, ok := cache.get(key, now); ok {
			return dist
		}
		dist := callDistanceCore(subject, candidate, modeKey, cwModelNorm, rttyModelNorm)
		cache.put(key, dist, now)
		return dist
	}
	return callDistanceCore(subject, candidate, modeKey, cwModelNorm, rttyModelNorm)
}

func distanceCacheKey(subject, candidate, mode, cwModel, rttyModel string) string {
	return strings.ToUpper(subject) + "|" + strings.ToUpper(candidate) + "|" + mode + "|" + cwModel + "|" + rttyModel
}

// callDistanceCore picks the distance function based on mode/model without caching.
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

// callDistance is retained for tests; it routes to the core distance function
// after normalizing mode/model inputs (no caching).
func callDistance(subject, candidate, mode, cwModel, rttyModel string) int {
	modeKey := strings.ToUpper(strings.TrimSpace(mode))
	return callDistanceCore(
		subject,
		candidate,
		modeKey,
		normalizeCWDistanceModel(cwModel),
		normalizeRTTYDistanceModel(rttyModel),
	)
}

// cwCallDistance computes Levenshtein at the callsign level but uses
// Morse-aware substitution costs so CW confusability is reflected in the
// distance. Insert/delete of whole characters cost 1; substitution uses a
// Morse distance table built from weighted/normalized dot/dash edits.
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

	prev := make([]int, lb+1)
	cur := make([]int, lb+1)

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

// rttyCallDistance mirrors cwCallDistance but uses Baudot/ITA2-aware costs.
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

	prev := make([]int, lb+1)
	cur := make([]int, lb+1)

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

// buildRuneCostTable creates a dense cost matrix for the provided codebook using
// the supplied cost function on the code strings. The tables are tiny (tens of
// entries) and avoid per-call dynamic programming when computing character
// distances.
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

// morsePatternCost computes a normalized, weighted edit distance between two
// Morse code strings (dot/dash). Insert/delete cost 1, dot<->dash substitution
// costs 2, then we normalize by (maxLen+1) to bound the result and scale into a
// small integer range for use as a substitution cost in cwCallDistance.
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

func getMorseWeights() morseWeightSet {
	return morseWeightSet{
		ins:   morseInsertCost,
		del:   morseDeleteCost,
		sub:   morseSubCost,
		scale: morseScale,
	}
}

// baudotPatternCost mirrors morsePatternCost but operates on ITA2 codes used
// for RTTY. It uses weighted edit costs and normalization to keep substitution
// costs small and comparable to character-level insert/delete.
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
