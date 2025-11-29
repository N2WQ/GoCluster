package spot

import (
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
}

var correctionEligibleModes = map[string]struct{}{
	"CW":   {},
	"RTTY": {},
	"SSB":  {},
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

var sharedDistanceCache distanceCache

// SetFrequencyToleranceHz updates the frequency similarity window in Hz.
func SetFrequencyToleranceHz(hz float64) {
	if hz <= 0 {
		frequencyToleranceKHz = 0.5
		return
	}
	frequencyToleranceKHz = hz / 1000.0
}

// SuggestCallCorrection analyzes recent spots on the same frequency and determines
// whether there is overwhelming evidence that the subject spot's DX call should
// be corrected. IMPORTANT: This function only suggests a correction. The caller
// (e.g., the main pipeline when call correction is enabled) decides whether to
// apply it and is responsible for updating any caches or deduplication structures.
//
// Parameters:
//   - subject: the spot we are evaluating.
//   - others: a slice of other recent spots (e.g., from a ring buffer or stat tracker).
//   - settings: consensus thresholds (min reporters, freshness).
//   - now: the time reference used to evaluate recency. Passing it as an argument
//     rather than calling time.Now() simplifies deterministic testing.
//
// Returns:
//   - correctedCall: the most likely callsign if consensus is met.
//   - supporters: how many unique spotters contributed to the correction.
//   - ok: true if a correction is recommended, false otherwise.
func SuggestCallCorrection(subject *Spot, others []*Spot, settings CorrectionSettings, now time.Time) (correctedCall string, supporters int, correctedConfidence int, subjectConfidence int, totalReporters int, ok bool) {
	if subject == nil {
		return "", 0, 0, 0, 0, false
	}

	cfg := normalizeCorrectionSettings(settings)
	subjectCall := strings.ToUpper(strings.TrimSpace(subject.DXCall))
	if subjectCall == "" {
		return "", 0, 0, 0, 0, false
	}
	subjectReporter := strings.ToUpper(strings.TrimSpace(subject.DECall))
	subjectVotes := map[string]struct{}{}
	allReporters := map[string]struct{}{}
	if subjectReporter != "" && passesSNRThreshold(subject, cfg) {
		subjectVotes[subjectReporter] = struct{}{}
		allReporters[subjectReporter] = struct{}{}
	}

	type candidate struct {
		reporters map[string]struct{}
		lastSeen  time.Time
	}

	candidates := make(map[string]*candidate)

	for _, other := range others {
		if other == nil {
			continue
		}
		otherCall := strings.ToUpper(strings.TrimSpace(other.DXCall))
		if otherCall == "" {
			continue
		}
		// Normalize spotter identifier.
		reporter := strings.ToUpper(strings.TrimSpace(other.DECall))
		if reporter == "" {
			continue
		}
		if !passesSNRThreshold(other, cfg) {
			continue
		}
		allReporters[reporter] = struct{}{}
		if reporter == subjectReporter {
			if otherCall == subjectCall {
				subjectVotes[reporter] = struct{}{}
			}
			continue
		}
		// Require frequency overlap within the tolerance window.
		if math.Abs(other.Frequency-subject.Frequency) > frequencyToleranceKHz {
			continue
		}
		// Enforce recency: the supporting spot must be newer than (now - recency window).
		if now.Sub(other.Time) > cfg.RecencyWindow {
			continue
		}

		if otherCall == subjectCall {
			subjectVotes[reporter] = struct{}{}
			continue
		}

		stats, exists := candidates[otherCall]
		if !exists {
			stats = &candidate{
				reporters: make(map[string]struct{}),
			}
			candidates[otherCall] = stats
		}
		stats.reporters[reporter] = struct{}{}
		if other.Time.After(stats.lastSeen) {
			stats.lastSeen = other.Time
		}
	}

	var (
		bestCall       string
		bestCount      int
		bestTime       time.Time
		bestConfidence int
	)
	subjectCount := len(subjectVotes)
	totalReporters = len(allReporters)
	if totalReporters == 0 {
		return "", 0, 0, 0, 0, false
	}
	subjectConfidence = len(subjectVotes) * 100 / totalReporters

	for call, stats := range candidates {
		count := len(stats.reporters)
		if count < cfg.MinConsensusReports {
			continue
		}
		if count < subjectCount+cfg.MinAdvantage {
			continue
		}
		cacheCfg := distanceCacheConfig{
			size: cfg.DistanceCacheSize,
			ttl:  cfg.DistanceCacheTTL,
		}
		distance := cachedCallDistance(subjectCall, call, subject.Mode, cfg.DistanceModelCW, cfg.DistanceModelRTTY, cacheCfg, now)
		if cfg.MaxEditDistance >= 0 && distance > cfg.MaxEditDistance {
			continue
		}
		// Apply stricter consensus requirements for more distant corrections so we
		// don't accept a larger edit with the same evidence as a near edit.
		minReports := cfg.MinConsensusReports
		minAdvantage := cfg.MinAdvantage
		minConf := cfg.MinConfidencePercent
		if distance >= 3 {
			minReports += cfg.Distance3ExtraReports     // require more unique supporters
			minAdvantage += cfg.Distance3ExtraAdvantage // require a larger lead over the subject call
			minConf += cfg.Distance3ExtraConfidence     // require a higher share of total reporters
		}
		if count < minReports {
			continue
		}
		if count < subjectCount+minAdvantage {
			continue
		}
		confidence := count * 100 / totalReporters
		if confidence < minConf {
			continue
		}
		// Prefer the candidate with the most unique spotters. In a tie, take the
		// most recent one so we gravitate toward the freshest consensus.
		if count > bestCount || (count == bestCount && stats.lastSeen.After(bestTime)) {
			bestCall = call
			bestCount = count
			bestTime = stats.lastSeen
			bestConfidence = confidence
		}
	}

	if bestCall == "" {
		return "", 0, 0, subjectConfidence, totalReporters, false
	}
	return bestCall, bestCount, bestConfidence, subjectConfidence, totalReporters, true
}

// normalizeCorrectionSettings fills in safe defaults so callers can omit config
// while unit tests can deliberately pass tiny values.
func normalizeCorrectionSettings(settings CorrectionSettings) CorrectionSettings {
	cfg := settings
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

// cachedCallDistance wraps callDistanceCore with an optional memoization layer
// controlled by size/ttl. It normalizes mode/models before building the cache key.
func cachedCallDistance(subject, candidate, mode, cwModel, rttyModel string, cacheCfg distanceCacheConfig, now time.Time) int {
	modeKey := strings.ToUpper(strings.TrimSpace(mode))
	cwModelNorm := normalizeCWDistanceModel(cwModel)
	rttyModelNorm := normalizeRTTYDistanceModel(rttyModel)

	if cacheCfg.size > 0 && cacheCfg.ttl > 0 {
		sharedDistanceCache.configure(cacheCfg.size, cacheCfg.ttl)
		key := distanceCacheKey(subject, candidate, modeKey, cwModelNorm, rttyModelNorm)
		if dist, ok := sharedDistanceCache.get(key, now); ok {
			return dist
		}
		dist := callDistanceCore(subject, candidate, modeKey, cwModelNorm, rttyModelNorm)
		sharedDistanceCache.put(key, dist, now)
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

// cwCallDistance computes Levenshtein at the callsign level but uses Morse-aware
// substitution costs so CW confusability is reflected in the distance.
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
	morseRuneIndex, morseCostTable = buildRuneCostTable(morseCodes)
	baudotRuneIndex, baudotCostTable = buildRuneCostTable(baudotCodes)
}

// buildRuneCostTable creates a dense cost matrix for the provided codebook using
// legacyLevenshtein on the code strings. The tables are tiny (tens of entries)
// and avoid per-call dynamic programming when computing character distances.
func buildRuneCostTable(codebook map[rune]string) (map[rune]int, [][]int) {
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
			table[i][j] = legacyLevenshtein(a, b)
		}
	}
	return index, table
}
