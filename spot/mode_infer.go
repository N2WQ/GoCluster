package spot

import (
	"container/list"
	"log"
	"strings"
	"time"
)

// ModeInferenceSettings configures how the cluster assigns a mode when the
// spot comment lacks an explicit mode token.
//
// The inference pipeline is:
//  1. Explicit mode (already set on the spot)
//  2. Recent DXcall+frequency cache (integer kHz)
//  3. Digital frequency map (explicit skimmer/PSKReporter evidence + seeds)
//  4. Allocation-based fallback (FinalizeMode)
//
// All caches are bounded and TTL-limited to keep memory stable under load.
type ModeInferenceSettings struct {
	DXFreqCacheTTL        time.Duration
	DXFreqCacheSize       int
	DigitalWindow         time.Duration
	DigitalMinCorroborate int
	DigitalSeedTTL        time.Duration
	DigitalCacheSize      int
	DigitalSeeds          []ModeSeed
}

// ModeSeed pre-populates the digital frequency map with a known mode.
// Seeds expire unless refreshed by explicit skimmer/PSKReporter spots.
type ModeSeed struct {
	FrequencyKHz int
	Mode         string
}

// ModeAssigner applies the agreed mode inference pipeline.
// It is designed for single-goroutine use in the output pipeline and is not
// concurrency-safe without external synchronization.
type ModeAssigner struct {
	now      func() time.Time
	fallback func(freqKHz float64) string
	dxCache  *dxFreqCache
	digital  *digitalFreqMap
}

// Purpose: Construct a ModeAssigner with defaults applied.
// Key aspects: Wires cache sizes/TTLs and fallback allocation logic.
// Upstream: main startup wiring.
// Downstream: newModeAssigner.
// NewModeAssigner builds a mode assigner using the provided settings.
func NewModeAssigner(settings ModeInferenceSettings) *ModeAssigner {
	return newModeAssigner(settings, time.Now, func(freq float64) string {
		return FinalizeMode("", freq)
	})
}

// Purpose: Internal constructor with injected time and fallback functions.
// Key aspects: Normalizes settings and seeds the digital frequency map.
// Upstream: NewModeAssigner and tests.
// Downstream: newDXFreqCache, newDigitalFreqMap, seedDigitalMap.
func newModeAssigner(settings ModeInferenceSettings, now func() time.Time, fallback func(freqKHz float64) string) *ModeAssigner {
	if settings.DXFreqCacheTTL <= 0 {
		settings.DXFreqCacheTTL = 5 * time.Minute
	}
	if settings.DXFreqCacheSize <= 0 {
		settings.DXFreqCacheSize = 50000
	}
	if settings.DigitalWindow <= 0 {
		settings.DigitalWindow = 5 * time.Minute
	}
	if settings.DigitalMinCorroborate <= 0 {
		settings.DigitalMinCorroborate = 10
	}
	if settings.DigitalSeedTTL <= 0 {
		settings.DigitalSeedTTL = 6 * time.Hour
	}
	if settings.DigitalCacheSize <= 0 {
		settings.DigitalCacheSize = 5000
	}
	assigner := &ModeAssigner{
		now:      now,
		fallback: fallback,
		dxCache:  newDXFreqCache(settings.DXFreqCacheSize, settings.DXFreqCacheTTL),
		digital:  newDigitalFreqMap(settings.DigitalCacheSize, settings.DigitalWindow, settings.DigitalMinCorroborate, settings.DigitalSeedTTL),
	}
	assigner.seedDigitalMap(settings.DigitalSeeds)
	return assigner
}

// Assign fills in the spot.Mode when it is missing. The caller supplies whether
// the mode was explicitly present in the comment/source before inference so
// we can avoid feeding inferred data back into the digital map.
// Purpose: Assign a mode to a spot using the inference pipeline.
// Key aspects: Checks explicit mode, DX+freq cache, digital map, then fallback.
// Upstream: processOutputSpots.
// Downstream: dxCache, digital map, and fallback.
func (a *ModeAssigner) Assign(s *Spot, explicitMode bool) string {
	if a == nil || s == nil {
		return ""
	}
	now := a.now()

	// Respect explicit modes first.
	if explicitMode {
		mode := NormalizeVoiceMode(s.Mode, s.Frequency)
		mode = strings.ToUpper(strings.TrimSpace(mode))
		s.Mode = mode
		if a.shouldObserveDigital(s, mode) {
			a.digital.Observe(freqKeyFromSpot(s), mode, s.DECall, now)
		}
		a.dxCache.Set(dxKeyFromSpot(s), mode, now)
		return mode
	}

	key := dxKeyFromSpot(s)
	if cached, ok := a.dxCache.Get(key, now); ok {
		s.Mode = cached
		a.dxCache.Set(key, cached, now)
		return cached
	}

	if inferred := a.digital.Infer(freqKeyFromSpot(s), now); inferred != "" {
		s.Mode = inferred
		a.dxCache.Set(key, inferred, now)
		return inferred
	}

	mode := ""
	if a.fallback != nil {
		mode = a.fallback(s.Frequency)
	}
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if mode != "" {
		s.Mode = mode
		a.dxCache.Set(key, mode, now)
	}
	return mode
}

// Purpose: Decide whether to feed a spot into the digital map.
// Key aspects: Only explicit modes from skimmer sources are observed.
// Upstream: Assign explicit-mode branch.
// Downstream: IsSkimmerSource and isSeedMode.
func (a *ModeAssigner) shouldObserveDigital(s *Spot, mode string) bool {
	if s == nil {
		return false
	}
	if !isSeedMode(mode) {
		return false
	}
	if !IsSkimmerSource(s.SourceType) {
		return false
	}
	return true
}

// Purpose: Seed the digital frequency map with configured modes.
// Key aspects: Validates seed modes and uses seed TTL handling.
// Upstream: newModeAssigner.
// Downstream: digital.Seed and isSeedMode.
func (a *ModeAssigner) seedDigitalMap(seeds []ModeSeed) {
	if a == nil || a.digital == nil || len(seeds) == 0 {
		return
	}
	now := a.now()
	for _, seed := range seeds {
		if seed.FrequencyKHz <= 0 {
			continue
		}
		mode := strings.ToUpper(strings.TrimSpace(seed.Mode))
		if !isSeedMode(mode) {
			log.Printf("Mode inference: ignoring unsupported digital seed mode %q for %d kHz", seed.Mode, seed.FrequencyKHz)
			continue
		}
		a.digital.Seed(seed.FrequencyKHz, mode, now)
	}
}

// dxFreqKey identifies a DX call + integer kHz bucket.
type dxFreqKey struct {
	call string
	freq int
}

func dxKeyFromSpot(s *Spot) dxFreqKey {
	// Purpose: Build the DX+frequency cache key for a spot.
	// Key aspects: Normalizes DX call and uses integer kHz bucket.
	// Upstream: Assign.
	// Downstream: NormalizeCallsign and freqKeyFromSpot.
	call := s.DXCallNorm
	if call == "" {
		call = NormalizeCallsign(s.DXCall)
	}
	return dxFreqKey{
		call: call,
		freq: freqKeyFromSpot(s),
	}
}

// freqKeyFromSpot truncates the spot frequency to integer kHz, matching the
// agreed DXcall+frequency cache key.
// Purpose: Convert spot frequency to integer kHz for cache keys.
// Key aspects: Rejects nil/invalid frequency.
// Upstream: Assign and dxKeyFromSpot.
// Downstream: None (pure conversion).
func freqKeyFromSpot(s *Spot) int {
	if s == nil {
		return 0
	}
	if s.Frequency <= 0 {
		return 0
	}
	return int(s.Frequency)
}

type dxFreqCache struct {
	max     int
	ttl     time.Duration
	lru     *list.List
	entries map[dxFreqKey]*list.Element
}

type dxFreqEntry struct {
	key      dxFreqKey
	mode     string
	lastSeen time.Time
}

func newDXFreqCache(max int, ttl time.Duration) *dxFreqCache {
	// Purpose: Construct the DX+frequency cache with LRU eviction.
	// Key aspects: Enforces minimum size and stores TTL.
	// Upstream: newModeAssigner.
	// Downstream: list.New and map allocation.
	if max < 1 {
		max = 1
	}
	return &dxFreqCache{
		max:     max,
		ttl:     ttl,
		lru:     list.New(),
		entries: make(map[dxFreqKey]*list.Element),
	}
}

func (c *dxFreqCache) Get(key dxFreqKey, now time.Time) (string, bool) {
	// Purpose: Retrieve a cached mode for a DX+frequency key.
	// Key aspects: Enforces TTL and refreshes LRU position.
	// Upstream: Assign cache check.
	// Downstream: list operations and map access.
	if c == nil {
		return "", false
	}
	if elem, ok := c.entries[key]; ok {
		entry := elem.Value.(*dxFreqEntry)
		if c.ttl > 0 && now.Sub(entry.lastSeen) > c.ttl {
			c.lru.Remove(elem)
			delete(c.entries, key)
			return "", false
		}
		c.lru.MoveToFront(elem)
		return entry.mode, true
	}
	return "", false
}

func (c *dxFreqCache) Set(key dxFreqKey, mode string, now time.Time) {
	// Purpose: Store a mode for a DX+frequency key.
	// Key aspects: Updates LRU and evicts oldest when full.
	// Upstream: Assign after inference.
	// Downstream: evictOldest and list operations.
	if c == nil || key.call == "" || key.freq <= 0 || strings.TrimSpace(mode) == "" {
		return
	}
	if elem, ok := c.entries[key]; ok {
		entry := elem.Value.(*dxFreqEntry)
		entry.mode = mode
		entry.lastSeen = now
		c.lru.MoveToFront(elem)
		return
	}
	entry := &dxFreqEntry{key: key, mode: mode, lastSeen: now}
	elem := c.lru.PushFront(entry)
	c.entries[key] = elem
	if c.lru.Len() > c.max {
		c.evictOldest()
	}
}

func (c *dxFreqCache) evictOldest() {
	// Purpose: Remove the least recently used entry.
	// Key aspects: No-op on empty cache.
	// Upstream: dxFreqCache.Set.
	// Downstream: list back removal and map delete.
	if c == nil {
		return
	}
	elem := c.lru.Back()
	if elem == nil {
		return
	}
	entry := elem.Value.(*dxFreqEntry)
	delete(c.entries, entry.key)
	c.lru.Remove(elem)
}

type digitalFreqMap struct {
	max              int
	window           time.Duration
	minCorroborators int
	seedTTL          time.Duration
	lru              *list.List
	entries          map[int]*list.Element
}

type digitalFreqEntry struct {
	freq     int
	lastSeen time.Time
	modes    map[string]*digitalModeEvidence
}

type digitalModeEvidence struct {
	spotters     map[string]time.Time
	seedUntil    time.Time
	lastExplicit time.Time
}

const maxDigitalSpottersPerMode = 2048

func newDigitalFreqMap(max int, window time.Duration, minCorroborators int, seedTTL time.Duration) *digitalFreqMap {
	// Purpose: Construct the digital frequency map with LRU eviction.
	// Key aspects: Normalizes min values and records corroborator thresholds.
	// Upstream: newModeAssigner.
	// Downstream: list.New and map allocation.
	if max < 1 {
		max = 1
	}
	if minCorroborators < 1 {
		minCorroborators = 1
	}
	return &digitalFreqMap{
		max:              max,
		window:           window,
		minCorroborators: minCorroborators,
		seedTTL:          seedTTL,
		lru:              list.New(),
		entries:          make(map[int]*list.Element),
	}
}

func (m *digitalFreqMap) Seed(freq int, mode string, now time.Time) {
	// Purpose: Seed a frequency bucket with a known digital mode.
	// Key aspects: Applies seed TTL and touches LRU.
	// Upstream: ModeAssigner.seedDigitalMap.
	// Downstream: getOrCreate and ensureMode.
	if m == nil || freq <= 0 || strings.TrimSpace(mode) == "" {
		return
	}
	entry := m.getOrCreate(freq, now)
	evidence := entry.ensureMode(mode)
	evidence.seedUntil = now.Add(m.seedTTL)
	entry.lastSeen = now
	m.touch(entry)
	m.evictIfNeeded()
}

func (m *digitalFreqMap) Observe(freq int, mode string, spotter string, now time.Time) {
	// Purpose: Record a skimmer observation for a digital mode at a frequency.
	// Key aspects: Normalizes spotter, prunes old evidence, caps spotter count.
	// Upstream: ModeAssigner.Assign explicit mode path.
	// Downstream: getOrCreate, ensureMode, pruneSpotters.
	if m == nil || freq <= 0 || strings.TrimSpace(mode) == "" {
		return
	}
	spotter = NormalizeCallsign(spotter)
	if spotter == "" {
		return
	}
	entry := m.getOrCreate(freq, now)
	evidence := entry.ensureMode(mode)
	m.pruneSpotters(evidence, now)
	if len(evidence.spotters) >= maxDigitalSpottersPerMode {
		m.evictOldestSpotter(evidence)
	}
	evidence.spotters[spotter] = now
	evidence.lastExplicit = now
	if !evidence.seedUntil.IsZero() && m.seedTTL > 0 {
		evidence.seedUntil = now.Add(m.seedTTL)
	}
	entry.lastSeen = now
	m.touch(entry)
	m.evictIfNeeded()
}

func (m *digitalFreqMap) Infer(freq int, now time.Time) string {
	// Purpose: Infer a digital mode for a frequency using evidence and seeds.
	// Key aspects: Prefers corroborated explicit evidence, falls back to seeds.
	// Upstream: ModeAssigner.Assign when no cache hit.
	// Downstream: pruneSpotters, pruneEntryIfEmpty, and LRU touch.
	if m == nil || freq <= 0 {
		return ""
	}
	elem, ok := m.entries[freq]
	if !ok {
		return ""
	}
	entry := elem.Value.(*digitalFreqEntry)
	bestMode := ""
	bestCount := 0
	bestSeen := time.Time{}
	seedCandidate := ""

	for mode, evidence := range entry.modes {
		m.pruneSpotters(evidence, now)
		count := len(evidence.spotters)
		if !evidence.seedUntil.IsZero() && now.After(evidence.seedUntil) {
			evidence.seedUntil = time.Time{}
		}
		if count >= m.minCorroborators {
			if count > bestCount || (count == bestCount && evidence.lastExplicit.After(bestSeen)) {
				bestMode = mode
				bestCount = count
				bestSeen = evidence.lastExplicit
			}
			continue
		}
		if evidence.seedUntil.After(now) {
			if seedCandidate == "" {
				seedCandidate = mode
			} else if seedCandidate != mode {
				seedCandidate = ""
			}
		}
	}

	if bestMode != "" {
		entry.lastSeen = now
		m.touch(entry)
		return bestMode
	}
	if seedCandidate != "" {
		entry.lastSeen = now
		m.touch(entry)
		return seedCandidate
	}
	m.pruneEntryIfEmpty(entry, freq)
	return ""
}

func (m *digitalFreqMap) getOrCreate(freq int, now time.Time) *digitalFreqEntry {
	// Purpose: Fetch or create a frequency entry and update LRU.
	// Key aspects: Refreshes lastSeen and moves to front.
	// Upstream: Seed and Observe.
	// Downstream: list operations and map access.
	if elem, ok := m.entries[freq]; ok {
		entry := elem.Value.(*digitalFreqEntry)
		entry.lastSeen = now
		m.touch(entry)
		return entry
	}
	entry := &digitalFreqEntry{
		freq:     freq,
		lastSeen: now,
		modes:    make(map[string]*digitalModeEvidence),
	}
	elem := m.lru.PushFront(entry)
	m.entries[freq] = elem
	return entry
}

func (m *digitalFreqMap) touch(entry *digitalFreqEntry) {
	// Purpose: Move the entry to the front of the LRU list.
	// Key aspects: No-op when entry is nil or missing.
	// Upstream: getOrCreate and Infer.
	// Downstream: list.MoveToFront.
	if m == nil || entry == nil {
		return
	}
	if elem, ok := m.entries[entry.freq]; ok {
		m.lru.MoveToFront(elem)
	}
}

func (m *digitalFreqMap) evictIfNeeded() {
	// Purpose: Evict oldest frequency entries when over capacity.
	// Key aspects: Repeats until size is within limit.
	// Upstream: Seed and Observe.
	// Downstream: list.Back and map delete.
	if m == nil {
		return
	}
	for m.lru.Len() > m.max {
		elem := m.lru.Back()
		if elem == nil {
			return
		}
		entry := elem.Value.(*digitalFreqEntry)
		delete(m.entries, entry.freq)
		m.lru.Remove(elem)
	}
}

func (entry *digitalFreqEntry) ensureMode(mode string) *digitalModeEvidence {
	// Purpose: Ensure a mode evidence bucket exists for a frequency.
	// Key aspects: Normalizes mode and allocates spotter map.
	// Upstream: Seed and Observe.
	// Downstream: strings.ToUpper and map allocation.
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if mode == "" {
		return nil
	}
	if evidence, ok := entry.modes[mode]; ok {
		return evidence
	}
	evidence := &digitalModeEvidence{spotters: make(map[string]time.Time)}
	entry.modes[mode] = evidence
	return evidence
}

func (m *digitalFreqMap) pruneSpotters(evidence *digitalModeEvidence, now time.Time) {
	// Purpose: Remove spotter observations outside the evidence window.
	// Key aspects: Uses m.window cutoff to prune stale entries.
	// Upstream: Observe and Infer.
	// Downstream: map delete.
	if m == nil || evidence == nil || len(evidence.spotters) == 0 {
		return
	}
	cutoff := now.Add(-m.window)
	for call, seen := range evidence.spotters {
		if seen.Before(cutoff) {
			delete(evidence.spotters, call)
		}
	}
}

func (m *digitalFreqMap) evictOldestSpotter(evidence *digitalModeEvidence) {
	// Purpose: Evict the oldest spotter to cap per-mode memory.
	// Key aspects: Picks minimum lastSeen timestamp.
	// Upstream: Observe.
	// Downstream: map delete.
	if evidence == nil || len(evidence.spotters) == 0 {
		return
	}
	var (
		oldestCall string
		oldestSeen time.Time
	)
	for call, seen := range evidence.spotters {
		if oldestCall == "" || seen.Before(oldestSeen) {
			oldestCall = call
			oldestSeen = seen
		}
	}
	if oldestCall != "" {
		delete(evidence.spotters, oldestCall)
	}
}

func (m *digitalFreqMap) pruneEntryIfEmpty(entry *digitalFreqEntry, freq int) {
	// Purpose: Drop empty mode entries and remove empty frequency buckets.
	// Key aspects: Clears modes with no spotters and no seed TTL.
	// Upstream: Infer.
	// Downstream: map delete and list.Remove.
	if m == nil || entry == nil {
		return
	}
	for mode, evidence := range entry.modes {
		if len(evidence.spotters) == 0 && evidence.seedUntil.IsZero() {
			delete(entry.modes, mode)
		}
	}
	if len(entry.modes) == 0 {
		if elem, ok := m.entries[freq]; ok {
			delete(m.entries, freq)
			m.lru.Remove(elem)
		}
	}
}

func isSeedMode(mode string) bool {
	// Purpose: Check if a mode is eligible for digital seeding.
	// Key aspects: Limits to FT4/FT8/JS8.
	// Upstream: seedDigitalMap and shouldObserveDigital.
	// Downstream: strings.ToUpper.
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "FT4", "FT8", "JS8":
		return true
	default:
		return false
	}
}
