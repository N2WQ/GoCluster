package spot

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultQualityShards          = 64
	defaultQualityTTL             = 24 * time.Hour
	defaultQualityMaxEntries      = 200000
	defaultQualityCleanupInterval = 10 * time.Minute
)

// CallQualityOptions configures the bounded call-quality store.
type CallQualityOptions struct {
	Shards          int
	TTL             time.Duration
	MaxEntries      int
	CleanupInterval time.Duration
	PinPriors       *bool
}

// CallQualityCounts summarizes pinned and dynamic entry counts.
type CallQualityCounts struct {
	Pinned  int
	Dynamic int
}

// callFreqKey scopes a call to a frequency bin.
type callFreqKey struct {
	Call string
	Bin  int
}

// CallQualityStore tracks per-call, per-bin quality scores used as anchors.
// It is sharded and bounded: dynamic entries are evicted by TTL and size caps,
// while pinned priors are retained indefinitely when enabled.
type CallQualityStore struct {
	shards          []qualityShard
	ttl             time.Duration
	maxEntries      int
	perShardMax     int
	cleanupInterval time.Duration
	pinPriors       bool

	cleanupMu sync.Mutex
	quit      chan struct{}
}

type qualityEntry struct {
	score   int
	updated time.Time
}

type qualityShard struct {
	mu      sync.Mutex
	pinned  map[callFreqKey]int
	dynamic map[callFreqKey]*qualityEntry
}

// NewCallQualityStore constructs an empty call quality store with default bounds.
// Key aspects: Initializes sharded maps; cleanup is opt-in via StartCleanup.
// Upstream: global callQuality initialization.
// Downstream: NewCallQualityStoreWithOptions.
// NewCallQualityStore constructs a bounded quality store with defaults.
func NewCallQualityStore() *CallQualityStore {
	return NewCallQualityStoreWithOptions(CallQualityOptions{})
}

// NewCallQualityStoreWithOptions constructs a bounded call quality store with custom options.
// Key aspects: Applies defaults when options are unset.
// Upstream: ConfigureCallQualityStore and tests.
// Downstream: shard allocation.
func NewCallQualityStoreWithOptions(opts CallQualityOptions) *CallQualityStore {
	shards := opts.Shards
	if shards <= 0 {
		shards = defaultQualityShards
	}
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = defaultQualityTTL
	}
	maxEntries := opts.MaxEntries
	if maxEntries <= 0 {
		maxEntries = defaultQualityMaxEntries
	}
	cleanup := opts.CleanupInterval
	if cleanup <= 0 {
		cleanup = defaultQualityCleanupInterval
	}
	pinPriors := true
	if opts.PinPriors != nil {
		pinPriors = *opts.PinPriors
	}
	perShard := maxEntries / shards
	if maxEntries%shards != 0 {
		perShard++
	}
	if perShard <= 0 {
		perShard = 1
	}

	store := &CallQualityStore{
		shards:          make([]qualityShard, shards),
		ttl:             ttl,
		maxEntries:      maxEntries,
		perShardMax:     perShard,
		cleanupInterval: cleanup,
		pinPriors:       pinPriors,
	}
	for i := range store.shards {
		store.shards[i] = qualityShard{
			pinned:  make(map[callFreqKey]int),
			dynamic: make(map[callFreqKey]*qualityEntry, perShard),
		}
	}
	return store
}

// StartCleanup starts the periodic cleanup goroutine.
// Key aspects: Prunes expired dynamic entries at a bounded cadence.
// Upstream: ConfigureCallQualityStore or main startup.
// Downstream: cleanupLoop.
func (s *CallQualityStore) StartCleanup() {
	if s == nil || s.cleanupInterval <= 0 || s.ttl <= 0 {
		return
	}
	s.cleanupMu.Lock()
	if s.quit != nil {
		s.cleanupMu.Unlock()
		return
	}
	s.quit = make(chan struct{})
	s.cleanupMu.Unlock()

	go s.cleanupLoop()
}

// StopCleanup stops the periodic cleanup goroutine.
// Key aspects: Closes the quit channel and clears it.
// Upstream: ConfigureCallQualityStore or shutdown.
// Downstream: channel close only.
func (s *CallQualityStore) StopCleanup() {
	if s == nil {
		return
	}
	s.cleanupMu.Lock()
	if s.quit != nil {
		close(s.quit)
		s.quit = nil
	}
	s.cleanupMu.Unlock()
}

func (s *CallQualityStore) cleanupLoop() {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.cleanup(time.Now().UTC())
		case <-s.quit:
			return
		}
	}
}

func (s *CallQualityStore) cleanup(now time.Time) {
	if s == nil || s.ttl <= 0 {
		return
	}
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		s.pruneExpiredLocked(shard, now)
		shard.mu.Unlock()
	}
}

// ConfigureCallQualityStore configures the global call-quality store with bounded defaults.
// Key aspects: Stops prior cleanup goroutine and swaps the global store.
// Upstream: main startup/config loading.
// Downstream: NewCallQualityStoreWithOptions and StartCleanup.
func ConfigureCallQualityStore(ttl time.Duration, maxEntries int, cleanupInterval time.Duration, pinPriors bool) {
	opts := CallQualityOptions{
		TTL:             ttl,
		MaxEntries:      maxEntries,
		CleanupInterval: cleanupInterval,
		PinPriors:       &pinPriors,
	}
	store := NewCallQualityStoreWithOptions(opts)
	store.StartCleanup()
	old := callQuality.Swap(store)
	if old != nil {
		old.StopCleanup()
	}
}

// Purpose: Convert a frequency into a bin index.
// Key aspects: Uses bin size and returns -1 for global priors.
// Upstream: CallQualityStore methods.
// Downstream: integer math.
// freqBinHz returns the integer bin for a given frequency in Hz and bin size.
func freqBinHz(freqHz float64, binSizeHz int) int {
	if binSizeHz <= 0 {
		binSizeHz = 1000
	}
	if freqHz <= 0 {
		return -1 // sentinel for global priors that apply to all bins
	}
	return int(freqHz) / binSizeHz
}

// Get retrieves the quality score for a call/bin.
// Key aspects: Falls back to global prior bin (-1) when no bin-specific entry exists.
// Upstream: correction logic and IsGood.
// Downstream: freqBinHz and shard access.
// Get returns the quality score for a call in the given bin.
func (s *CallQualityStore) Get(call string, freqHz float64, binSizeHz int) int {
	return s.getWithTime(call, freqHz, binSizeHz, time.Now().UTC())
}

func (s *CallQualityStore) getWithTime(call string, freqHz float64, binSizeHz int, now time.Time) int {
	if s == nil {
		return 0
	}
	call = strings.ToUpper(strings.TrimSpace(call))
	if call == "" {
		return 0
	}
	bin := freqBinHz(freqHz, binSizeHz)
	if score, present := s.scoreFor(call, bin, now); present {
		return score
	}
	if bin != -1 {
		if score, _ := s.scoreFor(call, -1, now); score != 0 {
			return score
		}
	}
	return 0
}

// Add adjusts a call/bin quality score by delta.
// Key aspects: Normalizes call and skips zero deltas.
// Upstream: correction anchors and priors loading.
// Downstream: freqBinHz and shard mutation.
// Add adjusts the dynamic quality score for a call/bin by delta.
func (s *CallQualityStore) Add(call string, freqHz float64, binSizeHz int, delta int) {
	s.addWithTime(call, freqHz, binSizeHz, delta, time.Now().UTC())
}

func (s *CallQualityStore) addWithTime(call string, freqHz float64, binSizeHz int, delta int, now time.Time) {
	if s == nil {
		return
	}
	call = strings.ToUpper(strings.TrimSpace(call))
	if call == "" || delta == 0 {
		return
	}
	key := callFreqKey{Call: call, Bin: freqBinHz(freqHz, binSizeHz)}
	shard := s.shardFor(call, key.Bin)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	entry := shard.dynamic[key]
	if entry != nil && s.ttl > 0 && now.Sub(entry.updated) > s.ttl {
		delete(shard.dynamic, key)
		entry = nil
	}
	if entry == nil {
		shard.dynamic[key] = &qualityEntry{score: delta, updated: now}
	} else {
		entry.score += delta
		entry.updated = now
	}
	if s.perShardMax > 0 && len(shard.dynamic) > s.perShardMax {
		if s.ttl > 0 {
			s.pruneExpiredLocked(shard, now)
		}
		if len(shard.dynamic) > s.perShardMax {
			s.evictOldestLocked(shard)
		}
	}
}

// AddPinned inserts a pinned prior (non-expiring when enabled).
// Key aspects: Adds to pinned map or falls back to dynamic when pinning disabled.
// Upstream: LoadCallQualityPriors.
// Downstream: shard map mutation.
func (s *CallQualityStore) AddPinned(call string, freqHz float64, binSizeHz int, score int) {
	if s == nil {
		return
	}
	if !s.pinPriors {
		s.Add(call, freqHz, binSizeHz, score)
		return
	}
	call = strings.ToUpper(strings.TrimSpace(call))
	if call == "" || score == 0 {
		return
	}
	key := callFreqKey{Call: call, Bin: freqBinHz(freqHz, binSizeHz)}
	shard := s.shardFor(call, key.Bin)
	shard.mu.Lock()
	shard.pinned[key] = shard.pinned[key] + score
	shard.mu.Unlock()
}

// IsGood reports whether a call meets the quality threshold.
// Key aspects: Uses config bin size and threshold.
// Upstream: call correction decision logic.
// Downstream: Get.
// IsGood reports whether the call meets the configured quality threshold in the bin.
func (s *CallQualityStore) IsGood(call string, freqHz float64, cfg *CorrectionSettings) bool {
	if cfg == nil {
		return false
	}
	return s.Get(call, freqHz, cfg.QualityBinHz) >= cfg.QualityGoodThreshold
}

// Counts returns total pinned/dynamic entries across all shards.
func (s *CallQualityStore) Counts() CallQualityCounts {
	if s == nil {
		return CallQualityCounts{}
	}
	counts := CallQualityCounts{}
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		counts.Pinned += len(shard.pinned)
		counts.Dynamic += len(shard.dynamic)
		shard.mu.Unlock()
	}
	return counts
}

func (s *CallQualityStore) scoreFor(call string, bin int, now time.Time) (int, bool) {
	if s == nil {
		return 0, false
	}
	shard := s.shardFor(call, bin)
	key := callFreqKey{Call: call, Bin: bin}
	shard.mu.Lock()
	defer shard.mu.Unlock()
	score := 0
	present := false
	if v, ok := shard.pinned[key]; ok {
		score += v
		present = true
	}
	if entry, ok := shard.dynamic[key]; ok {
		if entry == nil {
			delete(shard.dynamic, key)
		} else if s.ttl > 0 && now.Sub(entry.updated) > s.ttl {
			delete(shard.dynamic, key)
		} else {
			score += entry.score
			present = true
		}
	}
	return score, present
}

func (s *CallQualityStore) pruneExpiredLocked(shard *qualityShard, now time.Time) {
	if s == nil || shard == nil || s.ttl <= 0 {
		return
	}
	for key, entry := range shard.dynamic {
		if entry == nil {
			delete(shard.dynamic, key)
			continue
		}
		if now.Sub(entry.updated) > s.ttl {
			delete(shard.dynamic, key)
		}
	}
}

func (s *CallQualityStore) evictOldestLocked(shard *qualityShard) {
	if s == nil || shard == nil || s.perShardMax <= 0 {
		return
	}
	keep := s.perShardMax
	if keep >= 10 {
		keep = s.perShardMax - s.perShardMax/10
		if keep < 1 {
			keep = 1
		}
	}
	remove := len(shard.dynamic) - keep
	if remove <= 0 {
		return
	}
	items := make([]qualityEntryItem, 0, len(shard.dynamic))
	for key, entry := range shard.dynamic {
		if entry == nil {
			continue
		}
		items = append(items, qualityEntryItem{key: key, updated: entry.updated})
	}
	if len(items) == 0 {
		return
	}
	sort.Slice(items, func(i, j int) bool { return items[i].updated.Before(items[j].updated) })
	if remove > len(items) {
		remove = len(items)
	}
	for i := 0; i < remove; i++ {
		delete(shard.dynamic, items[i].key)
	}
}

type qualityEntryItem struct {
	key     callFreqKey
	updated time.Time
}

func (s *CallQualityStore) shardFor(call string, bin int) *qualityShard {
	if s == nil || len(s.shards) == 0 {
		return nil
	}
	hash := uint32(2166136261)
	for i := 0; i < len(call); i++ {
		hash ^= uint32(call[i])
		hash *= 16777619
	}
	hash ^= uint32(bin)
	hash *= 16777619
	return &s.shards[hash%uint32(len(s.shards))]
}

// callQuality is the shared store used by correction anchors.
var callQuality atomic.Pointer[CallQualityStore]

func init() {
	store := NewCallQualityStore()
	store.StartCleanup()
	callQuality.Store(store)
}

// GlobalCallQualityCounts returns pinned/dynamic counts for the global store.
func GlobalCallQualityCounts() CallQualityCounts {
	store := callQuality.Load()
	if store == nil {
		return CallQualityCounts{}
	}
	return store.Counts()
}

func currentCallQuality() *CallQualityStore {
	return callQuality.Load()
}
