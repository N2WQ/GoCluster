package spot

import (
	"sort"
	"strings"
	"sync"
	"time"

	"dxcluster/strutil"
)

const (
	defaultRecentBandShards              = 64
	defaultRecentBandMaxEntries          = 200000
	defaultRecentBandCleanupInterval     = 10 * time.Minute
	defaultRecentBandMaxSpottersPerEntry = 64
)

// RecentBandOptions configures the bounded recent-on-band store.
type RecentBandOptions struct {
	Window             time.Duration
	Shards             int
	MaxEntries         int
	CleanupInterval    time.Duration
	MaxSpottersPerCall int
}

type recentBandKey struct {
	call string
	band string
	mode string
}

type recentBandEntry struct {
	spotters map[string]time.Time
	lastSeen time.Time
}

type recentBandShard struct {
	mu      sync.Mutex
	entries map[recentBandKey]*recentBandEntry
}

// RecentBandStore tracks recently observed call/band/mode tuples with bounded
// memory. Admission is based on unique spotters seen within the configured
// window; stale spotters and entries are pruned on reads, writes, and periodic
// cleanup.
type RecentBandStore struct {
	shards             []recentBandShard
	window             time.Duration
	maxEntries         int
	perShardMax        int
	cleanupInterval    time.Duration
	maxSpottersPerCall int

	cleanupMu sync.Mutex
	quit      chan struct{}
}

// NewRecentBandStore returns a bounded recent-on-band store with defaults.
func NewRecentBandStore(window time.Duration) *RecentBandStore {
	return NewRecentBandStoreWithOptions(RecentBandOptions{Window: window})
}

// NewRecentBandStoreWithOptions returns a bounded recent-on-band store.
func NewRecentBandStoreWithOptions(opts RecentBandOptions) *RecentBandStore {
	window := opts.Window
	if window <= 0 {
		window = 12 * time.Hour
	}
	shards := opts.Shards
	if shards <= 0 {
		shards = defaultRecentBandShards
	}
	maxEntries := opts.MaxEntries
	if maxEntries <= 0 {
		maxEntries = defaultRecentBandMaxEntries
	}
	cleanup := opts.CleanupInterval
	if cleanup <= 0 {
		cleanup = defaultRecentBandCleanupInterval
	}
	maxSpotters := opts.MaxSpottersPerCall
	if maxSpotters <= 0 {
		maxSpotters = defaultRecentBandMaxSpottersPerEntry
	}

	perShard := maxEntries / shards
	if maxEntries%shards != 0 {
		perShard++
	}
	if perShard <= 0 {
		perShard = 1
	}

	store := &RecentBandStore{
		shards:             make([]recentBandShard, shards),
		window:             window,
		maxEntries:         maxEntries,
		perShardMax:        perShard,
		cleanupInterval:    cleanup,
		maxSpottersPerCall: maxSpotters,
	}
	for i := range store.shards {
		store.shards[i] = recentBandShard{
			entries: make(map[recentBandKey]*recentBandEntry, perShard),
		}
	}
	return store
}

// StartCleanup starts the periodic stale-entry cleanup loop.
func (s *RecentBandStore) StartCleanup() {
	if s == nil || s.cleanupInterval <= 0 || s.window <= 0 {
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

// StopCleanup stops the periodic stale-entry cleanup loop.
func (s *RecentBandStore) StopCleanup() {
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

// Record stores one observed report for (call, band, mode) from a spotter.
func (s *RecentBandStore) Record(call, band, mode, spotter string, seenAt time.Time) {
	if s == nil {
		return
	}
	key, ok := s.normalizeKey(call, band, mode)
	if !ok {
		return
	}
	spotter = normalizeRecentBandSpotter(spotter)
	if spotter == "" {
		return
	}
	if seenAt.IsZero() {
		seenAt = time.Now().UTC()
	} else {
		seenAt = seenAt.UTC()
	}
	cutoff := seenAt.Add(-s.window)
	shard := s.shardFor(key)
	if shard == nil {
		return
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry := shard.entries[key]
	if entry == nil {
		if len(shard.entries) >= s.perShardMax {
			s.evictOneEntryLocked(shard, cutoff)
		}
		if len(shard.entries) >= s.perShardMax {
			return
		}
		entry = &recentBandEntry{
			spotters: make(map[string]time.Time, 4),
		}
		shard.entries[key] = entry
	}

	s.pruneEntryLocked(entry, cutoff)
	if prev, exists := entry.spotters[spotter]; !exists || seenAt.After(prev) {
		entry.spotters[spotter] = seenAt
	}
	if seenAt.After(entry.lastSeen) {
		entry.lastSeen = seenAt
	}
	if len(entry.spotters) > s.maxSpottersPerCall {
		s.trimSpottersLocked(entry)
	}
	if len(entry.spotters) == 0 {
		delete(shard.entries, key)
	}
}

// HasRecentSupport reports whether the call has at least minUnique distinct
// spotters within the configured window on the same band and mode.
func (s *RecentBandStore) HasRecentSupport(call, band, mode string, minUnique int, now time.Time) bool {
	return s.RecentSupportCount(call, band, mode, now) >= s.normalizeMinUnique(minUnique)
}

// RecentSupportCount returns the number of unique spotters still active in the
// current window for the (call, band, mode) key.
func (s *RecentBandStore) RecentSupportCount(call, band, mode string, now time.Time) int {
	if s == nil {
		return 0
	}
	key, ok := s.normalizeKey(call, band, mode)
	if !ok {
		return 0
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	cutoff := now.Add(-s.window)
	shard := s.shardFor(key)
	if shard == nil {
		return 0
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry := shard.entries[key]
	if entry == nil {
		return 0
	}
	s.pruneEntryLocked(entry, cutoff)
	if len(entry.spotters) == 0 {
		delete(shard.entries, key)
		return 0
	}
	return len(entry.spotters)
}

// ActiveCallCount returns the number of distinct calls with non-stale recent
// support records across all bands/modes.
func (s *RecentBandStore) ActiveCallCount(now time.Time) int {
	if s == nil || s.window <= 0 {
		return 0
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	cutoff := now.Add(-s.window)
	calls := make(map[string]struct{})
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		for key, entry := range shard.entries {
			s.pruneEntryLocked(entry, cutoff)
			if len(entry.spotters) == 0 {
				delete(shard.entries, key)
				continue
			}
			calls[key.call] = struct{}{}
		}
		shard.mu.Unlock()
	}
	return len(calls)
}

// ActiveCallCountsByBand returns active distinct-call counts for each band.
// A call is counted once per band even when it appears in multiple modes.
func (s *RecentBandStore) ActiveCallCountsByBand(now time.Time) map[string]int {
	if s == nil || s.window <= 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	cutoff := now.Add(-s.window)
	byBand := make(map[string]map[string]struct{})
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		for key, entry := range shard.entries {
			s.pruneEntryLocked(entry, cutoff)
			if len(entry.spotters) == 0 {
				delete(shard.entries, key)
				continue
			}
			if byBand[key.band] == nil {
				byBand[key.band] = make(map[string]struct{})
			}
			byBand[key.band][key.call] = struct{}{}
		}
		shard.mu.Unlock()
	}
	out := make(map[string]int, len(byBand))
	for band, calls := range byBand {
		out[band] = len(calls)
	}
	return out
}

func (s *RecentBandStore) cleanupLoop() {
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

func (s *RecentBandStore) cleanup(now time.Time) {
	if s == nil || s.window <= 0 {
		return
	}
	cutoff := now.Add(-s.window)
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		for key, entry := range shard.entries {
			s.pruneEntryLocked(entry, cutoff)
			if len(entry.spotters) == 0 {
				delete(shard.entries, key)
			}
		}
		for len(shard.entries) > s.perShardMax {
			s.evictOneEntryLocked(shard, cutoff)
		}
		shard.mu.Unlock()
	}
}

func (s *RecentBandStore) normalizeKey(call, band, mode string) (recentBandKey, bool) {
	call = normalizeRecentBandCall(call)
	band = NormalizeBand(band)
	mode = normalizeRecentBandMode(mode)
	if call == "" || band == "" || band == "???" || mode == "" {
		return recentBandKey{}, false
	}
	return recentBandKey{call: call, band: band, mode: mode}, true
}

func (s *RecentBandStore) normalizeMinUnique(minUnique int) int {
	if minUnique <= 0 {
		minUnique = 2
	}
	if minUnique > s.maxSpottersPerCall {
		return s.maxSpottersPerCall
	}
	return minUnique
}

func (s *RecentBandStore) shardFor(key recentBandKey) *recentBandShard {
	if s == nil || len(s.shards) == 0 {
		return nil
	}
	idx := int(hashRecentBandKey(key) % uint64(len(s.shards)))
	return &s.shards[idx]
}

func (s *RecentBandStore) evictOneEntryLocked(shard *recentBandShard, cutoff time.Time) {
	if shard == nil || len(shard.entries) == 0 {
		return
	}
	// First pass: opportunistically prune stale entries.
	for key, entry := range shard.entries {
		s.pruneEntryLocked(entry, cutoff)
		if len(entry.spotters) == 0 {
			delete(shard.entries, key)
		}
	}
	if len(shard.entries) < s.perShardMax {
		return
	}

	// Bounded fallback: evict the oldest remaining entry in this shard.
	var victim recentBandKey
	victimSet := false
	var oldest time.Time
	for key, entry := range shard.entries {
		if !victimSet || entry.lastSeen.Before(oldest) {
			victim = key
			oldest = entry.lastSeen
			victimSet = true
		}
	}
	if victimSet {
		delete(shard.entries, victim)
	}
}

func (s *RecentBandStore) pruneEntryLocked(entry *recentBandEntry, cutoff time.Time) {
	if entry == nil || len(entry.spotters) == 0 {
		return
	}
	for spotter, seenAt := range entry.spotters {
		if seenAt.Before(cutoff) {
			delete(entry.spotters, spotter)
		}
	}
	var latest time.Time
	for _, seenAt := range entry.spotters {
		if seenAt.After(latest) {
			latest = seenAt
		}
	}
	entry.lastSeen = latest
}

func (s *RecentBandStore) trimSpottersLocked(entry *recentBandEntry) {
	if entry == nil || len(entry.spotters) <= s.maxSpottersPerCall {
		return
	}
	type spotterSeen struct {
		spotter string
		seenAt  time.Time
	}
	all := make([]spotterSeen, 0, len(entry.spotters))
	for spotter, seenAt := range entry.spotters {
		all = append(all, spotterSeen{spotter: spotter, seenAt: seenAt})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].seenAt.Equal(all[j].seenAt) {
			return all[i].spotter < all[j].spotter
		}
		return all[i].seenAt.Before(all[j].seenAt)
	})
	remove := len(all) - s.maxSpottersPerCall
	for i := 0; i < remove; i++ {
		delete(entry.spotters, all[i].spotter)
	}
	var latest time.Time
	for _, seenAt := range entry.spotters {
		if seenAt.After(latest) {
			latest = seenAt
		}
	}
	entry.lastSeen = latest
}

func normalizeRecentBandCall(call string) string {
	return strutil.NormalizeUpper(call)
}

func normalizeRecentBandMode(mode string) string {
	return strutil.NormalizeUpper(mode)
}

func normalizeRecentBandSpotter(spotter string) string {
	return strutil.NormalizeUpper(spotter)
}

func hashRecentBandKey(key recentBandKey) uint64 {
	const (
		offset64 = 1469598103934665603
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	mix := func(s string) {
		for i := 0; i < len(s); i++ {
			h ^= uint64(s[i])
			h *= prime64
		}
	}
	mix(strings.ToUpper(key.call))
	h ^= '|'
	h *= prime64
	mix(strings.ToUpper(key.band))
	h ^= '|'
	h *= prime64
	mix(strings.ToUpper(key.mode))
	return h
}
