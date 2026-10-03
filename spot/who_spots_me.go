// File role: Owns the bounded rolling recent-heard store behind WHOSPOTSME.
// Crawler notes: Start here for WHOSPOTSME key normalization, window expiry,
// per-key country caps, shard ownership, and cleanup/eviction coupling.
// Related docs: README.md, commands/README.md,
// docs/decisions/ADR-0071-whospotsme-rolling-country-summary.md.
package spot

import (
	"strings"
	"sync"
	"time"
)

const (
	defaultWhoSpotsMeWindow          = 10 * time.Minute
	defaultWhoSpotsMeShards          = 64
	defaultWhoSpotsMeMaxEntries      = 32768
	defaultWhoSpotsMeMaxCountries    = 256
	defaultWhoSpotsMeCleanupInterval = time.Minute
)

// WhoSpotsMeCountryCount is the compact result shape used by the telnet command:
// support needs counts by country, not the raw spotter identities retained in
// the rolling store.
type WhoSpotsMeCountryCount struct {
	ADIF  int
	Count int
}

// WhoSpotsMeOptions makes the store bounds explicit for tests and future
// operator tuning. The default production shape is a bounded rolling window,
// not an unbounded archive of who has ever heard a call.
type WhoSpotsMeOptions struct {
	Window               time.Duration
	Shards               int
	MaxEntries           int
	MaxCountriesPerEntry int
	CleanupInterval      time.Duration
}

// WhoSpotsMeStore retains accepted recent observations for the WHOSPOTSME
// command. The intent is retrospective local evidence: it records what this
// cluster recently accepted, using rolling second buckets so old evidence
// expires deterministically and memory is bounded by active keys, countries per
// key, and the configured time window.
type WhoSpotsMeStore struct {
	shards               []whoSpotsMeShard
	buckets              []whoSpotsMeBucket
	window               time.Duration
	windowSeconds        int64
	maxEntries           int
	perShardMax          int
	maxCountriesPerEntry int
	cleanupInterval      time.Duration

	mu         sync.Mutex
	latestUnix int64

	cleanupMu sync.Mutex
	quit      chan struct{}
}

// whoSpotsMeShard limits lock contention for per-call/band summaries. Entries
// are bounded by perShardMax and are also removed when their rolling buckets
// expire.
type whoSpotsMeShard struct {
	mu      sync.RWMutex
	entries map[whoSpotsMeKey]*whoSpotsMeEntry
}

type whoSpotsMeKey struct {
	call string
	band string
}

type whoSpotsMeCountryKey struct {
	adif      int
	continent string
}

type whoSpotsMeRecordKey struct {
	key     whoSpotsMeKey
	country whoSpotsMeCountryKey
}

// whoSpotsMeEntry holds the current aggregate for one call+band. The bucket
// ring owns exact per-second decrements; totals are only a fast query view.
type whoSpotsMeEntry struct {
	totals   map[whoSpotsMeCountryKey]int
	lastSeen int64
}

// whoSpotsMeBucket records the increments for one second so expiry can subtract
// exactly what was admitted, including multiple spots for the same country.
type whoSpotsMeBucket struct {
	second int64
	counts map[whoSpotsMeRecordKey]int
}

func NewWhoSpotsMeStore(window time.Duration) *WhoSpotsMeStore {
	return NewWhoSpotsMeStoreWithOptions(WhoSpotsMeOptions{Window: window})
}

// NewWhoSpotsMeStoreWithOptions normalizes every bound before allocation. The
// bucket ring size is the time-window bound; shard and country caps bound the
// cardinality of active keys and per-key detail.
func NewWhoSpotsMeStoreWithOptions(opts WhoSpotsMeOptions) *WhoSpotsMeStore {
	window := opts.Window
	if window <= 0 {
		window = defaultWhoSpotsMeWindow
	}
	windowSeconds := int64(window / time.Second)
	if windowSeconds <= 0 {
		windowSeconds = int64(defaultWhoSpotsMeWindow / time.Second)
		window = time.Duration(windowSeconds) * time.Second
	}

	shardCount := opts.Shards
	if shardCount <= 0 {
		shardCount = defaultWhoSpotsMeShards
	}

	maxEntries := opts.MaxEntries
	if maxEntries <= 0 {
		maxEntries = defaultWhoSpotsMeMaxEntries
	}
	perShardMax := (maxEntries + shardCount - 1) / shardCount
	if perShardMax <= 0 {
		perShardMax = 1
	}

	maxCountries := opts.MaxCountriesPerEntry
	if maxCountries <= 0 {
		maxCountries = defaultWhoSpotsMeMaxCountries
	}

	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = defaultWhoSpotsMeCleanupInterval
	}

	shards := make([]whoSpotsMeShard, shardCount)
	for i := range shards {
		shards[i].entries = make(map[whoSpotsMeKey]*whoSpotsMeEntry)
	}

	return &WhoSpotsMeStore{
		shards:               shards,
		buckets:              make([]whoSpotsMeBucket, windowSeconds),
		window:               window,
		windowSeconds:        windowSeconds,
		maxEntries:           maxEntries,
		perShardMax:          perShardMax,
		maxCountriesPerEntry: maxCountries,
		cleanupInterval:      cleanupInterval,
	}
}

func (s *WhoSpotsMeStore) Window() time.Duration {
	if s == nil {
		return 0
	}
	return s.window
}

// StartCleanup advances the rolling window even during quiet periods so old
// observations expire without waiting for the next spot or command query.
func (s *WhoSpotsMeStore) StartCleanup() {
	if s == nil || s.cleanupInterval <= 0 {
		return
	}
	startPeriodicCleanup(&s.cleanupMu, &s.quit, s.cleanupInterval, func() {
		s.cleanup(time.Now().UTC())
	})
}

func (s *WhoSpotsMeStore) StopCleanup() {
	if s == nil {
		return
	}
	stopPeriodicCleanup(&s.cleanupMu, &s.quit)
}

// Record admits one accepted observation. It drops invalid country metadata and
// over-cap countries rather than widening the support contract beyond the
// compact per-continent summary the telnet command displays.
func (s *WhoSpotsMeStore) Record(call, band string, countryADIF int, continent string, seenAt time.Time) {
	if s == nil || countryADIF <= 0 {
		return
	}
	key, ok := s.normalizeKey(call, band)
	if !ok {
		return
	}
	country, ok := normalizeWhoSpotsMeCountry(countryADIF, continent)
	if !ok {
		return
	}
	seenAt = normalizeWhoSpotsMeTime(seenAt)
	second := seenAt.Unix()

	s.mu.Lock()
	if !s.admitSecondLocked(second) {
		s.mu.Unlock()
		return
	}

	shard := s.shardFor(key)
	shard.mu.Lock()
	entry := shard.entries[key]
	if entry == nil {
		if len(shard.entries) >= s.perShardMax {
			s.evictOneEntryLocked(shard)
		}
		entry = &whoSpotsMeEntry{totals: make(map[whoSpotsMeCountryKey]int)}
		shard.entries[key] = entry
	}
	if _, exists := entry.totals[country]; !exists && len(entry.totals) >= s.maxCountriesPerEntry {
		shard.mu.Unlock()
		s.mu.Unlock()
		return
	}
	entry.totals[country]++
	entry.lastSeen = second
	shard.mu.Unlock()

	bucket := &s.buckets[int(second%s.windowSeconds)]
	if bucket.counts == nil {
		bucket.counts = make(map[whoSpotsMeRecordKey]int)
	}
	bucket.counts[whoSpotsMeRecordKey{key: key, country: country}]++
	s.mu.Unlock()
}

// CountryCountsByContinent returns the current aggregate for one call+band after
// first advancing expiry to now. The returned map is detached from retained
// state so command formatting cannot race the store.
func (s *WhoSpotsMeStore) CountryCountsByContinent(call, band string, now time.Time) map[string][]WhoSpotsMeCountryCount {
	if s == nil {
		return nil
	}
	key, ok := s.normalizeKey(call, band)
	if !ok {
		return nil
	}
	now = normalizeWhoSpotsMeTime(now)

	s.mu.Lock()
	s.admitSecondLocked(now.Unix())
	s.mu.Unlock()

	shard := s.shardFor(key)
	shard.mu.RLock()
	entry := shard.entries[key]
	if entry == nil || len(entry.totals) == 0 {
		shard.mu.RUnlock()
		return nil
	}
	out := make(map[string][]WhoSpotsMeCountryCount)
	for country, count := range entry.totals {
		if count <= 0 {
			continue
		}
		out[country.continent] = append(out[country.continent], WhoSpotsMeCountryCount{
			ADIF:  country.adif,
			Count: count,
		})
	}
	shard.mu.RUnlock()
	if len(out) == 0 {
		return nil
	}
	return out
}

// ActiveKeyCount is diagnostic-only cardinality visibility for the retained
// call+band map.
func (s *WhoSpotsMeStore) ActiveKeyCount() int {
	if s == nil {
		return 0
	}
	total := 0
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.RLock()
		total += len(shard.entries)
		shard.mu.RUnlock()
	}
	return total
}

func (s *WhoSpotsMeStore) cleanup(now time.Time) {
	if s == nil {
		return
	}
	now = normalizeWhoSpotsMeTime(now)
	s.mu.Lock()
	s.admitSecondLocked(now.Unix())
	s.mu.Unlock()
}

// admitSecondLocked is the window owner. It advances the second-bucket ring,
// expires old increments, and rejects observations that are older than the
// current rolling window. Callers hold s.mu.
func (s *WhoSpotsMeStore) admitSecondLocked(target int64) bool {
	if s == nil || s.windowSeconds <= 0 || target <= 0 {
		return false
	}
	if s.latestUnix == 0 {
		s.latestUnix = target
		bucket := &s.buckets[int(target%s.windowSeconds)]
		s.resetBucketLocked(bucket, target)
		return true
	}
	if target > s.latestUnix {
		if target-s.latestUnix >= s.windowSeconds {
			for i := range s.buckets {
				s.expireBucketLocked(&s.buckets[i])
				s.resetBucketLocked(&s.buckets[i], 0)
			}
			for i := range s.shards {
				shard := &s.shards[i]
				shard.mu.Lock()
				clear(shard.entries)
				shard.mu.Unlock()
			}
			s.latestUnix = target
			bucket := &s.buckets[int(target%s.windowSeconds)]
			s.resetBucketLocked(bucket, target)
			return true
		}
		for second := s.latestUnix + 1; second <= target; second++ {
			bucket := &s.buckets[int(second%s.windowSeconds)]
			s.expireBucketLocked(bucket)
			s.resetBucketLocked(bucket, second)
		}
		s.latestUnix = target
		return true
	}
	return target > s.latestUnix-s.windowSeconds
}

// expireBucketLocked subtracts a bucket's exact increments from shard totals so
// the query view converges with the rolling window. Callers hold s.mu; this
// method also takes the relevant shard lock for entry updates.
func (s *WhoSpotsMeStore) expireBucketLocked(bucket *whoSpotsMeBucket) {
	if bucket == nil || len(bucket.counts) == 0 {
		return
	}
	for recordKey, count := range bucket.counts {
		shard := s.shardFor(recordKey.key)
		shard.mu.Lock()
		entry := shard.entries[recordKey.key]
		if entry != nil {
			next := entry.totals[recordKey.country] - count
			if next > 0 {
				entry.totals[recordKey.country] = next
			} else {
				delete(entry.totals, recordKey.country)
			}
			if len(entry.totals) == 0 {
				delete(shard.entries, recordKey.key)
			}
		}
		shard.mu.Unlock()
	}
	clear(bucket.counts)
}

func (s *WhoSpotsMeStore) resetBucketLocked(bucket *whoSpotsMeBucket, second int64) {
	if bucket == nil {
		return
	}
	bucket.second = second
	if bucket.counts != nil {
		clear(bucket.counts)
	}
}

func (s *WhoSpotsMeStore) evictOneEntryLocked(shard *whoSpotsMeShard) {
	if s == nil || shard == nil || len(shard.entries) == 0 {
		return
	}
	var oldestKey whoSpotsMeKey
	oldestSecond := int64(0)
	first := true
	for key, entry := range shard.entries {
		if entry == nil {
			continue
		}
		if first || entry.lastSeen < oldestSecond {
			first = false
			oldestKey = key
			oldestSecond = entry.lastSeen
		}
	}
	if first {
		return
	}
	countries := shard.entries[oldestKey].totals
	delete(shard.entries, oldestKey)
	s.scrubKeyFromBucketsLocked(oldestKey, countries)
}

// scrubKeyFromBucketsLocked removes secondary bucket references after a primary
// entry eviction. Without this coupling, later bucket expiry could retain stale
// keys or subtract from a newly admitted entry with the same key.
// The selected entry's totals contain every country still represented in its
// buckets. The store and shard locks keep that set fixed through eviction, so
// direct deletes need no secondary index. Sparse buckets retain the old scan
// when it examines fewer records than traversing the victim's country set.
func (s *WhoSpotsMeStore) scrubKeyFromBucketsLocked(key whoSpotsMeKey, countries map[whoSpotsMeCountryKey]int) {
	for i := range s.buckets {
		bucket := &s.buckets[i]
		if len(bucket.counts) == 0 {
			continue
		}
		if len(bucket.counts) <= len(countries) {
			for recordKey := range bucket.counts {
				if recordKey.key == key {
					delete(bucket.counts, recordKey)
				}
			}
			continue
		}
		for country := range countries {
			delete(bucket.counts, whoSpotsMeRecordKey{key: key, country: country})
		}
	}
}

func (s *WhoSpotsMeStore) normalizeKey(call, band string) (whoSpotsMeKey, bool) {
	call = NormalizeSpotDXCallsign(call)
	band = NormalizeBand(band)
	if call == "" || band == "" || band == "???" || !IsValidBand(band) {
		return whoSpotsMeKey{}, false
	}
	return whoSpotsMeKey{call: call, band: band}, true
}

func (s *WhoSpotsMeStore) shardFor(key whoSpotsMeKey) *whoSpotsMeShard {
	if s == nil || len(s.shards) == 0 {
		return nil
	}
	idx := int(hashWhoSpotsMeKey(key) % uint64(len(s.shards)))
	return &s.shards[idx]
}

func normalizeWhoSpotsMeCountry(adif int, continent string) (whoSpotsMeCountryKey, bool) {
	continent = strings.TrimSpace(strings.ToUpper(continent))
	switch continent {
	case "AF", "AN", "AS", "EU", "NA", "OC", "SA":
		return whoSpotsMeCountryKey{adif: adif, continent: continent}, true
	default:
		return whoSpotsMeCountryKey{}, false
	}
}

func normalizeWhoSpotsMeTime(t time.Time) time.Time {
	if t.IsZero() {
		t = time.Now().UTC()
	}
	return t.UTC().Truncate(time.Second)
}

func hashWhoSpotsMeKey(key whoSpotsMeKey) uint64 {
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
	return h
}
