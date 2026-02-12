package spot

import (
	"container/list"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
)

var callsignPattern = regexp.MustCompile(`^[A-Z0-9]+(?:[/-][A-Z0-9#]+)*$`)

const (
	minCallsignLength = 3
	maxCallsignLength = 15
)

var portableSuffixes = []string{"/QRP", "/MM", "/AM", "/M", "/P"}

// CallCache is a tiny concurrency-safe cache to avoid repeating the same normalization
// work for hot call/spotter strings during bursts.
type CallCache struct {
	mu       sync.Mutex
	lru      *list.List
	entries  map[string]*list.Element
	capacity int
	ttl      time.Duration
}

type cacheEntry struct {
	key     string
	value   string
	expires time.Time
}

// NewCallCache constructs a bounded normalization cache.
// Key aspects: Defaults capacity/TTL when unset.
// Upstream: global cache initialization and tests.
// Downstream: list.New and map allocation.
// NewCallCache creates a bounded FIFO cache sized for short-lived reuse.
func NewCallCache(capacity int, ttl time.Duration) *CallCache {
	if capacity <= 0 {
		capacity = 1024
	}
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return &CallCache{
		lru:      list.New(),
		entries:  make(map[string]*list.Element, capacity),
		capacity: capacity,
		ttl:      ttl,
	}
}

// Get looks up a cached normalized callsign.
// Key aspects: Enforces TTL and maintains LRU ordering.
// Upstream: NormalizeCallsign.
// Downstream: list operations and map access under lock.
// Get returns a cached value for the raw key.
func (c *CallCache) Get(key string) (string, bool) {
	if c == nil {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.entries[key]
	if !ok || elem == nil {
		return "", false
	}
	entry := elem.Value.(cacheEntry)
	if c.ttl > 0 && time.Now().UTC().After(entry.expires) {
		c.lru.Remove(elem)
		delete(c.entries, key)
		return "", false
	}
	// Move to front (most recently used).
	c.lru.MoveToFront(elem)
	return entry.value, true
}

// Add inserts or updates a cached normalization entry.
// Key aspects: Evicts oldest item when capacity is exceeded.
// Upstream: NormalizeCallsign.
// Downstream: list operations and map mutation under lock.
// Add inserts or updates a cached value, evicting the oldest item when full.
func (c *CallCache) Add(key, val string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.lru = list.New()
		c.entries = make(map[string]*list.Element, c.capacity)
	}
	if elem, exists := c.entries[key]; exists && elem != nil {
		elem.Value = cacheEntry{key: key, value: val, expires: time.Now().UTC().Add(c.ttl)}
		c.lru.MoveToFront(elem)
		return
	}
	elem := c.lru.PushFront(cacheEntry{key: key, value: val, expires: time.Now().UTC().Add(c.ttl)})
	c.entries[key] = elem
	if c.lru.Len() > c.capacity {
		oldest := c.lru.Back()
		if oldest != nil {
			entry := oldest.Value.(cacheEntry)
			c.lru.Remove(oldest)
			delete(c.entries, entry.key)
		}
	}
}

var normalizeCallCache = NewCallCache(4096, 10*time.Minute)

// ConfigureNormalizeCallCache rebuilds the global normalization cache with the
// provided limits so operators can tune size/TTL via config.
// Purpose: Replace the global normalization cache with new limits.
// Key aspects: Reinitializes cache to size/TTL; old entries discarded.
// Upstream: main startup and config loading.
// Downstream: NewCallCache.
func ConfigureNormalizeCallCache(size int, ttl time.Duration) {
	normalizeCallCache = NewCallCache(size, ttl)
}

// NormalizeCallsign normalizes a callsign for consistent comparisons.
// Key aspects: Uppercases, trims, converts dots to slashes, strips trailing slash,
// and removes portable suffixes like /P or /MM.
// Upstream: All parsing and validation paths.
// Downstream: normalizeCallCache and strings operations.
// NormalizeCallsign uppercases the string, trims whitespace, and removes trailing dots or slashes.
func NormalizeCallsign(call string) string {
	if cached, ok := normalizeCallCache.Get(call); ok {
		return cached
	}
	normalized := strings.ToUpper(strings.TrimSpace(call))
	normalized = strings.ReplaceAll(normalized, ".", "/")
	normalized = strings.TrimSuffix(normalized, "/")
	normalized = strings.TrimSpace(normalized)
	for _, suffix := range portableSuffixes {
		if strings.HasSuffix(normalized, suffix) {
			normalized = strings.TrimSuffix(normalized, suffix)
			break
		}
	}
	normalizeCallCache.Add(call, normalized)
	return normalized
}

// Purpose: Validate a normalized callsign for basic format rules.
// Key aspects: Length bounds (3..15), digit presence, and regex match.
// Upstream: IsValidCallsign.
// Downstream: callsignPattern and unicode checks.
func validateNormalizedCallsign(call string) bool {
	if call == "" {
		return false
	}
	if len(call) < minCallsignLength || len(call) > maxCallsignLength {
		return false
	}
	if strings.IndexFunc(call, unicode.IsDigit) < 0 {
		return false
	}
	return callsignPattern.MatchString(call)
}

// MaxCallsignLength returns the maximum allowed callsign length.
// Key aspects: Exposes the validation limit for other packages.
// Upstream: PSKReporter and other callers.
// Downstream: maxCallsignLength constant.
func MaxCallsignLength() int {
	return maxCallsignLength
}

// IsValidCallsign validates a raw callsign input.
// Key aspects: Normalizes then validates format.
// Upstream: Parser and dedupe validation.
// Downstream: NormalizeCallsign and validateNormalizedCallsign.
// IsValidCallsign applies format checks to make sure it looks like a valid amateur call.
func IsValidCallsign(call string) bool {
	normalized := NormalizeCallsign(call)
	return validateNormalizedCallsign(normalized)
}

// IsValidNormalizedCallsign validates an already-normalized callsign.
// Key aspects: Assumes NormalizeCallsign was already applied by the caller.
// Upstream: Ingest paths that normalize once.
// Downstream: validateNormalizedCallsign.
func IsValidNormalizedCallsign(call string) bool {
	return validateNormalizedCallsign(call)
}

// IsBeaconCall reports whether a callsign is a beacon identifier.
// Key aspects: Normalizes and checks /B suffix.
// Upstream: RefreshBeaconFlag and other beacon detection.
// Downstream: NormalizeCallsign and strings.HasSuffix.
// IsBeaconCall reports whether the normalized callsign ends with /B.
func IsBeaconCall(call string) bool {
	normalized := NormalizeCallsign(call)
	return strings.HasSuffix(normalized, "/B")
}
