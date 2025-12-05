// Package cty loads and queries the CTY prefix database so spots can be
// enriched with continent/zone/country metadata and validated quickly using a
// cache-backed longest-prefix lookup.
package cty

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"howett.net/plist"
)

// PrefixInfo describes the metadata stored for each CTY entry.
type PrefixInfo struct {
	Country       string  `plist:"Country"`
	Prefix        string  `plist:"Prefix"`
	ADIF          int     `plist:"ADIF"`
	CQZone        int     `plist:"CQZone"`
	ITUZone       int     `plist:"ITUZone"`
	Continent     string  `plist:"Continent"`
	Latitude      float64 `plist:"Latitude"`
	Longitude     float64 `plist:"Longitude"`
	GMTOffset     float64 `plist:"GMTOffset"`
	ExactCallsign bool    `plist:"ExactCallsign"`
}

// CTYDatabase holds the plist data and sorted keys for longest-prefix lookup.
type CTYDatabase struct {
	Data map[string]PrefixInfo
	Keys []string
	// cache stores normalized callsign lookups (hits and misses) with a bounded LRU.
	cacheMu   sync.Mutex
	cacheList *list.List
	cacheMap  map[string]*list.Element
	cacheCap  int
	// metrics track lookup/caching behavior for stats reporting.
	totalLookups       atomic.Uint64
	cacheHits          atomic.Uint64
	cacheEntries       atomic.Uint64
	validated          atomic.Uint64
	validatedFromCache atomic.Uint64
}

type cacheEntry struct {
	info *PrefixInfo
	ok   bool
}

type cacheItem struct {
	key   string
	entry cacheEntry
}

const defaultCacheCapacity = 50000

// LookupMetrics summarizes callsign lookup behavior.
type LookupMetrics struct {
	TotalLookups       uint64
	CacheHits          uint64
	CacheEntries       uint64
	Validated          uint64
	ValidatedFromCache uint64
}

// LoadCTYDatabase loads cty.plist into a lookup database.
func LoadCTYDatabase(path string) (*CTYDatabase, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open cty plist: %w", err)
	}
	defer f.Close()
	return LoadCTYDatabaseFromReader(f)
}

// LoadCTYDatabaseFromReader decodes CTY data from an io.Reader (exposed for testing).
// It normalizes keys to uppercase and pre-sorts them longest-first for prefix
// search speed.
func LoadCTYDatabaseFromReader(r io.ReadSeeker) (*CTYDatabase, error) {
	data, err := decodeCTYData(r)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) == len(keys[j]) {
			return keys[i] < keys[j]
		}
		return len(keys[i]) > len(keys[j])
	})
	return &CTYDatabase{
		Data:      data,
		Keys:      keys,
		cacheCap:  defaultCacheCapacity,
		cacheList: list.New(),
		cacheMap:  make(map[string]*list.Element, defaultCacheCapacity),
	}, nil
}

func decodeCTYData(r io.ReadSeeker) (map[string]PrefixInfo, error) {
	var raw map[string]PrefixInfo
	decoder := plist.NewDecoder(r)
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode plist: %w", err)
	}
	data := make(map[string]PrefixInfo, len(raw))
	for k, v := range raw {
		norm := strings.ToUpper(strings.TrimSpace(k))
		data[norm] = v
	}
	return data, nil
}

var suffixes = []string{"/QRP", "/P", "/M", "/MM", "/AM"}

func normalizeCallsign(cs string) string {
	cs = strings.ToUpper(strings.TrimSpace(cs))
	for _, suf := range suffixes {
		if strings.HasSuffix(cs, suf) {
			return strings.TrimSuffix(cs, suf)
		}
	}
	return cs
}

// LookupCallsign returns metadata for the callsign or false if unknown. Results
// are memoized (including misses) to cut repeated plist scans during bursts.
func (db *CTYDatabase) LookupCallsign(cs string) (*PrefixInfo, bool) {
	cs = normalizeCallsign(cs)
	db.totalLookups.Add(1)
	if entry, ok := db.cacheGet(cs); ok {
		db.cacheHits.Add(1)
		if entry.ok {
			db.validated.Add(1)
			db.validatedFromCache.Add(1)
		}
		return entry.info, entry.ok
	}

	info, ok := db.lookupCallsignNoCache(cs)
	if ok {
		db.validated.Add(1)
	}

	entry := cacheEntry{info: info, ok: ok}
	db.cacheStore(cs, entry)
	return entry.info, entry.ok
}

func (db *CTYDatabase) lookupCallsignNoCache(cs string) (*PrefixInfo, bool) {
	if info, ok := db.Data[cs]; ok {
		return clonePrefix(info), true
	}

	for _, key := range db.Keys {
		if len(key) > len(cs) {
			continue
		}
		if strings.HasPrefix(cs, key) {
			info := db.Data[key]
			return clonePrefix(info), true
		}
	}
	return nil, false
}

// KeysWithPrefix returns all known CTY keys starting with prefix (used for testing).
func (db *CTYDatabase) KeysWithPrefix(pref string) []string {
	norm := strings.ToUpper(strings.TrimSpace(pref))
	matches := make([]string, 0)
	for _, key := range db.Keys {
		if strings.HasPrefix(key, norm) {
			matches = append(matches, key)
		}
	}
	return matches
}

func clonePrefix(info PrefixInfo) *PrefixInfo {
	copy := info
	return &copy
}

func (db *CTYDatabase) cacheGet(cs string) (cacheEntry, bool) {
	if db == nil || db.cacheCap <= 0 {
		return cacheEntry{}, false
	}
	db.cacheMu.Lock()
	defer db.cacheMu.Unlock()
	elem, ok := db.cacheMap[cs]
	if !ok {
		return cacheEntry{}, false
	}
	db.cacheList.MoveToFront(elem)
	item := elem.Value.(*cacheItem)
	return item.entry, true
}

func (db *CTYDatabase) cacheStore(cs string, entry cacheEntry) {
	if db == nil || db.cacheCap <= 0 {
		return
	}
	db.cacheMu.Lock()
	defer db.cacheMu.Unlock()

	// Update in-place when present to avoid churn.
	if elem, ok := db.cacheMap[cs]; ok {
		elem.Value.(*cacheItem).entry = entry
		db.cacheList.MoveToFront(elem)
		db.cacheEntries.Store(uint64(len(db.cacheMap)))
		return
	}

	elem := db.cacheList.PushFront(&cacheItem{key: cs, entry: entry})
	db.cacheMap[cs] = elem

	// Evict least-recently-used entries when capacity is exceeded.
	if db.cacheCap > 0 && len(db.cacheMap) > db.cacheCap {
		if tail := db.cacheList.Back(); tail != nil {
			db.cacheList.Remove(tail)
			if item, ok := tail.Value.(*cacheItem); ok {
				delete(db.cacheMap, item.key)
			}
		}
	}
	db.cacheEntries.Store(uint64(len(db.cacheMap)))
}

// Metrics returns snapshot of lookup/cache counters.
func (db *CTYDatabase) Metrics() LookupMetrics {
	if db == nil {
		return LookupMetrics{}
	}
	return LookupMetrics{
		TotalLookups:       db.totalLookups.Load(),
		CacheHits:          db.cacheHits.Load(),
		CacheEntries:       db.cacheEntries.Load(),
		Validated:          db.validated.Load(),
		ValidatedFromCache: db.validatedFromCache.Load(),
	}
}
