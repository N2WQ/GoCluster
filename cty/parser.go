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
	// trie stores all CTY keys (exact calls and prefixes) in a read-only prefix trie
	// so longest-prefix matches can be resolved in O(L) time where L is the callsign
	// length (typically < 15 bytes).
	trie ctyTrie
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

// ctyTrie implements a read-only prefix trie over CTY keys.
//
// It enables longest-prefix matching without scanning all known prefixes:
// walk the callsign bytes from the root; every time we land on a terminal node,
// remember that key as the best match so far. The last terminal observed is the
// longest prefix that matches the callsign.
//
// Nodes are stored in a slice so child links are small integer indices, keeping
// memory usage predictable and avoiding pointer-heavy trees.
type ctyTrie struct {
	nodes []ctyTrieNode
}

type ctyTrieNode struct {
	next        map[byte]int
	terminalKey string
}

// Purpose: Build a read-only trie over CTY keys for longest-prefix lookups.
// Key aspects: Stores nodes in a slice with integer child indexes for compactness.
// Upstream: LoadCTYDatabaseFromReader.
// Downstream: ctyTrie.longestPrefixKey.
func buildCTYTrie(keys []string) ctyTrie {
	tr := ctyTrie{nodes: []ctyTrieNode{{next: make(map[byte]int)}}}
	for _, key := range keys {
		if key == "" {
			continue
		}
		state := 0
		for i := 0; i < len(key); i++ {
			ch := key[i]
			next := tr.nodes[state].next
			if next == nil {
				next = make(map[byte]int)
				tr.nodes[state].next = next
			}
			child, ok := next[ch]
			if !ok {
				child = len(tr.nodes)
				tr.nodes = append(tr.nodes, ctyTrieNode{})
				next[ch] = child
			}
			state = child
		}
		tr.nodes[state].terminalKey = key
	}
	return tr
}

// Purpose: Find the longest CTY prefix matching a callsign.
// Key aspects: Walks the trie once, tracking the latest terminal key.
// Upstream: CTYDatabase.lookupCallsignNoCache.
// Downstream: trie node traversal.
func (tr *ctyTrie) longestPrefixKey(cs string) (string, bool) {
	if tr == nil || len(tr.nodes) == 0 || cs == "" {
		return "", false
	}
	state := 0
	best := ""
	for i := 0; i < len(cs); i++ {
		next := tr.nodes[state].next
		if next == nil {
			break
		}
		child, ok := next[cs[i]]
		if !ok {
			break
		}
		state = child
		if tr.nodes[state].terminalKey != "" {
			best = tr.nodes[state].terminalKey
		}
	}
	if best == "" {
		return "", false
	}
	return best, true
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

// Purpose: Load the CTY plist file from disk into an in-memory database.
// Key aspects: Opens the file and delegates parsing/normalization.
// Upstream: main.go startup, cmd/ctylookup.
// Downstream: LoadCTYDatabaseFromReader.
func LoadCTYDatabase(path string) (*CTYDatabase, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open cty plist: %w", err)
	}
	defer f.Close()
	return LoadCTYDatabaseFromReader(f)
}

// Purpose: Decode CTY data from a reader into a lookup database.
// Key aspects: Normalizes keys, sorts by length for deterministic ordering, and builds a trie.
// Upstream: LoadCTYDatabase, tests.
// Downstream: decodeCTYData, buildCTYTrie.
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
	trie := buildCTYTrie(keys)
	return &CTYDatabase{
		Data:      data,
		Keys:      keys,
		trie:      trie,
		cacheCap:  defaultCacheCapacity,
		cacheList: list.New(),
		cacheMap:  make(map[string]*list.Element, defaultCacheCapacity),
	}, nil
}

// Purpose: Decode and normalize CTY plist data.
// Key aspects: Uppercases/trim keys to normalize prefixes.
// Upstream: LoadCTYDatabaseFromReader.
// Downstream: plist decoder.
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

// Purpose: Normalize callsigns for CTY lookup.
// Key aspects: Uppercases, trims, and strips portable suffixes.
// Upstream: CTYDatabase.LookupCallsign.
// Downstream: strings helpers.
func normalizeCallsign(cs string) string {
	cs = strings.ToUpper(strings.TrimSpace(cs))
	for _, suf := range suffixes {
		if strings.HasSuffix(cs, suf) {
			return strings.TrimSuffix(cs, suf)
		}
	}
	return cs
}

// Purpose: Resolve CTY metadata for a callsign, with caching.
// Key aspects: Normalizes callsign, caches hits/misses, and updates metrics.
// Upstream: main.go enrichment, RBN/PSKReporter clients, cmd/ctylookup.
// Downstream: cacheGet, lookupCallsignNoCache, cacheStore.
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

// Purpose: Resolve CTY metadata without using the cache.
// Key aspects: Checks exact callsign first, then longest-prefix in trie.
// Upstream: CTYDatabase.LookupCallsign.
// Downstream: ctyTrie.longestPrefixKey, clonePrefix.
func (db *CTYDatabase) lookupCallsignNoCache(cs string) (*PrefixInfo, bool) {
	if info, ok := db.Data[cs]; ok {
		return clonePrefix(info), true
	}

	if key, ok := db.trie.longestPrefixKey(cs); ok {
		info := db.Data[key]
		return clonePrefix(info), true
	}
	return nil, false
}

// Purpose: Return CTY keys that share a prefix (test helper).
// Key aspects: Normalizes prefix and scans the sorted key list.
// Upstream: Tests.
// Downstream: None.
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

// Purpose: Clone a PrefixInfo to avoid sharing mutable references.
// Key aspects: Returns a copy on the heap.
// Upstream: lookupCallsignNoCache.
// Downstream: None.
func clonePrefix(info PrefixInfo) *PrefixInfo {
	copy := info
	return &copy
}

// Purpose: Fetch a cached lookup result for a callsign.
// Key aspects: Uses an LRU list protected by a mutex.
// Upstream: CTYDatabase.LookupCallsign.
// Downstream: cacheList/cacheMap mutation.
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

// Purpose: Store a lookup result in the LRU cache.
// Key aspects: Updates existing entries and evicts LRU on overflow.
// Upstream: CTYDatabase.LookupCallsign.
// Downstream: cacheList/cacheMap mutation.
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

// Purpose: Return snapshot of CTY lookup/cache metrics.
// Key aspects: Uses atomic loads for lock-free reads.
// Upstream: Metrics reporting/diagnostics.
// Downstream: atomic counters.
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
