// Package cty loads and queries the CTY prefix database so spots can be
// enriched with continent/zone/country metadata and validated quickly using a
// longest-prefix lookup.
package cty

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
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
	// metrics track lookup/caching behavior for stats reporting. Cache metrics are
	// retained for compatibility but remain zero because the LRU is disabled.
	totalLookups       atomic.Uint64
	cacheHits          atomic.Uint64
	cacheEntries       atomic.Uint64
	validated          atomic.Uint64
	validatedFromCache atomic.Uint64
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

// LookupMetrics summarizes callsign lookup behavior.
type LookupMetrics struct {
	TotalLookups       uint64
	CacheHits          uint64
	CacheEntries       uint64
	Validated          uint64
	ValidatedFromCache uint64
}

// LoadCTYDatabase loads the CTY plist file from disk into an in-memory database.
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

// LoadCTYDatabaseFromReader decodes CTY data from a reader into a lookup database.
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
		Data: data,
		Keys: keys,
		trie: trie,
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

// Purpose: Normalize callsigns for CTY lookup.
// Key aspects: Assumes callers already normalized portable suffixes.
// Upstream: CTYDatabase.LookupCallsign.
// Downstream: strings helpers.
func normalizeCallsign(cs string) string {
	return cs
}

// LookupCallsign resolves CTY metadata for a callsign.
// Key aspects: Assumes input is already normalized (uppercased, portable suffixes stripped).
// Upstream: main.go enrichment, RBN/PSKReporter clients, cmd/ctylookup.
// Downstream: lookupCallsignNoCache.
func (db *CTYDatabase) LookupCallsign(cs string) (*PrefixInfo, bool) {
	cs = normalizeCallsign(cs)
	db.totalLookups.Add(1)

	info, ok := db.lookupCallsignNoCache(cs)
	if ok {
		db.validated.Add(1)
	}

	return info, ok
}

// LookupCallsignPortable resolves CTY metadata for portable callsigns with slash segments.
// Key aspects: Chooses the shortest slash segment that matches CTY, order-independent.
// Upstream: main.go CTY gate/enrichment, RBN/PSKReporter clients.
// Downstream: lookupCallsignNoCache.
// LookupCallsignPortable prefers location prefixes (e.g., N2WQ/VE3 -> VE3) while
// preserving the full callsign. It assumes cs is already normalized.
func (db *CTYDatabase) LookupCallsignPortable(cs string) (*PrefixInfo, bool) {
	cs = strings.TrimSpace(cs)
	if cs == "" {
		return nil, false
	}
	db.totalLookups.Add(1)

	info, ok := db.lookupCallsignPortableNoCache(cs)
	if ok {
		db.validated.Add(1)
	}

	return info, ok
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

// Purpose: Resolve CTY metadata for slash calls without using the cache.
// Key aspects: Picks the shortest matching segment; ignores /B beacon suffix; falls back to full callsign.
// Upstream: LookupCallsignPortable.
// Downstream: lookupCallsignNoCache.
func (db *CTYDatabase) lookupCallsignPortableNoCache(cs string) (*PrefixInfo, bool) {
	if db == nil || cs == "" {
		return nil, false
	}
	lookup := cs
	if strings.HasSuffix(lookup, "/B") {
		lookup = strings.TrimSuffix(lookup, "/B")
	}
	if !strings.Contains(lookup, "/") {
		return db.lookupCallsignNoCache(lookup)
	}
	var (
		bestSeg  string
		bestInfo *PrefixInfo
		bestLen  int
	)
	segments := strings.Split(lookup, "/")
	for _, seg := range segments {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		info, ok := db.lookupCallsignNoCache(seg)
		if !ok {
			continue
		}
		segLen := len(seg)
		if bestInfo == nil || segLen < bestLen || (segLen == bestLen && seg < bestSeg) {
			bestSeg = seg
			bestLen = segLen
			bestInfo = info
		}
	}
	if bestInfo != nil {
		return bestInfo, true
	}
	return db.lookupCallsignNoCache(lookup)
}

// KeysWithPrefix returns CTY keys that share a prefix (test helper).
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

// Metrics returns snapshot of CTY lookup/cache metrics.
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
