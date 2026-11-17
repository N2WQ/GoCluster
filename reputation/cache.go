// File role: Shared, bounded IP lookup retention for main and Cymru results.
// Each shard owns exact capacity, reusable slots and oldest-written FIFO links;
// reads never refresh age and overflow never scans records for a victim.
package reputation

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxCachePayloadBytes = 1024 // Address plus every LookupResult string; larger results remain usable uncached.
	cacheNoSlot          = -1
)

type cacheNamespace uint8

const (
	cacheMain cacheNamespace = iota
	cacheCymru
	cacheNamespaceCount
)

type cacheEntry struct {
	value       LookupResult
	expiresAt   time.Time
	ipinfoEpoch uint64
	negative    bool
	localIPInfo bool
}

// ttlCache is a namespace/TTL view of one Gate-owned store. Main enrichment and
// DNS records remain distinct even for the same address and share every shard.
type ttlCache struct {
	store      *lookupCacheStore
	namespace  cacheNamespace
	ttl        time.Duration
	negativeTT time.Duration
}

type lookupCacheStore struct {
	shards        []cacheShard
	maxEntries    int
	earlyBypasses [cacheNamespaceCount]atomic.Uint64 // Oversized keys need neither hashing nor a shard lock.
}

type cacheKey struct {
	namespace cacheNamespace
	address   string
}

type cacheSlot struct {
	key   cacheKey
	entry cacheEntry
	prev  int
	next  int // FIFO link while occupied; free-list link after removal.
}

// Each map index identifies exactly one occupied slot. Removed slots release
// key/value references and join the free list; fresh indices are used only up
// to quota. No eviction history, growing queue or secondary expiry index exists.
type cacheShard struct {
	mu          sync.Mutex
	quota       int
	items       map[cacheKey]int
	slots       []cacheSlot
	used        int
	free        int
	head        int
	tail        int
	counts      [cacheNamespaceCount]int
	ownedBytes  int
	evictions   uint64
	expirations uint64
	bypasses    uint64
}

func newTTLCache(shards int, ttl, negativeTTL time.Duration, maxEntries int) *ttlCache {
	return &ttlCache{
		store: newLookupCacheStore(shards, maxEntries),
		ttl:   ttl, negativeTT: negativeTTL,
	}
}

func newLookupCacheStore(shards, maxEntries int) *lookupCacheStore {
	if shards <= 0 {
		shards = 16
	}
	if maxEntries <= 0 {
		maxEntries = 100000
	}
	shards = min(shards, maxEntries)
	store := &lookupCacheStore{shards: make([]cacheShard, shards), maxEntries: maxEntries}
	for i := range store.shards {
		shard := &store.shards[i]
		shard.quota = maxEntries / shards
		if i < maxEntries%shards {
			shard.quota++
		}
		shard.free, shard.head, shard.tail = cacheNoSlot, cacheNoSlot, cacheNoSlot
	}
	return store
}

// get lazily removes the requested expired entry. Equality remains a hit, as in
// the original cache; idle expired records stay bounded by the same hard cap.
func (c *ttlCache) get(key string, now time.Time) (LookupResult, bool, bool) {
	return c.getWithIPInfo(key, now, nil)
}

// getWithIPInfo reads only the optional provenance alongside the result.
// Cymru uses the same cache read without requesting provenance.
func (c *ttlCache) getWithIPInfo(key string, now time.Time, origin *ipinfoCacheOrigin) (LookupResult, bool, bool) {
	if c == nil || c.store == nil || len(key) > maxCachePayloadBytes || strings.TrimSpace(key) == "" {
		return LookupResult{}, false, false
	}
	shard := &c.store.shards[c.shardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	index, ok := shard.items[cacheKey{c.namespace, key}]
	if !ok {
		return LookupResult{}, false, false
	}
	entry := &shard.slots[index].entry
	if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
		shard.remove(index)
		shard.expirations++
		return LookupResult{}, false, false
	}
	if origin != nil {
		origin.epoch, origin.local = entry.ipinfoEpoch, entry.localIPInfo
	}
	return entry.value, true, entry.negative
}

type ipinfoCacheOrigin struct {
	epoch uint64
	local bool
}

// set admits bounded exact copies. Updating a key refreshes its write order;
// reading it does not. Full shards remove one FIFO head before adding membership,
// so concurrent writers cannot transiently exceed the sum of shard quotas.
func (c *ttlCache) set(key string, value LookupResult, now time.Time, negative bool) {
	c.setEntry(key, now, cacheEntry{value: value, negative: negative})
}

func (c *ttlCache) setEntry(key string, now time.Time, entry cacheEntry) {
	if c == nil || c.store == nil {
		return
	}
	if len(key) > maxCachePayloadBytes {
		c.store.earlyBypasses[c.namespace].Add(1)
		return // Such a key could never have an admitted old record.
	}
	if strings.TrimSpace(key) == "" {
		return
	}
	bytes, admitted := cachePayloadSize(key, entry.value)
	shard := &c.store.shards[c.shardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	index, exists := shard.items[cacheKey{c.namespace, key}]
	if exists && shard.slots[index].entry.ipinfoEpoch > entry.ipinfoEpoch {
		return // A lookup from a retired epoch cannot displace current evidence.
	}
	if !admitted {
		shard.bypasses++
		if exists {
			shard.remove(index) // An uncached update must not leave the old value readable.
		}
		return
	}
	if exists {
		shard.ownedBytes -= cacheStoredBytes(shard.slots[index])
		shard.unlink(index)
	} else {
		index = shard.acquire()
		shard.slots[index].key = cacheKey{c.namespace, strings.Clone(key)}
		shard.items[shard.slots[index].key] = index
		shard.counts[c.namespace]++
	}
	slot := &shard.slots[index]
	entry.value = ownLookupResult(entry.value, slot.entry.value)
	ttl := c.ttl
	if entry.negative {
		ttl = c.negativeTT
	}
	if ttl > 0 {
		entry.expiresAt = now.Add(ttl)
	}
	slot.entry = entry
	shard.ownedBytes += bytes
	shard.append(index)
}

// acquire allocates each shard's fixed backing once, then reuses removed slots.
// Initial allocation is separate from steady overflow: no record walk, free-slot
// search or per-eviction list-node allocation is needed once backing exists.
func (s *cacheShard) acquire() int {
	if s.slots == nil {
		s.slots = make([]cacheSlot, s.quota)
		s.items = make(map[cacheKey]int, s.quota)
	}
	if s.free == cacheNoSlot && s.used == s.quota {
		s.remove(s.head)
		s.evictions++
	}
	if s.free != cacheNoSlot {
		index := s.free
		s.free = s.slots[index].next
		return index
	}
	index := s.used
	s.used++
	return index
}

func (s *cacheShard) unlink(index int) {
	slot := &s.slots[index]
	if slot.prev == cacheNoSlot {
		s.head = slot.next
	} else {
		s.slots[slot.prev].next = slot.next
	}
	if slot.next == cacheNoSlot {
		s.tail = slot.prev
	} else {
		s.slots[slot.next].prev = slot.prev
	}
}

func (s *cacheShard) append(index int) {
	slot := &s.slots[index]
	slot.prev, slot.next = s.tail, cacheNoSlot
	if s.tail == cacheNoSlot {
		s.head = index
	} else {
		s.slots[s.tail].next = index
	}
	s.tail = index
}

func (s *cacheShard) remove(index int) {
	slot := &s.slots[index]
	delete(s.items, slot.key)
	s.counts[slot.key.namespace]--
	s.ownedBytes -= cacheStoredBytes(*slot)
	s.unlink(index)
	*slot = cacheSlot{prev: cacheNoSlot, next: s.free}
	s.free = index
}

// Subtract rather than sum unchecked input lengths: admission must remain safe
// even for unusually large source fields. Stored values passed this check.
func cachePayloadSize(key string, value LookupResult) (int, bool) {
	remaining := maxCachePayloadBytes
	for _, text := range [...]string{key, value.ASN, value.CountryCode, value.CountryName, value.ContinentCode, value.Source} {
		if len(text) > remaining {
			return 0, false
		}
		remaining -= len(text)
	}
	return maxCachePayloadBytes - remaining, true
}

func cacheStoredBytes(slot cacheSlot) int {
	v := slot.entry.value
	return len(slot.key.address) + len(v.ASN) + len(v.CountryCode) + len(v.CountryName) + len(v.ContinentCode) + len(v.Source)
}

// Keep equal, already-owned fields on refresh. New/changed fields own their
// exact text independently, so a substring cannot pin an oversized source or an
// obsolete key/value block after removal. Warm unchanged refreshes allocate zero.
func ownLookupResult(value, previous LookupResult) LookupResult {
	if value.ASN != previous.ASN {
		previous.ASN = strings.Clone(value.ASN)
	}
	if value.CountryCode != previous.CountryCode {
		previous.CountryCode = strings.Clone(value.CountryCode)
	}
	if value.CountryName != previous.CountryName {
		previous.CountryName = strings.Clone(value.CountryName)
	}
	if value.ContinentCode != previous.ContinentCode {
		previous.ContinentCode = strings.Clone(value.ContinentCode)
	}
	if value.Source != previous.Source {
		previous.Source = strings.Clone(value.Source)
	}
	previous.FetchedAt = value.FetchedAt
	return previous
}

func (c *ttlCache) shardIndex(key string) int {
	return int(fnv32a(key) % uint32(len(c.store.shards)))
}

func fnv32a(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	hash := uint32(offset32)
	for i := 0; i < len(s); i++ {
		hash ^= uint32(s[i])
		hash *= prime32
	}
	return hash
}
