package peer

import (
	"container/heap"
	"strings"
	"sync"
	"time"
)

type dedupeAdmission uint8

const (
	dedupeDuplicate dedupeAdmission = iota
	dedupeAccepted
	dedupeFull
)

// One owned index entry is also the expiry heap item. Fixed buckets and the
// preallocated heap cannot grow when many exact keys collide.
type dedupeItem = boundedEntry[string, int64]
type dedupeExpiry []*dedupeItem

func (h dedupeExpiry) Len() int           { return len(h) }
func (h dedupeExpiry) Less(i, j int) bool { return h[i].value < h[j].value }
func (h dedupeExpiry) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *dedupeExpiry) Push(value any) {
	item, ok := value.(*dedupeItem)
	if !ok {
		panic("peer: invalid dedupe expiry item")
	}
	*h = append(*h, item)
}
func (h *dedupeExpiry) Pop() any {
	old := *h
	n := len(old) - 1
	item := old[n]
	old[n] = nil
	*h = old[:n]
	return item
}

// dedupeCache owns exact keys and one expiry index entry per admitted key.
// Input callers can reach the lock in a different order from their captured
// elapsed times, so expiration uses a heap rather than assuming FIFO time.
// Duplicate hits never update age or add index entries. Both count and key-byte
// limits are enforced before insertion; no unexpired entry is ever evicted.
type dedupeCache struct {
	epoch                   time.Time
	initialized             bool
	mu                      sync.Mutex
	items                   *boundedIndex[string, int64]
	expiry                  dedupeExpiry
	ttl                     time.Duration
	limit, byteLimit, bytes int
	refused                 uint64
}

func newDedupeCache(ttl time.Duration) *dedupeCache { return newBoundedDedupe(ttl, 131072, 64<<20) }
func newBoundedDedupe(ttl time.Duration, limit, byteLimit int) *dedupeCache {
	return &dedupeCache{items: newFixedIndex[string, int64](limit), ttl: ttl, limit: limit, byteLimit: byteLimit, expiry: make(dedupeExpiry, 0, limit)}
}
func (c *dedupeCache) admit(key string, now time.Time) dedupeAdmission {
	if c == nil || key == "" {
		return dedupeFull
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(now)
	if _, ok := c.items.Get(key); ok {
		return dedupeDuplicate
	}
	if c.items.Len() >= c.limit || len(key) > c.byteLimit-c.bytes {
		c.refused++
		return dedupeFull
	}
	key = strings.Clone(key)
	if !c.initialized {
		c.epoch = now
		c.initialized = true
	}
	item := c.items.Set(key, int64(now.Sub(c.epoch)))
	// The same mutex guards count admission and insertion, so Set cannot refuse
	// after the existing limit check. No second expiry entry is made for a hit.
	c.bytes += len(key)
	heap.Push(&c.expiry, item)
	return dedupeAccepted
}
func (c *dedupeCache) markSeen(key string, now time.Time) bool {
	return c.admit(key, now) == dedupeAccepted
}
func (c *dedupeCache) prune(now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(now)
}
func (c *dedupeCache) pruneLocked(now time.Time) {
	for len(c.expiry) > 0 {
		item := c.expiry[0]
		if int64(now.Sub(c.epoch))-item.value <= int64(c.ttl) {
			break
		}
		heap.Pop(&c.expiry)
		c.bytes -= len(item.key)
		// Delete clears the popped entry, including its key and chain link.
		// Account first and never leave a cleared entry in the expiry heap.
		c.items.Delete(item.key)
	}
}
func (c *dedupeCache) occupancy() (entries, bytes int, refused uint64) {
	if c == nil {
		return 0, 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.items.Len(), c.bytes, c.refused
}
func (c *dedupeCache) contains(key string, now time.Time) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(now)
	_, ok := c.items.Get(key)
	return ok
}
