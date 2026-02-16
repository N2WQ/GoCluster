package solarweather

import (
	"container/list"
	"sync"
	"time"
)

type pathKey struct {
	userCell uint16
	dxCell   uint16
	userGrid string
	dxGrid   string
	band     string
}

type gateState struct {
	daylight bool
	highLat  bool
	lastSeen time.Time
}

type gateEntry struct {
	key   pathKey
	state gateState
}

// GateCache is a bounded LRU cache for per-path gate state.
type GateCache struct {
	mu    sync.Mutex
	max   int
	ttl   time.Duration
	items map[pathKey]*list.Element
	lru   *list.List
}

func NewGateCache(cfg GateCacheConfig) *GateCache {
	max := cfg.MaxEntries
	if max < 1 {
		max = 1
	}
	ttl := time.Duration(cfg.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &GateCache{
		max:   max,
		ttl:   ttl,
		items: make(map[pathKey]*list.Element, max),
		lru:   list.New(),
	}
}

func (c *GateCache) Get(key pathKey, now time.Time) (gateState, bool) {
	if c == nil {
		return gateState{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem := c.items[key]
	if elem == nil {
		return gateState{}, false
	}
	entry, ok := elem.Value.(gateEntry)
	if !ok {
		c.removeElement(elem)
		return gateState{}, false
	}
	if c.ttl > 0 && now.Sub(entry.state.lastSeen) > c.ttl {
		c.removeElement(elem)
		return gateState{}, false
	}
	entry.state.lastSeen = now
	elem.Value = entry
	c.lru.MoveToFront(elem)
	return entry.state, true
}

func (c *GateCache) Put(key pathKey, state gateState, now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem := c.items[key]; elem != nil {
		entry, ok := elem.Value.(gateEntry)
		if !ok {
			c.removeElement(elem)
		} else {
			entry.state = state
			entry.state.lastSeen = now
			elem.Value = entry
			c.lru.MoveToFront(elem)
			return
		}
	}
	entry := gateEntry{key: key, state: state}
	entry.state.lastSeen = now
	elem := c.lru.PushFront(entry)
	c.items[key] = elem
	for len(c.items) > c.max {
		c.evictOldest()
	}
}

func (c *GateCache) evictOldest() {
	oldest := c.lru.Back()
	if oldest == nil {
		return
	}
	c.removeElement(oldest)
}

func (c *GateCache) removeElement(elem *list.Element) {
	entry, ok := elem.Value.(gateEntry)
	if ok {
		delete(c.items, entry.key)
	} else {
		for key, candidate := range c.items {
			if candidate == elem {
				delete(c.items, key)
				break
			}
		}
	}
	c.lru.Remove(elem)
}
