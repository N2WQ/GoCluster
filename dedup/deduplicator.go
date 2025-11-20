package dedup

import (
	"log"
	"sync"
	"time"

	"dxcluster/spot"
)

// Deduplicator removes duplicate spots within a time window. A zero or negative
// window effectively disables filtering while keeping the pipeline topology
// intact (the component simply never flags duplicates).
type Deduplicator struct {
	window          time.Duration
	preferStronger  bool
	shards          []cacheShard
	inputChan       chan *spot.Spot
	outputChan      chan *spot.Spot
	shutdown        chan struct{}
	cleanupInterval time.Duration
}

// cacheShard keeps a portion of the dedup cache guarded by its own lock.
// Sharding the map eliminates the single global mutex on the hot path.
type cacheShard struct {
	mu             sync.Mutex
	cache          map[uint32]cachedEntry
	processedCount uint64
	duplicateCount uint64
}

// cachedEntry tracks when we last saw a hash and the associated SNR so we can
// optionally choose the strongest representative when duplicates collide.
type cachedEntry struct {
	when time.Time
	snr  int
}

// shardCount must remain a power of two so we can use bit masking for fast shard selection.
const shardCount = 64

// NewDeduplicator creates a new deduplicator with the specified window. Passing
// a zero window disables suppression but still allows metrics/visibility.
func NewDeduplicator(window time.Duration, preferStronger bool) *Deduplicator {
	shards := make([]cacheShard, shardCount)
	for i := range shards {
		shards[i].cache = make(map[uint32]cachedEntry)
	}
	return &Deduplicator{
		window:          window,
		preferStronger:  preferStronger,
		shards:          shards,
		inputChan:       make(chan *spot.Spot, 1000),
		outputChan:      make(chan *spot.Spot, 1000),
		shutdown:        make(chan struct{}),
		cleanupInterval: 60 * time.Second, // Clean cache every 60 seconds
	}
}

// Start begins the deduplication processing loop
func (d *Deduplicator) Start() {
	log.Println("Deduplicator: Starting unified processing loop for ALL sources")

	// Start the main processing goroutine
	go d.process()

	// Start the cache cleanup goroutine
	go d.cleanupLoop()
}

// Stop stops the deduplicator
func (d *Deduplicator) Stop() {
	log.Println("Deduplicator: Stopping...")
	close(d.shutdown)
}

// GetInputChannel returns the input channel for spots
func (d *Deduplicator) GetInputChannel() chan<- *spot.Spot {
	return d.inputChan
}

// GetOutputChannel returns the output channel for deduplicated spots
func (d *Deduplicator) GetOutputChannel() <-chan *spot.Spot {
	return d.outputChan
}

// process is the main processing loop
func (d *Deduplicator) process() {
	for {
		select {
		case <-d.shutdown:
			log.Println("Deduplicator: Process loop stopped")
			return
		case s := <-d.inputChan:
			hash := s.Hash32()
			shard := d.shardFor(hash)

			shard.mu.Lock()
			shard.processedCount++

			dup, lastSeen := isDuplicateLocked(shard.cache, hash, s.Time, d.window)
			if dup {
				// Optionally favor the stronger SNR when a duplicate collides within the window.
				if d.preferStronger && s.Report > lastSeen.snr {
					// Replace the cached timestamp/SNR with the stronger spot and forward it.
					shard.cache[hash] = cachedEntry{when: s.Time, snr: s.Report}
					shard.mu.Unlock()
				} else {
					shard.duplicateCount++
					shard.mu.Unlock()
					continue // Skip duplicate (logging handled by stats display)
				}
			} else {
				// Add to cache
				shard.cache[hash] = cachedEntry{when: s.Time, snr: s.Report}
				shard.mu.Unlock()
			}

			// Send to output channel
			select {
			case d.outputChan <- s:
				// Successfully sent
			default:
				log.Println("Deduplicator: Output channel full, dropping spot")
			}
		}
	}
}

// isDuplicateLocked checks if a spot is a duplicate within a shard.
// Caller must hold the shard mutex. When the window is zero the function always
// returns false, effectively bypassing deduplication.
func isDuplicateLocked(cache map[uint32]cachedEntry, hash uint32, spotTime time.Time, window time.Duration) (bool, cachedEntry) {
	lastSeen, exists := cache[hash]
	if !exists {
		return false, cachedEntry{}
	}

	// Check if within dedup window
	age := spotTime.Sub(lastSeen.when)
	if age < 0 {
		age = -age // Handle out-of-order spots
	}

	return age < window, lastSeen
}

// cleanupLoop periodically removes expired entries from the cache
func (d *Deduplicator) cleanupLoop() {
	ticker := time.NewTicker(d.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.shutdown:
			log.Println("Deduplicator: Cleanup loop stopped")
			return
		case <-ticker.C:
			d.cleanup()
		}
	}
}

// cleanup removes expired entries from the cache
func (d *Deduplicator) cleanup() {
	now := time.Now().UTC()
	removed := 0
	for i := range d.shards {
		shard := &d.shards[i]
		shard.mu.Lock()
		for hash, lastSeen := range shard.cache {
			age := now.Sub(lastSeen.when)
			if age > d.window {
				delete(shard.cache, hash)
				removed++
			}
		}
		shard.mu.Unlock()
	}

	if removed > 0 {
		log.Printf("Deduplicator: Cleaned %d expired entries from cache", removed)
	}
}

// GetStats returns current deduplication statistics
func (d *Deduplicator) GetStats() (processed uint64, duplicates uint64, cacheSize int) {
	for i := range d.shards {
		shard := &d.shards[i]
		shard.mu.Lock()
		processed += shard.processedCount
		duplicates += shard.duplicateCount
		cacheSize += len(shard.cache)
		shard.mu.Unlock()
	}
	return processed, duplicates, cacheSize
}

func (d *Deduplicator) shardFor(hash uint32) *cacheShard {
	idx := hash & (shardCount - 1)
	return &d.shards[idx]
}
