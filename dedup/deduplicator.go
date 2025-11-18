package dedup

import (
	"log"
	"sync"
	"time"

	"dxcluster/spot"
)

// Deduplicator removes duplicate spots within a time window
type Deduplicator struct {
	window          time.Duration
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
	cache          map[uint32]time.Time
	processedCount uint64
	duplicateCount uint64
}

// shardCount must remain a power of two so we can use bit masking for fast shard selection.
const shardCount = 64

// NewDeduplicator creates a new deduplicator with the specified window
func NewDeduplicator(window time.Duration) *Deduplicator {
	shards := make([]cacheShard, shardCount)
	for i := range shards {
		shards[i].cache = make(map[uint32]time.Time)
	}
	return &Deduplicator{
		window:          window,
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

			if isDuplicateLocked(shard.cache, hash, s.Time, d.window) {
				shard.duplicateCount++
				shard.mu.Unlock()
				continue // Skip duplicate (logging handled by stats display)
			}

			// Add to cache
			shard.cache[hash] = s.Time
			shard.mu.Unlock()

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
// Caller must hold the shard mutex.
func isDuplicateLocked(cache map[uint32]time.Time, hash uint32, spotTime time.Time, window time.Duration) bool {
	lastSeen, exists := cache[hash]
	if !exists {
		return false
	}

	// Check if within dedup window
	age := spotTime.Sub(lastSeen)
	if age < 0 {
		age = -age // Handle out-of-order spots
	}

	return age < window
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
			age := now.Sub(lastSeen)
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
