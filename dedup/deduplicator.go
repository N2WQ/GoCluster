// Package dedup implements a shard-locked deduplication cache that suppresses
// identical spots within a configurable time window. All sources feed into this
// component before entering the shared ring buffer.
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
	when      time.Time
	snr       int
	hasReport bool
}

// shardCount must remain a power of two so we can use bit masking for fast shard selection.
const shardCount = 64

// NewDeduplicator creates a new deduplicator with the specified window. Passing
// a zero window disables suppression but still allows metrics/visibility.
func NewDeduplicator(window time.Duration, preferStronger bool, outputBuffer int) *Deduplicator {
	if outputBuffer <= 0 {
		outputBuffer = 1000
	}
	// Size the input channel generously to absorb upstream bursts; tie it to the
	// output buffer so input can queue at least as much as we can emit.
	inputBuffer := outputBuffer
	if inputBuffer < 10000 {
		inputBuffer = 10000
	}
	shards := make([]cacheShard, shardCount)
	for i := range shards {
		shards[i].cache = make(map[uint32]cachedEntry)
	}
	return &Deduplicator{
		window:          window,
		preferStronger:  preferStronger,
		shards:          shards,
		inputChan:       make(chan *spot.Spot, inputBuffer),
		outputChan:      make(chan *spot.Spot, outputBuffer),
		shutdown:        make(chan struct{}),
		cleanupInterval: 60 * time.Second, // Clean cache every 60 seconds
	}
}

// Start begins the deduplication processing loop and the background cleanup
// goroutine. Safe to call once during startup.
func (d *Deduplicator) Start() {
	log.Println("Deduplicator: Starting unified processing loop for ALL sources")

	// Start the main processing goroutine
	go d.process()

	// Start the cache cleanup goroutine
	go d.cleanupLoop()
}

// Stop signals the processing and cleanup loops to exit.
func (d *Deduplicator) Stop() {
	log.Println("Deduplicator: Stopping...")
	close(d.shutdown)
}

// GetInputChannel returns the input channel for spots. Each spot is checked
// against the windowed cache and either forwarded or dropped.
func (d *Deduplicator) GetInputChannel() chan<- *spot.Spot {
	return d.inputChan
}

// GetOutputChannel returns the output channel for deduplicated spots. Consumers
// read from this to continue the pipeline (ring buffer, telnet broadcast, etc.).
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
				upgradeToReported := s.HasReport && !lastSeen.hasReport
				// Optionally favor the stronger SNR when a duplicate collides within the window.
				stronger := d.preferStronger && s.Report > lastSeen.snr
				if upgradeToReported || stronger {
					// Replace the cached timestamp/SNR with the stronger or newly reported spot and forward it.
					shard.cache[hash] = cachedEntry{when: s.Time, snr: s.Report, hasReport: s.HasReport}
					shard.mu.Unlock()
				} else {
					shard.duplicateCount++
					shard.mu.Unlock()
					continue // Skip duplicate (logging handled by stats display)
				}
			} else {
				// Add to cache
				shard.cache[hash] = cachedEntry{when: s.Time, snr: s.Report, hasReport: s.HasReport}
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

// cleanupLoop periodically removes expired entries from the cache so the
// footprint stays bounded when dedup is enabled.
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
		// Phase 1: collect expired keys under a short lock.
		shard.mu.Lock()
		toDelete := make([]uint32, 0, len(shard.cache)/10)
		for hash, lastSeen := range shard.cache {
			if now.Sub(lastSeen.when) > d.window {
				toDelete = append(toDelete, hash)
			}
		}
		shard.mu.Unlock()

		// Phase 2: delete under lock.
		if len(toDelete) > 0 {
			shard.mu.Lock()
			for _, hash := range toDelete {
				delete(shard.cache, hash)
				removed++
			}
			shard.mu.Unlock()
		}
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
