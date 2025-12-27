// Package dedup implements a shard-locked deduplication cache that suppresses
// identical spots within a configurable time window. All sources feed into this
// component before entering the shared ring buffer.
package dedup

import (
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
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
	lastProcessed   atomic.Int64
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

// Purpose: Construct a deduplicator with windowed suppression and channels.
// Key aspects: Sizes input/output buffers and initializes shard maps.
// Upstream: main startup wiring.
// Downstream: cache shard allocation and channel creation.
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

// Purpose: Start dedup processing and cleanup loops.
// Key aspects: Spawns goroutines for processing and cleanup.
// Upstream: main startup.
// Downstream: process and cleanupLoop goroutines.
// Start begins the deduplication processing loop and the background cleanup
// goroutine. Safe to call once during startup.
func (d *Deduplicator) Start() {
	log.Println("Deduplicator: Starting unified processing loop for ALL sources")

	// Start the main processing goroutine
	// Purpose: Consume dedup input channel and forward unique spots.
	// Key aspects: Runs until shutdown is closed.
	// Upstream: Deduplicator.Start.
	// Downstream: d.process.
	go d.process()

	// Start the cache cleanup goroutine
	// Purpose: Periodically purge expired cache entries.
	// Key aspects: Runs until shutdown is closed.
	// Upstream: Deduplicator.Start.
	// Downstream: d.cleanupLoop.
	go d.cleanupLoop()
}

// Purpose: Signal processing and cleanup loops to exit.
// Key aspects: Closing shutdown unblocks the goroutines.
// Upstream: main shutdown.
// Downstream: channel close only.
// Stop signals the processing and cleanup loops to exit.
func (d *Deduplicator) Stop() {
	log.Println("Deduplicator: Stopping...")
	close(d.shutdown)
}

// Purpose: Expose the deduplicator input channel.
// Key aspects: Callers send spots for deduplication.
// Upstream: ingest pipelines (RBN, PSKReporter, peer).
// Downstream: d.inputChan.
// GetInputChannel returns the input channel for spots. Each spot is checked
// against the windowed cache and either forwarded or dropped.
func (d *Deduplicator) GetInputChannel() chan<- *spot.Spot {
	return d.inputChan
}

// Purpose: Expose the deduplicator output channel.
// Key aspects: Consumers read unique spots from this channel.
// Upstream: pipeline output stage.
// Downstream: d.outputChan.
// GetOutputChannel returns the output channel for deduplicated spots. Consumers
// read from this to continue the pipeline (ring buffer, telnet broadcast, etc.).
func (d *Deduplicator) GetOutputChannel() <-chan *spot.Spot {
	return d.outputChan
}

// Purpose: Main processing loop for dedup input.
// Key aspects: Reads input channel until shutdown is closed.
// Upstream: goroutine started in Start.
// Downstream: processSpot.
// process is the main processing loop
func (d *Deduplicator) process() {
	for {
		select {
		case <-d.shutdown:
			log.Println("Deduplicator: Process loop stopped")
			return
		case s := <-d.inputChan:
			if s == nil {
				continue
			}
			d.lastProcessed.Store(time.Now().UTC().UnixNano())
			d.processSpot(s)
		}
	}
}

// Purpose: Deduplicate a single spot and forward if accepted.
// Key aspects: Uses shard locks, optional stronger-SNR replacement, and output channel.
// Upstream: process loop.
// Downstream: isDuplicateLocked, shardFor, and d.outputChan.
func (d *Deduplicator) processSpot(s *spot.Spot) {
	// Purpose: Recover from panics during spot processing.
	// Key aspects: Logs stack trace and continues.
	// Upstream: processSpot.
	// Downstream: log.Printf and debug.Stack.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Deduplicator: panic processing spot: %v\n%s", r, debug.Stack())
		}
	}()
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
			return // Skip duplicate (logging handled by stats display)
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

// Purpose: Return the timestamp of the most recent processed spot.
// Key aspects: Reads the atomic timestamp; zero indicates no activity.
// Upstream: pipeline health monitor.
// Downstream: time.Unix.
// LastProcessedAt returns the timestamp of the most recent spot seen by the deduper.
// A zero time means no spots have been processed yet.
func (d *Deduplicator) LastProcessedAt() time.Time {
	if d == nil {
		return time.Time{}
	}
	ns := d.lastProcessed.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}

// Purpose: Check if a spot hash is a duplicate within the time window.
// Key aspects: Caller must hold the shard lock; handles out-of-order time.
// Upstream: processSpot.
// Downstream: time.Sub comparisons.
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

// Purpose: Periodically remove expired cache entries.
// Key aspects: Ticker-driven cleanup until shutdown.
// Upstream: goroutine started in Start.
// Downstream: cleanup.
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

// Purpose: Remove expired cache entries across all shards.
// Key aspects: Two-phase deletion to minimize lock time.
// Upstream: cleanupLoop.
// Downstream: shard cache mutation.
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

// Purpose: Return deduplication stats across all shards.
// Key aspects: Aggregates processed/duplicate counts and cache size.
// Upstream: stats display.
// Downstream: shard counters under lock.
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

// Purpose: Pick the shard for a given hash.
// Key aspects: Uses bitmask with power-of-two shard count.
// Upstream: processSpot.
// Downstream: shard selection only.
func (d *Deduplicator) shardFor(hash uint32) *cacheShard {
	idx := hash & (shardCount - 1)
	return &d.shards[idx]
}
