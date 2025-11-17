// Package dedup provides hash-based spot deduplication for the DX Cluster Server.
//
// The deduplicator is the centerpiece of the unified architecture where ALL spot sources
// (RBN, PSKReporter, upstream clusters, MQTT) feed into a single deduplication engine.
//
// Architecture:
//   RBN Network ──┐
//   PSKReporter ──┼─→ Input Channel → Dedup Engine → Output Channel → Ring Buffer
//   Other Sources─┘
//
// How Deduplication Works:
//  1. Each spot is hashed using Spot.Hash32() (DXCall|DECall|FreqKHz|TimeMinute)
//  2. Hash is checked against cache (map[uint32]time.Time)
//  3. If hash exists and hasn't expired: DUPLICATE - drop the spot
//  4. If hash is new or expired: UNIQUE - add to cache and forward to output
//  5. Cache entries expire after configurable window (typically 120 seconds)
//  6. Periodic cleanup removes expired hashes (every 30 seconds)
//
// Key Features:
//   - 32-bit FNV-1a hashing for speed and good distribution
//   - Time-based expiry (configurable window, typically 120s)
//   - Thread-safe concurrent access via RWMutex
//   - Statistics tracking (total spots, duplicates, cache size)
//   - Buffered channels (1000 spots) for high throughput
//   - Graceful shutdown with final statistics
package dedup

import (
	"fmt"
	"log"
	"sync"
	"time"

	"dxcluster/spot"
)

// Deduplicator implements hash-based duplicate spot detection.
//
// The deduplicator maintains a cache of recently-seen spot hashes with expiry times.
// All spots from all sources flow through a single input channel, are checked for
// duplicates, and unique spots flow out to the output channel.
//
// Fields:
//   - window: Time duration for considering spots as duplicates (typically 120s)
//   - cache: Map of hash → expiry time for duplicate detection
//   - mu: Read-write mutex for thread-safe cache access
//   - ticker: Timer for periodic cache cleanup (every 30s)
//   - stopCh: Channel for coordinating graceful shutdown
//   - inputChan: Buffered channel receiving spots from ALL sources
//   - outputChan: Buffered channel sending deduplicated spots to ring buffer
//   - Statistics: Counters for total spots, duplicates, and kept newer duplicates
//
// Thread Safety:
//   - Process() uses write lock for cache modifications
//   - cleanup() uses write lock for removing expired entries
//   - GetStats() uses read lock for reading counters
type Deduplicator struct {
	window time.Duration           // Deduplication time window (typically 120 seconds)
	cache  map[uint32]time.Time    // Hash → expiry time mapping
	mu     sync.RWMutex            // Read-write mutex for thread-safe access
	ticker *time.Ticker            // Periodic cleanup timer (30 second intervals)
	stopCh chan struct{}           // Shutdown coordination channel

	// Unified architecture channels
	inputChan  chan *spot.Spot     // Input from ALL sources (RBN, PSKReporter, clusters, MQTT)
	outputChan chan *spot.Spot     // Output to ring buffer (deduplicated spots only)

	// Statistics counters
	totalSpots     uint64           // Total spots processed
	duplicateSpots uint64           // Duplicate spots detected and dropped
	keptNewest     uint64           // Count of times we kept a newer duplicate
}

// NewDeduplicator creates and initializes a new hash-based deduplicator.
//
// Parameters:
//   - window: Time duration for deduplication (typically 120 seconds)
//             Spots with the same hash within this window are considered duplicates
//
// Returns:
//   - *Deduplicator: Initialized deduplicator with running cleanup goroutine
//
// The deduplicator is created with:
//   - Empty hash cache
//   - 30-second cleanup ticker (automatically started)
//   - Buffered input channel (capacity 1000 spots)
//   - Buffered output channel (capacity 1000 spots)
//   - Background cleanup goroutine (running)
//
// After creation, call Start() to begin processing spots from the input channel.
//
// Typical usage:
//   dedup := dedup.NewDeduplicator(120 * time.Second)
//   dedup.Start()
func NewDeduplicator(window time.Duration) *Deduplicator {
	d := &Deduplicator{
		window:     window,
		cache:      make(map[uint32]time.Time),
		ticker:     time.NewTicker(30 * time.Second), // Cleanup every 30 seconds
		stopCh:     make(chan struct{}),
		inputChan:  make(chan *spot.Spot, 1000),     // Buffer 1000 input spots
		outputChan: make(chan *spot.Spot, 1000),     // Buffer 1000 output spots
	}

	// Start background cleanup goroutine
	// This runs continuously and removes expired hashes every 30 seconds
	go d.cleanupLoop()

	log.Printf("Deduplicator created with %v window", window)

	return d
}

// Start begins the unified deduplication processing loop.
// This launches a goroutine that continuously processes spots from the input channel.
//
// The processing loop:
//  1. Reads spots from inputChan
//  2. Checks each spot for duplicates using Process()
//  3. Forwards unique spots to outputChan
//  4. Drops duplicate spots
//  5. Runs until Stop() is called
//
// This method is non-blocking - it starts the processing goroutine and returns immediately.
//
// IMPORTANT: All spot sources (RBN, PSKReporter, etc.) must feed into the input channel
// obtained via GetInputChannel(). The deduplicated output is available via GetOutputChannel().
func (d *Deduplicator) Start() {
	log.Println("Deduplicator: Starting unified processing loop for ALL sources")
	go d.processLoop()
}

// processLoop is the main processing goroutine that handles all incoming spots.
// It consumes from inputChan and produces to outputChan.
//
// Flow:
//  1. Wait for spot on inputChan or shutdown signal
//  2. Call Process() to check if spot is duplicate
//  3. If unique: forward to outputChan
//  4. If duplicate: drop (do not forward)
//  5. Repeat until stopCh is closed
//
// This goroutine runs continuously from Start() until Stop() is called.
// On shutdown, it closes the output channel to signal downstream consumers.
func (d *Deduplicator) processLoop() {
	for {
		select {
		case <-d.stopCh:
			// Shutdown signal received
			log.Println("Deduplicator: Processing loop shutting down")
			close(d.outputChan) // Signal downstream that no more spots are coming
			return

		case spot := <-d.inputChan:
			// Process spot through duplicate detection
			shouldBroadcast, _ := d.Process(spot)

			if shouldBroadcast {
				// Not a duplicate - send to output channel for ring buffer
				d.outputChan <- spot
			}
			// If duplicate, spot is silently dropped (not sent to output)
		}
	}
}

// GetInputChannel returns the send-only input channel where ALL spot sources should feed.
//
// Returns:
//   - chan<- *spot.Spot: Send-only channel for submitting spots to the deduplicator
//
// ALL spot sources must use this channel:
//   - RBN client
//   - PSKReporter client
//   - Upstream cluster connections
//   - MQTT sources
//   - Manual spot entry
//
// Example usage:
//   dedupInput := deduplicator.GetInputChannel()
//   dedupInput <- spotFromRBN
func (d *Deduplicator) GetInputChannel() chan<- *spot.Spot {
	return d.inputChan
}

// GetOutputChannel returns the receive-only output channel that feeds the ring buffer.
//
// Returns:
//   - <-chan *spot.Spot: Receive-only channel for consuming deduplicated spots
//
// Only unique (non-duplicate) spots are sent to this channel.
// The ring buffer and broadcast system consume from this channel.
//
// Example usage:
//   dedupOutput := deduplicator.GetOutputChannel()
//   for spot := range dedupOutput {
//       buffer.Add(spot)
//       telnetServer.BroadcastSpot(spot)
//   }
func (d *Deduplicator) GetOutputChannel() <-chan *spot.Spot {
	return d.outputChan
}

// Process checks if a spot is a duplicate using hash-based detection.
//
// Parameters:
//   - s: Spot to check for duplication
//
// Returns:
//   - shouldBroadcast: true if spot is unique and should be forwarded
//   - isDuplicate: true if spot is a duplicate and should be dropped
//
// Algorithm:
//  1. Compute 32-bit hash of spot (DXCall|DECall|FreqKHz|TimeMinute)
//  2. Check if hash exists in cache
//  3. If exists and not expired: DUPLICATE
//  4. If new or expired: UNIQUE - add to cache with new expiry
//
// Thread Safety: Uses write lock (exclusive access to cache)
//
// Statistics: Increments totalSpots always, duplicateSpots for duplicates
//
// This method is kept for backward compatibility and is used internally by processLoop().
// It's also available for direct use in testing or legacy code paths.
func (d *Deduplicator) Process(s *spot.Spot) (bool, bool) {
	// Compute 32-bit hash of the spot
	hash := s.Hash32()

	d.mu.Lock()
	defer d.mu.Unlock()

	d.totalSpots++
	now := time.Now()

	// Debug logging: show first few hashes to verify dedup is working
	if d.totalSpots <= 5 {
		log.Printf("DEBUG: Spot #%d: %s on %.1f kHz -> hash 0x%08x",
			d.totalSpots, s.DXCall, s.Frequency, hash)
	}

	// Check if we've seen this hash recently
	if expiry, exists := d.cache[hash]; exists {
		// Hash exists - check if it's still within the deduplication window
		if now.Before(expiry) {
			// It's a duplicate within the window
			d.duplicateSpots++

			log.Printf("DUPLICATE: %s on %.1f kHz (hash 0x%08x) - %d dupes so far",
				s.DXCall, s.Frequency, hash, d.duplicateSpots)

			// Log statistics periodically (every 10 duplicates)
			if d.duplicateSpots%10 == 0 {
				log.Printf("Dedup stats: %d spots, %d duplicates (%.1f%%), cache size=%d, window=%v",
					d.totalSpots, d.duplicateSpots,
					float64(d.duplicateSpots)/float64(d.totalSpots)*100,
					len(d.cache), d.window)
			}

			return false, true // Don't broadcast, it's a duplicate
		}
		// Hash exists but expired - treat as new spot (falls through to add)
	}

	// New spot or expired hash - add to cache with new expiry time
	d.cache[hash] = now.Add(d.window)

	// Debug logging: show cache additions for first few spots
	if d.totalSpots <= 5 {
		log.Printf("DEBUG: Added hash 0x%08x to cache, expires at %v",
			hash, now.Add(d.window).Format("15:04:05"))
	}

	return true, false // Broadcast it, not a duplicate
}

// cleanupLoop is a background goroutine that periodically removes expired hashes.
//
// Runs continuously with a 30-second ticker until Stop() is called.
// This prevents the cache from growing unbounded by removing expired entries.
//
// On each tick:
//  1. Calls cleanup() to scan and remove expired hashes
//  2. Logs number of entries removed and remaining
//
// This goroutine is started automatically by NewDeduplicator().
func (d *Deduplicator) cleanupLoop() {
	for {
		select {
		case <-d.stopCh:
			// Shutdown signal received
			return
		case <-d.ticker.C:
			// Cleanup timer fired (every 30 seconds)
			d.cleanup()
		}
	}
}

// cleanup removes expired hashes from the cache.
//
// Algorithm:
//  1. Acquire write lock (exclusive access to cache)
//  2. Iterate through all cache entries
//  3. Delete entries where current time > expiry time
//  4. Log number of entries removed
//
// Thread Safety: Uses write lock (blocks all other cache operations)
// Time Complexity: O(n) where n is cache size
//
// This is called automatically every 30 seconds by cleanupLoop().
// Without cleanup, the cache would grow unbounded as new hashes are added.
func (d *Deduplicator) cleanup() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	removed := 0

	// Scan all cache entries and delete expired ones
	for hash, expiry := range d.cache {
		if now.After(expiry) {
			delete(d.cache, hash)
			removed++
		}
	}

	// Log cleanup results (only if something was removed)
	if removed > 0 {
		log.Printf("Dedup cleanup: removed %d expired hashes, %d remain in cache",
			removed, len(d.cache))
	}
}

// GetStats returns current deduplication statistics.
//
// Returns:
//   - map[string]interface{}: Statistics dictionary with keys:
//     - cache_size: Current number of hashes in cache
//     - total_spots: Total spots processed since startup
//     - duplicate_spots: Number of duplicates detected and dropped
//     - duplicate_rate: Percentage of spots that were duplicates (e.g., "15.3%")
//     - window_seconds: Deduplication window in seconds
//     - kept_newest: Count of newer duplicates that were kept (currently unused)
//
// Thread Safety: Uses read lock (allows concurrent stat reads)
//
// Example output:
//   {
//     "cache_size": 437,
//     "total_spots": 1250,
//     "duplicate_spots": 192,
//     "duplicate_rate": "15.4%",
//     "window_seconds": 120,
//     "kept_newest": 0
//   }
func (d *Deduplicator) GetStats() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Calculate duplicate rate percentage
	dupRate := 0.0
	if d.totalSpots > 0 {
		dupRate = float64(d.duplicateSpots) / float64(d.totalSpots) * 100
	}

	return map[string]interface{}{
		"cache_size":      len(d.cache),
		"total_spots":     d.totalSpots,
		"duplicate_spots": d.duplicateSpots,
		"duplicate_rate":  fmt.Sprintf("%.1f%%", dupRate),
		"window_seconds":  d.window.Seconds(),
		"kept_newest":     d.keptNewest,
	}
}

// Stop gracefully shuts down the deduplicator.
//
// Shutdown sequence:
//  1. Stops the cleanup ticker
//  2. Closes stopCh to signal goroutines to exit
//  3. Closes inputChan to stop accepting new spots
//  4. Logs final statistics
//
// After Stop() is called:
//   - No new spots will be accepted on input channel
//   - Processing loop will drain and exit
//   - Output channel will be closed
//   - Cleanup loop will exit
//
// This should be called during application shutdown to ensure clean termination.
//
// Example:
//   deduplicator.Stop()
//   // Wait briefly for goroutines to exit
func (d *Deduplicator) Stop() {
	log.Println("Deduplicator: Stopping...")

	// Stop the cleanup ticker
	d.ticker.Stop()

	// Signal all goroutines to stop
	close(d.stopCh)

	// Close input channel to stop accepting new spots
	// This will cause processLoop to exit after draining
	close(d.inputChan)

	// Log final statistics for analysis
	stats := d.GetStats()
	log.Printf("Deduplicator stopped. Final stats: %+v", stats)
}
