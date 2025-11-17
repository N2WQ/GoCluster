package dedup

import (
	"log"
	"sync"
	"time"

	"dxcluster/spot"
)

// Deduplicator removes duplicate spots within a time window
type Deduplicator struct {
	mu              sync.RWMutex
	window          time.Duration
	cache           map[uint32]time.Time // hash -> last seen time
	inputChan       chan *spot.Spot
	outputChan      chan *spot.Spot
	shutdown        chan struct{}
	processedCount  uint64
	duplicateCount  uint64
	cleanupInterval time.Duration
}

// NewDeduplicator creates a new deduplicator with the specified window
func NewDeduplicator(window time.Duration) *Deduplicator {
	return &Deduplicator{
		window:          window,
		cache:           make(map[uint32]time.Time),
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
			d.mu.Lock()
			d.processedCount++
			hash := s.Hash32()

			if d.isDuplicate(s) {
				d.duplicateCount++
				d.mu.Unlock()
				continue // Skip duplicate (logging handled by stats display)
			}

			// Add to cache
			d.cache[hash] = s.Time

			d.mu.Unlock()

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

// isDuplicate checks if a spot is a duplicate
// Must be called with lock held
func (d *Deduplicator) isDuplicate(s *spot.Spot) bool {
	hash := s.Hash32()

	lastSeen, exists := d.cache[hash]
	if !exists {
		return false
	}

	// Check if within dedup window
	age := s.Time.Sub(lastSeen)
	if age < 0 {
		age = -age // Handle out-of-order spots
	}

	return age < d.window
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
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now().UTC()
	removed := 0

	for hash, lastSeen := range d.cache {
		age := now.Sub(lastSeen)
		if age > d.window {
			delete(d.cache, hash)
			removed++
		}
	}

	if removed > 0 {
		log.Printf("Deduplicator: Cleaned %d expired entries from cache", removed)
	}
}

// GetStats returns current deduplication statistics
func (d *Deduplicator) GetStats() (processed uint64, duplicates uint64, cacheSize int) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.processedCount, d.duplicateCount, len(d.cache)
}
