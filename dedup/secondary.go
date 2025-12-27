// Package dedup also provides a late-stage secondary deduplicator used to
// thin broadcast volume without altering the main ring buffer history. It
// shares the same CPU-efficient hashing approach as the primary dedupe
// engine but keys on DE metadata (DXCC + CQ zone) plus DX call and frequency.
package dedup

import (
	"encoding/binary"
	"sync"
	"time"

	"dxcluster/spot"

	"github.com/zeebo/xxh3"
)

// secondaryShardCount is kept small and power-of-two for fast masking.
const secondaryShardCount = 64

// SecondaryDeduper drops repeat spots within a time window using a metadata
// key composed of:
//   - Frequency (integer kHz from existing 0.1 kHz rounding)
//   - DE ADIF (spotter DXCC)
//   - DE CQ zone
//   - Normalized DX call
//   - Source class (human vs skimmer)
//
// It is intended
// to run after call correction/harmonic/frequency adjustments and before
// broadcast so the ring buffer remains intact.
type SecondaryDeduper struct {
	window          time.Duration
	preferStronger  bool
	shards          []secondaryShard
	cleanupInterval time.Duration
	shutdown        chan struct{}
}

type secondaryShard struct {
	mu             sync.Mutex
	cache          map[uint32]secondaryEntry
	processedCount uint64
	duplicateCount uint64
}

type secondaryEntry struct {
	when      time.Time
	snr       int
	hasReport bool
}

// NewSecondaryDeduper builds a deduper. A non-positive window disables
// suppression (ShouldForward always returns true).
func NewSecondaryDeduper(window time.Duration, preferStronger bool) *SecondaryDeduper {
	shards := make([]secondaryShard, secondaryShardCount)
	for i := range shards {
		shards[i].cache = make(map[uint32]secondaryEntry)
	}
	return &SecondaryDeduper{
		window:          window,
		preferStronger:  preferStronger,
		shards:          shards,
		cleanupInterval: 60 * time.Second,
		shutdown:        make(chan struct{}),
	}
}

// Start launches a periodic cleanup loop to bound memory.
func (d *SecondaryDeduper) Start() {
	go d.cleanupLoop()
}

// Stop terminates the cleanup loop.
func (d *SecondaryDeduper) Stop() {
	close(d.shutdown)
}

// ShouldForward returns true when the spot is not a duplicate within the
// configured window. When preferStronger is enabled, a stronger SNR replaces
// the cached entry and the new spot is forwarded.
// When required metadata is missing (DE DXCC or DE CQ zone <=0) it returns true to avoid
// false positives.
func (d *SecondaryDeduper) ShouldForward(s *spot.Spot) bool {
	if d == nil || d.window <= 0 || s == nil {
		return true
	}
	deDXCC := s.DEMetadata.ADIF
	deZone := s.DEMetadata.CQZone
	if deDXCC <= 0 || deZone <= 0 {
		return true
	}

	hash := secondaryHash(s, deDXCC, deZone)
	shard := d.shardFor(hash)

	shard.mu.Lock()
	shard.processedCount++

	dup, lastSeen := isSecondaryDuplicateLocked(shard.cache, hash, s.Time, d.window)
	if dup {
		upgradeToReported := s.HasReport && !lastSeen.hasReport
		stronger := d.preferStronger && s.Report > lastSeen.snr
		if upgradeToReported || stronger {
			// Track and forward the stronger or newly reported representative.
			shard.cache[hash] = secondaryEntry{when: s.Time, snr: s.Report, hasReport: s.HasReport}
			shard.mu.Unlock()
			return true
		}
		shard.duplicateCount++
		shard.mu.Unlock()
		return false
	}

	shard.cache[hash] = secondaryEntry{when: s.Time, snr: s.Report, hasReport: s.HasReport}
	shard.mu.Unlock()
	return true
}

// GetStats returns processed, duplicates, and cache size totals.
func (d *SecondaryDeduper) GetStats() (processed uint64, duplicates uint64, cacheSize int) {
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

func (d *SecondaryDeduper) shardFor(hash uint32) *secondaryShard {
	idx := hash & (secondaryShardCount - 1)
	return &d.shards[idx]
}

func (d *SecondaryDeduper) cleanupLoop() {
	ticker := time.NewTicker(d.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.shutdown:
			return
		case <-ticker.C:
			d.cleanup()
		}
	}
}

func (d *SecondaryDeduper) cleanup() {
	if d.window <= 0 {
		return
	}
	now := time.Now().UTC()
	for i := range d.shards {
		shard := &d.shards[i]
		shard.mu.Lock()
		for hash, entry := range shard.cache {
			age := now.Sub(entry.when)
			if age > d.window {
				delete(shard.cache, hash)
			}
		}
		shard.mu.Unlock()
	}
}

const (
	secondaryClassHuman   byte = 1
	secondaryClassSkimmer byte = 2
)

// secondaryHash mirrors the primary hash structure (minute + kHz + fixed DX
// call) but swaps DE callsign for DE DXCC + DE CQ zone, appends a source class
// discriminator, and omits the time. The time window is enforced by the cache
// itself so the hash is stable across minute boundaries, collapsing within the
// configured window. The source class split ensures a skimmer spot cannot
// suppress a human spot (and vice versa) during broadcast-only dedupe.
func secondaryHash(s *spot.Spot, deDXCC int, deZone int) uint32 {
	var buf [32]byte
	freq := uint32(s.Frequency)
	binary.LittleEndian.PutUint32(buf[0:4], freq)
	binary.LittleEndian.PutUint16(buf[4:6], uint16(deDXCC))
	binary.LittleEndian.PutUint16(buf[6:8], uint16(deZone))
	writeFixedCallNormalized(buf[8:20], s.DXCall)
	buf[20] = secondarySourceClass(s)
	return uint32(xxh3.Hash(buf[:]))
}

// secondarySourceClass normalizes a spot to its secondary-dedupe class.
// Skimmer sources are always treated as a distinct class from all human inputs.
func secondarySourceClass(s *spot.Spot) byte {
	if s != nil && spot.IsSkimmerSource(s.SourceType) {
		return secondaryClassSkimmer
	}
	return secondaryClassHuman
}

// writeFixedCallNormalized uppercases, trims, and pads/truncates into 12 bytes.
func writeFixedCallNormalized(dst []byte, call string) {
	const maxLen = 12
	call = spot.NormalizeCallsign(call)
	n := 0
	for i := 0; i < len(call) && n < maxLen; i++ {
		ch := call[i]
		if ch >= 'a' && ch <= 'z' {
			ch = ch - ('a' - 'A')
		}
		dst[n] = ch
		n++
	}
	for n < maxLen {
		dst[n] = 0
		n++
	}
}

// isSecondaryDuplicateLocked checks if a spot is a duplicate within a shard.
func isSecondaryDuplicateLocked(cache map[uint32]secondaryEntry, hash uint32, spotTime time.Time, window time.Duration) (bool, secondaryEntry) {
	lastSeen, exists := cache[hash]
	if !exists {
		return false, secondaryEntry{}
	}
	age := spotTime.Sub(lastSeen.when)
	if age < 0 {
		age = -age
	}
	return age < window, lastSeen
}
