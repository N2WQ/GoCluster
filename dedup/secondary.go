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
//   - Time truncated to the minute
//   - Frequency (integer kHz from existing 0.1 kHz rounding)
//   - DE ADIF (spotter DXCC)
//   - DE CQ zone
//   - Normalized DX call
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
	when time.Time
	snr  int
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
		if d.preferStronger && s.Report > lastSeen.snr {
			// Track the stronger representative but do not emit another broadcast.
			shard.cache[hash] = secondaryEntry{when: s.Time, snr: s.Report}
		}
		shard.duplicateCount++
		shard.mu.Unlock()
		return false
	}

	shard.cache[hash] = secondaryEntry{when: s.Time, snr: s.Report}
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

// secondaryHash mirrors the primary hash structure (minute + kHz + fixed DX
// call) but swaps DE callsign for DE DXCC + DE CQ zone.
func secondaryHash(s *spot.Spot, deDXCC int, deZone int) uint32 {
	var buf [32]byte
	t := s.Time.Truncate(time.Minute).Unix()
	binary.LittleEndian.PutUint64(buf[0:8], uint64(t))
	freq := uint32(s.Frequency)
	binary.LittleEndian.PutUint32(buf[8:12], freq)
	binary.LittleEndian.PutUint16(buf[12:14], uint16(deDXCC))
	binary.LittleEndian.PutUint16(buf[14:16], uint16(deZone))
	writeFixedCallNormalized(buf[16:28], s.DXCall)
	return uint32(xxh3.Hash(buf[:]))
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
