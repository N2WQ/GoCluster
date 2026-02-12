// Package dedup also provides a late-stage secondary deduplicator used to
// thin broadcast volume without altering the main ring buffer history. It
// shares the same CPU-efficient hashing approach as the primary dedupe
// engine but keys on DE metadata (DXCC + grid2 + band) plus DX call.
package dedup

import (
	"encoding/binary"
	"strings"
	"sync"
	"time"

	"dxcluster/spot"

	"github.com/zeebo/xxh3"
)

// secondaryShardCount is kept small and power-of-two for fast masking.
const secondaryShardCount = 64

const (
	secondaryCompactMinPeak     = 1024
	secondaryCompactShrinkRatio = 0.5
)

// SecondaryKeyMode selects which DE metadata dimension is hashed for secondary dedupe.
type SecondaryKeyMode uint8

const (
	SecondaryKeyGrid2 SecondaryKeyMode = iota
	SecondaryKeyCQZone
)

// SecondaryDeduper drops repeat spots within a time window using a metadata
// key composed of:
//   - Band (normalized string packed into 4 bytes)
//   - DE ADIF (spotter DXCC)
//   - DE grid2 prefix or CQ zone (policy-dependent)
//   - Normalized DX call
//   - Source class (human vs skimmer)
//
// It is intended
// to run after call correction/harmonic/frequency adjustments and before
// broadcast so the ring buffer remains intact.
type SecondaryDeduper struct {
	window          time.Duration
	preferStronger  bool
	keyMode         SecondaryKeyMode
	shards          []secondaryShard
	cleanupInterval time.Duration
	shutdown        chan struct{}
}

type secondaryShard struct {
	mu             sync.Mutex
	cache          map[uint32]secondaryEntry
	processedCount uint64
	duplicateCount uint64
	peak           int
}

type secondaryEntry struct {
	when      time.Time
	snr       int
	hasReport bool
}

// NewSecondaryDeduper constructs a secondary deduper for broadcast-only suppression.
// Key aspects: Initializes shard maps and window settings.
// Upstream: main startup when secondary dedupe is enabled.
// Downstream: shard allocation and state init.
// NewSecondaryDeduper builds a deduper. A non-positive window disables
// suppression (ShouldForward always returns true).
func NewSecondaryDeduper(window time.Duration, preferStronger bool) *SecondaryDeduper {
	return NewSecondaryDeduperWithKey(window, preferStronger, SecondaryKeyGrid2)
}

// NewSecondaryDeduperWithKey constructs a secondary deduper with a specific hash mode.
// Key aspects: Allows CQ zone hashing without changing existing callers.
// Upstream: main startup when secondary dedupe is enabled.
// Downstream: shard allocation and state init.
// NewSecondaryDeduperWithKey builds a deduper with an explicit key mode.
func NewSecondaryDeduperWithKey(window time.Duration, preferStronger bool, keyMode SecondaryKeyMode) *SecondaryDeduper {
	shards := make([]secondaryShard, secondaryShardCount)
	for i := range shards {
		shards[i].cache = make(map[uint32]secondaryEntry)
	}
	return &SecondaryDeduper{
		window:          window,
		preferStronger:  preferStronger,
		keyMode:         keyMode,
		shards:          shards,
		cleanupInterval: 60 * time.Second,
		shutdown:        make(chan struct{}),
	}
}

// Start starts the cleanup loop for secondary dedupe.
// Key aspects: Spawns a goroutine that prunes expired entries.
// Upstream: main startup.
// Downstream: cleanupLoop goroutine.
// Start launches a periodic cleanup loop to bound memory.
func (d *SecondaryDeduper) Start() {
	// Purpose: Periodically purge expired entries.
	// Key aspects: Runs until Stop closes shutdown.
	// Upstream: SecondaryDeduper.Start.
	// Downstream: d.cleanupLoop.
	go d.cleanupLoop()
}

// Stop stops the cleanup loop.
// Key aspects: Closing shutdown unblocks cleanupLoop.
// Upstream: main shutdown.
// Downstream: channel close only.
// Stop terminates the cleanup loop.
func (d *SecondaryDeduper) Stop() {
	close(d.shutdown)
}

// ShouldForward reports whether a spot should pass secondary dedupe.
// Key aspects: Uses DE DXCC/grid2 + DX/band + source class and optional stronger SNR.
// Upstream: processOutputSpots broadcast stage.
// Downstream: secondaryHash and isSecondaryDuplicateLocked.
// ShouldForward returns true when the spot is not a duplicate within the
// configured window. When preferStronger is enabled, a stronger SNR replaces
// the cached entry and the new spot is forwarded.
// When required metadata is missing (DE DXCC <=0) it returns true to avoid false positives.
func (d *SecondaryDeduper) ShouldForward(s *spot.Spot) bool {
	if d == nil || d.window <= 0 || s == nil {
		return true
	}
	deDXCC := s.DEMetadata.ADIF
	if deDXCC <= 0 {
		return true
	}

	hash := secondaryHash(s, deDXCC, d.keyMode)
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
			d.updateShardPeakLocked(shard)
			shard.mu.Unlock()
			return true
		}
		shard.duplicateCount++
		shard.mu.Unlock()
		return false
	}

	shard.cache[hash] = secondaryEntry{when: s.Time, snr: s.Report, hasReport: s.HasReport}
	d.updateShardPeakLocked(shard)
	shard.mu.Unlock()
	return true
}

// GetStats returns secondary dedupe stats across shards.
// Key aspects: Aggregates processed/duplicate counts and cache size.
// Upstream: stats display.
// Downstream: shard counters under lock.
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

// Purpose: Pick the shard for a given hash.
// Key aspects: Uses bitmask with power-of-two shard count.
// Upstream: ShouldForward.
// Downstream: shard selection only.
func (d *SecondaryDeduper) shardFor(hash uint32) *secondaryShard {
	idx := hash & (secondaryShardCount - 1)
	return &d.shards[idx]
}

func (d *SecondaryDeduper) updateShardPeakLocked(shard *secondaryShard) {
	if shard == nil {
		return
	}
	if size := len(shard.cache); size > shard.peak {
		shard.peak = size
	}
}

func (d *SecondaryDeduper) maybeCompactShardLocked(shard *secondaryShard) {
	if shard == nil {
		return
	}
	if shard.peak < secondaryCompactMinPeak {
		return
	}
	threshold := int(float64(shard.peak) * secondaryCompactShrinkRatio)
	if len(shard.cache) >= threshold {
		return
	}
	next := make(map[uint32]secondaryEntry, len(shard.cache))
	for k, v := range shard.cache {
		next[k] = v
	}
	shard.cache = next
	shard.peak = len(next)
}

// Purpose: Periodically purge expired secondary dedupe entries.
// Key aspects: Ticker-driven loop until shutdown.
// Upstream: goroutine started in Start.
// Downstream: cleanup.
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

// Purpose: Remove expired entries from the secondary cache.
// Key aspects: Iterates shards and deletes old entries.
// Upstream: cleanupLoop.
// Downstream: shard cache mutation.
func (d *SecondaryDeduper) cleanup() {
	if d.window <= 0 {
		return
	}
	now := time.Now().UTC()
	for i := range d.shards {
		shard := &d.shards[i]
		shard.mu.Lock()
		removed := false
		for hash, entry := range shard.cache {
			age := now.Sub(entry.when)
			if age > d.window {
				delete(shard.cache, hash)
				removed = true
			}
		}
		if removed {
			d.maybeCompactShardLocked(shard)
		}
		shard.mu.Unlock()
	}
}

const (
	secondaryClassHuman   byte = 1
	secondaryClassSkimmer byte = 2
)

// Purpose: Hash a spot into the secondary dedupe keyspace.
// Key aspects: Includes band, DE DXCC/grid2 or CQ zone, DX call, and source class.
// Upstream: ShouldForward.
// Downstream: writeFixedCallNormalized and secondarySourceClass.
// secondaryHash mirrors the primary hash structure (minute + kHz + fixed DX
// call) but keys on band + DE DXCC + DE grid2 or CQ zone, appends a source class
// discriminator, and omits the time. The time window is enforced by the cache
// itself so the hash is stable across minute boundaries, collapsing within the
// configured window. The source class split ensures a skimmer spot cannot
// suppress a human spot (and vice versa) during broadcast-only dedupe. A
// missing or malformed grid/zone falls back to a zeroed bucket so behavior
// remains deterministic.
func secondaryHash(s *spot.Spot, deDXCC int, keyMode SecondaryKeyMode) uint32 {
	var buf [32]byte
	if s != nil {
		s.EnsureNormalized()
	}
	bandKey := bandKeyNumeric(s)
	binary.LittleEndian.PutUint32(buf[0:4], bandKey)
	binary.LittleEndian.PutUint16(buf[4:6], uint16(deDXCC))
	switch keyMode {
	case SecondaryKeyCQZone:
		binary.LittleEndian.PutUint16(buf[6:8], fixedCQZone(s))
	default:
		grid2 := fixedGrid2(s)
		buf[6] = grid2[0]
		buf[7] = grid2[1]
	}
	writeFixedCallNormalized(buf[8:20], s.DXCallNorm)
	buf[20] = secondarySourceClass(s)
	return uint32(xxh3.Hash(buf[:]))
}

// bandKeyNumeric packs the band string into 4 bytes for hashing. Falls back to
// FreqToBand when BandNorm is empty.
func bandKeyNumeric(s *spot.Spot) uint32 {
	if s == nil {
		return 0
	}
	band := s.BandNorm
	if band == "" {
		band = spot.FreqToBand(s.Frequency)
	}
	band = strings.ToUpper(strings.TrimSpace(band))
	var buf [4]byte
	for i := 0; i < len(band) && i < len(buf); i++ {
		buf[i] = band[i]
	}
	return binary.LittleEndian.Uint32(buf[:])
}

// fixedGrid2 extracts a two-byte grid prefix, uppercased and zero-padded when
// missing. It avoids allocating by operating on normalized fields.
func fixedGrid2(s *spot.Spot) [2]byte {
	var out [2]byte
	if s == nil {
		return out
	}
	grid := s.DEGrid2
	if grid == "" && len(s.DEGridNorm) >= 2 {
		grid = s.DEGridNorm[:2]
	}
	if len(grid) >= 2 {
		out[0] = toUpperASCII(grid[0])
		out[1] = toUpperASCII(grid[1])
	} else if len(grid) == 1 {
		out[0] = toUpperASCII(grid[0])
		out[1] = 0
	}
	return out
}

// fixedCQZone returns the CQ zone or zero when missing/invalid.
func fixedCQZone(s *spot.Spot) uint16 {
	if s == nil {
		return 0
	}
	zone := s.DEMetadata.CQZone
	if zone <= 0 || zone > 40 {
		return 0
	}
	return uint16(zone)
}

func toUpperASCII(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return b - 32
	}
	return b
}

// Purpose: Normalize a spot into human vs skimmer class for dedupe.
// Key aspects: Skimmer sources are distinct from all human sources.
// Upstream: secondaryHash.
// Downstream: spot.IsSkimmerSource.
// secondarySourceClass normalizes a spot to its secondary-dedupe class.
// Skimmer sources are always treated as a distinct class from all human inputs.
func secondarySourceClass(s *spot.Spot) byte {
	if s != nil && spot.IsSkimmerSource(s.SourceType) {
		return secondaryClassSkimmer
	}
	return secondaryClassHuman
}

// Purpose: Write a normalized callsign into a fixed-width buffer.
// Key aspects: Assumes input is already normalized; zero-pads to 12 bytes.
// Upstream: secondaryHash.
// Downstream: None.
// writeFixedCallNormalized pads/truncates into 12 bytes.
func writeFixedCallNormalized(dst []byte, call string) {
	const maxLen = 12
	n := 0
	for i := 0; i < len(call) && n < maxLen; i++ {
		dst[n] = call[i]
		n++
	}
	for n < maxLen {
		dst[n] = 0
		n++
	}
}

// Purpose: Check if a secondary key is a duplicate within the time window.
// Key aspects: Caller must hold shard lock; handles out-of-order timestamps.
// Upstream: ShouldForward.
// Downstream: time.Sub comparisons.
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
