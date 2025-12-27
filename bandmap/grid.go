package bandmap

import (
	"math"
	"strings"
	"sync"
	"time"
)

// ShardCount determines how many shards guard the frequency bins per mode.
const ShardCount = 64

// MaxBinCapacity bounds the slice length per 100 Hz bin to avoid unbounded memory growth.
const MaxBinCapacity = 50

// SpotEntry is a lightweight copy of spot data needed for correction logic.
// All frequencies are stored in integer Hertz; callers must convert from kHz upstream.
type SpotEntry struct {
	Call    string // DX callsign
	Spotter string // DE callsign
	Mode    string // Mode (e.g., CW, RTTY)
	FreqHz  uint32 // Frequency in Hz (not kHz)
	Time    int64  // Unix timestamp (seconds)
	SNR     int    // Signal-to-noise ratio (dB)
}

// BandMap is a mode-partitioned spatial index over 100 Hz bins.
// It performs no validation; callers must enforce admission policy.
type BandMap struct {
	mu    sync.RWMutex
	grids map[string]*ShardedGrid
}

// ShardedGrid holds shards for a single mode.
type ShardedGrid struct {
	shards [ShardCount]*Shard
}

// Shard guards a map of binID->entries for a subset of the band.
type Shard struct {
	mu   sync.RWMutex
	bins map[uint32][]SpotEntry
}

// Purpose: Construct an empty bandmap index.
// Key aspects: Initializes the mode grid map; shards are created on demand.
// Upstream: Spot correction/index initialization in the main pipeline.
// Downstream: BandMap.Add, BandMap.Get, BandMap.Prune.
func New() *BandMap {
	return &BandMap{grids: make(map[string]*ShardedGrid)}
}

// Purpose: Insert a spot entry into the per-mode sharded grid.
// Key aspects: Uses 100 Hz bins and caps bin length to avoid unbounded growth.
// Upstream: Mode correction/index updates in the main pipeline.
// Downstream: getOrCreateGrid, shard/bin storage.
func (bm *BandMap) Add(mode string, entry SpotEntry) {
	modeKey := strings.ToUpper(mode)
	grid := bm.getOrCreateGrid(modeKey)

	binID := entry.FreqHz / 100
	shardIdx := binID % ShardCount
	shard := grid.shards[shardIdx]

	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.bins == nil {
		shard.bins = make(map[uint32][]SpotEntry)
	}
	list := shard.bins[binID]
	list = append(list, entry)
	if len(list) > MaxBinCapacity {
		list = list[1:]
	}
	shard.bins[binID] = list
}

// Purpose: Fetch entries in a frequency window for a given mode.
// Key aspects: Scans bins in range and filters by recency when requested.
// Upstream: Call correction and frequency evidence lookups.
// Downstream: Shard bin lookup; time.Now for cutoff.
func (bm *BandMap) Get(mode string, centerFreqHz uint32, windowHz uint32, maxAgeSeconds int64) []SpotEntry {
	modeKey := strings.ToUpper(mode)

	bm.mu.RLock()
	grid, ok := bm.grids[modeKey]
	bm.mu.RUnlock()
	if !ok {
		return nil
	}

	var minFreq, maxFreq uint32
	if windowHz >= centerFreqHz {
		minFreq = 0
	} else {
		minFreq = centerFreqHz - windowHz
	}
	if math.MaxUint32-windowHz < centerFreqHz {
		maxFreq = math.MaxUint32
	} else {
		maxFreq = centerFreqHz + windowHz
	}
	minBin := minFreq / 100
	maxBin := maxFreq / 100

	cutoff := int64(0)
	if maxAgeSeconds > 0 {
		cutoff = time.Now().Unix() - maxAgeSeconds
	}

	results := make([]SpotEntry, 0, 64)
	for b := minBin; b <= maxBin; b++ {
		shardIdx := b % ShardCount
		shard := grid.shards[shardIdx]
		shard.mu.RLock()
		if shard.bins != nil {
			if entries, exists := shard.bins[b]; exists {
				for _, e := range entries {
					if e.Time >= cutoff {
						results = append(results, e)
					}
				}
			}
		}
		shard.mu.RUnlock()
		if b == math.MaxUint32 {
			break
		}
	}
	return results
}

// Purpose: Drop stale entries across all modes.
// Key aspects: Iterates shards and compacts slices in place.
// Upstream: Periodic cleanup goroutine in correction pipeline.
// Downstream: Shard bin mutation; time.Now for cutoff.
func (bm *BandMap) Prune(maxAgeSeconds int64) {
	if maxAgeSeconds <= 0 {
		return
	}
	cutoff := time.Now().Unix() - maxAgeSeconds

	bm.mu.RLock()
	grids := make([]*ShardedGrid, 0, len(bm.grids))
	for _, g := range bm.grids {
		grids = append(grids, g)
	}
	bm.mu.RUnlock()

	for _, grid := range grids {
		for i := 0; i < ShardCount; i++ {
			shard := grid.shards[i]
			shard.mu.Lock()
			if shard.bins != nil {
				for binID, entries := range shard.bins {
					n := 0
					for _, e := range entries {
						if e.Time >= cutoff {
							entries[n] = e
							n++
						}
					}
					entries = entries[:n]
					if len(entries) == 0 {
						delete(shard.bins, binID)
					} else {
						shard.bins[binID] = entries
					}
				}
			}
			shard.mu.Unlock()
		}
	}
}

// Purpose: Fetch or allocate the sharded grid for a mode.
// Key aspects: Double-checked locking to reduce contention.
// Upstream: BandMap.Add.
// Downstream: Shard allocation and map mutation.
func (bm *BandMap) getOrCreateGrid(mode string) *ShardedGrid {
	bm.mu.RLock()
	grid, ok := bm.grids[mode]
	bm.mu.RUnlock()
	if ok {
		return grid
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()
	if grid, ok = bm.grids[mode]; ok {
		return grid
	}
	grid = &ShardedGrid{}
	for i := 0; i < ShardCount; i++ {
		grid.shards[i] = &Shard{}
	}
	bm.grids[mode] = grid
	return grid
}
