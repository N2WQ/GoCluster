// File role: Owns retained path evidence storage for the predictor.
// Crawler notes: Start here for bucket keys, decay, shard bounds, receiver-cap
// slot retention, and the raw-vs-capped evidence that later becomes PATH
// diagnostics. Prediction policy lives in predictor.go.
// Related docs: pathreliability/README.md, data/config/path_reliability.yaml.
// Related tests: pathreliability/*store*_test.go, pathreliability/*receiver*_test.go.
package pathreliability

import (
	"math"
	"sync"
	"time"
)

const (
	defaultShards = 64
	ln2           = 0.6931471805599453

	inlineReceiverSlots = 6
)

// receiverSlot is bucket-owned retained state. Slots are deliberately fixed and
// small so capped receiver trust is bounded by the bucket maps, not by total
// historical receiver cardinality. Weight and count are both decayed effective
// values; neither is a lifetime receiver admission counter.
type receiverSlot struct {
	hash       uint64
	weight     float64
	lastUpdate int64
	count      float64
}

// bucket holds decaying histogram evidence for a directional path.
type bucket struct {
	weight float64
	count  uint32
	// lastUpdate stores Unix seconds.
	lastUpdate int64

	cappedWeight  float64
	cappedCount   float64
	rawSNRBins    snrHistogram
	cappedSNRBins snrHistogram
	slots         [inlineReceiverSlots]receiverSlot
	// extraSlots is allocated only when a bucket needs coarse receiver slots.
	// Keeping it behind the existing pointer avoids growing every raw bucket.
	extraSlots *bucketExtraState
}

type bucketExtraState struct {
	slots *[maxCoarseReceiverSlots - inlineReceiverSlots]receiverSlot
}

type shard struct {
	mu      sync.RWMutex
	buckets map[uint64]*bucket
	peak    int
}

// Store aggregates decaying FT8-equiv path evidence.
type Store struct {
	shards    []shard
	cfg       Config
	bandIndex BandIndex

	statsMu        sync.RWMutex
	statsRefreshMu sync.Mutex
	stats          pathStoreStatsSnapshot
}

type pathStoreStatsSnapshot struct {
	asOfUnix int64
	fine     int
	coarse   int
	byBand   []bandCounts
}

const (
	maxBucketObservationCount uint32 = 1<<32 - 1
	effectiveCountEpsilon            = 1e-9
)

// NewStore constructs a path store with normalized config.
func NewStore(cfg Config, bands []string) *Store {
	cfg.normalize()
	if len(bands) == 0 {
		bands = []string{"160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m", "6m", "4m", "2m", "1m"}
	}
	idx := NewBandIndex(bands)
	s := &Store{
		shards:    make([]shard, defaultShards),
		cfg:       cfg,
		bandIndex: idx,
	}
	for i := range s.shards {
		s.shards[i].buckets = make(map[uint64]*bucket)
	}
	s.stats = pathStoreStatsSnapshot{
		byBand: make([]bandCounts, len(idx.Bands())),
	}
	return s
}

// UpdateWithReceiverHash applies a new reading and attributes capped
// contribution trust to the normalized receiving station identity hash.
// A zero hash is intentionally unattributed and does not add capped trust.
func (s *Store) UpdateWithReceiverHash(receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, ft8DB float64, weight float64, now time.Time, receiverHash uint64) {
	if s == nil || !s.cfg.Enabled {
		return
	}
	if weight <= 0 || math.IsNaN(weight) {
		return
	}
	idx, ok := s.bandIndex.Lookup(band)
	if !ok {
		return
	}
	halfLife := s.bandIndex.HalfLifeSeconds(band, s.cfg)
	snrBin := snrHistogramBinIndex(ft8DB)
	if snrBin < 0 {
		return
	}
	if receiverCell == InvalidCell || senderCell == InvalidCell {
		// Still allow coarse update when fine cells are missing.
	} else {
		s.updateBucket(packKey(receiverCell, senderCell, idx), weight, now, halfLife, receiverHash, s.cfg.ReceiverFineSlots, snrBin)
	}
	if receiverCoarse != InvalidCell && senderCoarse != InvalidCell {
		s.updateBucket(packCoarseKey(receiverCoarse, senderCoarse, idx), weight, now, halfLife, receiverHash, s.cfg.ReceiverCoarseSlots, snrBin)
	}
}

func (s *Store) updateBucket(key uint64, weight float64, now time.Time, halfLifeSec int, receiverHash uint64, receiverSlots int, snrBin int) {
	if key == 0 {
		return
	}
	sh := &s.shards[key%uint64(len(s.shards))]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	nowSec := now.Unix()
	b, ok := sh.buckets[key]
	if !ok {
		b = &bucket{lastUpdate: nowSec}
		sh.buckets[key] = b
		if len(sh.buckets) > sh.peak {
			sh.peak = len(sh.buckets)
		}
	}
	elapsed := nowSec - b.lastUpdate
	decay := decayFactor(elapsed, halfLifeSec)
	if snrBin >= 0 && decay != 1 {
		b.rawSNRBins.decay(decay)
	}
	oldWeight := b.weight * decay
	newWeight := oldWeight + weight
	if newWeight <= 0 || math.IsNaN(newWeight) {
		b.weight = 0
		b.updateCapped(weight, nowSec, decay, receiverHash, receiverSlots, s.cfg, snrBin)
		b.lastUpdate = nowSec
		return
	}
	b.weight = newWeight
	if b.count < maxBucketObservationCount {
		b.count++
	}
	if snrBin >= 0 {
		b.rawSNRBins.add(snrBin, weight)
	}
	b.updateCapped(weight, nowSec, decay, receiverHash, receiverSlots, s.cfg, snrBin)
	b.lastUpdate = nowSec
}

func (b *bucket) updateCapped(weight float64, nowSec int64, decay float64, receiverHash uint64, receiverSlots int, cfg Config, snrBin int) {
	if b == nil {
		return
	}
	if cfg.ReceiverContributionMode == ReceiverContributionOff {
		return
	}
	b.cappedWeight *= decay
	b.cappedCount *= decay
	if snrBin >= 0 && decay != 1 {
		b.cappedSNRBins.decay(decay)
	}
	if b.cappedWeight <= 0 || math.IsNaN(b.cappedWeight) {
		b.cappedWeight = 0
	}
	if b.cappedCount <= 0 || math.IsNaN(b.cappedCount) {
		b.cappedCount = 0
	}
	b.decayReceiverSlots(decay, receiverSlots)
	if receiverHash == 0 || receiverSlots <= 0 || weight <= 0 || cfg.ReceiverMaxEffectiveWeight <= 0 || cfg.ReceiverMaxEffectiveCount == 0 {
		return
	}
	slot := b.selectReceiverSlot(receiverHash, receiverSlots)
	if slot == nil {
		return
	}
	if slot.hash != receiverHash {
		*slot = receiverSlot{hash: receiverHash}
	}
	remainingWeight := cfg.ReceiverMaxEffectiveWeight - slot.weight
	if remainingWeight <= effectiveCountEpsilon {
		return
	}
	remainingCount := float64(cfg.ReceiverMaxEffectiveCount) - slot.count
	if remainingCount <= effectiveCountEpsilon {
		return
	}
	acceptedFraction := 1.0
	if remainingCount < acceptedFraction {
		acceptedFraction = remainingCount
	}
	if weightLimitFraction := remainingWeight / weight; weightLimitFraction < acceptedFraction {
		acceptedFraction = weightLimitFraction
	}
	if acceptedFraction <= effectiveCountEpsilon || math.IsNaN(acceptedFraction) {
		return
	}
	acceptedWeight := weight * acceptedFraction
	slot.weight += acceptedWeight
	slot.count += acceptedFraction
	slot.lastUpdate = nowSec
	b.cappedWeight += acceptedWeight
	b.cappedCount += acceptedFraction
	if snrBin >= 0 {
		b.cappedSNRBins.add(snrBin, acceptedWeight)
	}
}

func (b *bucket) decayReceiverSlots(decay float64, receiverSlots int) {
	if b == nil || receiverSlots <= 0 {
		return
	}
	for i := 0; i < receiverSlots; i++ {
		slot := b.receiverSlotAt(i, false)
		if slot == nil || slot.hash == 0 {
			continue
		}
		slot.weight *= decay
		slot.count *= decay
		if slot.weight < 0 {
			slot.weight = 0
		}
		if slot.count < 0 || math.IsNaN(slot.count) {
			slot.count = 0
		}
		if slot.weight <= effectiveCountEpsilon && slot.count <= effectiveCountEpsilon {
			*slot = receiverSlot{}
		}
	}
}

func (b *bucket) selectReceiverSlot(hash uint64, receiverSlots int) *receiverSlot {
	if b == nil || hash == 0 || receiverSlots <= 0 {
		return nil
	}
	if receiverSlots > maxCoarseReceiverSlots {
		receiverSlots = maxCoarseReceiverSlots
	}
	empty := -1
	replace := -1
	var replaceWeight float64
	var replaceUpdate int64
	for i := 0; i < receiverSlots; i++ {
		slot := b.receiverSlotAt(i, false)
		if slot == nil || slot.hash == 0 {
			if empty < 0 {
				empty = i
			}
			continue
		}
		if slot.hash == hash {
			return slot
		}
		if replace < 0 || slot.weight < replaceWeight || (slot.weight == replaceWeight && slot.lastUpdate < replaceUpdate) {
			replace = i
			replaceWeight = slot.weight
			replaceUpdate = slot.lastUpdate
		}
	}
	if empty >= 0 {
		return b.receiverSlotAt(empty, true)
	}
	return b.receiverSlotAt(replace, true)
}

func (b *bucket) receiverSlotAt(index int, allocate bool) *receiverSlot {
	if b == nil || index < 0 {
		return nil
	}
	if index < inlineReceiverSlots {
		return &b.slots[index]
	}
	extraIndex := index - inlineReceiverSlots
	if extraIndex < 0 || extraIndex >= maxCoarseReceiverSlots-inlineReceiverSlots {
		return nil
	}
	if b.extraSlots == nil {
		if !allocate {
			return nil
		}
		b.extraSlots = &bucketExtraState{}
	}
	if b.extraSlots.slots == nil {
		if !allocate {
			return nil
		}
		b.extraSlots.slots = &[maxCoarseReceiverSlots - inlineReceiverSlots]receiverSlot{}
	}
	return &b.extraSlots.slots[extraIndex]
}

// Sample represents selected decayed path evidence with weight and counts.
type Sample struct {
	Weight                 float64
	AgeSec                 int64
	Count                  uint32
	ObservationCount       uint32
	RawCount               uint32
	RawWeight              float64
	CappedCount            uint32
	CappedWeight           float64
	CappedReceiverCount    uint32
	CappedReceiverCapacity uint32
	CapLimited             bool
}

type sampleWithBins struct {
	Sample
	P50DB   float64
	HasP50  bool
	snrBins snrHistogram
}

type bandCounts struct {
	fine   int
	coarse int
}

type weightHistogram struct {
	total int
	bins  []int
}

// Lookup returns the decayed samples for the given keys.
func (s *Store) Lookup(receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, now time.Time) (fine Sample, coarse Sample) {
	if s == nil || !s.cfg.Enabled {
		return
	}
	idx, ok := s.bandIndex.Lookup(band)
	if !ok {
		return
	}
	halfLife := s.bandIndex.HalfLifeSeconds(band, s.cfg)
	if receiverCell != InvalidCell && senderCell != InvalidCell {
		fine = s.sample(packKey(receiverCell, senderCell, idx), halfLife, now, s.cfg.ReceiverFineSlots)
	}
	if receiverCoarse != InvalidCell && senderCoarse != InvalidCell {
		coarse = s.sample(packCoarseKey(receiverCoarse, senderCoarse, idx), halfLife, now, s.cfg.ReceiverCoarseSlots)
	}
	return
}

func (s *Store) lookupWithDistribution(receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, now time.Time) (fine sampleWithBins, coarse sampleWithBins) {
	if s == nil || !s.cfg.Enabled {
		return
	}
	idx, ok := s.bandIndex.Lookup(band)
	if !ok {
		return
	}
	halfLife := s.bandIndex.HalfLifeSeconds(band, s.cfg)
	if receiverCell != InvalidCell && senderCell != InvalidCell {
		fine = s.sampleWithDistribution(packKey(receiverCell, senderCell, idx), halfLife, now, s.cfg.ReceiverFineSlots)
	}
	if receiverCoarse != InvalidCell && senderCoarse != InvalidCell {
		coarse = s.sampleWithDistribution(packCoarseKey(receiverCoarse, senderCoarse, idx), halfLife, now, s.cfg.ReceiverCoarseSlots)
	}
	return
}

func (s *Store) sample(key uint64, halfLife int, now time.Time, receiverSlots int) Sample {
	if key == 0 {
		return Sample{}
	}
	nowSec := now.Unix()
	sh := &s.shards[key%uint64(len(s.shards))]
	sh.mu.RLock()
	b := sh.buckets[key]
	if b == nil {
		sh.mu.RUnlock()
		return Sample{}
	}
	age := nowSec - b.lastUpdate
	if age < 0 {
		age = 0
	}
	staleAfter := s.staleAfterSeconds(halfLife)
	if staleAfter > 0 && age > staleAfter {
		sh.mu.RUnlock()
		return Sample{}
	}
	decay := decayFactor(age, halfLife)
	snap := bucket{
		weight:       b.weight,
		count:        b.count,
		lastUpdate:   b.lastUpdate,
		cappedWeight: b.cappedWeight,
		cappedCount:  b.cappedCount,
	}
	cappedReceiverCount := uint32(0)
	cappedReceiverCapacity := uint32(0)
	if s.cfg.ReceiverContributionMode != ReceiverContributionOff {
		cappedReceiverCount = b.receiverCountAfterDecay(receiverSlots, decay)
		cappedReceiverCapacity = receiverSlotCapacity(receiverSlots)
	}
	sh.mu.RUnlock()
	rawWeight := snap.weight * decay
	if rawWeight <= 0 {
		return Sample{}
	}
	cappedWeight := rawWeight
	cappedCount := snap.count
	capLimited := false
	if s.cfg.ReceiverContributionMode != ReceiverContributionOff {
		cappedWeight = snap.cappedWeight * decay
		if cappedWeight <= 0 {
			cappedWeight = 0
		}
		cappedCount = effectiveCountToUint32(snap.cappedCount * decay)
		capLimited = receiverCapLimited(snap.count, cappedCount, rawWeight, cappedWeight)
	}
	activeWeight := rawWeight
	activeCount := snap.count
	if s.cfg.ReceiverContributionMode == ReceiverContributionEnforce {
		activeWeight = cappedWeight
		activeCount = cappedCount
	}
	if activeWeight <= 0 {
		return Sample{
			AgeSec:                 age,
			Count:                  activeCount,
			ObservationCount:       snap.count,
			RawCount:               snap.count,
			RawWeight:              rawWeight,
			CappedCount:            cappedCount,
			CappedWeight:           cappedWeight,
			CappedReceiverCount:    cappedReceiverCount,
			CappedReceiverCapacity: cappedReceiverCapacity,
			CapLimited:             capLimited,
		}
	}
	return Sample{
		Weight:                 activeWeight,
		AgeSec:                 age,
		Count:                  activeCount,
		ObservationCount:       snap.count,
		RawCount:               snap.count,
		RawWeight:              rawWeight,
		CappedCount:            cappedCount,
		CappedWeight:           cappedWeight,
		CappedReceiverCount:    cappedReceiverCount,
		CappedReceiverCapacity: cappedReceiverCapacity,
		CapLimited:             capLimited,
	}
}

func (s *Store) sampleWithDistribution(key uint64, halfLife int, now time.Time, receiverSlots int) sampleWithBins {
	if key == 0 {
		return sampleWithBins{}
	}
	nowSec := now.Unix()
	sh := &s.shards[key%uint64(len(s.shards))]
	sh.mu.RLock()
	b := sh.buckets[key]
	if b == nil {
		sh.mu.RUnlock()
		return sampleWithBins{}
	}
	age := nowSec - b.lastUpdate
	if age < 0 {
		age = 0
	}
	staleAfter := s.staleAfterSeconds(halfLife)
	if staleAfter > 0 && age > staleAfter {
		sh.mu.RUnlock()
		return sampleWithBins{}
	}
	decay := decayFactor(age, halfLife)
	snap := bucket{
		weight:        b.weight,
		count:         b.count,
		lastUpdate:    b.lastUpdate,
		cappedWeight:  b.cappedWeight,
		cappedCount:   b.cappedCount,
		rawSNRBins:    b.rawSNRBins,
		cappedSNRBins: b.cappedSNRBins,
	}
	cappedReceiverCount := uint32(0)
	cappedReceiverCapacity := uint32(0)
	if s.cfg.ReceiverContributionMode != ReceiverContributionOff {
		cappedReceiverCount = b.receiverCountAfterDecay(receiverSlots, decay)
		cappedReceiverCapacity = receiverSlotCapacity(receiverSlots)
	}
	sh.mu.RUnlock()
	rawWeight := snap.weight * decay
	if rawWeight <= 0 {
		return sampleWithBins{}
	}
	cappedWeight := rawWeight
	cappedCount := snap.count
	capLimited := false
	if s.cfg.ReceiverContributionMode != ReceiverContributionOff {
		cappedWeight = snap.cappedWeight * decay
		if cappedWeight <= 0 {
			cappedWeight = 0
		}
		cappedCount = effectiveCountToUint32(snap.cappedCount * decay)
		capLimited = receiverCapLimited(snap.count, cappedCount, rawWeight, cappedWeight)
	}
	activeWeight := rawWeight
	activeCount := snap.count
	activeSNRBins := snap.rawSNRBins
	if s.cfg.ReceiverContributionMode == ReceiverContributionEnforce {
		activeWeight = cappedWeight
		activeCount = cappedCount
		activeSNRBins = snap.cappedSNRBins
	}
	activeSNRBins.decay(decay)
	p50DB, hasP50 := activeSNRBins.p50DB()
	if activeWeight <= 0 {
		return sampleWithBins{
			Sample: Sample{
				AgeSec:                 age,
				Count:                  activeCount,
				ObservationCount:       snap.count,
				RawCount:               snap.count,
				RawWeight:              rawWeight,
				CappedCount:            cappedCount,
				CappedWeight:           cappedWeight,
				CappedReceiverCount:    cappedReceiverCount,
				CappedReceiverCapacity: cappedReceiverCapacity,
				CapLimited:             capLimited,
			},
		}
	}
	return sampleWithBins{
		Sample: Sample{
			Weight:                 activeWeight,
			AgeSec:                 age,
			Count:                  activeCount,
			ObservationCount:       snap.count,
			RawCount:               snap.count,
			RawWeight:              rawWeight,
			CappedCount:            cappedCount,
			CappedWeight:           cappedWeight,
			CappedReceiverCount:    cappedReceiverCount,
			CappedReceiverCapacity: cappedReceiverCapacity,
			CapLimited:             capLimited,
		},
		P50DB:   p50DB,
		HasP50:  hasP50,
		snrBins: activeSNRBins,
	}
}

func receiverCapLimited(rawCount uint32, cappedCount uint32, rawWeight float64, cappedWeight float64) bool {
	const epsilon = 1e-9
	return rawCount > cappedCount || rawWeight > cappedWeight+epsilon
}

func effectiveCountToUint32(count float64) uint32 {
	if count <= 0 || math.IsNaN(count) {
		return 0
	}
	if count >= float64(maxBucketObservationCount) {
		return maxBucketObservationCount
	}
	return uint32(math.Floor(count + effectiveCountEpsilon))
}

func receiverSlotCapacity(receiverSlots int) uint32 {
	if receiverSlots <= 0 {
		return 0
	}
	if receiverSlots > maxCoarseReceiverSlots {
		receiverSlots = maxCoarseReceiverSlots
	}
	return uint32(receiverSlots)
}

func (b *bucket) receiverCountAfterDecay(receiverSlots int, decay float64) uint32 {
	if b == nil || receiverSlots <= 0 {
		return 0
	}
	if receiverSlots > maxCoarseReceiverSlots {
		receiverSlots = maxCoarseReceiverSlots
	}
	var count uint32
	for i := 0; i < receiverSlots; i++ {
		slot := b.receiverSlotAt(i, false)
		if slot == nil || slot.hash == 0 {
			continue
		}
		if slot.weight*decay > effectiveCountEpsilon || slot.count*decay > effectiveCountEpsilon {
			count++
		}
	}
	return count
}

// PurgeStale removes buckets older than stale-after.
func (s *Store) PurgeStale(now time.Time) int {
	if s == nil {
		return 0
	}
	removed := 0
	bands := s.bandIndex.Bands()
	staleAfterByBand := make([]int64, len(bands))
	for i, band := range bands {
		halfLife := s.bandIndex.HalfLifeSeconds(band, s.cfg)
		staleAfterByBand[i] = s.staleAfterSeconds(halfLife)
	}
	nowSec := now.Unix()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k, b := range sh.buckets {
			if b == nil {
				delete(sh.buckets, k)
				removed++
				continue
			}
			age := nowSec - b.lastUpdate
			if age < 0 {
				age = 0
			}
			isCoarse := k&0xFFFF != 0
			var idx uint16
			if isCoarse {
				idx = uint16((k >> 32) & 0xFFFF)
			} else {
				idx = uint16((k >> 48) & 0xFFFF)
			}
			if int(idx) >= len(staleAfterByBand) {
				continue
			}
			staleAfter := staleAfterByBand[idx]
			if staleAfter > 0 && age > staleAfter {
				delete(sh.buckets, k)
				removed++
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

// Compact rebuilds shard maps when they have shrunk far below their peak size.
// It preserves all live entries and returns the number of shards compacted.
func (s *Store) Compact(minPeak int, shrinkRatio float64) int {
	if s == nil {
		return 0
	}
	if minPeak <= 0 {
		minPeak = 1000
	}
	if shrinkRatio <= 0 || shrinkRatio >= 1 {
		shrinkRatio = 0.5
	}
	compacted := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		current := len(sh.buckets)
		if current > sh.peak {
			sh.peak = current
		}
		threshold := int(float64(sh.peak) * shrinkRatio)
		if sh.peak >= minPeak && current < threshold {
			if current == 0 {
				sh.buckets = make(map[uint64]*bucket)
			} else {
				next := make(map[uint64]*bucket, current)
				for k, v := range sh.buckets {
					if v != nil {
						next[k] = v
					}
				}
				sh.buckets = next
			}
			sh.peak = len(sh.buckets)
			compacted++
		}
		sh.mu.Unlock()
	}
	return compacted
}

// TotalBuckets returns the total number of buckets across all shards.
func (s *Store) TotalBuckets() int {
	if s == nil {
		return 0
	}
	total := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		total += len(sh.buckets)
		sh.mu.RUnlock()
	}
	return total
}

// Stats returns the last refreshed counts of active fine/coarse buckets.
func (s *Store) Stats(_ time.Time) (fine int, coarse int) {
	if s == nil || !s.cfg.Enabled {
		return 0, 0
	}
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	return s.stats.fine, s.stats.coarse
}

// StatsByBand returns the last refreshed counts of active fine/coarse buckets
// per band.
func (s *Store) StatsByBand(_ time.Time) []bandCounts {
	if s == nil || !s.cfg.Enabled {
		return nil
	}
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	if len(s.stats.byBand) == 0 {
		return nil
	}
	counts := make([]bandCounts, len(s.stats.byBand))
	copy(counts, s.stats.byBand)
	return counts
}

// RefreshStatsSnapshot recomputes the cached active-bucket counts used by
// operator-facing stats displays. It is intentionally explicit so ingest writes
// never scan the whole store.
func (s *Store) RefreshStatsSnapshot(now time.Time) {
	if s == nil || !s.cfg.Enabled {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	nowSec := now.Unix()

	s.statsRefreshMu.Lock()
	defer s.statsRefreshMu.Unlock()

	bands := s.bandIndex.Bands()
	counts := make([]bandCounts, len(bands))
	staleAfterByBand := make([]int64, len(bands))
	for i, band := range bands {
		halfLife := s.bandIndex.HalfLifeSeconds(band, s.cfg)
		staleAfterByBand[i] = s.staleAfterSeconds(halfLife)
	}
	fine := 0
	coarse := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for key, b := range sh.buckets {
			if b == nil {
				continue
			}
			age := nowSec - b.lastUpdate
			if age < 0 {
				age = 0
			}
			isCoarse := key&0xFFFF != 0
			var idx uint16
			if isCoarse {
				idx = uint16((key >> 32) & 0xFFFF)
			} else {
				idx = uint16((key >> 48) & 0xFFFF)
			}
			if int(idx) >= len(counts) {
				continue
			}
			staleAfter := staleAfterByBand[idx]
			if staleAfter > 0 && age > staleAfter {
				continue
			}
			if isCoarse {
				counts[idx].coarse++
				coarse++
			} else {
				counts[idx].fine++
				fine++
			}
		}
		sh.mu.RUnlock()
	}

	s.statsMu.Lock()
	s.stats = pathStoreStatsSnapshot{
		asOfUnix: nowSec,
		fine:     fine,
		coarse:   coarse,
		byBand:   counts,
	}
	s.statsMu.Unlock()
}

// WeightHistogramByBand returns per-band bucket weight histograms for active buckets.
// edges defines the ascending bin boundaries (len+1 bins, last bin is >= last edge).
func (s *Store) WeightHistogramByBand(now time.Time, edges []float64) []weightHistogram {
	if s == nil || !s.cfg.Enabled {
		return nil
	}
	bands := s.bandIndex.Bands()
	if len(bands) == 0 {
		return nil
	}
	if len(edges) == 0 {
		return nil
	}
	binCount := len(edges) + 1
	counts := make([]weightHistogram, len(bands))
	for i := range counts {
		counts[i].bins = make([]int, binCount)
	}
	halfLives := make([]int, len(bands))
	for i, band := range bands {
		halfLives[i] = s.bandIndex.HalfLifeSeconds(band, s.cfg)
	}
	staleAfterByBand := make([]int64, len(bands))
	for i, halfLife := range halfLives {
		staleAfterByBand[i] = s.staleAfterSeconds(halfLife)
	}
	nowSec := now.Unix()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for key, b := range sh.buckets {
			if b == nil {
				continue
			}
			age := nowSec - b.lastUpdate
			if age < 0 {
				age = 0
			}
			isCoarse := key&0xFFFF != 0
			var idx uint16
			if isCoarse {
				idx = uint16((key >> 32) & 0xFFFF)
			} else {
				idx = uint16((key >> 48) & 0xFFFF)
			}
			if int(idx) >= len(counts) {
				continue
			}
			staleAfter := staleAfterByBand[idx]
			if staleAfter > 0 && age > staleAfter {
				continue
			}
			halfLife := halfLives[idx]
			decay := decayFactor(age, halfLife)
			decayedWeight := float64(b.weight) * decay
			if decayedWeight <= 0 {
				continue
			}
			bin := weightBinIndex(decayedWeight, edges)
			counts[idx].total++
			counts[idx].bins[bin]++
		}
		sh.mu.RUnlock()
	}
	return counts
}

func weightBinIndex(weight float64, edges []float64) int {
	for i, edge := range edges {
		if weight < edge {
			return i
		}
	}
	return len(edges)
}

func decayFactor(ageSec int64, halfLifeSec int) float64 {
	if halfLifeSec <= 0 || ageSec <= 0 {
		return 1
	}
	return math.Exp(-ln2 * float64(ageSec) / float64(halfLifeSec))
}

func (s *Store) staleAfterSeconds(halfLifeSec int) int64 {
	if s == nil {
		return 0
	}
	if s.cfg.StaleAfterHalfLifeMultiplier > 0 && halfLifeSec > 0 {
		seconds := math.Ceil(float64(halfLifeSec) * s.cfg.StaleAfterHalfLifeMultiplier)
		if seconds < 1 {
			seconds = 1
		}
		return int64(seconds)
	}
	if s.cfg.StaleAfterSeconds <= 0 {
		return 0
	}
	return int64(s.cfg.StaleAfterSeconds)
}
