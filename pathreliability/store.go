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
	// lastUpdate is the monotonic accounting reference in Unix seconds.
	lastUpdate int64
	// newestObservation is independent of reference advancement and includes
	// validated cap-rejected/numerically unusable raw reports. Epoch zero is valid.
	newestObservation int64

	cappedWeight float64
	cappedCount  float64
	snrBins      snrHistogram
	slots        [inlineReceiverSlots]receiverSlot
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
	shards     []shard
	cfg        Config
	bandIndex  BandIndex
	admissions admissionCounters

	statsMu        sync.RWMutex
	statsRefreshMu sync.Mutex
	stats          pathStoreStatsSnapshot
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
func (s *Store) UpdateWithReceiverHash(receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, ft8DB float64, weight float64, times ObservationTimes, receiverHash uint64) ReportAdmission {
	return s.updateReport(receiverCell, senderCell, receiverCoarse, senderCoarse, band, ft8DB, weight, &times, receiverHash, false, BucketCombined)
}

// updateReport validates original input before applying an optional predictor
// beacon limit. One entry owns validation, diagnostics, and fine/coarse writes;
// its outcome is recorded once, independently of bucket or cap admission.
func (s *Store) updateReport(receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, ft8DB float64, weight float64, times *ObservationTimes, receiverHash uint64, isBeacon bool, class BucketClass) ReportAdmission {
	if s == nil || !s.cfg.Enabled || class != BucketCombined {
		return ReportAdmission{Status: AdmissionInactive}
	}
	admission := validateReport(times, weight)
	if admission.Status != AdmissionAccepted {
		s.admissions.record(admission.Status)
	}
	if !admission.Accepted() {
		return admission
	}
	if isBeacon && s.cfg.BeaconWeightCap > 0 && weight > s.cfg.BeaconWeightCap {
		weight = s.cfg.BeaconWeightCap
	}
	idx, ok := s.bandIndex.Lookup(band)
	if !ok {
		return admission
	}
	halfLife := s.bandIndex.HalfLifeSeconds(band, s.cfg)
	snrBin := snrHistogramBinIndex(ft8DB)
	if snrBin < 0 {
		return admission
	}
	observedSec, arrivedSec := admission.EffectiveObservation.Unix(), times.ArrivedAt.Unix()
	mass, count := agedContribution(weight, nonnegativeSecondsSince(arrivedSec, observedSec), halfLife)
	if receiverCell == InvalidCell || senderCell == InvalidCell {
		// Still allow coarse update when fine cells are missing.
	} else {
		s.updateBucket(packKey(receiverCell, senderCell, idx), weight, mass, count, observedSec, arrivedSec, halfLife, receiverHash, s.cfg.ReceiverFineSlots, snrBin)
	}
	if receiverCoarse != InvalidCell && senderCoarse != InvalidCell {
		s.updateBucket(packCoarseKey(receiverCoarse, senderCoarse, idx), weight, mass, count, observedSec, arrivedSec, halfLife, receiverHash, s.cfg.ReceiverCoarseSlots, snrBin)
	}
	return admission
}

// updateBucket ages existing and incoming evidence to one monotonic accounting
// reference. Count/freshness describe eligible raw reports, independently of
// usable histogram mass and receiver admission.
func (s *Store) updateBucket(key uint64, weight, mass, count float64, observedSec, arrivedSec int64, halfLifeSec int, receiverHash uint64, receiverSlots int, snrBin int) {
	if key == 0 {
		return
	}
	sh := &s.shards[key%uint64(len(s.shards))]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	b := sh.buckets[key]
	if b == nil {
		b = &bucket{lastUpdate: arrivedSec, newestObservation: observedSec}
		sh.buckets[key] = b
		if len(sh.buckets) > sh.peak {
			sh.peak = len(sh.buckets)
		}
	}
	reference := max(b.lastUpdate, arrivedSec)
	elapsed := nonnegativeSecondsSince(reference, b.lastUpdate)
	decay := decayFactor(elapsed, halfLifeSec)
	if decay != 1 {
		b.weight = decayedMass(b.weight, elapsed, halfLifeSec, decay)
		b.cappedWeight = decayedMass(b.cappedWeight, elapsed, halfLifeSec, decay)
		b.cappedCount *= decay
		b.snrBins.decay(decay)
		if s.cfg.ReceiverContributionMode != ReceiverContributionOff {
			b.decayReceiverSlots(decay, receiverSlots)
		}
	}
	b.lastUpdate = reference
	b.newestObservation = max(b.newestObservation, observedSec)
	if b.count < maxBucketObservationCount {
		b.count++
	}
	// Both buckets normally share arrival's incoming factor. A captured bucket
	// ahead of arrival needs its own later reference, without amplifying weight.
	if reference > arrivedSec {
		mass, count = agedContribution(weight, nonnegativeSecondsSince(reference, observedSec), halfLifeSec)
	}
	if !finitePositiveSum(b.weight, mass) {
		return
	}
	activeCapped := s.cfg.ReceiverContributionMode == ReceiverContributionEnforce
	var rawBin float32
	if activeCapped {
		if !usableHistogramIncrement(mass) {
			return
		}
	} else {
		var safe bool
		rawBin, safe = b.snrBins.addition(snrBin, mass)
		if !safe {
			return
		}
	}
	b.weight += mass
	if !activeCapped {
		b.snrBins[snrBin] = rawBin
	}
	if s.cfg.ReceiverContributionMode != ReceiverContributionOff {
		b.updateCapped(mass, count, reference, receiverHash, receiverSlots, &s.cfg, snrBin)
	}
}

func finitePositiveSum(current, increment float64) bool {
	sum := current + increment
	return current >= 0 && sum > 0 && sum <= math.MaxFloat64
}

// updateCapped previews receiver ownership and the final weighted increment.
// No allocation or eviction occurs until that increment is safe. Raw usable
// mass remains recorded when only cap scaling makes the capped mass unusable.
func (b *bucket) updateCapped(mass, count float64, reference int64, receiverHash uint64, receiverSlots int, cfg *Config, snrBin int) {
	if cfg.ReceiverContributionMode == ReceiverContributionOff || receiverHash == 0 || receiverSlots <= 0 || cfg.ReceiverMaxEffectiveWeight <= 0 || cfg.ReceiverMaxEffectiveCount == 0 {
		return
	}
	index := b.selectReceiverSlotIndex(receiverHash, receiverSlots)
	if index < 0 {
		return
	}
	var previous receiverSlot
	if slot := b.receiverSlotAt(index, false); slot != nil && slot.hash == receiverHash {
		previous = *slot
	}
	remainingWeight := cfg.ReceiverMaxEffectiveWeight - previous.weight
	remainingCount := float64(cfg.ReceiverMaxEffectiveCount) - previous.count
	if remainingWeight <= effectiveCountEpsilon || remainingCount <= effectiveCountEpsilon {
		return
	}
	fraction := math.Min(1, remainingWeight/mass)
	if count > 0 {
		fraction = math.Min(fraction, remainingCount/count)
	}
	if !(fraction > effectiveCountEpsilon) {
		return
	}
	acceptedWeight, acceptedCount := mass*fraction, count*fraction
	if !usableHistogramIncrement(acceptedWeight) || !finitePositiveSum(b.cappedWeight, acceptedWeight) || !finitePositiveSum(previous.weight, acceptedWeight) {
		return
	}
	activeCapped := cfg.ReceiverContributionMode == ReceiverContributionEnforce
	var cappedBin float32
	if activeCapped {
		var safe bool
		cappedBin, safe = b.snrBins.addition(snrBin, acceptedWeight)
		if !safe {
			return
		}
	}
	slot := b.receiverSlotAt(index, true)
	*slot = receiverSlot{hash: receiverHash, weight: previous.weight + acceptedWeight, count: previous.count + acceptedCount, lastUpdate: reference}
	b.cappedWeight += acceptedWeight
	b.cappedCount += acceptedCount
	if activeCapped {
		b.snrBins[snrBin] = cappedBin
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

func (b *bucket) selectReceiverSlotIndex(hash uint64, receiverSlots int) int {
	if b == nil || hash == 0 || receiverSlots <= 0 {
		return -1
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
			return i
		}
		if replace < 0 || slot.weight < replaceWeight || (slot.weight == replaceWeight && slot.lastUpdate < replaceUpdate) {
			replace = i
			replaceWeight = slot.weight
			replaceUpdate = slot.lastUpdate
		}
	}
	if empty >= 0 {
		return empty
	}
	return replace
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
	// SamplingUnix is the common evaluation time, even for count-only evidence.
	SamplingUnix           int64
	HasSamplingTime        bool
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
	P50DB                   float64
	HasP50                  bool
	snrBins                 snrHistogram
	distributionUnavailable bool
}

// bucketCapture owns copied receiver values; no retained sidecar pointer leaves
// a shard lock. Presence is explicit because Unix epoch zero is a valid clock.
type bucketCapture struct {
	present                      bool
	weight                       float64
	count                        uint32
	reference, newestObservation int64
	cappedWeight, cappedCount    float64
	bins                         snrHistogram
	slots                        [maxCoarseReceiverSlots]receiverCapture
}

type receiverCapture struct{ weight, count float64 }

func (s *Store) captureBucket(key uint64, receiverSlots int, distribution bool) bucketCapture {
	var capture bucketCapture
	s.captureBucketInto(key, receiverSlots, distribution, &capture)
	return capture
}

func (s *Store) captureBucketInto(key uint64, receiverSlots int, distribution bool, capture *bucketCapture) {
	if key == 0 {
		return
	}
	sh := &s.shards[key%uint64(len(s.shards))]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	b := sh.buckets[key]
	if b == nil {
		return
	}
	*capture = bucketCapture{present: true, weight: b.weight, count: b.count,
		reference: b.lastUpdate, newestObservation: b.newestObservation,
		cappedWeight: b.cappedWeight, cappedCount: b.cappedCount}
	if distribution {
		capture.bins = b.snrBins
	}
	if s.cfg.ReceiverContributionMode != ReceiverContributionOff {
		for i := 0; i < min(receiverSlots, maxCoarseReceiverSlots); i++ {
			if slot := b.receiverSlotAt(i, false); slot != nil {
				if slot.hash != 0 {
					capture.slots[i] = receiverCapture{weight: slot.weight, count: slot.count}
				}
			}
		}
	}
}

// sampleDetail separates bin materialization from quantile work. Production
// validates complete needed components when it writes selected/merged scratch
// bins; only the final distribution needs a median. Diagnostic lookup helpers
// can still explicitly request individual quantiles.
type sampleDetail uint8

const (
	sampleScalars sampleDetail = iota
	sampleDistribution
	sampleQuantile
)

// Lookup evaluates both overlapping candidates at one captured sampling time.
func (s *Store) Lookup(receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, now time.Time) (fine Sample, coarse Sample) {
	candidates := s.lookupCandidateSet(receiverCell, senderCell, receiverCoarse, senderCoarse, band, now, false, sampleScalars)
	return candidates[0].Sample, candidates[1].Sample
}

func (s *Store) lookupCandidates(receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, now time.Time, includeReverse bool) [4]sampleWithBins {
	return s.lookupCandidateSet(receiverCell, senderCell, receiverCoarse, senderCoarse, band, now, includeReverse, sampleDistribution)
}

func (s *Store) lookupCandidateSet(receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, now time.Time, includeReverse bool, detail sampleDetail) (samples [4]sampleWithBins) {
	if s == nil || !s.cfg.Enabled {
		return
	}
	idx, ok := s.bandIndex.Lookup(band)
	if !ok {
		return
	}
	var keys [4]uint64
	if receiverCell != InvalidCell && senderCell != InvalidCell {
		keys[0] = packKey(receiverCell, senderCell, idx)
		if includeReverse {
			keys[2] = packKey(senderCell, receiverCell, idx)
		}
	}
	if receiverCoarse != InvalidCell && senderCoarse != InvalidCell {
		keys[1] = packCoarseKey(receiverCoarse, senderCoarse, idx)
		if includeReverse {
			keys[3] = packCoarseKey(senderCoarse, receiverCoarse, idx)
		}
	}
	var captures [4]bucketCapture
	reference := now.Unix()
	length := 2
	if includeReverse {
		length = 4
	}
	for i := 0; i < length; i++ {
		slots := s.cfg.ReceiverFineSlots
		if i%2 == 1 {
			slots = s.cfg.ReceiverCoarseSlots
		}
		s.captureBucketInto(keys[i], slots, detail != sampleScalars, &captures[i])
		if captures[i].present {
			reference = max(reference, captures[i].reference)
		}
	}
	halfLife := s.bandIndex.HalfLifeSeconds(band, s.cfg)
	for i := 0; i < length; i++ {
		slots := s.cfg.ReceiverFineSlots
		if i%2 == 1 {
			slots = s.cfg.ReceiverCoarseSlots
		}
		samples[i] = s.evaluateCapture(&captures[i], halfLife, reference, slots, detail)
	}
	return
}

func (s *Store) evaluateCapture(capture *bucketCapture, halfLife int, reference int64, receiverSlots int, detail sampleDetail) sampleWithBins {
	result := sampleWithBins{Sample: Sample{SamplingUnix: reference, HasSamplingTime: true}}
	if !capture.present {
		return result
	}
	age := nonnegativeSecondsSince(reference, capture.newestObservation)
	if staleAfter := s.staleAfterSeconds(halfLife); staleAfter > 0 && age > staleAfter {
		return result
	}
	elapsed := nonnegativeSecondsSince(reference, capture.reference)
	decay := decayFactor(elapsed, halfLife)
	rawWeight := decayedMass(capture.weight, elapsed, halfLife, decay)
	cappedWeight, cappedCount := rawWeight, capture.count
	var receiverCount, capacity uint32
	capLimited := false
	if s.cfg.ReceiverContributionMode != ReceiverContributionOff {
		cappedWeight = decayedMass(capture.cappedWeight, elapsed, halfLife, decay)
		cappedCount = effectiveCountToUint32(capture.cappedCount * decay)
		capLimited = receiverCapLimited(capture.count, cappedCount, rawWeight, cappedWeight)
		capacity = receiverSlotCapacity(receiverSlots)
		for i := 0; i < min(receiverSlots, maxCoarseReceiverSlots); i++ {
			slot := capture.slots[i]
			if slot.weight*decay > effectiveCountEpsilon || slot.count*decay > effectiveCountEpsilon {
				receiverCount++
			}
		}
	}
	activeWeight, activeCount := rawWeight, capture.count
	if s.cfg.ReceiverContributionMode == ReceiverContributionEnforce {
		activeWeight, activeCount = cappedWeight, cappedCount
	}
	result.Sample = Sample{SamplingUnix: reference, HasSamplingTime: true,
		Weight: activeWeight, AgeSec: age, Count: activeCount,
		ObservationCount: capture.count, RawCount: capture.count, RawWeight: rawWeight,
		CappedCount: cappedCount, CappedWeight: cappedWeight,
		CappedReceiverCount: receiverCount, CappedReceiverCapacity: capacity, CapLimited: capLimited}
	if math.IsNaN(activeWeight) || math.IsInf(activeWeight, 0) || activeWeight < 0 {
		result.distributionUnavailable = detail != sampleScalars
		return result
	}
	if detail == sampleScalars || !(activeWeight > 0) {
		return result
	}
	result.snrBins = capture.bins
	arithmeticOK := true
	if decay != 1 {
		arithmeticOK = result.snrBins.decay(decay)
	}
	if detail == sampleQuantile {
		result.P50DB, result.HasP50 = result.snrBins.p50DB()
		result.distributionUnavailable = !arithmeticOK || !result.HasP50
	} else {
		// Every needed component is validated by addScaled, shifted, or the
		// final p50 calculation. Empty positive distributions fail those same
		// boundaries, without rescanning all four candidate arrays here.
		result.distributionUnavailable = !arithmeticOK
	}
	if result.distributionUnavailable {
		result.HasP50 = false
	}
	return result
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
			age := nonnegativeSecondsSince(max(nowSec, b.lastUpdate), b.newestObservation)
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

// Compact rebuilds maps after local or distributed store contraction.
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
	distributed := s.distributedContraction(minPeak, shrinkRatio)
	compacted := 0
	for i := range s.shards {
		if s.shards[i].compactIfContracted(minPeak, shrinkRatio, distributed) {
			compacted++
		}
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

func decayFactor(ageSec int64, halfLifeSec int) float64 {
	if halfLifeSec <= 0 || ageSec <= 0 {
		return 1
	}
	return math.Exp2(-float64(ageSec) / float64(halfLifeSec))
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
		return ceilNonnegativeInt64(seconds)
	}
	if s.cfg.StaleAfterSeconds <= 0 {
		return 0
	}
	return int64(s.cfg.StaleAfterSeconds)
}
