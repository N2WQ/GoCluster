// File role: Owns path prediction orchestration and final glyph diagnostics.
// Crawler notes: Start here for PATH class calculation, insufficient reasons,
// receive/transmit merge policy, receiver-cap enforcement/shadow semantics, and
// the observation-floor contract used by telnet display and filters.
// Related docs: pathreliability/README.md, README.md, data/config/PATH_PREDICTIONS.md.
// Related tests: pathreliability/*_test.go, telnet/*path*_test.go.
package pathreliability

import (
	"math"
	"time"
)

// Predictor coordinates store access and presentation mapping.
type Predictor struct {
	combined *Store
	cfg      Config
}

// NewPredictor builds a predictor with normalized configuration.
func NewPredictor(cfg Config, bands []string) *Predictor {
	cfg.normalize()
	return &Predictor{
		combined: NewStore(cfg, bands),
		cfg:      cfg,
	}
}

// Config returns the normalized configuration in use.
func (p *Predictor) Config() Config {
	if p == nil {
		return Config{}
	}
	return p.cfg
}

// Update ingests a spot contribution (FT8-equiv) with optional beacon weight cap.
func (p *Predictor) Update(bucket BucketClass, receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, ft8dB float64, weight float64, now time.Time, isBeacon bool) {
	p.UpdateWithReceiverHash(bucket, receiverCell, senderCell, receiverCoarse, senderCoarse, band, ft8dB, weight, now, isBeacon, 0)
}

// UpdateWithReceiverHash ingests a spot contribution and attributes capped
// trust to the normalized receiving station identity hash.
func (p *Predictor) UpdateWithReceiverHash(bucket BucketClass, receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, ft8dB float64, weight float64, now time.Time, isBeacon bool, receiverHash uint64) {
	if p == nil || !p.cfg.Enabled {
		return
	}
	w := weight
	if isBeacon && p.cfg.BeaconWeightCap > 0 && w > p.cfg.BeaconWeightCap {
		w = p.cfg.BeaconWeightCap
	}
	power := p.cfg.powerFromDB(ft8dB)
	switch bucket {
	case BucketCombined:
		if p.combined != nil {
			p.combined.UpdateWithReceiverHashDB(receiverCell, senderCell, receiverCoarse, senderCoarse, band, ft8dB, power, w, now, receiverHash)
		}
	default:
		return
	}
}

// PredictionSource describes which bucket family produced the glyph.
type PredictionSource uint8

const (
	SourceInsufficient PredictionSource = iota
	SourceCombined
)

// InsufficientReason describes why a prediction returned the insufficient glyph.
type InsufficientReason uint8

const (
	InsufficientNone InsufficientReason = iota
	InsufficientNoSample
	InsufficientLowCount
	InsufficientLowWeight
	InsufficientStale
)

func (r InsufficientReason) String() string {
	switch r {
	case InsufficientNone:
		return "none"
	case InsufficientNoSample:
		return "no_sample"
	case InsufficientLowCount:
		return "low_count"
	case InsufficientLowWeight:
		return "low_weight"
	case InsufficientStale:
		return "stale"
	default:
		return "unknown"
	}
}

// Result carries merged glyph and diagnostics.
type Result struct {
	Glyph              string
	Value              float64 // power
	MeanDB             float64
	HasMeanDB          bool
	P50DB              float64
	HasP50             bool
	P50Glyph           string
	Weight             float64
	AgeSec             int64
	Count              uint32
	RawCount           uint32
	RawWeight          float64
	CappedCount        uint32
	CappedWeight       float64
	CapLimited         bool
	CapWouldBlock      bool
	Source             PredictionSource
	InsufficientReason InsufficientReason
}

// Predict returns a single merged glyph for the path.
func (p *Predictor) Predict(userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, mode string, noisePenalty float64, now time.Time) Result {
	minObservationCount := 0
	if p != nil {
		minObservationCount = p.cfg.MinObservationCount
	}
	return p.PredictWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, band, mode, noisePenalty, minObservationCount, now)
}

// PredictWithMinObservationCount returns a merged glyph using the supplied
// observation floor. In shadow/off modes the floor is applied to raw selected
// count; in enforce mode it is applied to capped selected count. Callers may
// pass a floor above the configured default when applying stricter per-session
// display or filter policy.
func (p *Predictor) PredictWithMinObservationCount(userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, mode string, noisePenalty float64, minObservationCount int, now time.Time) Result {
	return p.predictWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, band, mode, noisePenalty, minObservationCount, now, false)
}

// PredictWithMinObservationCountAndDistribution includes shadow SNR
// distribution diagnostics for callers that render PATHP50 output.
func (p *Predictor) PredictWithMinObservationCountAndDistribution(userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, mode string, noisePenalty float64, minObservationCount int, now time.Time) Result {
	return p.predictWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, band, mode, noisePenalty, minObservationCount, now, true)
}

func (p *Predictor) predictWithMinObservationCount(userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, mode string, noisePenalty float64, minObservationCount int, now time.Time, includeDistribution bool) Result {
	insufficient := "?"
	if p != nil && p.cfg.GlyphSymbols.Insufficient != "" {
		insufficient = p.cfg.GlyphSymbols.Insufficient
	}
	if p == nil || !p.cfg.Enabled {
		return Result{Glyph: insufficient, Source: SourceInsufficient}
	}

	modeKey := normalizeMode(mode)
	makeResult := func(sample Sample, p50DB float64, hasP50 bool, source PredictionSource, capWouldBlock bool) Result {
		meanDB, hasMeanDB := 0.0, false
		p50Glyph := ""
		if includeDistribution {
			meanDB, hasMeanDB = meanDBForSample(sample)
			if hasP50 {
				p50Glyph = GlyphForPower(p.cfg.powerFromDB(p50DB), modeKey, p.cfg)
			}
		}
		return Result{
			Glyph:         GlyphForPower(sample.Value, modeKey, p.cfg),
			Value:         sample.Value,
			MeanDB:        meanDB,
			HasMeanDB:     hasMeanDB,
			P50DB:         p50DB,
			HasP50:        hasP50,
			P50Glyph:      p50Glyph,
			Weight:        sample.Weight,
			AgeSec:        sample.AgeSec,
			Count:         sample.Count,
			RawCount:      sample.RawCount,
			RawWeight:     sample.RawWeight,
			CappedCount:   sample.CappedCount,
			CappedWeight:  sample.CappedWeight,
			CapLimited:    sample.CapLimited,
			CapWouldBlock: capWouldBlock,
			Source:        source,
		}
	}
	makeInsufficient := func(sample Sample, p50DB float64, hasP50 bool, reason InsufficientReason, capWouldBlock bool) Result {
		meanDB, hasMeanDB := 0.0, false
		p50Glyph := ""
		if includeDistribution {
			meanDB, hasMeanDB = meanDBForSample(sample)
			if hasP50 {
				p50Glyph = GlyphForPower(p.cfg.powerFromDB(p50DB), modeKey, p.cfg)
			}
		}
		return Result{
			Glyph:              insufficient,
			Value:              sample.Value,
			MeanDB:             meanDB,
			HasMeanDB:          hasMeanDB,
			P50DB:              p50DB,
			HasP50:             hasP50,
			P50Glyph:           p50Glyph,
			Weight:             sample.Weight,
			AgeSec:             sample.AgeSec,
			Count:              sample.Count,
			RawCount:           sample.RawCount,
			RawWeight:          sample.RawWeight,
			CappedCount:        sample.CappedCount,
			CappedWeight:       sample.CappedWeight,
			CapLimited:         sample.CapLimited,
			CapWouldBlock:      capWouldBlock,
			Source:             SourceInsufficient,
			InsufficientReason: reason,
		}
	}

	merged, p50DB, hasP50, reason, ok := p.mergeFromStore(p.combined, userCell, dxCell, userCoarse, dxCoarse, band, noisePenalty, now, includeDistribution)
	if minObservationCount < p.cfg.MinObservationCount {
		minObservationCount = p.cfg.MinObservationCount
	}
	capWouldBlock := p.capWouldBlock(merged, minObservationCount)
	if ok && merged.Weight >= p.cfg.MinEffectiveWeight && countMeetsMinimum(merged.Count, minObservationCount) {
		return makeResult(merged, p50DB, hasP50, SourceCombined, capWouldBlock)
	}
	if ok {
		if reason == InsufficientNone {
			if !countMeetsMinimum(merged.Count, minObservationCount) {
				reason = InsufficientLowCount
			} else {
				reason = InsufficientLowWeight
			}
		}
		return makeInsufficient(merged, p50DB, hasP50, reason, capWouldBlock)
	}
	if reason == InsufficientNone {
		reason = InsufficientNoSample
	}
	return makeInsufficient(merged, p50DB, hasP50, reason, capWouldBlock)
}

func countMeetsMinimum(count uint32, min int) bool {
	return min <= 0 || uint64(count) >= uint64(min)
}

func meanDBForSample(sample Sample) (float64, bool) {
	if sample.Value <= 0 {
		return 0, false
	}
	return powerToDB(sample.Value), true
}

func (p *Predictor) capWouldBlock(sample Sample, minObservationCount int) bool {
	if p == nil || p.cfg.ReceiverContributionMode != ReceiverContributionShadow || !sample.CapLimited {
		return false
	}
	if !countMeetsMinimum(sample.CappedCount, minObservationCount) {
		return true
	}
	return sample.CappedWeight < p.cfg.MinEffectiveWeight
}

func mergeSamples(receive Sample, transmit Sample, cfg Config, noisePenalty float64) (Sample, bool) {
	hasReceive := sampleHasEvidence(receive)
	hasTransmit := sampleHasEvidence(transmit)
	if !hasReceive && !hasTransmit {
		return Sample{}, false
	}
	receiveActive := receive.Weight > 0
	transmitActive := transmit.Weight > 0
	receivePower := receive.Value
	if receiveActive && noisePenalty > 0 {
		receivePower = ApplyNoisePower(receivePower, noisePenalty, cfg)
	}
	if receiveActive && transmitActive {
		mergedPower := cfg.MergeReceiveWeight*receivePower + cfg.MergeTransmitWeight*transmit.Value
		mergedWeight := cfg.MergeReceiveWeight*receive.Weight + cfg.MergeTransmitWeight*transmit.Weight
		return Sample{
			Value:        mergedPower,
			Weight:       mergedWeight,
			AgeSec:       weightedSampleAge(receive, transmit),
			Count:        saturatingAddCounts(receive.Count, transmit.Count),
			RawCount:     saturatingAddCounts(sampleRawCount(receive), sampleRawCount(transmit)),
			RawWeight:    cfg.MergeReceiveWeight*sampleRawWeight(receive) + cfg.MergeTransmitWeight*sampleRawWeight(transmit),
			CappedCount:  saturatingAddCounts(sampleCappedCount(receive), sampleCappedCount(transmit)),
			CappedWeight: cfg.MergeReceiveWeight*sampleCappedWeight(receive) + cfg.MergeTransmitWeight*sampleCappedWeight(transmit),
			CapLimited:   receive.CapLimited || transmit.CapLimited,
		}, true
	}
	if receiveActive {
		return singleDirectionMerge(receive, transmit, receivePower, cfg), true
	}
	if transmitActive {
		return singleDirectionMerge(transmit, receive, transmit.Value, cfg), true
	}
	return Sample{
		AgeSec:       maxSampleAge(receive, transmit),
		Count:        saturatingAddCounts(receive.Count, transmit.Count),
		RawCount:     saturatingAddCounts(sampleRawCount(receive), sampleRawCount(transmit)),
		RawWeight:    sampleRawWeight(receive) + sampleRawWeight(transmit),
		CappedCount:  saturatingAddCounts(sampleCappedCount(receive), sampleCappedCount(transmit)),
		CappedWeight: sampleCappedWeight(receive) + sampleCappedWeight(transmit),
		CapLimited:   receive.CapLimited || transmit.CapLimited,
	}, true
}

func mergeSamplesWithDistribution(receive sampleWithBins, transmit sampleWithBins, cfg Config, noisePenalty float64) (sampleWithBins, bool) {
	merged, ok := mergeSamples(receive.Sample, transmit.Sample, cfg, noisePenalty)
	if !ok {
		return sampleWithBins{}, false
	}
	receiveActive := receive.Weight > 0
	transmitActive := transmit.Weight > 0
	var bins snrHistogram
	if receiveActive && transmitActive {
		receiveBins := receive.snrBins
		if noisePenalty > 0 {
			receiveBins = receiveBins.shifted(-noisePenalty)
		}
		bins.addScaled(receiveBins, cfg.MergeReceiveWeight)
		bins.addScaled(transmit.snrBins, cfg.MergeTransmitWeight)
	} else if receiveActive {
		receiveBins := receive.snrBins
		if noisePenalty > 0 {
			receiveBins = receiveBins.shifted(-noisePenalty)
		}
		bins.addScaled(receiveBins, cfg.ReverseHintDiscount)
	} else if transmitActive {
		bins.addScaled(transmit.snrBins, cfg.ReverseHintDiscount)
	}
	p50DB, hasP50 := bins.p50DB()
	return sampleWithBins{
		Sample:  merged,
		P50DB:   p50DB,
		HasP50:  hasP50,
		snrBins: bins,
	}, true
}

func singleDirectionMerge(active Sample, other Sample, value float64, cfg Config) Sample {
	rawWeight := sampleRawWeight(active)
	cappedWeight := sampleCappedWeight(active)
	active.Value = value
	active.Weight *= cfg.ReverseHintDiscount
	active.RawWeight = rawWeight * cfg.ReverseHintDiscount
	active.CappedWeight = cappedWeight * cfg.ReverseHintDiscount
	active.RawCount = saturatingAddCounts(sampleRawCount(active), sampleRawCount(other))
	active.CappedCount = saturatingAddCounts(sampleCappedCount(active), sampleCappedCount(other))
	active.CapLimited = active.CapLimited || other.CapLimited
	return active
}

func maxSampleAge(left Sample, right Sample) int64 {
	if left.AgeSec >= right.AgeSec {
		return left.AgeSec
	}
	return right.AgeSec
}

func sampleHasEvidence(sample Sample) bool {
	return sample.Weight > 0 || sample.RawWeight > 0 || sample.RawCount > 0 || sample.CappedWeight > 0 || sample.CappedCount > 0
}

func sampleRawCount(sample Sample) uint32 {
	if sample.RawCount > 0 {
		return sample.RawCount
	}
	return sample.Count
}

func sampleCappedCount(sample Sample) uint32 {
	if sample.CappedCount > 0 || sample.CapLimited {
		return sample.CappedCount
	}
	return sample.Count
}

func sampleRawWeight(sample Sample) float64 {
	if sample.RawWeight > 0 {
		return sample.RawWeight
	}
	return sample.Weight
}

func sampleCappedWeight(sample Sample) float64 {
	if sample.CappedWeight > 0 || sample.CapLimited {
		return sample.CappedWeight
	}
	return sample.Weight
}

func (p *Predictor) mergeFromStore(store *Store, userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, noisePenalty float64, now time.Time, includeDistribution bool) (Sample, float64, bool, InsufficientReason, bool) {
	if store == nil {
		return Sample{}, 0, false, InsufficientNoSample, false
	}
	if includeDistribution {
		merged, reason, ok := p.mergeFromStoreWithDistribution(store, userCell, dxCell, userCoarse, dxCoarse, band, noisePenalty, now)
		return merged.Sample, merged.P50DB, merged.HasP50, reason, ok
	}
	// Receive (DX->user): receiver=user, sender=dx.
	rFine, rCoarse := store.Lookup(userCell, dxCell, userCoarse, dxCoarse, band, now)
	receive := SelectSample(rFine, rCoarse, p.cfg.MinFineWeight, p.cfg.FineOnlyWeight)

	// Transmit (user->DX): receiver=dx, sender=user.
	tFine, tCoarse := store.Lookup(dxCell, userCell, dxCoarse, userCoarse, band, now)
	transmit := SelectSample(tFine, tCoarse, p.cfg.MinFineWeight, p.cfg.FineOnlyWeight)

	receive, transmit, staleDropped, staleCount := p.applyFreshnessGate(store, band, receive, transmit)
	merged, ok := mergeSamples(receive, transmit, p.cfg, noisePenalty)
	reason := InsufficientNone
	if staleDropped {
		reason = InsufficientStale
	}
	if !ok {
		if reason == InsufficientNone {
			reason = InsufficientNoSample
		}
		return Sample{Count: staleCount, RawCount: staleCount, CappedCount: staleCount}, 0, false, reason, false
	}
	return merged, 0, false, reason, true
}

func (p *Predictor) mergeFromStoreWithDistribution(store *Store, userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, noisePenalty float64, now time.Time) (sampleWithBins, InsufficientReason, bool) {
	if store == nil {
		return sampleWithBins{}, InsufficientNoSample, false
	}
	rFine, rCoarse := store.lookupWithDistribution(userCell, dxCell, userCoarse, dxCoarse, band, now)
	receive := selectSampleWithDistribution(rFine, rCoarse, p.cfg.MinFineWeight, p.cfg.FineOnlyWeight)

	tFine, tCoarse := store.lookupWithDistribution(dxCell, userCell, dxCoarse, userCoarse, band, now)
	transmit := selectSampleWithDistribution(tFine, tCoarse, p.cfg.MinFineWeight, p.cfg.FineOnlyWeight)

	receiveSample, transmitSample, staleDropped, staleCount := p.applyFreshnessGate(store, band, receive.Sample, transmit.Sample)
	if !sampleHasEvidence(receiveSample) {
		receive = sampleWithBins{}
	} else {
		receive.Sample = receiveSample
	}
	if !sampleHasEvidence(transmitSample) {
		transmit = sampleWithBins{}
	} else {
		transmit.Sample = transmitSample
	}
	merged, ok := mergeSamplesWithDistribution(receive, transmit, p.cfg, noisePenalty)
	reason := InsufficientNone
	if staleDropped {
		reason = InsufficientStale
	}
	if !ok {
		if reason == InsufficientNone {
			reason = InsufficientNoSample
		}
		return sampleWithBins{Sample: Sample{Count: staleCount, RawCount: staleCount, CappedCount: staleCount}}, reason, false
	}
	return merged, reason, true
}

func (p *Predictor) applyFreshnessGate(store *Store, band string, receive Sample, transmit Sample) (Sample, Sample, bool, uint32) {
	maxAge := p.maxPredictionAgeSeconds(store, band)
	if maxAge <= 0 {
		return receive, transmit, false, 0
	}
	staleDropped := false
	var staleCount uint32
	if sampleHasEvidence(receive) && receive.AgeSec > maxAge {
		staleCount = saturatingAddCounts(staleCount, receive.Count)
		receive = Sample{}
		staleDropped = true
	}
	if sampleHasEvidence(transmit) && transmit.AgeSec > maxAge {
		staleCount = saturatingAddCounts(staleCount, transmit.Count)
		transmit = Sample{}
		staleDropped = true
	}
	return receive, transmit, staleDropped, staleCount
}

func (p *Predictor) maxPredictionAgeSeconds(store *Store, band string) int64 {
	if p == nil || store == nil || p.cfg.MaxPredictionAgeHalfLifeMultiplier <= 0 {
		return 0
	}
	halfLife := store.bandIndex.HalfLifeSeconds(band, p.cfg)
	if halfLife <= 0 {
		return 0
	}
	seconds := math.Ceil(float64(halfLife) * p.cfg.MaxPredictionAgeHalfLifeMultiplier)
	if seconds < 1 {
		return 1
	}
	return int64(seconds)
}

// PurgeStale runs a stale purge and returns removed count.
func (p *Predictor) PurgeStale(now time.Time) int {
	if p == nil {
		return 0
	}
	removed := 0
	if p.combined != nil {
		removed += p.combined.PurgeStale(now)
	}
	return removed
}

// Compact rebuilds shard maps that have shrunk far below peak size.
func (p *Predictor) Compact(minPeak int, shrinkRatio float64) int {
	if p == nil || !p.cfg.Enabled || p.combined == nil {
		return 0
	}
	return p.combined.Compact(minPeak, shrinkRatio)
}

// TotalBuckets returns the total buckets across all stores.
func (p *Predictor) TotalBuckets() int {
	if p == nil || !p.cfg.Enabled || p.combined == nil {
		return 0
	}
	return p.combined.TotalBuckets()
}

// RefreshStatsSnapshot recomputes the cached bucket-count snapshot used by
// operator-facing stats displays.
func (p *Predictor) RefreshStatsSnapshot(now time.Time) {
	if p == nil || !p.cfg.Enabled || p.combined == nil {
		return
	}
	p.combined.RefreshStatsSnapshot(now)
}

// PredictorStats returns counts of active fine/coarse buckets (non-stale).
type PredictorStats struct {
	CombinedFine   int
	CombinedCoarse int
}

// BandBucketStats reports fine/coarse bucket counts per band.
type BandBucketStats struct {
	Band   string
	Fine   int
	Coarse int
}

// BandWeightHistogram reports per-band bucket weight distributions.
type BandWeightHistogram struct {
	Band  string
	Total int
	Bins  []int
}

// WeightHistogram carries histogram edges and per-band counts.
type WeightHistogram struct {
	Edges []float64
	Bands []BandWeightHistogram
}

// weightHistogramEdges defines decayed weight bins for log-only histograms.
var weightHistogramEdges = []float64{1, 2, 3, 5, 10}

// Stats returns counts of active fine/coarse buckets for the combined store.
func (p *Predictor) Stats(now time.Time) PredictorStats {
	if p == nil || !p.cfg.Enabled {
		return PredictorStats{}
	}
	stats := PredictorStats{}
	if p.combined != nil {
		stats.CombinedFine, stats.CombinedCoarse = p.combined.Stats(now)
	}
	return stats
}

// StatsByBand returns per-band bucket counts for the combined store.
func (p *Predictor) StatsByBand(now time.Time) []BandBucketStats {
	if p == nil || !p.cfg.Enabled {
		return nil
	}
	if p.combined == nil {
		return nil
	}
	bands := p.combined.bandIndex.Bands()
	if len(bands) == 0 {
		return nil
	}
	counts := p.combined.StatsByBand(now)
	stats := make([]BandBucketStats, len(bands))
	for i, band := range bands {
		entry := BandBucketStats{Band: band}
		if i < len(counts) {
			entry.Fine = counts[i].fine
			entry.Coarse = counts[i].coarse
		}
		stats[i] = entry
	}
	return stats
}

// WeightHistogramByBand returns per-band bucket weight histograms for the combined store.
func (p *Predictor) WeightHistogramByBand(now time.Time) WeightHistogram {
	if p == nil || !p.cfg.Enabled || p.combined == nil {
		return WeightHistogram{}
	}
	bands := p.combined.bandIndex.Bands()
	if len(bands) == 0 {
		return WeightHistogram{}
	}
	counts := p.combined.WeightHistogramByBand(now, weightHistogramEdges)
	if len(counts) == 0 {
		return WeightHistogram{}
	}
	entries := make([]BandWeightHistogram, len(bands))
	for i, band := range bands {
		entry := BandWeightHistogram{Band: band}
		if i < len(counts) {
			entry.Total = counts[i].total
			entry.Bins = append([]int(nil), counts[i].bins...)
		}
		entries[i] = entry
	}
	return WeightHistogram{
		Edges: append([]float64(nil), weightHistogramEdges...),
		Bands: entries,
	}
}
