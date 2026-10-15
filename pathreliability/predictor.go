// File role: Owns path prediction orchestration and final glyph diagnostics.
// Crawler notes: Start here for PATH class calculation, insufficient reasons,
// receive/transmit merge policy, receiver-cap enforcement semantics, and
// the observation-floor contract used by telnet display and filters.
// Related docs: pathreliability/README.md, README.md, data/config/PATH_PREDICTIONS.md.
// Related tests: pathreliability/*_test.go, telnet/*path*_test.go.
package pathreliability

import "time"

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
func (p *Predictor) Update(bucket BucketClass, receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, ft8dB float64, weight float64, times ObservationTimes, isBeacon bool) ReportAdmission {
	if p == nil {
		return ReportAdmission{}
	}
	return p.combined.updateReport(receiverCell, senderCell, receiverCoarse, senderCoarse, band, ft8dB, weight, &times, 0, isBeacon, bucket)
}

// UpdateWithReceiverHash ingests a spot contribution and attributes capped
// trust to the normalized receiving station identity hash.
func (p *Predictor) UpdateWithReceiverHash(bucket BucketClass, receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, ft8dB float64, weight float64, times ObservationTimes, isBeacon bool, receiverHash uint64) ReportAdmission {
	if p == nil {
		return ReportAdmission{}
	}
	return p.combined.updateReport(receiverCell, senderCell, receiverCoarse, senderCoarse, band, ft8dB, weight, &times, receiverHash, isBeacon, bucket)
}

// AdmissionStats reports input outcomes once per report, before fine/coarse
// fanout. Counters are bounded in cardinality and never retain report identities.
func (p *Predictor) AdmissionStats() AdmissionStats {
	if p == nil || p.combined == nil {
		return AdmissionStats{}
	}
	return p.combined.admissions.snapshot()
}

// PredictionSource describes which bucket family produced the glyph.
type PredictionSource uint8

const (
	SourceInsufficient PredictionSource = iota
	SourceCombined
	SourceVOACAPClosed
	SourceVOACAPAligned
	SourceVOACAPSparseUpgrade
	SourceVOACAPOpen
	SourceNative160
)

// InsufficientReason describes why a prediction returned the insufficient glyph.
type InsufficientReason uint8

const (
	InsufficientNone InsufficientReason = iota
	InsufficientNoSample
	InsufficientLowCount
	InsufficientLowReceiver
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
	case InsufficientLowReceiver:
		return "low_receiver"
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
	Glyph    string
	Class    string
	P50DB    float64
	HasP50   bool
	P50Glyph string
	// Observed provenance is independent of fallback forecasts and display time.
	ObservedSamplingUnix          int64
	HasObservedSamplingTime       bool
	ObservedDistributionAvailable bool
	ObservedRecentSupport         ReportSupport
	Weight                        float64
	AgeSec                        int64
	Count                         uint32
	ObservationCount              uint32
	RawCount                      uint32
	RawWeight                     float64
	CappedCount                   uint32
	CappedWeight                  float64
	ReceiverCount                 uint32
	ReceiverRequired              uint32
	CapLimited                    bool
	CapWouldBlock                 bool
	Source                        PredictionSource
	BeaconRX                      bool
	InsufficientReason            InsufficientReason
	VOACAPFT8SNRDB                int
	VOACAPSSN                     int
	VOACAPAgeSec                  int64
	VOACAPHourUTC                 int
	VOACAPFrequencyMHz            float64
	VOACAPReqSNRReliability       float64
	VOACAPHasReqSNRReliability    bool
	Native160Checked              bool
	Native160Emitted              bool
	Native160DisplayDisabled      bool
	Native160Unknown              bool
	Native160UserDaylight         bool
	Native160DXDaylight           bool
	Native160UserTwilight         bool
	Native160DXTwilight           bool
	Native160DaylightFraction     float64
	Native160CivilDarkFraction    float64
	Native160CivilTwilightDeg     float64
	Native160ClosedMaxDarkFrac    float64
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
// observation floor. Off/shadow use recent usable reports; enforce uses recent
// cap-admitted reports. Neither signal weights nor display discounts scale this
// fractional floor. Callers may pass a floor above the configured default when
// applying stricter per-session display or filter policy.
func (p *Predictor) PredictWithMinObservationCount(userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, mode string, noisePenalty float64, minObservationCount int, now time.Time) Result {
	receiverMinObservationCount := 0
	if p != nil {
		receiverMinObservationCount = p.cfg.MinObservationCount
	}
	return p.predictWithSelectedEvidence(userCell, dxCell, userCoarse, dxCoarse, band, mode, noisePenalty, minObservationCount, receiverMinObservationCount, SourceCombined, false, now, p.mergeFromStoreWithDistribution)
}

// PredictBeaconReceiveOnlyWithMinObservationCount returns a beacon path glyph
// from the DX-to-user receive leg only. The supplied floor can raise recent
// report support, while receiver diversity remains derived from the
// beacon configured floor.
func (p *Predictor) PredictBeaconReceiveOnlyWithMinObservationCount(userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, mode string, noisePenalty float64, minObservationCount int, now time.Time) Result {
	if p == nil {
		return Result{Glyph: "?", Source: SourceInsufficient, BeaconRX: true}
	}
	return p.predictWithSelectedEvidence(userCell, dxCell, userCoarse, dxCoarse, band, mode, noisePenalty, minObservationCount, p.cfg.BeaconMinObservationCount, SourceCombined, true, now, p.receiveFromStoreWithDistribution)
}

type evidenceSelector func(store *Store, userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, noisePenalty float64, now time.Time) (sampleWithBins, InsufficientReason, bool)

func (p *Predictor) predictWithSelectedEvidence(userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, mode string, noisePenalty float64, minObservationCount int, receiverMinObservationCount int, source PredictionSource, beaconRX bool, now time.Time, selectEvidence evidenceSelector) Result {
	insufficient := "?"
	if p != nil && p.cfg.GlyphSymbols.Insufficient != "" {
		insufficient = p.cfg.GlyphSymbols.Insufficient
	}
	if p == nil || !p.cfg.Enabled {
		return Result{Glyph: insufficient, Source: SourceInsufficient, BeaconRX: beaconRX}
	}

	modeKey := normalizeMode(mode)
	makeResult := func(sample Sample, p50DB float64, hasP50 bool, source PredictionSource, capWouldBlock bool) Result {
		class := classUnlikely
		glyph := p.cfg.GlyphSymbols.Unlikely
		if hasP50 {
			class = ClassForDB(p50DB, modeKey, p.cfg)
			glyph = glyphForClass(class, p.cfg)
		}
		return Result{
			Glyph:                         glyph,
			Class:                         class,
			P50DB:                         p50DB,
			HasP50:                        hasP50,
			P50Glyph:                      glyph,
			ObservedSamplingUnix:          sample.SamplingUnix,
			HasObservedSamplingTime:       sample.HasSamplingTime,
			ObservedDistributionAvailable: hasP50,
			ObservedRecentSupport:         sample.RecentSupport,
			Weight:                        sample.Weight,
			AgeSec:                        sample.AgeSec,
			Count:                         sample.Count,
			ObservationCount:              sampleObservationCount(sample),
			RawCount:                      sample.RawCount,
			RawWeight:                     sample.RawWeight,
			CappedCount:                   sample.CappedCount,
			CappedWeight:                  sample.CappedWeight,
			ReceiverCount:                 sample.CappedReceiverCount,
			ReceiverRequired:              p.requiredReceiverCount(sample, receiverMinObservationCount),
			CapLimited:                    sample.CapLimited,
			CapWouldBlock:                 capWouldBlock,
			Source:                        source,
			BeaconRX:                      beaconRX,
		}
	}
	makeInsufficient := func(sample Sample, p50DB float64, hasP50 bool, reason InsufficientReason, capWouldBlock bool) Result {
		p50Glyph := ""
		if hasP50 {
			p50Glyph = GlyphForDB(p50DB, modeKey, p.cfg)
		}
		return Result{
			Glyph:                         insufficient,
			Class:                         "",
			P50DB:                         p50DB,
			HasP50:                        hasP50,
			P50Glyph:                      p50Glyph,
			ObservedSamplingUnix:          sample.SamplingUnix,
			HasObservedSamplingTime:       sample.HasSamplingTime,
			ObservedDistributionAvailable: hasP50,
			ObservedRecentSupport:         sample.RecentSupport,
			Weight:                        sample.Weight,
			AgeSec:                        sample.AgeSec,
			Count:                         sample.Count,
			ObservationCount:              sampleObservationCount(sample),
			RawCount:                      sample.RawCount,
			RawWeight:                     sample.RawWeight,
			CappedCount:                   sample.CappedCount,
			CappedWeight:                  sample.CappedWeight,
			ReceiverCount:                 sample.CappedReceiverCount,
			ReceiverRequired:              p.requiredReceiverCount(sample, receiverMinObservationCount),
			CapLimited:                    sample.CapLimited,
			CapWouldBlock:                 capWouldBlock,
			Source:                        SourceInsufficient,
			BeaconRX:                      beaconRX,
			InsufficientReason:            reason,
		}
	}

	merged, reason, ok := selectEvidence(p.combined, userCell, dxCell, userCoarse, dxCoarse, band, noisePenalty, now)
	if minObservationCount < receiverMinObservationCount {
		minObservationCount = receiverMinObservationCount
	}
	// Shadow describes gates on a selected population, not absent/all-stale
	// diagnostic counts. It does not simulate an alternative capped median.
	capWouldBlock := ok && p.capWouldBlock(merged.Sample, minObservationCount, receiverMinObservationCount)
	countOK := merged.RecentSupport.meetsMinimum(minObservationCount)
	receiverOK := p.receiverGateMeetsMinimum(merged.Sample, receiverMinObservationCount)
	if ok && merged.Weight >= p.cfg.MinEffectiveWeight && countOK && receiverOK && merged.HasP50 {
		return makeResult(merged.Sample, merged.P50DB, merged.HasP50, source, capWouldBlock)
	}
	if ok {
		if reason == InsufficientNone {
			if !countOK {
				reason = InsufficientLowCount
			} else if !receiverOK {
				reason = InsufficientLowReceiver
			} else if merged.Weight < p.cfg.MinEffectiveWeight {
				reason = InsufficientLowWeight
			} else {
				reason = InsufficientNoSample
			}
		}
		return makeInsufficient(merged.Sample, merged.P50DB, merged.HasP50, reason, capWouldBlock)
	}
	if reason == InsufficientNone {
		reason = InsufficientNoSample
	}
	return makeInsufficient(merged.Sample, merged.P50DB, merged.HasP50, reason, capWouldBlock)
}

func (p *Predictor) receiverGateMeetsMinimum(sample Sample, minObservationCount int) bool {
	if p == nil || p.cfg.ReceiverContributionMode != ReceiverContributionEnforce {
		return true
	}
	required := p.requiredReceiverCount(sample, minObservationCount)
	return required == 0 || sample.CappedReceiverCount >= required
}

func (p *Predictor) requiredReceiverCount(sample Sample, minObservationCount int) uint32 {
	if p == nil {
		return 0
	}
	return requiredReceiverCount(minObservationCount, p.cfg.ReceiverMaxEffectiveCount, sample.CappedReceiverCapacity)
}

func requiredReceiverCount(minObservationCount int, maxEffectiveCount uint32, capacity uint32) uint32 {
	if minObservationCount <= 0 || maxEffectiveCount == 0 || capacity == 0 {
		return 0
	}
	required := (uint64(minObservationCount) + uint64(maxEffectiveCount) - 1) / uint64(maxEffectiveCount)
	if required > uint64(capacity) {
		return capacity
	}
	return uint32(required)
}

func (p *Predictor) capWouldBlock(sample Sample, minObservationCount, receiverMinObservationCount int) bool {
	if p == nil || p.cfg.ReceiverContributionMode != ReceiverContributionShadow {
		return false
	}
	if !sample.RecentSupport.AdmittedAvailable || !reportUnitsMeetMinimum(sample.RecentSupport.Admitted, minObservationCount) {
		return true
	}
	required := p.requiredReceiverCount(sample, receiverMinObservationCount)
	if required > 0 && sample.CappedReceiverCount < required {
		return true
	}
	return sample.CappedWeight < p.cfg.MinEffectiveWeight
}

func mergeSamples(receive Sample, transmit Sample, cfg Config) (Sample, bool) {
	support := mergeReportSupport(receive.RecentSupport, transmit.RecentSupport, cfg.ReceiverContributionMode)
	hasReceive := sampleHasEvidence(receive)
	hasTransmit := sampleHasEvidence(transmit)
	if !hasReceive && !hasTransmit {
		return sampleWithSamplingTime(Sample{RecentSupport: support}, receive, transmit), false
	}
	receiveActive := receive.Weight > 0
	transmitActive := transmit.Weight > 0
	if receiveActive && transmitActive {
		mergedWeight := cfg.MergeReceiveWeight*receive.Weight + cfg.MergeTransmitWeight*transmit.Weight
		return sampleWithSamplingTime(Sample{
			RecentSupport:          support,
			Weight:                 mergedWeight,
			AgeSec:                 weightedSampleAge(receive, transmit),
			Count:                  saturatingAddCounts(receive.Count, transmit.Count),
			ObservationCount:       saturatingAddCounts(sampleObservationCount(receive), sampleObservationCount(transmit)),
			RawCount:               saturatingAddCounts(sampleRawCount(receive), sampleRawCount(transmit)),
			RawWeight:              cfg.MergeReceiveWeight*sampleRawWeight(receive) + cfg.MergeTransmitWeight*sampleRawWeight(transmit),
			CappedCount:            saturatingAddCounts(sampleCappedCount(receive), sampleCappedCount(transmit)),
			CappedWeight:           cfg.MergeReceiveWeight*sampleCappedWeight(receive) + cfg.MergeTransmitWeight*sampleCappedWeight(transmit),
			CappedReceiverCount:    saturatingAddCounts(receive.CappedReceiverCount, transmit.CappedReceiverCount),
			CappedReceiverCapacity: saturatingAddCounts(receive.CappedReceiverCapacity, transmit.CappedReceiverCapacity),
			CapLimited:             receive.CapLimited || transmit.CapLimited,
		}, receive, transmit), true
	}
	if receiveActive {
		return singleDirectionMerge(receive, transmit, cfg), true
	}
	if transmitActive {
		return singleDirectionMerge(transmit, receive, cfg), true
	}
	return sampleWithSamplingTime(Sample{
		RecentSupport:          support,
		AgeSec:                 maxSampleAge(receive, transmit),
		Count:                  saturatingAddCounts(receive.Count, transmit.Count),
		ObservationCount:       saturatingAddCounts(sampleObservationCount(receive), sampleObservationCount(transmit)),
		RawCount:               saturatingAddCounts(sampleRawCount(receive), sampleRawCount(transmit)),
		RawWeight:              sampleRawWeight(receive) + sampleRawWeight(transmit),
		CappedCount:            saturatingAddCounts(sampleCappedCount(receive), sampleCappedCount(transmit)),
		CappedWeight:           sampleCappedWeight(receive) + sampleCappedWeight(transmit),
		CappedReceiverCount:    saturatingAddCounts(receive.CappedReceiverCount, transmit.CappedReceiverCount),
		CappedReceiverCapacity: saturatingAddCounts(receive.CappedReceiverCapacity, transmit.CappedReceiverCapacity),
		CapLimited:             receive.CapLimited || transmit.CapLimited,
	}, receive, transmit), true
}

func mergeSamplesWithDistribution(receive sampleWithBins, transmit sampleWithBins, cfg Config, noisePenalty float64) (sampleWithBins, bool) {
	merged, ok := mergeSamples(receive.Sample, transmit.Sample, cfg)
	if !ok {
		if receive.distributionUnavailable || transmit.distributionUnavailable {
			return observedSampleDistribution(merged, snrHistogram{}, true), true
		}
		return sampleWithBins{Sample: merged}, false
	}
	if receive.distributionUnavailable || transmit.distributionUnavailable {
		return observedSampleDistribution(merged, snrHistogram{}, true), true
	}
	receiveActive := receive.Weight > 0
	transmitActive := transmit.Weight > 0
	var bins snrHistogram
	unavailable := false
	if receiveActive && transmitActive {
		receiveBins := receive.snrBins
		unavailable = receive.distributionUnavailable || transmit.distributionUnavailable
		if noisePenalty > 0 {
			var shifted bool
			receiveBins, shifted = receiveBins.shifted(-noisePenalty)
			unavailable = unavailable || !shifted
		}
		if !unavailable {
			unavailable = !bins.addScaled(&receiveBins, cfg.MergeReceiveWeight) || !bins.addScaled(&transmit.snrBins, cfg.MergeTransmitWeight)
		}
	} else if receiveActive {
		receiveBins := receive.snrBins
		unavailable = receive.distributionUnavailable
		if noisePenalty > 0 {
			var shifted bool
			receiveBins, shifted = receiveBins.shifted(-noisePenalty)
			unavailable = unavailable || !shifted
		}
		if !unavailable {
			unavailable = !bins.addScaled(&receiveBins, cfg.ReverseHintDiscount)
		}
	} else if transmitActive {
		unavailable = transmit.distributionUnavailable
		if !unavailable {
			unavailable = !bins.addScaled(&transmit.snrBins, cfg.ReverseHintDiscount)
		}
	}
	return observedSampleDistribution(merged, bins, unavailable), true
}

func singleDirectionMerge(active Sample, other Sample, cfg Config) Sample {
	active.RecentSupport = mergeReportSupport(active.RecentSupport, other.RecentSupport, cfg.ReceiverContributionMode)
	rawWeight := sampleRawWeight(active)
	cappedWeight := sampleCappedWeight(active)
	active.Weight *= cfg.ReverseHintDiscount
	active.RawWeight = rawWeight * cfg.ReverseHintDiscount
	active.CappedWeight = cappedWeight * cfg.ReverseHintDiscount
	active.ObservationCount = sampleObservationCount(active)
	active.RawCount = saturatingAddCounts(sampleRawCount(active), sampleRawCount(other))
	active.CappedCount = saturatingAddCounts(sampleCappedCount(active), sampleCappedCount(other))
	active.CapLimited = active.CapLimited || other.CapLimited
	return sampleWithSamplingTime(active, active, other)
}

func maxSampleAge(left Sample, right Sample) int64 {
	if left.AgeSec >= right.AgeSec {
		if left.AgeSec < 0 {
			return 0
		}
		return left.AgeSec
	}
	if right.AgeSec < 0 {
		return 0
	}
	return right.AgeSec
}

func sampleHasEvidence(sample Sample) bool {
	return sample.Weight != 0 || sample.RawWeight != 0 || sample.RawCount > 0 || sample.ObservationCount > 0 || sample.CappedWeight != 0 || sample.CappedCount > 0 || sample.Count > 0
}

func sampleObservationCount(sample Sample) uint32 {
	if sample.ObservationCount > 0 {
		return sample.ObservationCount
	}
	if sample.RawCount > 0 {
		return sample.RawCount
	}
	return sample.Count
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

func (p *Predictor) mergeFromStoreWithDistribution(store *Store, userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, noisePenalty float64, now time.Time) (sampleWithBins, InsufficientReason, bool) {
	return p.selectedFromStore(store, userCell, dxCell, userCoarse, dxCoarse, band, noisePenalty, now, false)
}

func (p *Predictor) receiveFromStoreWithDistribution(store *Store, userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, noisePenalty float64, now time.Time) (sampleWithBins, InsufficientReason, bool) {
	return p.selectedFromStore(store, userCell, dxCell, userCoarse, dxCoarse, band, noisePenalty, now, true)
}

func (p *Predictor) selectedFromStore(store *Store, userCell, dxCell CellID, userCoarse, dxCoarse CellID, band string, noisePenalty float64, now time.Time, beacon bool) (sampleWithBins, InsufficientReason, bool) {
	if store == nil {
		return sampleWithBins{}, InsufficientNoSample, false
	}
	var query candidateQuery
	store.lookupQueryInto(userCell, dxCell, userCoarse, dxCoarse, band, now, !beacon, sampleDistribution, &query)
	selected := query.selectPopulation(p.cfg, p.maxPredictionAgeSeconds(store, band), beacon)
	receive, transmit := query.sample(selected.receive), query.sample(selected.transmit)
	reason := InsufficientNone
	if selected.staleDropped {
		reason = InsufficientStale
	}
	var merged sampleWithBins
	var ok bool
	if beacon {
		merged = receive
		ok = sampleHasEvidence(receive.Sample) || receive.distributionUnavailable
		if ok {
			bins, unavailable := receive.snrBins, receive.distributionUnavailable
			if noisePenalty > 0 {
				var shifted bool
				bins, shifted = bins.shifted(-noisePenalty)
				unavailable = unavailable || !shifted
			}
			merged = observedSampleDistribution(receive.Sample, bins, unavailable)
		}
	} else {
		merged, ok = mergeSamplesWithDistribution(receive, transmit, p.cfg, noisePenalty)
	}
	merged.CappedReceiverCount, merged.CappedReceiverCapacity = query.receiverUnion(selected.receive, selected.transmit)
	if !ok {
		if reason == InsufficientNone {
			reason = InsufficientNoSample
		}
		merged.Count = selected.staleCounts.active
		merged.RawCount = selected.staleCounts.raw
		merged.ObservationCount = selected.staleCounts.observations
		merged.CappedCount = selected.staleCounts.capped
		merged.RecentSupport = selected.staleCounts.recent
	}
	// Mode provenance is known even for explicit zero/absent evidence. It never
	// follows positive quantities or legacy integer fallback rules.
	merged.RecentSupport.AdmittedAvailable = p.cfg.ReceiverContributionMode != ReceiverContributionOff
	merged.RecentSupport.Enforced = p.cfg.ReceiverContributionMode == ReceiverContributionEnforce
	return merged, reason, ok
}

// staleSampleCounts preserves separate diagnostics only when all selected
// evidence is dropped. Stale counts never enter a surviving sample's floor.
type staleSampleCounts struct {
	active, raw, observations, capped uint32
	recent                            ReportSupport
}

func (counts *staleSampleCounts) add(sample Sample) {
	counts.active = saturatingAddCounts(counts.active, sample.Count)
	counts.raw = saturatingAddCounts(counts.raw, sampleRawCount(sample))
	counts.observations = saturatingAddCounts(counts.observations, sampleObservationCount(sample))
	counts.capped = saturatingAddCounts(counts.capped, sampleCappedCount(sample))
	counts.recent.RawUsable += sample.RecentSupport.RawUsable
	counts.recent.Admitted += sample.RecentSupport.Admitted
}

func (p *Predictor) maxPredictionAgeSeconds(store *Store, band string) int64 {
	if p == nil || store == nil || p.cfg.MaxPredictionAgeHalfLifeMultiplier <= 0 {
		return 0
	}
	halfLife := store.bandIndex.HalfLifeSeconds(band, p.cfg)
	if halfLife <= 0 {
		return 0
	}
	seconds := ceilNonnegativeInt64(float64(halfLife) * p.cfg.MaxPredictionAgeHalfLifeMultiplier)
	if seconds < 1 {
		return 1
	}
	return seconds
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
	Available bool
	AsOfUnix  int64
	Edges     []float64
	Bands     []BandWeightHistogram
}

// PredictorStatsSnapshot carries one copied publication of totals and band
// counts. Unavailable scans may retain HasSnapshot=true with an explicitly
// dated earlier publication; consumers must not label it current.
type PredictorStatsSnapshot struct {
	Available   bool
	HasSnapshot bool
	AsOfUnix    int64
	Stats       PredictorStats
	Bands       []BandBucketStats
}

// weightHistogramEdges defines decayed weight bins for log-only histograms.
var weightHistogramEdges = []float64{1, 2, 3, 5, 10}

// Stats returns counts of active fine/coarse buckets for the combined store.
func (p *Predictor) Stats(_ time.Time) PredictorStats {
	return p.StatsSnapshot().Stats
}

// StatsByBand returns per-band bucket counts for the combined store.
func (p *Predictor) StatsByBand(_ time.Time) []BandBucketStats {
	return p.StatsSnapshot().Bands
}

// StatsSnapshot exposes cached counts at one reference; it never initiates a
// store scan. Returned band counts are caller-owned and bounded by band count.
func (p *Predictor) StatsSnapshot() PredictorStatsSnapshot {
	if p == nil || !p.cfg.Enabled || p.combined == nil {
		return PredictorStatsSnapshot{}
	}
	bands := p.combined.bandIndex.Bands()
	snapshot := p.combined.statsSnapshot()
	stats := make([]BandBucketStats, len(bands))
	for i, band := range bands {
		entry := BandBucketStats{Band: band}
		if i < len(snapshot.byBand) {
			entry.Fine = snapshot.byBand[i].fine
			entry.Coarse = snapshot.byBand[i].coarse
		}
		stats[i] = entry
	}
	return PredictorStatsSnapshot{
		Available: snapshot.available, HasSnapshot: snapshot.hasSnapshot, AsOfUnix: snapshot.asOfUnix,
		Stats: PredictorStats{CombinedFine: snapshot.fine, CombinedCoarse: snapshot.coarse}, Bands: stats,
	}
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
	snapshot := p.combined.weightHistogramSnapshot(now, weightHistogramEdges)
	if !snapshot.available {
		return WeightHistogram{AsOfUnix: snapshot.asOfUnix}
	}
	entries := make([]BandWeightHistogram, len(bands))
	for i, band := range bands {
		entry := BandWeightHistogram{Band: band}
		if i < len(snapshot.byBand) {
			entry.Total = snapshot.byBand[i].total
			entry.Bins = append([]int(nil), snapshot.byBand[i].bins...)
		}
		entries[i] = entry
	}
	return WeightHistogram{
		Available: true,
		AsOfUnix:  snapshot.asOfUnix,
		Edges:     append([]float64(nil), weightHistogramEdges...),
		Bands:     entries,
	}
}
