// File role: Owns receiver-cap shadow candidate state and merge helpers for
// path reliability diagnostics.
// Crawler notes: Start here when comparing candidate
// receiver_shadow_max_effective_counts behavior in Path cap shadow logs.
// Candidate p50 histograms are optional and remain diagnostic-only; active
// p50/glyph scoring remains in predictor.go and active histograms remain in
// store.go.
// Related docs: pathreliability/README.md, docs/decisions/ADR-0132-path-receiver-cap-shadow-candidates.md, docs/decisions/ADR-0133-path-receiver-cap-p50-shadow-histograms.md.
// Related tests: pathreliability/receiver_test.go, pathreliability/store_bench_test.go.
package pathreliability

import "math"

// ReceiverCapShadowSampleCandidate carries selected evidence for one candidate
// receiver count cap. Gate diagnostics use Count/Weight; optional p50 shadow
// diagnostics use the private fixed-bin SNR histogram without changing active
// glyph behavior.
type ReceiverCapShadowSampleCandidate struct {
	MaxEffectiveCount      uint32
	Count                  uint32
	Weight                 float64
	CappedReceiverCount    uint32
	CappedReceiverCapacity uint32
	snrBins                snrHistogram
	hasSNRBins             bool
}

// ReceiverCapShadowSample carries fixed-cardinality candidate evidence through
// the same fine/coarse and receive/transmit selection path as the active sample.
type ReceiverCapShadowSample struct {
	Count      int
	Candidates [ReceiverShadowCapCandidateCount]ReceiverCapShadowSampleCandidate
}

// ReceiverCapShadowCandidate is the per-prediction operator-visible outcome for
// one candidate cap.
type ReceiverCapShadowCandidate struct {
	MaxEffectiveCount uint32
	Pass              bool
	LowCount          bool
	LowReceiver       bool
	LowWeight         bool
	Block             bool
	P50Pass           bool
	P50DB             float64
	HasP50            bool
	P50Class          string
	P50Glyph          string
}

// ReceiverCapShadowSummary is a bounded, allocation-free set of cap-shadow
// outcomes. Count is zero outside receiver shadow mode.
type ReceiverCapShadowSummary struct {
	Count      int
	Candidates [ReceiverShadowCapCandidateCount]ReceiverCapShadowCandidate
}

type receiverCapShadowBucketState struct {
	lanes [ReceiverShadowCapCandidateCount]receiverCapShadowLane
}

type receiverCapShadowLane struct {
	weight float64
	count  float64
	slots  [inlineReceiverSlots]receiverSlot
	// snrBins is allocated only when candidate p50 diagnostics are enabled for
	// a bucket that has accepted shadow evidence.
	snrBins *snrHistogram
	// extraSlots is allocated only for coarse buckets that use slots 7-12.
	extraSlots *[maxCoarseReceiverSlots - inlineReceiverSlots]receiverSlot
}

type receiverCapShadowRawSample struct {
	Count      int
	Candidates [ReceiverShadowCapCandidateCount]receiverCapShadowRawCandidate
}

type receiverCapShadowRawCandidate struct {
	MaxEffectiveCount      uint32
	Count                  float64
	Weight                 float64
	ReceiverSlotCounts     [maxCoarseReceiverSlots]float64
	ReceiverSlotWeights    [maxCoarseReceiverSlots]float64
	CappedReceiverCapacity uint32
	SNRBins                snrHistogram
	HasSNRBins             bool
}

func (s *receiverCapShadowBucketState) update(weight float64, nowSec int64, decay float64, receiverHash uint64, receiverSlots int, cfg Config, snrBin int) {
	if s == nil || cfg.ReceiverMaxEffectiveWeight <= 0 || len(cfg.ReceiverShadowMaxEffectiveCounts) != ReceiverShadowCapCandidateCount {
		return
	}
	for i := 0; i < ReceiverShadowCapCandidateCount; i++ {
		s.lanes[i].update(weight, nowSec, decay, receiverHash, receiverSlots, cfg.ReceiverShadowMaxEffectiveCounts[i], cfg.ReceiverMaxEffectiveWeight, cfg.ReceiverShadowP50Enabled, snrBin)
	}
}

func (s *receiverCapShadowBucketState) snapshot(cfg Config, receiverSlots int) receiverCapShadowRawSample {
	if s == nil || len(cfg.ReceiverShadowMaxEffectiveCounts) != ReceiverShadowCapCandidateCount {
		return receiverCapShadowRawSample{}
	}
	var out receiverCapShadowRawSample
	out.Count = ReceiverShadowCapCandidateCount
	for i := 0; i < ReceiverShadowCapCandidateCount; i++ {
		candidate := s.lanes[i].snapshotCandidate(cfg.ReceiverShadowMaxEffectiveCounts[i], receiverSlots)
		if cfg.ReceiverShadowP50Enabled && s.lanes[i].snrBins != nil {
			candidate.SNRBins = *s.lanes[i].snrBins
			candidate.HasSNRBins = true
		}
		out.Candidates[i] = candidate
	}
	return out
}

func (s receiverCapShadowRawSample) decayed(decay float64) ReceiverCapShadowSample {
	if s.Count <= 0 {
		return ReceiverCapShadowSample{}
	}
	if s.Count > ReceiverShadowCapCandidateCount {
		s.Count = ReceiverShadowCapCandidateCount
	}
	var out ReceiverCapShadowSample
	out.Count = s.Count
	for i := 0; i < s.Count; i++ {
		weight := s.Candidates[i].Weight * decay
		if weight <= 0 || math.IsNaN(weight) {
			weight = 0
		}
		candidate := ReceiverCapShadowSampleCandidate{
			MaxEffectiveCount:      s.Candidates[i].MaxEffectiveCount,
			Count:                  effectiveCountToUint32(s.Candidates[i].Count * decay),
			Weight:                 weight,
			CappedReceiverCount:    receiverCountFromSlotSnapshots(s.Candidates[i].ReceiverSlotCounts, s.Candidates[i].ReceiverSlotWeights, decay, s.Candidates[i].CappedReceiverCapacity),
			CappedReceiverCapacity: s.Candidates[i].CappedReceiverCapacity,
		}
		if s.Candidates[i].HasSNRBins {
			candidate.snrBins = s.Candidates[i].SNRBins
			candidate.snrBins.decay(decay)
			candidate.hasSNRBins = true
		}
		out.Candidates[i] = candidate
	}
	return out
}

func (l *receiverCapShadowLane) snapshotCandidate(maxEffectiveCount uint32, receiverSlots int) receiverCapShadowRawCandidate {
	candidate := receiverCapShadowRawCandidate{
		MaxEffectiveCount:      maxEffectiveCount,
		Count:                  l.count,
		Weight:                 l.weight,
		CappedReceiverCapacity: receiverSlotCapacity(receiverSlots),
	}
	if receiverSlots > maxCoarseReceiverSlots {
		receiverSlots = maxCoarseReceiverSlots
	}
	for i := 0; i < receiverSlots; i++ {
		slot := l.receiverSlotAt(i, false)
		if slot == nil || slot.hash == 0 {
			continue
		}
		candidate.ReceiverSlotCounts[i] = slot.count
		candidate.ReceiverSlotWeights[i] = slot.weight
	}
	return candidate
}

func (l *receiverCapShadowLane) update(weight float64, nowSec int64, decay float64, receiverHash uint64, receiverSlots int, maxEffectiveCount uint32, maxEffectiveWeight float64, trackP50 bool, snrBin int) {
	if l == nil {
		return
	}
	l.weight *= decay
	l.count *= decay
	if trackP50 && l.snrBins != nil && decay != 1 {
		l.snrBins.decay(decay)
	}
	if l.weight <= 0 || math.IsNaN(l.weight) {
		l.weight = 0
	}
	if l.count <= 0 || math.IsNaN(l.count) {
		l.count = 0
	}
	l.decayReceiverSlots(decay, receiverSlots)
	if receiverHash == 0 || receiverSlots <= 0 || weight <= 0 || maxEffectiveWeight <= 0 || maxEffectiveCount == 0 {
		return
	}
	slot := l.selectReceiverSlot(receiverHash, receiverSlots)
	if slot == nil {
		return
	}
	if slot.hash != receiverHash {
		*slot = receiverSlot{hash: receiverHash}
	}
	remainingWeight := maxEffectiveWeight - slot.weight
	if remainingWeight <= effectiveCountEpsilon {
		return
	}
	remainingCount := float64(maxEffectiveCount) - slot.count
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
	l.weight += acceptedWeight
	l.count += acceptedFraction
	if trackP50 && snrBin >= 0 {
		if l.snrBins == nil {
			l.snrBins = &snrHistogram{}
		}
		l.snrBins.add(snrBin, acceptedWeight)
	}
}

func (l *receiverCapShadowLane) decayReceiverSlots(decay float64, receiverSlots int) {
	if l == nil || receiverSlots <= 0 {
		return
	}
	for i := 0; i < receiverSlots; i++ {
		slot := l.receiverSlotAt(i, false)
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

func (l *receiverCapShadowLane) selectReceiverSlot(hash uint64, receiverSlots int) *receiverSlot {
	if l == nil || hash == 0 || receiverSlots <= 0 {
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
		slot := l.receiverSlotAt(i, false)
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
		return l.receiverSlotAt(empty, true)
	}
	return l.receiverSlotAt(replace, true)
}

func (l *receiverCapShadowLane) receiverSlotAt(index int, allocate bool) *receiverSlot {
	if l == nil || index < 0 {
		return nil
	}
	if index < inlineReceiverSlots {
		return &l.slots[index]
	}
	extraIndex := index - inlineReceiverSlots
	if extraIndex < 0 || extraIndex >= maxCoarseReceiverSlots-inlineReceiverSlots {
		return nil
	}
	if l.extraSlots == nil {
		if !allocate {
			return nil
		}
		l.extraSlots = &[maxCoarseReceiverSlots - inlineReceiverSlots]receiverSlot{}
	}
	return &l.extraSlots[extraIndex]
}

func mergeFineCoarseCapShadow(fine ReceiverCapShadowSample, coarse ReceiverCapShadowSample) ReceiverCapShadowSample {
	if fine.Count <= 0 {
		return coarse
	}
	if coarse.Count <= 0 {
		return fine
	}
	count := fine.Count
	if coarse.Count < count {
		count = coarse.Count
	}
	if count > ReceiverShadowCapCandidateCount {
		count = ReceiverShadowCapCandidateCount
	}
	var out ReceiverCapShadowSample
	out.Count = count
	for i := 0; i < count; i++ {
		out.Candidates[i] = ReceiverCapShadowSampleCandidate{
			MaxEffectiveCount:      fine.Candidates[i].MaxEffectiveCount,
			Count:                  maxCount(fine.Candidates[i].Count, coarse.Candidates[i].Count),
			Weight:                 fine.Candidates[i].Weight + coarse.Candidates[i].Weight,
			CappedReceiverCount:    maxCount(fine.Candidates[i].CappedReceiverCount, coarse.Candidates[i].CappedReceiverCount),
			CappedReceiverCapacity: maxCount(fine.Candidates[i].CappedReceiverCapacity, coarse.Candidates[i].CappedReceiverCapacity),
		}
	}
	return out
}

func selectCapShadowFineCoarse(fine ReceiverCapShadowSample, coarse ReceiverCapShadowSample, minFineWeight float64, fineOnlyWeight float64) ReceiverCapShadowSample {
	count := boundedCapShadowCount(fine, coarse)
	if count == 0 {
		return ReceiverCapShadowSample{}
	}
	var out ReceiverCapShadowSample
	out.Count = count
	for i := 0; i < count; i++ {
		fineCandidate := candidateAt(fine, i)
		coarseCandidate := candidateAt(coarse, i)
		hasFine := capShadowCandidateHasEvidence(fineCandidate)
		hasCoarse := capShadowCandidateHasEvidence(coarseCandidate)
		switch {
		case hasFine && !hasCoarse:
			out.Candidates[i] = fineCandidate
		case !hasFine && hasCoarse:
			out.Candidates[i] = coarseCandidate
		case !hasFine && !hasCoarse:
			out.Candidates[i] = ReceiverCapShadowSampleCandidate{MaxEffectiveCount: capShadowMaxEffectiveCount(fine, coarse, i)}
		case fineOnlyWeight > 0 && fineCandidate.Weight >= fineOnlyWeight:
			out.Candidates[i] = fineCandidate
		case minFineWeight > 0 && fineCandidate.Weight < minFineWeight:
			out.Candidates[i] = coarseCandidate
		default:
			out.Candidates[i] = mergeCapShadowFineCoarseCandidate(fineCandidate, coarseCandidate)
		}
		if out.Candidates[i].MaxEffectiveCount == 0 {
			out.Candidates[i].MaxEffectiveCount = capShadowMaxEffectiveCount(fine, coarse, i)
		}
	}
	return out
}

func candidateAt(sample ReceiverCapShadowSample, idx int) ReceiverCapShadowSampleCandidate {
	if idx >= 0 && idx < sample.Count {
		return sample.Candidates[idx]
	}
	return ReceiverCapShadowSampleCandidate{}
}

func capShadowCandidateHasEvidence(candidate ReceiverCapShadowSampleCandidate) bool {
	return candidate.Weight > 0 || candidate.Count > 0 || candidate.hasSNRBins
}

func mergeCapShadowFineCoarseCandidate(fine ReceiverCapShadowSampleCandidate, coarse ReceiverCapShadowSampleCandidate) ReceiverCapShadowSampleCandidate {
	out := ReceiverCapShadowSampleCandidate{
		MaxEffectiveCount:      fine.MaxEffectiveCount,
		Count:                  maxCount(fine.Count, coarse.Count),
		Weight:                 fine.Weight + coarse.Weight,
		CappedReceiverCount:    maxCount(fine.CappedReceiverCount, coarse.CappedReceiverCount),
		CappedReceiverCapacity: maxCount(fine.CappedReceiverCapacity, coarse.CappedReceiverCapacity),
	}
	if out.MaxEffectiveCount == 0 {
		out.MaxEffectiveCount = coarse.MaxEffectiveCount
	}
	if fine.hasSNRBins {
		out.snrBins.addScaled(fine.snrBins, 1)
		out.hasSNRBins = true
	}
	if coarse.hasSNRBins {
		out.snrBins.addScaled(coarse.snrBins, 1)
		out.hasSNRBins = true
	}
	return out
}

func mergeBothDirectionCapShadow(receive ReceiverCapShadowSample, transmit ReceiverCapShadowSample, cfg Config) ReceiverCapShadowSample {
	count := boundedCapShadowCount(receive, transmit)
	var out ReceiverCapShadowSample
	out.Count = count
	for i := 0; i < count; i++ {
		out.Candidates[i] = ReceiverCapShadowSampleCandidate{
			MaxEffectiveCount:      capShadowMaxEffectiveCount(receive, transmit, i),
			Count:                  saturatingAddCounts(receive.Candidates[i].Count, transmit.Candidates[i].Count),
			Weight:                 cfg.MergeReceiveWeight*receive.Candidates[i].Weight + cfg.MergeTransmitWeight*transmit.Candidates[i].Weight,
			CappedReceiverCount:    saturatingAddCounts(receive.Candidates[i].CappedReceiverCount, transmit.Candidates[i].CappedReceiverCount),
			CappedReceiverCapacity: saturatingAddCounts(receive.Candidates[i].CappedReceiverCapacity, transmit.Candidates[i].CappedReceiverCapacity),
		}
	}
	return out
}

func mergeCapShadowDirectionsWithDistribution(receive ReceiverCapShadowSample, transmit ReceiverCapShadowSample, cfg Config, noisePenalty float64) ReceiverCapShadowSample {
	count := boundedCapShadowCount(receive, transmit)
	var out ReceiverCapShadowSample
	out.Count = count
	for i := 0; i < count; i++ {
		receiveCandidate := candidateAt(receive, i)
		transmitCandidate := candidateAt(transmit, i)
		receiveActive := receiveCandidate.Weight > 0
		transmitActive := transmitCandidate.Weight > 0
		candidate := ReceiverCapShadowSampleCandidate{
			MaxEffectiveCount:      capShadowMaxEffectiveCount(receive, transmit, i),
			Count:                  saturatingAddCounts(receiveCandidate.Count, transmitCandidate.Count),
			CappedReceiverCount:    saturatingAddCounts(receiveCandidate.CappedReceiverCount, transmitCandidate.CappedReceiverCount),
			CappedReceiverCapacity: saturatingAddCounts(receiveCandidate.CappedReceiverCapacity, transmitCandidate.CappedReceiverCapacity),
		}
		switch {
		case receiveActive && transmitActive:
			candidate.Weight = cfg.MergeReceiveWeight*receiveCandidate.Weight + cfg.MergeTransmitWeight*transmitCandidate.Weight
			addCandidateBins(&candidate, receiveCandidate, cfg.MergeReceiveWeight, -noisePenalty)
			addCandidateBins(&candidate, transmitCandidate, cfg.MergeTransmitWeight, 0)
		case receiveActive:
			candidate.Weight = receiveCandidate.Weight * cfg.ReverseHintDiscount
			addCandidateBins(&candidate, receiveCandidate, cfg.ReverseHintDiscount, -noisePenalty)
		case transmitActive:
			candidate.Weight = transmitCandidate.Weight * cfg.ReverseHintDiscount
			addCandidateBins(&candidate, transmitCandidate, cfg.ReverseHintDiscount, 0)
		default:
			candidate.Weight = receiveCandidate.Weight + transmitCandidate.Weight
		}
		out.Candidates[i] = candidate
	}
	return out
}

func addCandidateBins(dst *ReceiverCapShadowSampleCandidate, src ReceiverCapShadowSampleCandidate, scale float64, shiftDB float64) {
	if dst == nil || !src.hasSNRBins || scale <= 0 {
		return
	}
	bins := src.snrBins
	if shiftDB != 0 {
		bins = bins.shifted(shiftDB)
	}
	dst.snrBins.addScaled(bins, scale)
	dst.hasSNRBins = true
}

func singleDirectionCapShadow(active ReceiverCapShadowSample, other ReceiverCapShadowSample, cfg Config) ReceiverCapShadowSample {
	count := boundedCapShadowCount(active, other)
	if count == 0 && active.Count > 0 {
		count = active.Count
		if count > ReceiverShadowCapCandidateCount {
			count = ReceiverShadowCapCandidateCount
		}
	}
	var out ReceiverCapShadowSample
	out.Count = count
	for i := 0; i < count; i++ {
		out.Candidates[i] = ReceiverCapShadowSampleCandidate{
			MaxEffectiveCount:      capShadowMaxEffectiveCount(active, other, i),
			Count:                  saturatingAddCounts(active.Candidates[i].Count, other.Candidates[i].Count),
			Weight:                 active.Candidates[i].Weight * cfg.ReverseHintDiscount,
			CappedReceiverCount:    active.Candidates[i].CappedReceiverCount,
			CappedReceiverCapacity: active.Candidates[i].CappedReceiverCapacity,
		}
	}
	return out
}

func inactiveDirectionCapShadow(receive ReceiverCapShadowSample, transmit ReceiverCapShadowSample) ReceiverCapShadowSample {
	count := boundedCapShadowCount(receive, transmit)
	var out ReceiverCapShadowSample
	out.Count = count
	for i := 0; i < count; i++ {
		out.Candidates[i] = ReceiverCapShadowSampleCandidate{
			MaxEffectiveCount:      capShadowMaxEffectiveCount(receive, transmit, i),
			Count:                  saturatingAddCounts(receive.Candidates[i].Count, transmit.Candidates[i].Count),
			Weight:                 receive.Candidates[i].Weight + transmit.Candidates[i].Weight,
			CappedReceiverCount:    saturatingAddCounts(receive.Candidates[i].CappedReceiverCount, transmit.Candidates[i].CappedReceiverCount),
			CappedReceiverCapacity: saturatingAddCounts(receive.Candidates[i].CappedReceiverCapacity, transmit.Candidates[i].CappedReceiverCapacity),
		}
	}
	return out
}

func boundedCapShadowCount(left ReceiverCapShadowSample, right ReceiverCapShadowSample) int {
	var count int
	switch {
	case left.Count <= 0:
		count = right.Count
	case right.Count <= 0:
		count = left.Count
	case right.Count < left.Count:
		count = right.Count
	default:
		count = left.Count
	}
	if count < 0 {
		return 0
	}
	if count > ReceiverShadowCapCandidateCount {
		return ReceiverShadowCapCandidateCount
	}
	return count
}

func capShadowMaxEffectiveCount(left ReceiverCapShadowSample, right ReceiverCapShadowSample, idx int) uint32 {
	if idx < left.Count && left.Candidates[idx].MaxEffectiveCount > 0 {
		return left.Candidates[idx].MaxEffectiveCount
	}
	if idx < right.Count {
		return right.Candidates[idx].MaxEffectiveCount
	}
	return 0
}

func receiverCountFromSlotSnapshots(counts [maxCoarseReceiverSlots]float64, weights [maxCoarseReceiverSlots]float64, decay float64, capacity uint32) uint32 {
	if capacity == 0 {
		return 0
	}
	limit := int(capacity)
	if limit > maxCoarseReceiverSlots {
		limit = maxCoarseReceiverSlots
	}
	var out uint32
	for i := 0; i < limit; i++ {
		if counts[i]*decay > effectiveCountEpsilon || weights[i]*decay > effectiveCountEpsilon {
			out++
		}
	}
	return out
}
