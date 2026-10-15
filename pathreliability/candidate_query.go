// File role: Owns bounded query-local physical evidence identity.
// Crawler notes: Role aliases share one captured/evaluated bucket. All captures
// use one maximum accounting reference; distinct buckets are not an atomic
// snapshot. Only copied values leave store locks, and no query state is retained.
// Related docs: pathreliability/README.md, docs/path-reliability-first-wave-execution.md.
package pathreliability

import (
	"math/bits"
	"time"
)

const (
	receiveFine = iota
	receiveCoarse
	transmitFine
	transmitCoarse
)

// Alias zero means absent; aliases 1..4 index the unique capture arrays.
type candidatePlan struct {
	keys  [4]uint64
	roles [4]uint8
	slots [4]int
	count int
}

func (p *candidatePlan) add(role int, key uint64, slots int) {
	if key == 0 {
		return
	}
	for i := 0; i < p.count; i++ {
		if p.keys[i] == key {
			p.roles[role] = uint8(i + 1)
			return
		}
	}
	p.keys[p.count], p.slots[p.count] = key, slots
	p.count++
	p.roles[role] = uint8(p.count)
}

type candidateState struct {
	storeValid bool
	liveSlots  uint16
}

type candidateQuery struct {
	plan         candidatePlan
	captures     [4]bucketCapture
	samples      [4]sampleWithBins
	states       [4]candidateState
	reference    int64
	hasReference bool
}

func (s *Store) lookupQueryInto(receiver, sender, receiverCoarse, senderCoarse CellID, band string, now time.Time, reverse bool, detail sampleDetail, query *candidateQuery) {
	*query = candidateQuery{}
	if s == nil || !s.cfg.Enabled {
		return
	}
	bandID, ok := s.bandIndex.Lookup(band)
	if !ok {
		return
	}
	query.reference, query.hasReference = now.Unix(), true
	query.plan.add(receiveFine, packKey(receiver, sender, bandID), s.cfg.ReceiverFineSlots)
	query.plan.add(receiveCoarse, packCoarseKey(receiverCoarse, senderCoarse, bandID), s.cfg.ReceiverCoarseSlots)
	if reverse {
		query.plan.add(transmitFine, packKey(sender, receiver, bandID), s.cfg.ReceiverFineSlots)
		query.plan.add(transmitCoarse, packCoarseKey(senderCoarse, receiverCoarse, bandID), s.cfg.ReceiverCoarseSlots)
	}
	for i := 0; i < query.plan.count; i++ {
		s.captureBucketInto(query.plan.keys[i], query.plan.slots[i], detail != sampleScalars, &query.captures[i])
		if query.captures[i].present {
			query.reference = max(query.reference, query.captures[i].reference)
		}
	}
	halfLife := s.bandIndex.HalfLifeSeconds(band, s.cfg)
	for i := 0; i < query.plan.count; i++ {
		query.states[i] = s.evaluateCaptureInto(&query.captures[i], halfLife, query.reference, query.plan.slots[i], detail, &query.samples[i])
	}
}

func (q *candidateQuery) sample(alias uint8) sampleWithBins {
	if alias == 0 {
		return sampleWithBins{Sample: Sample{SamplingUnix: q.reference, HasSamplingTime: q.hasReference}}
	}
	return q.samples[alias-1]
}

func (q *candidateQuery) stored(role int) bool {
	alias := q.plan.roles[role]
	return alias != 0 && q.states[alias-1].storeValid
}

func (q *candidateQuery) fresh(role int, maxAge int64) bool {
	return q.stored(role) && (maxAge <= 0 || q.samples[q.plan.roles[role]-1].AgeSec <= maxAge)
}

type selectedCandidates struct {
	receive, transmit uint8
	staleDropped      bool
	staleCounts       staleSampleCounts
}

// Selection precedes usability checks. A fresh but unusable coarse population
// blocks a fine retry; a required failed distribution never becomes a partial
// median. Fine/coarse containment is not assumed because H3 mapping is separate.
func (q *candidateQuery) selectPopulation(cfg Config, maxAge int64, beacon bool) selectedCandidates {
	fineRX, coarseRX := q.fresh(receiveFine, maxAge), q.fresh(receiveCoarse, maxAge)
	layer := receiveCoarse
	if beacon {
		if fineRX && q.samples[q.plan.roles[receiveFine]-1].Weight >= cfg.FineOnlyWeight {
			layer = receiveFine
		} else if !coarseRX && (fineRX || !q.stored(receiveCoarse)) {
			layer = receiveFine
		}
	} else {
		fineTX, coarseTX := q.fresh(transmitFine, maxAge), q.fresh(transmitCoarse, maxAge)
		distinctFine := q.plan.roles[receiveFine] != q.plan.roles[transmitFine]
		if fineRX && fineTX && distinctFine && q.samples[q.plan.roles[receiveFine]-1].Weight >= cfg.FineOnlyWeight && q.samples[q.plan.roles[transmitFine]-1].Weight >= cfg.FineOnlyWeight {
			layer = receiveFine
		} else if !coarseRX && !coarseTX && (fineRX || fineTX || (!q.stored(receiveCoarse) && !q.stored(transmitCoarse))) {
			layer = receiveFine
		}
	}
	roles := [2]int{layer, layer + 2}
	length := 2
	if beacon {
		length = 1
	}
	var selected selectedCandidates
	for direction := 0; direction < length; direction++ {
		role := roles[direction]
		alias := q.plan.roles[role]
		// Shared regional evidence has one receive role, one noise adjustment
		// and (for ordinary prediction) one single-direction discount.
		if alias == 0 || (direction == 1 && alias == q.plan.roles[roles[0]]) || !q.stored(role) {
			continue
		}
		if !q.fresh(role, maxAge) {
			selected.staleDropped = true
			selected.staleCounts.add(q.samples[alias-1].Sample)
			continue
		}
		if direction == 0 {
			selected.receive = alias
		} else {
			selected.transmit = alias
		}
	}
	return selected
}

// Each bucket has unique nonzero slot hashes. Count the cross-population
// intersection once, using existing epsilon-OR liveness before identity union.
// At most 12x12 comparisons; count-only populations supply no diversity.
func (q *candidateQuery) receiverUnion(left, right uint8) (count, capacity uint32) {
	if right == left {
		right = 0
	}
	leftActive := left != 0 && q.samples[left-1].Weight > 0
	rightActive := right != 0 && q.samples[right-1].Weight > 0
	if !leftActive && !rightActive {
		// Preserve the configured requirement for a wholly count-only query,
		// while granting no receiver diversity to unusable signal evidence.
		if left != 0 {
			capacity += q.samples[left-1].CappedReceiverCapacity
		}
		if right != 0 {
			capacity += q.samples[right-1].CappedReceiverCapacity
		}
		return 0, capacity
	}
	if left != 0 && !(q.samples[left-1].Weight > 0) {
		left = 0
	}
	if right != 0 && (!(q.samples[right-1].Weight > 0) || right == left) {
		right = 0
	}
	var a, b uint16
	if left != 0 {
		a = q.states[left-1].liveSlots
		capacity += q.samples[left-1].CappedReceiverCapacity
	}
	if right != 0 {
		b = q.states[right-1].liveSlots
		capacity += q.samples[right-1].CappedReceiverCapacity
	}
	count = uint32(bits.OnesCount16(a) + bits.OnesCount16(b))
	for a != 0 && b != 0 {
		i := bits.TrailingZeros16(a)
		a &^= 1 << i
		for remaining := b; remaining != 0; {
			j := bits.TrailingZeros16(remaining)
			remaining &^= 1 << j
			if q.captures[left-1].slots[i].hash == q.captures[right-1].slots[j].hash {
				count--
				b &^= 1 << j
				break
			}
		}
	}
	return count, capacity
}
