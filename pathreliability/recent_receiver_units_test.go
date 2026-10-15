package pathreliability

import "testing"

type recentReceiverFixtureSlot struct {
	hash          uint64
	units, weight float64
}

type recentReceiverUnionFixture struct {
	name                    string
	left, right             []recentReceiverFixtureSlot
	leftAlias, rightAlias   uint8
	leftDecay, rightDecay   float64
	leftWeight, rightWeight float64
	wantCount, wantCapacity uint32
}

func recentReceiverQuery(tc recentReceiverUnionFixture) candidateQuery {
	var q candidateQuery
	q.plan.count = 2
	for i, slots := range [][]recentReceiverFixtureSlot{tc.left, tc.right} {
		q.plan.slots[i] = 12
		q.captures[i].present = true
		q.states[i].storeValid = true
		q.samples[i].CappedReceiverCapacity = 12
		for j, slot := range slots {
			q.captures[i].slots[j] = receiverCapture{hash: slot.hash, count: slot.units, weight: slot.weight}
		}
	}
	q.samples[0].Weight, q.samples[1].Weight = tc.leftWeight, tc.rightWeight
	q.states[0].decay, q.states[1].decay = tc.leftDecay, tc.rightDecay
	return q
}

// This map is an independent, unbounded test oracle. Production must retain
// bounded query-local scratch; identities are combined before qualification.
func recentReceiverQuantityOracle(tc recentReceiverUnionFixture) (count, capacity uint32) {
	populations := []struct {
		alias         uint8
		slots         []recentReceiverFixtureSlot
		decay, weight float64
	}{
		{tc.leftAlias, tc.left, tc.leftDecay, tc.leftWeight},
		{tc.rightAlias, tc.right, tc.rightDecay, tc.rightWeight},
	}
	seen := make(map[uint8]bool)
	quantities := make(map[uint64]float64)
	anySignal := tc.leftAlias != 0 && tc.leftWeight > 0 || tc.rightAlias != 0 && tc.rightWeight > 0
	for _, population := range populations {
		if population.alias == 0 || seen[population.alias] {
			continue
		}
		seen[population.alias] = true
		if anySignal && population.weight <= 0 {
			continue
		}
		capacity += 12
		if !anySignal {
			continue
		}
		for _, slot := range population.slots {
			if slot.hash != 0 {
				quantities[slot.hash] += slot.units * population.decay
			}
		}
	}
	for _, units := range quantities {
		if units >= 1 {
			count++
		}
	}
	return count, capacity
}

func TestRecentReceiverUnion(t *testing.T) {
	one := func(hash uint64, units float64) []recentReceiverFixtureSlot {
		return []recentReceiverFixtureSlot{{hash: hash, units: units, weight: 1}}
	}
	base := func(name string, left, right []recentReceiverFixtureSlot, count uint32) recentReceiverUnionFixture {
		return recentReceiverUnionFixture{name: name, left: left, right: right, leftAlias: 1, rightAlias: 2,
			leftDecay: 1, rightDecay: 1, leftWeight: 1, rightWeight: 1, wantCount: count, wantCapacity: 24}
	}
	cases := []recentReceiverUnionFixture{
		base("below", one(1, 1-0x1p-20), nil, 0),
		base("equal", one(1, 1), nil, 1),
		base("above", one(1, 1+0x1p-20), nil, 1),
		base("weight_only", []recentReceiverFixtureSlot{{hash: 1, weight: 8}}, nil, 0),
		base("fractional_halves", one(1, .5), one(1, .5), 1),
		base("fractional_three_fifths", one(1, .6), one(1, .6), 1),
		base("distinct_hash_fractions", one(1, .6), one(2, .6), 0),
		base("tiny_completes_one", one(1, 1-0x1p-32), one(1, 0x1p-31), 1),
		base("unattributed", one(0, 8), nil, 0),
		base("empty", nil, nil, 0),
		base("permuted_quantities", []recentReceiverFixtureSlot{{1, .25, 1}, {2, .75, 1}, {3, 1, 1}, {4, 0, 8}},
			[]recentReceiverFixtureSlot{{3, .1, 1}, {1, .75, 1}, {2, .25, 1}, {5, 1, 1}}, 4),
	}
	shared := base("shared_once", one(1, .6), one(1, .6), 0)
	shared.rightAlias, shared.wantCapacity = 1, 12
	cases = append(cases, shared)
	differentReferences := base("different_references", one(1, 2), one(1, 1), 1)
	differentReferences.leftDecay, differentReferences.rightDecay = .25, .5
	cases = append(cases, differentReferences)
	rightOnly := base("right_only", nil, one(1, 1), 1)
	rightOnly.leftAlias, rightOnly.wantCapacity = 0, 12
	cases = append(cases, rightOnly)
	absent := base("absent", nil, nil, 0)
	absent.leftAlias, absent.rightAlias, absent.wantCapacity = 0, 0, 0
	cases = append(cases, absent)
	withoutSignal := base("noncontributing_opposite", one(1, 1), one(2, 8), 1)
	withoutSignal.rightWeight, withoutSignal.wantCapacity = 0, 12
	cases = append(cases, withoutSignal)
	countOnly := base("count_only_capacity", one(1, 8), one(2, 8), 0)
	countOnly.leftWeight, countOnly.rightWeight = 0, 0
	cases = append(cases, countOnly)
	full := base("capacity24", nil, nil, 24)
	fullOverlap := base("full_overlap", nil, nil, 12)
	partialOverlap := base("partial_overlap", nil, nil, 6)
	for i := uint64(0); i < 12; i++ {
		full.left = append(full.left, recentReceiverFixtureSlot{i + 1, 1, 1})
		full.right = append(full.right, recentReceiverFixtureSlot{i + 101, 1, 1})
		fullOverlap.left = append(fullOverlap.left, recentReceiverFixtureSlot{i + 1, .6, 1})
		fullOverlap.right = append(fullOverlap.right, recentReceiverFixtureSlot{12 - i, .6, 1})
		partialOverlap.left = append(partialOverlap.left, recentReceiverFixtureSlot{i + 1, .6, 1})
		partialOverlap.right = append(partialOverlap.right, recentReceiverFixtureSlot{i + 7, .6, 1})
	}
	cases = append(cases, full, fullOverlap, partialOverlap)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oracleCount, oracleCapacity := recentReceiverQuantityOracle(tc)
			if oracleCount != tc.wantCount || oracleCapacity != tc.wantCapacity {
				t.Fatalf("broken handwritten fixture: oracle=%d/%d want=%d/%d", oracleCount, oracleCapacity, tc.wantCount, tc.wantCapacity)
			}
			q := recentReceiverQuery(tc)
			before := q
			count, capacity := q.receiverUnion(tc.leftAlias, tc.rightAlias)
			if count != tc.wantCount || capacity != tc.wantCapacity || q != before {
				t.Fatalf("union=%d/%d, want=%d/%d; query mutated=%v", count, capacity, tc.wantCount, tc.wantCapacity, q != before)
			}
		})
	}
}

func TestRecentReceiverUnionPublic(t *testing.T) {
	for _, mode := range []string{ReceiverContributionShadow, ReceiverContributionEnforce} {
		for _, tc := range []struct {
			name                  string
			capWeight             float64
			shared, aged          bool
			wantRaw, wantAdmitted float64
			wantReceivers         uint32
			wantCappedCount       uint32
		}{
			{"half_units", .5, false, false, 2, 1, 1, 0},
			{"three_fifths", .6, false, false, 2, 1.2, 1, 0},
			{"tiny_crosses_one", 1 - 0x1p-32, false, true, 1 + 0x1p-31, (1 - 0x1p-32) + (1-0x1p-32)*0x1p-31, 1, 1},
			{"shared_once", .6, true, false, 1, .6, 0, 0},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				cfg := recentSupportConfig(mode, 10)
				cfg.ReceiverMaxEffectiveWeight = tc.capWeight
				p := NewPredictor(cfg, []string{"20m"})
				coarseTX := CellID(4)
				if tc.shared {
					coarseTX = 3
				}
				observed := int64(1000)
				if tc.aged {
					observed -= 310
				}
				seed := func(rx, tx CellID, at int64) {
					t.Helper()
					a := p.UpdateWithReceiverHash(BucketCombined, InvalidCell, InvalidCell, rx, tx, "20m", -10.5, 1,
						ObservationTimes{ObservedAt: recentSupportTime(at), ArrivedAt: recentSupportTime(at)}, false, 1)
					if !a.Accepted() {
						t.Fatalf("receiver quantity seed rejected: %+v", a)
					}
				}
				seed(3, coarseTX, observed)
				if !tc.shared {
					seed(coarseTX, 3, 1000)
				}
				got := p.Predict(InvalidCell, InvalidCell, 3, coarseTX, "20m", "FT8", 0, recentSupportTime(1000))
				recentSupportWant(t, got.ObservedRecentSupport, mode, tc.wantRaw, tc.wantAdmitted)
				wantSource, wantReason := SourceCombined, InsufficientNone
				if tc.shared && mode == ReceiverContributionEnforce {
					wantSource, wantReason = SourceInsufficient, InsufficientLowCount
				}
				if got.Source != wantSource || got.InsufficientReason != wantReason || got.ReceiverCount != tc.wantReceivers || got.ReceiverRequired != 1 || !got.HasP50 || got.P50DB != -10.5 {
					t.Fatalf("public receiver quantities were filtered, rounded, or duplicated: %+v", got)
				}
				if mode == ReceiverContributionShadow && got.CapWouldBlock != tc.shared {
					t.Fatalf("shadow did not use fractional admitted units/diversity: %+v", got)
				}
				if got.CappedCount != tc.wantCappedCount {
					t.Fatalf("legacy per-population integer diagnostics changed: %+v", got)
				}
			})
		}
	}
}

func TestRecentReceiverUnionRotationPreservesAggregate(t *testing.T) {
	for _, mode := range []string{ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			cfg := recentSupportConfig(mode, 10)
			cfg.MinObservationCount = 10
			p := NewPredictor(cfg, []string{"20m"})
			for hash := uint64(1); hash <= 20; hash++ {
				a := p.UpdateWithReceiverHash(BucketCombined, InvalidCell, InvalidCell, 3, 4, "20m", -10.5, 1,
					ObservationTimes{ObservedAt: recentSupportTime(0), ArrivedAt: recentSupportTime(0)}, false, hash)
				if !a.Accepted() {
					t.Fatalf("rotation seed rejected: %+v", a)
				}
			}
			capture := p.combined.captureBucket(0x30004, 12, false)
			seen := make(map[uint64]bool)
			var tracked float64
			for _, slot := range capture.slots {
				if slot.hash == 0 || slot.hash > 20 || seen[slot.hash] || slot.count != 1 {
					t.Fatalf("rotation lost bounded unique receiver ownership: %+v", capture.slots)
				}
				seen[slot.hash] = true
				tracked += slot.count
			}
			if len(seen) != 12 || tracked != 12 || capture.cappedCount != 20 || capture.recentRawUsable != 20 {
				t.Fatalf("aggregate support was reconstructed from rotated slots: %+v", capture)
			}
			fresh := p.Predict(InvalidCell, InvalidCell, 3, 4, "20m", "FT8", 0, recentSupportTime(0))
			recentSupportWant(t, fresh.ObservedRecentSupport, mode, 20, 20)
			if fresh.Source != SourceCombined || fresh.ReceiverCount != 12 || fresh.ReceiverRequired != 2 || fresh.RawCount != 20 || !fresh.HasP50 {
				t.Fatalf("fresh rotated support was not usable: %+v", fresh)
			}
			aged := p.Predict(InvalidCell, InvalidCell, 3, 4, "20m", "FT8", 0, recentSupportTime(10))
			recentSupportWant(t, aged.ObservedRecentSupport, mode, 10, 10)
			wantSource, wantReason := SourceCombined, InsufficientNone
			if mode == ReceiverContributionEnforce {
				wantSource, wantReason = SourceInsufficient, InsufficientLowReceiver
			}
			if aged.Source != wantSource || aged.InsufficientReason != wantReason || aged.ReceiverCount != 0 || aged.ReceiverRequired != 2 || aged.RawCount != 20 || aged.CappedCount != 10 || !aged.HasP50 || aged.P50DB != -10.5 || aged.CapWouldBlock != (mode == ReceiverContributionShadow) {
				t.Fatalf("aging confused aggregate volume with tracked receiver diversity: %+v", aged)
			}
		})
	}
}
