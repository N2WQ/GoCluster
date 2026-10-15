package pathreliability

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

func s6IdentityQuery(p *Predictor, path s6PolicyPath, now time.Time) candidateQuery {
	var query candidateQuery
	p.combined.lookupQueryInto(path.rx, path.tx, path.coarseRX, path.coarseTX, "20m", now, true, sampleDistribution, &query)
	return query
}

func TestS6CandidateCaptureIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		path   s6PolicyPath
		keys   [4]uint64
		roles  [4]uint8
		slots  [4]int
		unique int
	}{
		{"four_unique", s6PolicyQuery, [4]uint64{0x100020000, 0x30004, 0x200010000, 0x40003}, [4]uint8{1, 2, 3, 4}, [4]int{6, 12, 6, 12}, 4},
		{"both_layers_alias", s6PolicyPath{1, 1, 3, 3}, [4]uint64{0x100010000, 0x30003}, [4]uint8{1, 2, 1, 2}, [4]int{6, 12}, 2},
		{"one_fine_alias", s6PolicyPath{rx: 1, tx: 1}, [4]uint64{0x100010000}, [4]uint8{1, 0, 1, 0}, [4]int{6}, 1},
		{"one_coarse_alias", s6PolicyPath{coarseRX: 3, coarseTX: 3}, [4]uint64{0x30003}, [4]uint8{0, 1, 0, 1}, [4]int{12}, 1},
		{"no_cells", s6PolicyPath{}, [4]uint64{}, [4]uint8{}, [4]int{}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, now := NewPredictor(s6PolicyConfig(ReceiverContributionEnforce), []string{"20m"}), s6PolicyNow()
			s6PolicyReports(t, p, tc.path, 3, 3, -11, 1, now)
			if tc.unique == 4 {
				reverse := s6PolicyPath{tc.path.tx, tc.path.rx, tc.path.coarseTX, tc.path.coarseRX}
				s6PolicyReports(t, p, reverse, 4, 4, 4, 1, now)
			}
			query := s6IdentityQuery(p, tc.path, now)
			if query.plan.count != tc.unique || query.plan.keys != tc.keys || query.plan.roles != tc.roles || query.plan.slots != tc.slots {
				t.Fatalf("unexpected physical plan: %+v", query.plan)
			}
			for i := 0; i < query.plan.count; i++ {
				if !query.captures[i].present || !query.states[i].storeValid {
					t.Fatalf("capture %d presence/state: %+v %+v", i, query.captures[i], query.states[i])
				}
				wantCount := uint32(3)
				if tc.unique == 4 && i >= 2 {
					wantCount = 4
				}
				if query.samples[i].ObservationCount != wantCount || query.samples[i].Weight != float64(wantCount) {
					t.Fatalf("capture %d was duplicated or changed: %+v", i, query.samples[i])
				}
			}
		})
	}
}

func TestS6AliasedCaptureConcurrentUpdates(t *testing.T) {
	p, now := NewPredictor(s6PolicyConfig(ReceiverContributionShadow), []string{"20m"}), s6PolicyNow()
	path := s6PolicyPath{1, 1, 3, 3}
	s6PolicyReports(t, p, path, 1, 1, -11, 1, now)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	request, progress, done := make(chan struct{}), make(chan int), make(chan struct{})
	errors := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("alias writer did not retire")
		}
	})
	go func() {
		defer close(done)
		for round := 0; round < 32; round++ {
			select {
			case <-ctx.Done():
				return
			case <-request:
			}
			for i := 0; i < 64; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				admission := p.UpdateWithReceiverHash(BucketCombined, 1, 1, 3, 3, "20m", -11, 1,
					ObservationTimes{ObservedAt: now, ArrivedAt: now}, false, uint64(1+i%12))
				if !admission.Accepted() {
					errors <- fmt.Errorf("alias writer report rejected: %+v", admission)
					return
				}
			}
			select {
			case <-ctx.Done():
				return
			case progress <- round:
			}
		}
	}()
	intermediate := false
	for round := 0; round < 32; round++ {
		select {
		case request <- struct{}{}:
		case <-ctx.Done():
			t.Fatal("alias writer start deadline")
		}
		for i := 0; i < 64; i++ {
			query := s6IdentityQuery(p, path, now)
			if query.plan.count != 2 || query.plan.roles != [4]uint8{1, 2, 1, 2} {
				t.Fatalf("aliases split during writes: %+v", query.plan)
			}
			if query.sample(1) != query.sample(query.plan.roles[transmitFine]) || query.sample(2) != query.sample(query.plan.roles[transmitCoarse]) {
				t.Fatal("shared snapshots differ")
			}
			count := query.samples[0].ObservationCount
			intermediate = intermediate || (count > 1 && count < 2049)
		}
		select {
		case got := <-progress:
			if got != round {
				t.Fatalf("progress=%d, want %d", got, round)
			}
		case err := <-errors:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal("alias writer completion deadline")
		}
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("alias writer join deadline")
	}
	query := s6IdentityQuery(p, path, now)
	if !intermediate || query.samples[0].ObservationCount != 2049 || query.samples[1].ObservationCount != 2049 {
		t.Fatalf("no bounded concurrent progress: fine=%+v coarse=%+v", query.samples[0], query.samples[1])
	}
}

func TestS6CommonReferenceAcrossUniqueCandidates(t *testing.T) {
	t.Run("excluded_coarse_advances_qualifying_fine_pair", func(t *testing.T) {
		p, at := NewPredictor(s6PolicyConfig(ReceiverContributionEnforce), []string{"20m"}), s6PolicyNow()
		for _, reverse := range []bool{false, true} {
			s6PolicyReports(t, p, s6PolicyLayer(true, reverse), 40, 6, -11, 1, at)
		}
		s6PolicyReports(t, p, s6PolicyLayer(false, false), 6, 6, -20, 1, at.Add(180*time.Second))
		query := s6IdentityQuery(p, s6PolicyQuery, at)
		for i := 0; i < query.plan.count; i++ {
			if !query.samples[i].HasSamplingTime || query.samples[i].SamplingUnix != at.Unix()+180 {
				t.Fatalf("candidate %d has a separate clock: %+v", i, query.samples[i])
			}
		}
		got := s6PolicyPredict(p, s6PolicyQuery, 0, at, false)
		s6PolicyWant(t, got, -10.5, 20, 80)
		if !got.HasObservedSamplingTime || got.ObservedSamplingUnix != at.Unix()+180 || got.AgeSec != 180 {
			t.Fatalf("excluded reference ignored: %+v", got)
		}
	})
	for _, seconds := range []int64{0, -60} {
		t.Run(fmt.Sprintf("epoch_zero_query_%d", seconds), func(t *testing.T) {
			p, epoch := NewPredictor(s6PolicyConfig(ReceiverContributionOff), []string{"20m"}), time.Unix(0, 0).UTC()
			s6PolicyReports(t, p, s6PolicyLayer(false, false), 2, 2, -11, 1, epoch)
			query := s6IdentityQuery(p, s6PolicyQuery, time.Unix(seconds, 0))
			got := s6PolicyPredict(p, s6PolicyQuery, 0, time.Unix(seconds, 0), false)
			s6PolicyWant(t, got, -10.5, 1, 2)
			if !query.hasReference || query.reference != 0 || !got.HasObservedSamplingTime || got.ObservedSamplingUnix != 0 || got.AgeSec != 0 {
				t.Fatalf("epoch/backward clock lost: query=%+v result=%+v", query, got)
			}
		})
	}
	t.Run("beacon_never_captures_reverse_clock", func(t *testing.T) {
		p, at := NewPredictor(s6PolicyConfig(ReceiverContributionOff), []string{"20m"}), s6PolicyNow()
		s6PolicyReports(t, p, s6PolicyLayer(false, false), 8, 6, -11, 1, at)
		s6PolicyReports(t, p, s6PolicyLayer(true, true), 1, 1, 4, 1, at.Add(180*time.Second))
		beacon := s6PolicyPredict(p, s6PolicyQuery, 0, at, true)
		ordinary := s6PolicyPredict(p, s6PolicyQuery, 0, at, false)
		s6PolicyWant(t, beacon, -10.5, 8, 8)
		s6PolicyWant(t, ordinary, -10.5, 2, 8)
		if beacon.ObservedSamplingUnix != at.Unix() || ordinary.ObservedSamplingUnix != at.Unix()+180 || beacon.AgeSec != 0 || ordinary.AgeSec != 180 {
			t.Fatalf("reverse isolation: beacon=%+v ordinary=%+v", beacon, ordinary)
		}
	})
}

func TestS6ReceiverIntersectionMatchesSetOracle(t *testing.T) {
	a := [12]uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	for _, tc := range []struct {
		name         string
		b            [12]uint64
		aMask, bMask uint16
		left, right  uint8
		zeroRight    bool
		capacity     uint32
	}{
		{"disjoint", [12]uint64{101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112}, 0xfff, 0xfff, 1, 2, false, 24},
		{"identical", a, 0xfff, 0xfff, 1, 2, false, 24},
		{"partial", [12]uint64{7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}, 0xfff, 0xfff, 1, 2, false, 24},
		{"permuted", [12]uint64{12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, 0xfff, 0xfff, 1, 2, false, 24},
		{"empty_masks", a, 0, 0, 1, 2, false, 24},
		{"asymmetric_masks_and_slot_11", [12]uint64{12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, 0x805, 0x149, 1, 2, false, 24},
		{"noncontributing_opposite", a, 0xfff, 0xfff, 1, 2, true, 12},
		{"same_physical_population", a, 0xfff, 0xfff, 1, 1, false, 12},
		{"no_populations", a, 0xfff, 0xfff, 0, 0, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := candidateQuery{}
			for population, hashes := range [2][12]uint64{a, tc.b} {
				query.samples[population].Weight, query.samples[population].CappedReceiverCapacity = 1, 12
				for i, hash := range hashes {
					query.captures[population].slots[i].hash = hash
				}
			}
			query.states[0].liveSlots, query.states[1].liveSlots = tc.aMask, tc.bMask
			if tc.zeroRight {
				query.samples[1].Weight = 0
			}
			want := make(map[uint64]struct{})
			for _, alias := range []uint8{tc.left, tc.right} {
				if alias == 0 || query.samples[alias-1].Weight <= 0 {
					continue
				}
				for i, slot := range query.captures[alias-1].slots {
					if query.states[alias-1].liveSlots&(1<<i) != 0 {
						want[slot.hash] = struct{}{}
					}
				}
			}
			before := query
			count, capacity := query.receiverUnion(tc.left, tc.right)
			if count != uint32(len(want)) || capacity != tc.capacity || query != before {
				t.Fatalf("union=%d capacity=%d, want %d/%d; input changed=%v", count, capacity, len(want), tc.capacity, query != before)
			}
		})
	}
}

func TestS6ReceiverIdentityUnionPreservesLiveness(t *testing.T) {
	for _, mode := range []string{ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			p, now := NewPredictor(s6PolicyConfig(mode), []string{"20m"}), s6PolicyNow()
			s6PolicyReports(t, p, s6PolicyLayer(false, false), 6, 6, -11, 1, now)
			s6PolicyMutate(t, p, s6PolicyLayer(false, false), func(b *bucket) {
				b.slots = [6]receiverSlot{}
				b.slots[0] = receiverSlot{hash: 101, weight: math.Nextafter(1e-9, math.Inf(1))}
				b.slots[1] = receiverSlot{hash: 102, count: math.Nextafter(1e-9, math.Inf(1))}
				b.slots[2] = receiverSlot{hash: 103, weight: 1e-9, count: 1e-9}
				b.slots[3] = receiverSlot{hash: 104, weight: 0.8, count: 0.8}
				b.slots[4] = receiverSlot{hash: 105, weight: 0.6e-9, count: 0.6e-9}
				*b.receiverSlotAt(11, true) = receiverSlot{hash: 107, count: 2e-9}
			})
			query := s6IdentityQuery(p, s6PolicyQuery, now)
			alias := query.plan.roles[receiveCoarse]
			if query.states[alias-1].liveSlots != 0x80b {
				t.Fatalf("S6 epsilon-OR mask=%x, want 80b", query.states[alias-1].liveSlots)
			}
			got := s6PolicyPredict(p, s6PolicyQuery, 0, now, false)
			s6PolicyWant(t, got, -10.5, 3, 6)
			if got.ReceiverCount != 4 {
				t.Fatalf("fractional/OR liveness changed early: %+v", got)
			}
		})
	}
	t.Run("subepsilon_slots_not_summed_across_populations_in_s6", func(t *testing.T) {
		p, now := NewPredictor(s6PolicyConfig(ReceiverContributionEnforce), []string{"20m"}), s6PolicyNow()
		for _, reverse := range []bool{false, true} {
			path := s6PolicyLayer(false, reverse)
			s6PolicyReports(t, p, path, 6, 6, -11, 1, now)
			s6PolicyMutate(t, p, path, func(b *bucket) {
				b.slots = [6]receiverSlot{}
				b.slots[0] = receiverSlot{hash: 101, count: 0.6e-9}
			})
		}
		got := s6PolicyPredict(p, s6PolicyQuery, 0, now, false)
		if got.Source != SourceInsufficient || got.InsufficientReason != InsufficientLowReceiver || got.ReceiverCount != 0 || got.ReceiverRequired != 1 || !got.HasP50 {
			t.Fatalf("S6 combined subepsilon slot quantities: %+v", got)
		}
	})
}

func TestS6ReceiverCapacityTracksPhysicalPopulations(t *testing.T) {
	for _, tc := range []struct {
		name                                          string
		fine, shared, countOnlyOpposite, allCountOnly bool
		floor                                         int
		required, receivers                           uint32
	}{
		{"two_fine_populations", true, false, false, false, 201, 12, 6},
		{"two_coarse_populations", false, false, false, false, 201, 24, 6},
		{"shared_fine_hint", true, true, false, false, 201, 6, 6},
		{"shared_coarse_hint", false, true, false, false, 201, 12, 6},
		{"count_only_opposite_excluded", false, false, true, false, 201, 12, 6},
		{"all_count_only_retains_low_receiver", false, false, false, true, 21, 3, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, now := s6PolicyConfig(ReceiverContributionEnforce), s6PolicyNow()
			cfg.MinObservationCount = tc.floor
			p, query := NewPredictor(cfg, []string{"20m"}), s6PolicyQuery
			if tc.shared {
				query.tx, query.coarseTX = query.rx, query.coarseRX
			}
			for _, reverse := range []bool{false, true} {
				if tc.shared && reverse {
					continue
				}
				path := s6PolicyLayer(tc.fine, reverse)
				if tc.shared && tc.fine {
					path = s6PolicyPath{rx: 1, tx: 1}
				}
				if tc.shared && !tc.fine {
					path = s6PolicyPath{coarseRX: 3, coarseTX: 3}
				}
				s6PolicyReports(t, p, path, 24, 6, -11, 1, now)
				if tc.allCountOnly || (tc.countOnlyOpposite && reverse) {
					s6PolicyMutate(t, p, path, func(b *bucket) { b.weight, b.cappedWeight, b.snrBins = 0, 0, snrHistogram{} })
				}
			}
			got := s6PolicyPredict(p, query, 0, now, false)
			if got.ReceiverCount != tc.receivers || got.ReceiverRequired != tc.required || got.Source != SourceInsufficient {
				t.Fatalf("physical capacity was clamped to observed hashes or duplicated: %+v", got)
			}
			if tc.allCountOnly && got.InsufficientReason != InsufficientLowReceiver {
				t.Fatalf("unattributed evidence changed failure precedence: %+v", got)
			}
		})
	}
}

func s6IdentitySlots(t *testing.T, b *bucket, capacity int) map[uint64]int {
	t.Helper()
	seen := make(map[uint64]int)
	for i := 0; i < capacity; i++ {
		if slot := b.receiverSlotAt(i, false); slot != nil && slot.hash != 0 {
			seen[slot.hash]++
			if seen[slot.hash] != 1 {
				t.Fatalf("duplicate receiver %d at slot %d", slot.hash, i)
			}
		}
	}
	return seen
}

func TestReceiverSlotIdentityUniquenessLifecycle(t *testing.T) {
	for _, capacity := range []int{6, 12} {
		t.Run(fmt.Sprintf("slots_%d", capacity), func(t *testing.T) {
			cfg, b := s6PolicyConfig(ReceiverContributionEnforce), &bucket{}
			for i := 1; i <= capacity; i++ {
				b.updateCapped(1, 1, 1, uint64(i), capacity, &cfg, 14)
				s6IdentitySlots(t, b, capacity)
			}
			if capacity == 12 && (b.extraSlots == nil || b.extraSlots.slots == nil) {
				t.Fatal("12-slot population did not allocate its bounded sidecar")
			}
			*b.receiverSlotAt(0, false) = receiverSlot{}
			b.updateCapped(1, 1, 2, uint64(capacity), capacity, &cfg, 14)
			if b.receiverSlotAt(0, false).hash != 0 || b.receiverSlotAt(capacity-1, false).count != 2 {
				t.Fatal("earlier empty slot hid the later existing receiver")
			}
			s6IdentitySlots(t, b, capacity)
			for i := 0; i < 6; i++ {
				b.updateCapped(1, 1, 2, uint64(capacity), capacity, &cfg, 14)
			}
			beforeWeight, beforeCount, beforeBins := b.cappedWeight, b.cappedCount, b.snrBins
			var beforeSlots [12]receiverSlot
			for i := 0; i < capacity; i++ {
				beforeSlots[i] = *b.receiverSlotAt(i, false)
			}
			b.updateCapped(1, 1, 2, uint64(capacity), capacity, &cfg, 14)
			if b.cappedWeight != beforeWeight || b.cappedCount != beforeCount || b.snrBins != beforeBins || b.receiverSlotAt(capacity-1, false).count != 8 {
				t.Fatal("cap-rejected report changed accepted mass or ownership")
			}
			for i := 0; i < capacity; i++ {
				if *b.receiverSlotAt(i, false) != beforeSlots[i] {
					t.Fatalf("cap rejection changed receiver slot %d", i)
				}
			}
			b.updateCapped(1, 1, 2, 100, capacity, &cfg, 14)
			b.updateCapped(1, 1, 3, 200, capacity, &cfg, 14)
			seen := s6IdentitySlots(t, b, capacity)
			if len(seen) != capacity || seen[2] != 0 || seen[200] != 1 {
				t.Fatalf("wrong oldest minimum-weight eviction: %v", seen)
			}
			b.updateCapped(1, 1, 4, 2, capacity, &cfg, 14)
			if len(s6IdentitySlots(t, b, capacity)) != capacity {
				t.Fatal("revisiting evicted identity changed the slot bound")
			}
			b.decayReceiverSlots(0, capacity)
			if len(s6IdentitySlots(t, b, capacity)) != 0 {
				t.Fatal("zero decay retained receiver identities")
			}
			b.updateCapped(1, 1, 5, 999, capacity, &cfg, 14)
			if got := s6IdentitySlots(t, b, capacity); len(got) != 1 || got[999] != 1 {
				t.Fatalf("clear/reinsert retained an orphan identity: %v", got)
			}
		})
	}
}

func TestS6MinFineWeightRetainsOnlyConfigContract(t *testing.T) {
	for _, minimum := range []float64{1, 5, 19.5} {
		t.Run(fmt.Sprintf("minimum_%g", minimum), func(t *testing.T) {
			cfg, now := s6PolicyConfig(ReceiverContributionEnforce), s6PolicyNow()
			cfg.MinFineWeight = minimum
			p := NewPredictor(cfg, []string{"20m"})
			s6PolicyReports(t, p, s6PolicyLayer(true, false), 10, 6, -11, 1, now)
			s6PolicyReports(t, p, s6PolicyLayer(false, false), 6, 6, -20, 1, now)
			s6PolicyWant(t, s6PolicyPredict(p, s6PolicyQuery, 0, now, false), -19.5, 3, 6)
		})
	}
	t.Run("normalization_still_raises_fine_only_threshold", func(t *testing.T) {
		cfg, now := s6PolicyConfig(ReceiverContributionEnforce), s6PolicyNow()
		cfg.MinFineWeight = 25
		p := NewPredictor(cfg, []string{"20m"})
		if p.Config().FineOnlyWeight != 25 || p.Config().MinFineWeight != 25 {
			t.Fatalf("normalization relationship removed: %+v", p.Config())
		}
		for _, reverse := range []bool{false, true} {
			s6PolicyReports(t, p, s6PolicyLayer(true, reverse), 24, 6, -11, 1, now)
			s6PolicyReports(t, p, s6PolicyLayer(false, reverse), 6, 6, -20, 1, now)
		}
		s6PolicyWant(t, s6PolicyPredict(p, s6PolicyQuery, 0, now, false), -19.5, 6, 12)
		for _, reverse := range []bool{false, true} {
			s6PolicyReports(t, p, s6PolicyLayer(true, reverse), 1, 1, -11, 1, now)
		}
		s6PolicyWant(t, s6PolicyPredict(p, s6PolicyQuery, 0, now, false), -10.5, 25, 50)
	})
}

type s6MappedBoundary struct {
	grid         string
	fine, coarse CellID
}

func s6IdentityMappedBoundary(t *testing.T) (s6MappedBoundary, s6MappedBoundary) {
	t.Helper()
	requireH3Mappings(t)
	seen := make(map[CellID]s6MappedBoundary)
	// Fixed lexicographic global corpus, bounded at 32,400 valid grid centers.
	// Discovery uses only actual public encoders; no H3 containment assumption.
	for a := byte('A'); a <= 'R'; a++ {
		for b := byte('A'); b <= 'R'; b++ {
			for x := byte('0'); x <= '9'; x++ {
				for y := byte('0'); y <= '9'; y++ {
					grid := string([]byte{a, b, x, y})
					point := s6MappedBoundary{grid, EncodeCell(grid), EncodeCoarseCell(grid)}
					if point.fine == InvalidCell || point.coarse == InvalidCell {
						t.Fatalf("valid grid %s failed active H3 mappings", grid)
					}
					if prior, ok := seen[point.fine]; ok && prior.coarse != point.coarse {
						return prior, point
					}
					seen[point.fine] = point
				}
			}
		}
	}
	t.Fatal("fixed global corpus did not witness an independently mapped fine/coarse boundary")
	return s6MappedBoundary{}, s6MappedBoundary{}
}

func TestS6H3IndependentMappingBoundaries(t *testing.T) {
	a, b := s6IdentityMappedBoundary(t)
	var dx s6MappedBoundary
	for _, grid := range []string{"FN31", "JO22", "QF56", "PM95", "GG66"} {
		candidate := s6MappedBoundary{grid, EncodeCell(grid), EncodeCoarseCell(grid)}
		if candidate.fine != InvalidCell && candidate.coarse != InvalidCell && candidate.fine != a.fine && candidate.coarse != a.coarse && candidate.coarse != b.coarse {
			dx = candidate
			break
		}
	}
	if dx.fine == InvalidCell || a.fine != b.fine || a.coarse == b.coarse {
		t.Fatalf("invalid independent boundary: a=%+v b=%+v dx=%+v", a, b, dx)
	}
	p, now := NewPredictor(s6PolicyConfig(ReceiverContributionEnforce), []string{"20m"}), s6PolicyNow()
	pathA, pathB := s6PolicyPath{a.fine, dx.fine, a.coarse, dx.coarse}, s6PolicyPath{b.fine, dx.fine, b.coarse, dx.coarse}
	s6PolicyReports(t, p, pathB, 24, 6, -11, 1, now)
	s6PolicyReports(t, p, pathA, 6, 6, -20, 1, now)
	// Same fine population contains both sets of reports, but each independently
	// mapped coarse population must retain only its six or 24 physical reports.
	s6PolicyWant(t, s6PolicyPredict(p, pathA, 0, now, false), -19.5, 3, 6)
	s6PolicyWant(t, s6PolicyPredict(p, pathB, 0, now, false), -10.5, 12, 24)
}
