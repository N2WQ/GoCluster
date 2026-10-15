package pathreliability

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func accountingConfig(mode string, halfLife int) Config {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = mode
	cfg.BandHalfLifeSec = map[string]int{"20m": halfLife}
	cfg.StaleAfterHalfLifeMultiplier = 100000
	cfg.MaxPredictionAgeHalfLifeMultiplier = 100000
	cfg.ReceiverMaxEffectiveCount = 10
	cfg.ReceiverMaxEffectiveWeight = 8
	return cfg
}

func accountingReport(store *Store, observed, arrived int64, weight float64, hash uint64) {
	store.UpdateWithReceiverHash(1, 2, 3, 4, "20m", -5, weight, ObservationTimes{ObservedAt: time.Unix(observed, 0).UTC(), ArrivedAt: time.Unix(arrived, 0).UTC()}, hash)
}

func accountingBucket(store *Store, coarse bool) *bucket {
	key := packKey(1, 2, 0)
	if coarse {
		key = packCoarseKey(3, 4, 0)
	}
	// Used only by serial fixtures after all writers have joined.
	return store.shardForKey(key).buckets[key]
}

func TestObservationAccountingPermutationGolden(t *testing.T) {
	orders := [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
	observations := []int64{0, -200, 10}
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			for _, order := range orders {
				store := NewStore(accountingConfig(mode, 240), []string{"20m"})
				for _, index := range order {
					accountingReport(store, observations[index], 10, 1, 1)
				}
				fine, coarse := store.Lookup(1, 2, 3, 4, "20m", time.Unix(10, 0))
				for _, sample := range []Sample{fine, coarse} {
					if math.Abs(sample.Weight-2.516785807486235) > 1e-12 || sample.RawCount != 3 || sample.AgeSec != 0 || sample.SamplingUnix != 10 {
						t.Fatalf("order=%v sample=%+v", order, sample)
					}
				}
			}
		})
	}
}

func TestObservationAccountingNewestAgeDoesNotAdvanceWithArrival(t *testing.T) {
	store := NewStore(accountingConfig(ReceiverContributionOff, 240), []string{"20m"})
	accountingReport(store, 0, 0, 1, 1)
	accountingReport(store, -300, 50, 1, 1)
	fine, _ := store.Lookup(1, 2, 3, 4, "20m", time.Unix(70, 0))
	if fine.AgeSec != 70 || math.Abs(fine.Weight-1.160446138485072) > 1e-12 {
		t.Fatalf("sample=%+v", fine)
	}
	b := accountingBucket(store, false)
	if b.lastUpdate != 50 || b.newestObservation != 0 {
		t.Fatalf("reference=%d observation=%d", b.lastUpdate, b.newestObservation)
	}
}

func TestObservationAccountingPartialAdmissionGolden(t *testing.T) {
	for _, mode := range []string{ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			cfg := accountingConfig(mode, 240)
			cfg.ReceiverMaxEffectiveCount = 1
			store := NewStore(cfg, []string{"20m"})
			accountingReport(store, 0, 0, 0.75, 1)
			for _, coarse := range []bool{false, true} {
				b := accountingBucket(store, coarse)
				b.cappedCount = 0.75
				b.slots[0].count = 0.75
			}
			accountingReport(store, -240, 0, 2, 1)
			for _, coarse := range []bool{false, true} {
				b := accountingBucket(store, coarse)
				if b.weight != 1.75 || b.cappedWeight != 1.25 || b.cappedCount != 1 || b.slots[0].weight != 1.25 || b.slots[0].count != 1 {
					t.Fatalf("bucket=%+v", b)
				}
				var histogramMass float64
				for _, v := range b.snrBins {
					histogramMass += float64(v)
				}
				want := 1.75
				if mode == ReceiverContributionEnforce {
					want = 1.25
				}
				if histogramMass != want {
					t.Fatalf("histogram mass=%g want=%g", histogramMass, want)
				}
			}
		})
	}
}

func TestObservationAccountingUnusableDoesNotReplaceOrAllocate(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			store := NewStore(accountingConfig(mode, 1), []string{"20m"})
			for hash := uint64(1); hash <= 12; hash++ {
				accountingReport(store, 0, 0, 1, hash)
			}
			fineBefore, coarseBefore := *accountingBucket(store, false), *accountingBucket(store, true)
			var extraBefore [maxCoarseReceiverSlots - inlineReceiverSlots]receiverSlot
			if coarseBefore.extraSlots != nil {
				extraBefore = *coarseBefore.extraSlots.slots
			}
			accountingReport(store, -160, 0, 1, 99)
			for _, c := range []struct {
				b      *bucket
				before bucket
			}{{accountingBucket(store, false), fineBefore}, {accountingBucket(store, true), coarseBefore}} {
				if c.b.count != 13 || c.b.weight != c.before.weight || c.b.cappedWeight != c.before.cappedWeight || c.b.cappedCount != c.before.cappedCount || c.b.slots != c.before.slots {
					t.Fatalf("unusable report changed evidence: before=%+v after=%+v", c.before, c.b)
				}
			}
			if coarseBefore.extraSlots != nil && *coarseBefore.extraSlots.slots != extraBefore {
				t.Fatal("extra receiver replaced")
			}
			empty := NewStore(accountingConfig(mode, 1), []string{"20m"})
			accountingReport(empty, -160, 0, 1, 99)
			b := accountingBucket(empty, true)
			if b.extraSlots != nil || b.slots != [inlineReceiverSlots]receiverSlot{} || b.count != 1 || b.newestObservation != -160 {
				t.Fatalf("unusable report ownership=%+v", b)
			}
			fine, _ := empty.Lookup(1, 2, 3, 4, "20m", time.Unix(0, 0))
			if fine.RawCount != 1 || fine.Weight != 0 || fine.AgeSec != 160 {
				t.Fatalf("count-only lost=%+v", fine)
			}
		})
	}
}

func TestObservationAccountingCapOnlyUnderflowRetainsRawMass(t *testing.T) {
	for _, mode := range []string{ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			cfg := accountingConfig(mode, 1)
			cfg.ReceiverMaxEffectiveCount = 1
			store := NewStore(cfg, []string{"20m"})
			accountingReport(store, 0, 0, 1, 1)
			for _, coarse := range []bool{false, true} {
				b := accountingBucket(store, coarse)
				b.cappedCount = .75
				b.slots[0].count = .75
			}
			before := *accountingBucket(store, false)
			// p=1 and count room=.25: μ32 × .25 rounds to zero.
			accountingReport(store, 0, 0, 0x1p-149, 1)
			b := accountingBucket(store, false)
			if b.count != 2 || b.cappedWeight != before.cappedWeight || b.cappedCount != before.cappedCount || b.slots != before.slots {
				t.Fatalf("cap-only failure admitted=%+v", b)
			}
			// Use an empty raw bin so the retained raw histogram increment is visible.
			store.UpdateWithReceiverHash(1, 2, 3, 4, "20m", 5, 0x1p-149, ObservationTimes{ObservedAt: time.Unix(0, 0), ArrivedAt: time.Unix(0, 0)}, 1)
			want := uint32(0)
			if mode == ReceiverContributionShadow {
				want = 1
			}
			if bits := math.Float32bits(b.snrBins[snrHistogramBinIndex(5)]); bits != want {
				t.Fatalf("raw bin=%08x want=%08x", bits, want)
			}
		})
	}
}

func TestObservationAccountingWeightedFactorUnderflow(t *testing.T) {
	for _, age := range []int64{160, 1075} {
		store := NewStore(accountingConfig(ReceiverContributionEnforce, 1), []string{"20m"})
		weight, want := 0x1p100, 0x1p-60
		if age == 1075 {
			weight, want = 0x1p1000, 0x1p-75
		}
		accountingReport(store, -age, 0, weight, 1)
		b := accountingBucket(store, false)
		if b.weight != want || b.cappedWeight != want || float64(b.snrBins[snrHistogramBinIndex(-5)]) != want {
			t.Fatalf("age=%d bucket=%+v", age, b)
		}
		if age == 1075 && b.cappedCount != 0 {
			t.Fatalf("underflow count=%g", b.cappedCount)
		}
		if age == 1075 {
			before := b.slots
			accountingReport(store, -160, 0, 1, 99)
			if b.slots != before {
				t.Fatal("unusable report cleared a usable receiver without reference advancement")
			}
		}
	}
}

func TestObservationAccountingHistogramOverflowPreservesStateAndFreshness(t *testing.T) {
	store := NewStore(accountingConfig(ReceiverContributionOff, 1), []string{"20m"})
	accountingReport(store, 0, 0, float64(math.MaxFloat32), 1)
	accountingReport(store, 0, 0, 0x1p104, 2)
	b := accountingBucket(store, false)
	if b.count != 2 || b.weight != float64(math.MaxFloat32) || b.snrBins[snrHistogramBinIndex(-5)] != math.MaxFloat32 {
		t.Fatalf("overflow mutated=%+v", b)
	}
	accountingReport(store, 50, 50, math.MaxFloat64, 99)
	if b.count != 3 || b.newestObservation != 50 || b.lastUpdate != 50 {
		t.Fatalf("valid unusable report freshness=%+v", b)
	}
}

func TestObservationAccountingCommonTimeAndCopiedCoarseSlots(t *testing.T) {
	store := NewStore(accountingConfig(ReceiverContributionEnforce, 10), []string{"20m"})
	accountingReport(store, 0, 0, 1, 1)
	// A later reverse report must advance all four candidates at lookup.
	store.UpdateWithReceiverHash(2, 1, 4, 3, "20m", -5, 1, ObservationTimes{ObservedAt: time.Unix(20, 0), ArrivedAt: time.Unix(20, 0)}, 2)
	samples := store.lookupCandidates(1, 2, 3, 4, "20m", time.Unix(10, 0), true)
	for _, s := range samples {
		if s.SamplingUnix != 20 || !s.HasSamplingTime {
			t.Fatalf("mixed sampling=%+v", s)
		}
	}
	if samples[0].Weight != .25 || samples[2].Weight != 1 {
		t.Fatalf("weights=%g,%g", samples[0].Weight, samples[2].Weight)
	}
	for hash := uint64(2); hash <= 12; hash++ {
		accountingReport(store, 20, 20, 1, hash)
	}
	key := packCoarseKey(3, 4, 0)
	capture := store.captureBucket(key, 12, true)
	store.shardForKey(key).mu.Lock()
	clear(accountingBucket(store, true).extraSlots.slots[:])
	store.shardForKey(key).mu.Unlock()
	sample := store.evaluateCapture(&capture, 10, 20, 12, sampleQuantile)
	if sample.CappedReceiverCount != 11 || sample.RecentSupport.Admitted != 11.25 || sample.RecentSupport.RawUsable != 11.25 {
		t.Fatalf("capture retained mutable pointer or qualified aged quarter-unit receiver: %+v", sample.Sample)
	}
}

func TestObservationAccountingConcurrentPopulatedCoarseSlots(t *testing.T) {
	store := NewStore(accountingConfig(ReceiverContributionEnforce, 240), []string{"20m"})
	for hash := uint64(1); hash <= 12; hash++ {
		accountingReport(store, 0, 0, 1, hash)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				if worker%2 == 0 {
					accountingReport(store, int64(i), int64(i), 1, uint64(1+i%12))
				} else {
					samples := store.lookupCandidates(1, 2, 3, 4, "20m", time.Unix(int64(i), 0), true)
					for _, sample := range samples {
						if math.IsNaN(sample.Weight) || math.IsInf(sample.Weight, 0) {
							t.Error("nonfinite sampled weight")
						}
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	if accountingBucket(store, true).extraSlots == nil {
		t.Fatal("fixture failed to populate coarse sidecar")
	}
}

func TestObservationAccountingRetainedBucketBound(t *testing.T) {
	if got := unsafe.Sizeof(bucket{}); got != 456 {
		t.Fatalf("logical bucket size=%d, want approved 456", got)
	}
	if got := unsafe.Sizeof(bucketExtraState{}); got != 8 {
		t.Fatalf("sidecar owner grew=%d", got)
	}
}

func TestObservationAccountingLaggingLookupUsesBothReferences(t *testing.T) {
	store := NewStore(accountingConfig(ReceiverContributionOff, 10), []string{"20m"})
	store.UpdateWithReceiverHash(1, 2, InvalidCell, InvalidCell, "20m", -5, 1, ObservationTimes{ObservedAt: time.Unix(0, 0), ArrivedAt: time.Unix(0, 0)}, 1)
	store.UpdateWithReceiverHash(InvalidCell, InvalidCell, 3, 4, "20m", -5, 1, ObservationTimes{ObservedAt: time.Unix(20, 0), ArrivedAt: time.Unix(20, 0)}, 1)
	fine, coarse := store.Lookup(1, 2, 3, 4, "20m", time.Unix(10, 0))
	if fine.SamplingUnix != 20 || coarse.SamplingUnix != 20 || fine.Weight != .25 || coarse.Weight != 1 || fine.AgeSec != 20 || coarse.AgeSec != 0 {
		t.Fatalf("mixed lookup fine=%+v coarse=%+v", fine, coarse)
	}
}

func TestObservationAccountingLostHistogramCannotProduceSurvivingMedian(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			cfg := accountingConfig(mode, 1)
			cfg.MinObservationCount = 1
			cfg.BeaconMinObservationCount = 1
			cfg.MinEffectiveWeight = .01
			p := NewPredictor(cfg, []string{"20m"})
			p.UpdateWithReceiverHash(BucketCombined, 1, 2, InvalidCell, InvalidCell, "20m", -20, 0x1p-149, ObservationTimes{ObservedAt: time.Unix(0, 0), ArrivedAt: time.Unix(0, 0)}, false, 1)
			p.UpdateWithReceiverHash(BucketCombined, 2, 1, InvalidCell, InvalidCell, "20m", 10, 1, ObservationTimes{ObservedAt: time.Unix(1, 0), ArrivedAt: time.Unix(1, 0)}, false, 2)
			for _, beacon := range []bool{false, true} {
				result := p.Predict(1, 2, InvalidCell, InvalidCell, "20m", "FT8", 0, time.Unix(1, 0))
				if beacon {
					result = p.PredictBeaconReceiveOnlyWithMinObservationCount(1, 2, InvalidCell, InvalidCell, "20m", "FT8", 0, 1, time.Unix(1, 0))
				}
				if result.HasP50 || result.ObservedDistributionAvailable || !result.HasObservedSamplingTime || result.ObservedSamplingUnix != 1 || result.RawCount == 0 {
					t.Fatalf("beacon=%v unsafe component supplied median=%+v", beacon, result)
				}
			}
		})
	}
}

func TestObservationAccountingReferenceCannotRewind(t *testing.T) {
	store := NewStore(accountingConfig(ReceiverContributionOff, 10), []string{"20m"})
	accountingReport(store, 20, 20, 1, 1)
	accountingReport(store, 0, 0, 1, 1)
	b := accountingBucket(store, false)
	if b.lastUpdate != 20 || b.newestObservation != 20 || b.weight != 1.25 {
		t.Fatalf("lagging arrival rewound accounting=%+v", b)
	}
}

func TestObservationAccountingInsufficientFloorStillReportsQuantile(t *testing.T) {
	cfg := accountingConfig(ReceiverContributionEnforce, 240)
	cfg.MinObservationCount = 2
	cfg.BeaconMinObservationCount = 2
	cfg.MinEffectiveWeight = .01
	p := NewPredictor(cfg, []string{"20m"})
	p.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", -5, 1, ObservationTimes{ObservedAt: time.Unix(0, 0), ArrivedAt: time.Unix(0, 0)}, false, 1)
	for _, beacon := range []bool{false, true} {
		result := p.Predict(1, 2, 3, 4, "20m", "FT8", 0, time.Unix(0, 0))
		if beacon {
			result = p.PredictBeaconReceiveOnlyWithMinObservationCount(1, 2, 3, 4, "20m", "FT8", 0, 2, time.Unix(0, 0))
		}
		if result.Source != SourceInsufficient || result.InsufficientReason != InsufficientLowCount || !result.HasP50 || !result.ObservedDistributionAvailable || result.P50DB != -4.5 || result.P50Glyph == "" || result.Glyph != cfg.GlyphSymbols.Insufficient {
			t.Fatalf("beacon=%v floor hid usable quantile=%+v", beacon, result)
		}
	}
}

func TestObservationAccountingRemovedFineCoarseAdditionCannotOverflow(t *testing.T) {
	cfg := accountingConfig(ReceiverContributionOff, 240)
	cfg.MinObservationCount = 1
	cfg.MinEffectiveWeight = .01
	cfg.MergeReceiveWeight = .4
	cfg.MergeTransmitWeight = .6
	p := NewPredictor(cfg, []string{"20m"})
	now := time.Unix(0, 0)
	for i := 0; i < 6; i++ {
		accountingReport(p.combined, 0, 0, 1, 1)
	}
	for i := 0; i < 4; i++ {
		p.combined.UpdateWithReceiverHash(InvalidCell, InvalidCell, 3, 4, "20m", -5, 1, ObservationTimes{ObservedAt: now, ArrivedAt: now}, 1)
	}
	p.UpdateWithReceiverHash(BucketCombined, 2, 1, 4, 3, "20m", 10, 1, ObservationTimes{ObservedAt: now, ArrivedAt: now}, false, 2)
	accountingBucket(p.combined, false).snrBins[snrHistogramBinIndex(-5)] = math.MaxFloat32
	accountingBucket(p.combined, true).snrBins[snrHistogramBinIndex(-5)] = math.MaxFloat32
	result := p.Predict(1, 2, 3, 4, "20m", "FT8", 0, now)
	// Fine is excluded, so its large bin is never added to the coarse bin.
	// Remaining shift/directional failure boundaries have separate witnesses.
	if !result.HasP50 || !result.ObservedDistributionAvailable || result.P50DB != -4.5 || result.RawCount != 11 {
		t.Fatalf("excluded fine introduced a removed arithmetic failure=%+v", result)
	}
}

func TestObservationAccountingConnectedLiteralHistogramOracles(t *testing.T) {
	for _, twoDirections := range []bool{false, true} {
		t.Run(map[bool]string{false: "selected_coarse_only", true: "merge_bins_not_medians"}[twoDirections], func(t *testing.T) {
			cfg := accountingConfig(ReceiverContributionOff, 240)
			cfg.MinObservationCount = 1
			cfg.MinEffectiveWeight = .01
			if twoDirections {
				cfg.FineOnlyWeight = 100
				cfg.MergeReceiveWeight = .4
				cfg.MergeTransmitWeight = .6
			}
			p := NewPredictor(cfg, []string{"20m"})
			now := time.Unix(0, 0)
			add := func(rx, tx, rc, tc CellID, db float64, count int) {
				for i := 0; i < count; i++ {
					p.Update(BucketCombined, rx, tx, rc, tc, "20m", db, 1, ObservationTimes{ObservedAt: now, ArrivedAt: now}, false)
				}
			}
			wantP50, wantWeight, wantCount := -19.5, 8.0, uint32(16)
			if !twoDirections {
				add(1, 2, 3, 4, 18, 6)
				add(InvalidCell, InvalidCell, 3, 4, -20, 10)
				// Coarse contains six strong and ten weak reports. Fine
				// contributes no additional mass to the selected population.
			} else {
				add(1, 2, 3, 4, -20, 6)
				add(InvalidCell, InvalidCell, 3, 4, 10, 4)
				add(2, 1, 4, 3, 10, 8)
				add(InvalidCell, InvalidCell, 4, 3, -20, 12)
				// Coarse receive: six weak/four strong; transmit: twelve
				// weak/eight strong. Final .4/.6 mass is 9.6 weak/6.4 strong.
				wantP50, wantWeight, wantCount = -19.5, 16, 30
			}
			result := p.Predict(1, 2, 3, 4, "20m", "FT8", 0, now)
			if !result.HasP50 || result.P50DB != wantP50 || math.Abs(result.Weight-wantWeight) > 1e-12 || result.RawCount != wantCount || !result.ObservedDistributionAvailable {
				t.Fatalf("literal oracle mismatch=%+v; want p50=%g weight=%g count=%d", result, wantP50, wantWeight, wantCount)
			}
		})
	}
}

func TestObservationAccountingConnectedShiftAndScaleFailures(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			cfg := accountingConfig(mode, 240)
			cfg.MinObservationCount = 1
			cfg.BeaconMinObservationCount = 1
			cfg.MinEffectiveWeight = .01
			cfg.ReceiverMaxEffectiveWeight = float64(math.MaxFloat32)
			now := time.Unix(0, 0)
			times := ObservationTimes{ObservedAt: now, ArrivedAt: now}
			shift := NewPredictor(cfg, []string{"20m"})
			shift.UpdateWithReceiverHash(BucketCombined, 1, 2, InvalidCell, InvalidCell, "20m", -23, float64(math.MaxFloat32), times, false, 1)
			shift.UpdateWithReceiverHash(BucketCombined, 1, 2, InvalidCell, InvalidCell, "20m", -22, float64(math.MaxFloat32), times, false, 2)
			shift.UpdateWithReceiverHash(BucketCombined, 2, 1, InvalidCell, InvalidCell, "20m", 10, 1, times, false, 3)
			for _, beacon := range []bool{false, true} {
				result := shift.Predict(1, 2, InvalidCell, InvalidCell, "20m", "FT8", 100, now)
				if beacon {
					result = shift.PredictBeaconReceiveOnlyWithMinObservationCount(1, 2, InvalidCell, InvalidCell, "20m", "FT8", 100, 1, now)
				}
				if result.HasP50 || result.ObservedDistributionAvailable || result.RawCount == 0 {
					t.Fatalf("beacon=%v shift overflow produced observed median=%+v", beacon, result)
				}
			}
			cfg.MergeReceiveWeight = .25
			cfg.MergeTransmitWeight = .75
			scaled := NewPredictor(cfg, []string{"20m"})
			scaled.UpdateWithReceiverHash(BucketCombined, 1, 2, InvalidCell, InvalidCell, "20m", -20, 0x1p-149, times, false, 1)
			scaled.UpdateWithReceiverHash(BucketCombined, 2, 1, InvalidCell, InvalidCell, "20m", 10, 1, times, false, 2)
			result := scaled.Predict(1, 2, InvalidCell, InvalidCell, "20m", "FT8", 0, now)
			if result.HasP50 || result.ObservedDistributionAvailable || result.RawCount != 2 {
				t.Fatalf("underflowed needed component produced median=%+v", result)
			}
			beacon := scaled.PredictBeaconReceiveOnlyWithMinObservationCount(1, 2, InvalidCell, InvalidCell, "20m", "FT8", 0, 1, now)
			if !beacon.HasP50 || !beacon.ObservedDistributionAvailable || beacon.P50DB != -19.5 || beacon.InsufficientReason != InsufficientLowWeight {
				t.Fatalf("unscaled beacon lost usable diagnostic quantile=%+v", beacon)
			}
		})
	}
}

func TestObservationAccountingQuantileProbeUsesSelectedBandHalfLife(t *testing.T) {
	cfg := accountingConfig(ReceiverContributionOff, 240)
	cfg.BandHalfLifeSec = map[string]int{"20m": 20, "40m": 10}
	store := NewStore(cfg, []string{"20m", "40m"})
	now := time.Unix(0, 0)
	store.UpdateWithReceiverHash(1, 2, 3, 4, "40m", -6, 2, ObservationTimes{ObservedAt: now, ArrivedAt: now}, 1)
	fine, coarse := store.lookupWithDistribution(1, 2, 3, 4, "40m", time.Unix(10, 0))
	for _, sample := range []sampleWithBins{fine, coarse} {
		if sample.Weight != 1 || !sample.HasP50 || sample.P50DB != -5.5 || sample.SamplingUnix != 10 {
			t.Fatalf("selected band half-life ignored=%+v", sample)
		}
	}
}

func TestObservationAccountingAllStaleCountsRemainSeparate(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		for _, query := range []int64{20, 80} {
			t.Run(fmt.Sprintf("%s/query=%d", mode, query), func(t *testing.T) {
				cfg := accountingConfig(mode, 10)
				cfg.ReceiverMaxEffectiveCount = 5
				cfg.MinObservationCount = 1
				cfg.BeaconMinObservationCount = 1
				cfg.MaxPredictionAgeHalfLifeMultiplier = 1
				p := NewPredictor(cfg, []string{"20m"})
				for i := 0; i < 19; i++ {
					accountingReport(p.combined, 0, 0, 1, 1)
				}
				wantCapped := uint32(1)
				if query == 80 {
					wantCapped = 0
				}
				if mode == ReceiverContributionOff {
					wantCapped = 19
				}
				wantActive := uint32(19)
				if mode == ReceiverContributionEnforce {
					wantActive = wantCapped
				}
				for _, beacon := range []bool{false, true} {
					result := p.Predict(1, 2, 3, 4, "20m", "FT8", 0, time.Unix(query, 0))
					if beacon {
						result = p.PredictBeaconReceiveOnlyWithMinObservationCount(1, 2, 3, 4, "20m", "FT8", 0, 1, time.Unix(query, 0))
					}
					if result.RawCount != 19 || result.ObservationCount != 19 || result.Count != wantActive || result.CappedCount != wantCapped || result.Source != SourceInsufficient || result.InsufficientReason != InsufficientStale || result.Weight != 0 || result.HasP50 || result.ObservedDistributionAvailable || result.ObservedSamplingUnix != query {
						t.Fatalf("beacon=%v stale diagnostics=%+v", beacon, result)
					}
				}
			})
		}
	}
	var counts staleSampleCounts
	counts.add(Sample{Count: 2, RawCount: maxBucketObservationCount, ObservationCount: maxBucketObservationCount, CappedCount: 1})
	counts.add(Sample{Count: 3, RawCount: 11, ObservationCount: 11, CappedCount: 3})
	if counts.active != 5 || counts.raw != maxBucketObservationCount || counts.observations != maxBucketObservationCount || counts.capped != 4 {
		t.Fatalf("stale sums=%+v", counts)
	}
}

func TestObservationAccountingInvalidSourceCannotBeHiddenByAging(t *testing.T) {
	p := NewPredictor(accountingConfig(ReceiverContributionOff, 10), []string{"20m"})
	accountingReport(p.combined, 0, 0, 1, 1)
	for _, coarse := range []bool{false, true} {
		b := accountingBucket(p.combined, coarse)
		b.snrBins = snrHistogram{}
		b.snrBins[precisionWeakBin] = -math.SmallestNonzeroFloat32
		b.snrBins[precisionStrongBin] = 1
	}
	for _, beacon := range []bool{false, true} {
		result := p.Predict(1, 2, 3, 4, "20m", "FT8", 0, time.Unix(10, 0))
		if beacon {
			result = p.PredictBeaconReceiveOnlyWithMinObservationCount(1, 2, 3, 4, "20m", "FT8", 0, 1, time.Unix(10, 0))
		}
		if result.HasP50 || result.ObservedDistributionAvailable || result.RawCount != 1 {
			t.Fatalf("beacon=%v aging hid unsafe source=%+v", beacon, result)
		}
	}
}
