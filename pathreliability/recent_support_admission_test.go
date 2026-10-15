package pathreliability

import (
	"math"
	"testing"
	"time"
)

func recentSupportBothBuckets(t *testing.T, store *Store, check func(bucket)) {
	t.Helper()
	check(recentSupportBucket(t, store, false))
	check(recentSupportBucket(t, store, true))
}

func recentSupportStoredWant(t *testing.T, store *Store, mode string, raw, admitted float64, lifetime uint32) {
	t.Helper()
	recentSupportBothBuckets(t, store, func(b bucket) {
		wantAdmitted := admitted
		if mode == ReceiverContributionOff {
			wantAdmitted = 0
		}
		if b.recentRawUsable != raw || b.cappedCount != wantAdmitted || b.count != lifetime {
			t.Fatalf("stored units raw=%g admitted=%g lifetime=%d, want %g/%g/%d", b.recentRawUsable, b.cappedCount, b.count, raw, wantAdmitted, lifetime)
		}
	})
}

func TestRecentSupportAdmission(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			t.Run("precap_rejected", func(t *testing.T) {
				store := NewStore(recentSupportConfig(mode, 1), []string{"20m"})
				// 2^-160 is a positive float64 contribution but rounds to zero
				// in the binary32 histogram. Neither kind of support is earned.
				recentSupportReport(t, store, 0, 160, 1, 1)
				recentSupportStoredWant(t, store, mode, 0, 0, 1)
				recentSupportBothBuckets(t, store, func(b bucket) {
					if b.weight != 0 || b.cappedWeight != 0 || b.extraSlots != nil || b.slots != ([inlineReceiverSlots]receiverSlot{}) || b.lastUpdate != 160 || b.newestObservation != 0 {
						t.Fatalf("precap rejection acquired signal/receivers or lost valid clocks: %+v", b)
					}
				})
			})
			t.Run("raw_sum_overflow", func(t *testing.T) {
				store := NewStore(recentSupportConfig(mode, 10), []string{"20m"})
				recentSupportReport(t, store, 0, 0, 1, 1)
				for _, key := range []uint64{0x100020000, 0x30004} {
					recentSupportMutate(t, store, key, func(b *bucket) { b.weight = math.MaxFloat64 })
				}
				recentSupportReport(t, store, 0, 0, math.MaxFloat64, 1)
				recentSupportStoredWant(t, store, mode, 1, 1, 2)
				recentSupportBothBuckets(t, store, func(b bucket) {
					if b.weight != math.MaxFloat64 || b.snrBins[14] != 1 {
						t.Fatalf("unsafe raw sum changed numerical state: %+v", b)
					}
				})
			})
			t.Run("raw_histogram_stage", func(t *testing.T) {
				store := NewStore(recentSupportConfig(mode, 10), []string{"20m"})
				recentSupportReport(t, store, 0, 0, 1, 1)
				for _, key := range []uint64{0x100020000, 0x30004} {
					recentSupportMutate(t, store, key, func(b *bucket) { b.snrBins[14] = math.MaxFloat32 })
				}
				recentSupportReport(t, store, 0, 0, 0x1p104, 1)
				wantRaw := float64(1)
				if mode == ReceiverContributionEnforce {
					// Enforce checks representability here, not raw-bin addition.
					// Its subsequent cap fraction is too small to be admitted.
					wantRaw = 2
				}
				recentSupportStoredWant(t, store, mode, wantRaw, 1, 2)
				recentSupportBothBuckets(t, store, func(b bucket) {
					wantWeight := float64(1)
					if mode == ReceiverContributionEnforce {
						wantWeight = 0x1p104 // the old unit is absorbed by float64 addition.
					}
					if b.weight != wantWeight || b.snrBins[14] != math.MaxFloat32 {
						t.Fatalf("mode-specific numerical stage changed: %+v", b)
					}
				})
			})
			t.Run("rounded_bin_unchanged", func(t *testing.T) {
				cfg := recentSupportConfig(mode, 10)
				cfg.ReceiverMaxEffectiveWeight = 0x1p30
				store := NewStore(cfg, []string{"20m"})
				recentSupportReport(t, store, 0, 0, 0x1p25, 1)
				recentSupportReport(t, store, 0, 0, 1, 1)
				recentSupportStoredWant(t, store, mode, 2, 2, 2)
				recentSupportBothBuckets(t, store, func(b bucket) {
					if b.weight != 0x1p25+1 || b.snrBins[14] != 0x1p25 {
						t.Fatalf("representable but absorbed increment was treated as unusable: %+v", b)
					}
				})
			})
			t.Run("unit_count_underflow", func(t *testing.T) {
				store := NewStore(recentSupportConfig(mode, 1), []string{"20m"})
				// Robust weighted mass survives after the unweighted 2^-1075
				// report factor underflows. Signal cannot manufacture support.
				recentSupportReport(t, store, 0, 1075, 0x1p1000, 1)
				recentSupportStoredWant(t, store, mode, 0, 0, 1)
				fine, coarse := store.Lookup(1, 2, 3, 4, "20m", recentSupportTime(1075))
				for _, got := range []Sample{fine, coarse} {
					recentSupportWant(t, got.RecentSupport, mode, 0, 0)
					if got.Weight != 0x1p-75 || got.CappedReceiverCount != 0 {
						t.Fatalf("weighted mass and report underflow were conflated: %+v", got)
					}
				}
			})
			if mode == ReceiverContributionOff {
				return // Off does not execute cap admission; no skipped cases.
			}
			t.Run("cap_rejected", func(t *testing.T) {
				cfg := recentSupportConfig(mode, 10)
				cfg.ReceiverMaxEffectiveCount = 1
				store := NewStore(cfg, []string{"20m"})
				recentSupportReport(t, store, 0, 0, 1, 1)
				recentSupportReport(t, store, 0, 0, 1, 1)
				recentSupportStoredWant(t, store, mode, 2, 1, 2)
				recentSupportBothBuckets(t, store, func(b bucket) {
					if b.weight != 2 || b.cappedWeight != 1 || b.slots[0].count != 1 {
						t.Fatalf("cap-only rejection lost usable raw evidence: %+v", b)
					}
				})
			})
			t.Run("quarter_admitted", func(t *testing.T) {
				cfg := recentSupportConfig(mode, 10)
				cfg.ReceiverMaxEffectiveWeight = 1
				store := NewStore(cfg, []string{"20m"})
				recentSupportReport(t, store, 0, 0, .75, 1)
				recentSupportReport(t, store, 0, 0, 1, 1)
				recentSupportStoredWant(t, store, mode, 2, 1.25, 2)
				recentSupportBothBuckets(t, store, func(b bucket) {
					if b.weight != 1.75 || b.cappedWeight != 1 || b.slots[0].count != 1.25 {
						t.Fatalf("quarter admission scaled raw units or signal incorrectly: %+v", b)
					}
				})
				fine, _ := store.Lookup(1, 2, 3, 4, "20m", recentSupportTime(0))
				recentSupportWant(t, fine.RecentSupport, mode, 2, 1.25)
				if fine.CappedCount != 1 {
					t.Fatalf("legacy capped integer changed meaning: %+v", fine)
				}
			})
			t.Run("scaled_underflow", func(t *testing.T) {
				cfg := recentSupportConfig(mode, 10)
				cfg.ReceiverMaxEffectiveCount = 1
				store := NewStore(cfg, []string{"20m"})
				recentSupportReport(t, store, 10, 20, 1, 1)
				recentSupportReport(t, store, 0, 20, 1, 1)
				a := store.UpdateWithReceiverHash(1, 2, 3, 4, "20m", 4.5, 0x1p-149,
					ObservationTimes{ObservedAt: recentSupportTime(20), ArrivedAt: recentSupportTime(20)}, 1)
				if !a.Accepted() {
					t.Fatalf("scaled-underflow input was rejected: %+v", a)
				}
				recentSupportStoredWant(t, store, mode, 1.75, .75, 3)
				recentSupportBothBuckets(t, store, func(b bucket) {
					wantBin := uint32(1) // smallest binary32 survives the raw stage.
					if mode == ReceiverContributionEnforce {
						wantBin = 0 // quarter of that increment is unrepresentable.
					}
					if b.cappedWeight != .75 || b.slots[0].count != .75 || math.Float32bits(b.snrBins[29]) != wantBin {
						t.Fatalf("postcap underflow earned admitted units or lost raw support: %+v", b)
					}
				})
			})
			if mode == ReceiverContributionEnforce {
				t.Run("capped_histogram_rejected", func(t *testing.T) {
					cfg := recentSupportConfig(mode, 10)
					cfg.ReceiverMaxEffectiveWeight = math.MaxFloat64
					store := NewStore(cfg, []string{"20m"})
					recentSupportReport(t, store, 0, 0, 1, 1)
					for _, key := range []uint64{0x100020000, 0x30004} {
						recentSupportMutate(t, store, key, func(b *bucket) { b.snrBins[14] = math.MaxFloat32 })
					}
					recentSupportReport(t, store, 0, 0, 0x1p104, 1)
					recentSupportStoredWant(t, store, mode, 2, 1, 2)
					recentSupportBothBuckets(t, store, func(b bucket) {
						if b.weight != 0x1p104 || b.cappedWeight != 1 || b.slots[0].count != 1 || b.snrBins[14] != math.MaxFloat32 {
							t.Fatalf("active-bin failure was moved before raw support or partially committed: %+v", b)
						}
					})
				})
			}
		})
	}
}

func TestRecentSupportFreshness(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			t.Run("usable_rejected", func(t *testing.T) {
				store := NewStore(recentSupportConfig(mode, 10), []string{"20m"})
				recentSupportReport(t, store, 0, 0, 1, 1)
				recentSupportReport(t, store, 10, 10, 0x1p-160, 1)
				recentSupportStoredWant(t, store, mode, .5, .5, 2)
				fine, _ := store.Lookup(1, 2, 3, 4, "20m", recentSupportTime(10))
				recentSupportWant(t, fine.RecentSupport, mode, .5, .5)
				if fine.AgeSec != 0 || fine.Weight != .5 || fine.SamplingUnix != 10 {
					t.Fatalf("valid rejected report restored support or failed to advance freshness: %+v", fine)
				}
			})
			t.Run("invalid_time", func(t *testing.T) {
				store := NewStore(recentSupportConfig(mode, 10), []string{"20m"})
				recentSupportReport(t, store, 100, 100, 1, 1)
				beforeFine, beforeCoarse := recentSupportBucket(t, store, false), recentSupportBucket(t, store, true)
				for _, times := range []ObservationTimes{
					{ObservedAt: recentSupportTime(110).Add(120*time.Second + time.Nanosecond), ArrivedAt: recentSupportTime(110)},
					{ObservedAt: recentSupportTime(110)},
				} {
					if got := store.UpdateWithReceiverHash(1, 2, 3, 4, "20m", -10.5, 1, times, 1); got.Status != AdmissionRejectedTime {
						t.Fatalf("invalid timestamp admitted: %+v", got)
					}
				}
				if recentSupportBucket(t, store, false) != beforeFine || recentSupportBucket(t, store, true) != beforeCoarse || store.admissions.snapshot().RejectedTime != 2 {
					t.Fatal("invalid timestamps changed bucket support, clocks, or rejection accounting")
				}
			})
			t.Run("epoch_zero", func(t *testing.T) {
				store := NewStore(recentSupportConfig(mode, 10), []string{"20m"})
				recentSupportReport(t, store, 0, 0, 1, 1)
				fine, _ := store.Lookup(1, 2, 3, 4, "20m", recentSupportTime(0))
				recentSupportWant(t, fine.RecentSupport, mode, 1, 1)
				if !fine.HasSamplingTime || fine.SamplingUnix != 0 || fine.AgeSec != 0 {
					t.Fatalf("Unix epoch was treated as missing observation: %+v", fine)
				}
			})
			t.Run("clamped_future", func(t *testing.T) {
				store := NewStore(recentSupportConfig(mode, 10), []string{"20m"})
				a := store.UpdateWithReceiverHash(1, 2, 3, 4, "20m", -10.5, 1,
					ObservationTimes{ObservedAt: recentSupportTime(220), ArrivedAt: recentSupportTime(100)}, 1)
				if a.Status != AdmissionClampedFuture || !a.EffectiveObservation.Equal(recentSupportTime(100)) {
					t.Fatalf("inclusive future boundary changed: %+v", a)
				}
				recentSupportStoredWant(t, store, mode, 1, 1, 1)
				recentSupportBothBuckets(t, store, func(b bucket) {
					if b.newestObservation != 100 || b.lastUpdate != 100 || b.weight != 1 {
						t.Fatalf("clamped report used its future clock: %+v", b)
					}
				})
			})
			if mode != ReceiverContributionOff {
				t.Run("cap_rejected", func(t *testing.T) {
					cfg := recentSupportConfig(mode, 10)
					cfg.ReceiverMaxEffectiveCount = 1
					store := NewStore(cfg, []string{"20m"})
					recentSupportReport(t, store, 0, 0, 1, 1)
					// The remaining weight admits a fraction below the existing
					// cap-admission epsilon, despite ample representable raw mass.
					recentSupportReport(t, store, 10, 10, 1e10, 1)
					recentSupportStoredWant(t, store, mode, 1.5, .5, 2)
					fine, _ := store.Lookup(1, 2, 3, 4, "20m", recentSupportTime(10))
					recentSupportWant(t, fine.RecentSupport, mode, 1.5, .5)
					if fine.AgeSec != 0 || fine.CappedWeight != .5 {
						t.Fatalf("cap rejection changed freshness/support separation: %+v", fine)
					}
				})
			}
		})
	}
}

func TestRecentSupportPopulation(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode+"/no_support_retry", func(t *testing.T) {
			cfg := recentSupportConfig(mode, 10)
			cfg.MinObservationCount, cfg.ReceiverMaxEffectiveWeight = 21, 64
			p := NewPredictor(cfg, []string{"20m"})
			for _, direction := range [][4]CellID{{1, 2, 3, 4}, {2, 1, 4, 3}} {
				a := p.UpdateWithReceiverHash(BucketCombined, direction[0], direction[1], direction[2], direction[3], "20m", -10.5, 32,
					ObservationTimes{ObservedAt: recentSupportTime(0), ArrivedAt: recentSupportTime(0)}, false, 100)
				if !a.Accepted() {
					t.Fatalf("fine selection seed rejected: %+v", a)
				}
				recentSupportPredictorReports(t, p, direction[2], direction[3], 24, 1, 0)
			}
			got := p.Predict(1, 2, 3, 4, "20m", "FT8", 0, recentSupportTime(0))
			recentSupportWant(t, got.ObservedRecentSupport, mode, 2, 2)
			if got.Source != SourceInsufficient || got.InsufficientReason != InsufficientLowCount || got.RawCount != 2 || got.Weight != 32 || !got.HasP50 || got.P50DB != -10.5 {
				t.Fatalf("failed support retried a funded coarser population: %+v", got)
			}
		})
		t.Run(mode+"/overlapping_layers", func(t *testing.T) {
			cfg := recentSupportConfig(mode, 10)
			cfg.MinObservationCount = 19
			p := NewPredictor(cfg, []string{"20m"})
			for i := 0; i < 13; i++ {
				fineRX, fineTX := CellID(1), CellID(2)
				if i >= 6 {
					fineRX, fineTX = 5, 6
				}
				a := p.UpdateWithReceiverHash(BucketCombined, fineRX, fineTX, 3, 4, "20m", -10.5, 1,
					ObservationTimes{ObservedAt: recentSupportTime(0), ArrivedAt: recentSupportTime(0)}, false, uint64(1+i%6))
				if !a.Accepted() {
					t.Fatalf("overlap seed rejected: %+v", a)
				}
			}
			got := p.Predict(1, 2, 3, 4, "20m", "FT8", 0, recentSupportTime(0))
			recentSupportWant(t, got.ObservedRecentSupport, mode, 13, 13)
			if got.Source != SourceInsufficient || got.InsufficientReason != InsufficientLowCount || got.RawCount != 13 || got.Weight != 6.5 || !got.HasP50 {
				t.Fatalf("fine subset was added to its selected coarse population: %+v", got)
			}
		})
	}
}
