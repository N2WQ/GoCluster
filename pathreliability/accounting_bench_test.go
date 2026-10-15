package pathreliability

import (
	"fmt"
	"testing"
	"time"
)

func benchmarkAccountingUpdate(store *Store, observed, arrived time.Time, weight float64, hash uint64) {
	store.UpdateWithReceiverHash(1, 2, 3, 4, "20m", -5, weight, ObservationTimes{ObservedAt: observed, ArrivedAt: arrived}, hash)
}

// These fixtures keep the measured admission path active; the older four-hash
// loops settle into cap rejection and cannot establish its allocation/cost.
func BenchmarkAccountingAdmission(b *testing.B) {
	for _, mode := range []string{ReceiverContributionShadow, ReceiverContributionEnforce} {
		for _, partial := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/partial=%v", mode, partial), func(b *testing.B) {
				cfg := DefaultConfig()
				cfg.ReceiverContributionMode = mode
				cfg.BandHalfLifeSec = map[string]int{"20m": 240}
				if partial {
					cfg.ReceiverMaxEffectiveCount = 1
					cfg.ReceiverMaxEffectiveWeight = 8
				}
				store := NewStore(cfg, []string{"20m"})
				start := time.Unix(1_700_000_000, 0).UTC()
				for hash := uint64(1); hash <= 12; hash++ {
					benchmarkAccountingUpdate(store, start, start, 1, hash)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if partial {
						at := start.Add(time.Duration(i+1) * 240 * time.Second)
						benchmarkAccountingUpdate(store, at, at, 2, 1)
					} else {
						benchmarkAccountingUpdate(store, start, start, 1, uint64(i)+100)
					}
				}
				b.StopTimer()
				key := packCoarseKey(3, 4, 0)
				state := store.shardForKey(key).buckets[key]
				if state == nil || state.extraSlots == nil || state.extraSlots.slots == nil {
					b.Fatal("coarse sidecar not populated")
				}
				if state.count != uint32(b.N+12) {
					b.Fatalf("raw count=%d, want %d", state.count, b.N+12)
				}
				if partial {
					if state.slots[0].count != 1 || state.slots[0].weight < 1.5 || state.slots[0].weight > 2 {
						b.Fatalf("fixture stopped exercising partial admission: %+v", state.slots[0])
					}
				} else if state.cappedWeight != float64(b.N+12) {
					b.Fatalf("successful mass=%g want=%d", state.cappedWeight, b.N+12)
				}
			})
		}
	}
}

func BenchmarkAccountingPopulatedPrediction(b *testing.B) {
	for _, beacon := range []bool{false, true} {
		b.Run(fmt.Sprintf("beacon=%v", beacon), func(b *testing.B) {
			cfg := DefaultConfig()
			cfg.ReceiverContributionMode = ReceiverContributionEnforce
			cfg.MinEffectiveWeight = .1
			cfg.MinObservationCount = 1
			cfg.BeaconMinObservationCount = 1
			p := NewPredictor(cfg, []string{"20m"})
			now := time.Unix(1_700_000_000, 0).UTC()
			for hash := uint64(1); hash <= 12; hash++ {
				p.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", -5, 1, ObservationTimes{ObservedAt: now, ArrivedAt: now}, false, hash)
				p.UpdateWithReceiverHash(BucketCombined, 2, 1, 4, 3, "20m", -7, 1, ObservationTimes{ObservedAt: now, ArrivedAt: now}, false, hash)
			}
			b.ReportAllocs()
			b.ResetTimer()
			var result Result
			for i := 0; i < b.N; i++ {
				if beacon {
					result = p.PredictBeaconReceiveOnlyWithMinObservationCount(1, 2, 3, 4, "20m", "FT8", 0, 1, now)
				} else {
					result = p.Predict(1, 2, 3, 4, "20m", "FT8", 0, now)
				}
			}
			b.StopTimer()
			wantRaw := uint32(24)
			if beacon {
				wantRaw = 12
			}
			if !result.HasP50 || result.P50DB != -4.5 || result.ReceiverCount == 0 || result.RawCount != wantRaw || result.Source != SourceCombined || !result.ObservedDistributionAvailable || !result.HasObservedSamplingTime || result.ObservedSamplingUnix != now.Unix() {
				b.Fatalf("unavailable populated prediction: %+v", result)
			}
		})
	}
}
