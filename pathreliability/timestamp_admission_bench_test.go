package pathreliability

import (
	"math"
	"testing"
	"time"
)

func BenchmarkObservationAdmissionStore(b *testing.B) {
	for _, delay := range []time.Duration{0, time.Second, 30 * time.Second} {
		b.Run(delay.String(), func(b *testing.B) {
			cfg := DefaultConfig()
			cfg.ReceiverContributionMode = ReceiverContributionOff
			s := NewStore(cfg, []string{"20m"})
			observed := time.Unix(1000000, 0).UTC()
			times := ObservationTimes{ObservedAt: observed, ArrivedAt: observed.Add(delay)}
			s.UpdateWithReceiverHash(1, 2, 3, 4, "20m", -5, 1, times, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				s.UpdateWithReceiverHash(1, 2, 3, 4, "20m", -5, 1, times, 1)
			}
			b.StopTimer()
			fine, _ := s.Lookup(1, 2, 3, 4, "20m", times.ArrivedAt)
			if fine.RawCount != uint32(b.N+1) || !(fine.RawWeight > 0) || math.IsInf(fine.RawWeight, 0) {
				b.Fatalf("benchmark did not admit finite raw evidence: %+v", fine)
			}
		})
	}
}

func BenchmarkObservationAdmissionPredictor(b *testing.B) {
	for _, delay := range []time.Duration{0, time.Second, 30 * time.Second} {
		b.Run(delay.String(), func(b *testing.B) {
			cfg := DefaultConfig()
			cfg.ReceiverContributionMode = ReceiverContributionOff
			p := NewPredictor(cfg, []string{"20m"})
			observed := time.Unix(1000000, 0).UTC()
			times := ObservationTimes{ObservedAt: observed, ArrivedAt: observed.Add(delay)}
			p.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", -5, 1, times, false, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				p.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", -5, 1, times, false, 1)
			}
			b.StopTimer()
			fine, _ := p.combined.Lookup(1, 2, 3, 4, "20m", times.ArrivedAt)
			if fine.RawCount != uint32(b.N+1) || !(fine.RawWeight > 0) || math.IsInf(fine.RawWeight, 0) {
				b.Fatalf("benchmark did not admit finite raw evidence: %+v", fine)
			}
		})
	}
}
