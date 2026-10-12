package spot

import (
	"testing"
	"unsafe"
)

var supportBasisBenchmarkSink ResolverSupportBasis
var supportCompactBenchmarkSink ResolverSnapshot

func BenchmarkResolverSupportBasis(b *testing.B) {
	for _, tc := range []struct {
		name string
		s    ResolverSnapshot
		want ResolverSupportBasis
	}{
		{"legacy_positive", ResolverSnapshot{TotalWeightedSupportMilli: 1000}, ResolverSupportBasisWeighted},
		{"weighted_zero", ResolverSnapshot{SupportBasis: ResolverSupportBasisWeighted, TotalReporters: 2}, ResolverSupportBasisWeighted},
		{"unavailable_positive", ResolverSnapshot{SupportBasis: ResolverSupportBasisUnavailable, TotalWeightedSupportMilli: 1000}, ResolverSupportBasisUnavailable},
		{"raw_64_ranks", ResolverSnapshot{SupportBasis: ResolverSupportBasisReceivers, TotalReporters: 64, CandidateRanks: make([]ResolverCandidateSupport, 64)}, ResolverSupportBasisReceivers},
	} {
		b.Run(tc.name, func(b *testing.B) {
			if tc.s.EffectiveSupportBasis() != tc.want {
				b.Fatal("basis benchmark did not reach its declared semantic path")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				supportBasisBenchmarkSink = tc.s.EffectiveSupportBasis()
			}
			b.StopTimer()
			if supportBasisBenchmarkSink != tc.want {
				b.Fatal("basis benchmark lost correctness")
			}
		})
	}
}

func BenchmarkResolverSupportBasisCompact(b *testing.B) {
	s := ResolverSnapshot{SupportBasis: ResolverSupportBasisWeighted, Winner: "K1ABC", WinnerSupport: 2, TotalReporters: 2, TotalWeightedSupportMilli: 0,
		CandidateRanks:   []ResolverCandidateSupport{{Call: "K1ABC", Support: 2}},
		receiverEvidence: &ResolverEvidenceView{count: 2, candidates: []resolverEvidenceCandidate{{call: "K1ABC", receivers: []ResolverReceiverObservation{{Receiver: "N0AAA"}, {Receiver: "N0BBB"}}}}}}
	if compact := s.Compact(); compact.SupportBasis != ResolverSupportBasisWeighted || compact.ReceiverEvidence() != nil || compact.TotalReporters != 2 {
		b.Fatal("compaction benchmark did not preserve weighted zero")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		supportCompactBenchmarkSink = s.Compact()
	}
	b.StopTimer()
	b.ReportMetric(float64(unsafe.Sizeof(s)), "snapshot-B")
	if supportCompactBenchmarkSink.SupportBasis != ResolverSupportBasisWeighted || supportCompactBenchmarkSink.ReceiverEvidence() != nil || supportCompactBenchmarkSink.TotalWeightedSupportMilli != 0 {
		b.Fatal("compaction benchmark lost semantic correctness")
	}
}
