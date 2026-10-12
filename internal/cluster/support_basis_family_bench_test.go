// File role: Measures family support-basis checks with qualified scalar
// populations; these bounded helper costs make no delivery/load claim.
package cluster

import (
	"testing"

	"dxcluster/spot"
)

func BenchmarkSupportBasisFamilyPercent(b *testing.B) {
	for _, tc := range []struct {
		name                  string
		basis                 spot.ResolverSupportBasis
		weighted, totalWeight int
		want                  int
		usable                bool
	}{
		{"positive_weighted", spot.ResolverSupportBasisWeighted, 1850, 2850, 64, true},
		{"known_weighted_zero", spot.ResolverSupportBasisWeighted, 0, 1000, 0, false},
		{"positive_rounded_zero", spot.ResolverSupportBasisWeighted, 1, 101000, 0, true},
		{"explicit_receivers", spot.ResolverSupportBasisReceivers, 0, 0, 66, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			snapshot := spot.ResolverSnapshot{SupportBasis: tc.basis, TotalReporters: 3,
				TotalWeightedSupportMilli: tc.totalWeight, CandidateRanks: []spot.ResolverCandidateSupport{
					{Call: "W1AB", Support: 2, WeightedSupportMilli: tc.weighted},
				}}
			if tc.name == "positive_rounded_zero" {
				snapshot.TotalReporters, snapshot.CandidateRanks[0].Support = 102, 1
			}
			if percent, usable := familyPercent(snapshot, "W1AB"); usable != tc.usable || usable && percent != tc.want {
				b.Fatal("family basis benchmark failed its explicit population control")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				familyPercent(snapshot, "W1AB")
			}
		})
	}
}
