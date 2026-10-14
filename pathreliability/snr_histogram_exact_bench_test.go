package pathreliability

import (
	"math"
	"testing"
)

// Only the detached 684d145 measurement binary overrides this string via -X.
// Timed inputs remain identical; the old binary must demonstrate its known
// wrong result rather than silently bypassing correctness guards.
var exactP50BenchmarkRevision = "candidate"

func BenchmarkExactP50(b *testing.B) {
	tiny := float32(math.Ldexp(1, -100))
	var dense, lostTie snrHistogram
	for i := range dense {
		dense[i] = math.MaxFloat32
	}
	lostTie[0], lostTie[40], lostTie[49] = 1, float32(math.Ldexp(1, -50)), 1
	for i := 1; i <= 8; i++ {
		lostTie[i] = float32(math.Ldexp(1, -53))
	}
	cases := []struct {
		name              string
		h                 snrHistogram
		want, oldObserved float64
	}{
		{"sparse", snrHistogram{25: 1}, .5, .5},
		{"dense", dense, 0, 0},
		{"wide", snrHistogram{0: math.MaxFloat32, 25: math.SmallestNonzeroFloat32, 49: math.MaxFloat32}, .5, -11.75},
		{"true_tie", snrHistogram{5: 1, 30: tiny, 35: 1, 45: tiny}, 8, -7},
		{"near_tie", snrHistogram{5: 1, 30: tiny, 45: 1}, 5.5, -7},
		{"lost_tie", lostTie, -.5, 15.5},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			want := tc.want
			if exactP50BenchmarkRevision == "684d145" {
				want = tc.oldObserved
			}
			b.ReportAllocs()
			b.ResetTimer()
			var got float64
			var available bool
			for i := 0; i < b.N; i++ {
				got, available = tc.h.p50DB()
			}
			b.StopTimer()
			if !available || got != want {
				b.Fatalf("revision=%s p50=%g/%v expected=%g", exactP50BenchmarkRevision, got, available, want)
			}
		})
	}
}
