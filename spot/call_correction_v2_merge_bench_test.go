package spot

import "testing"

// This exact fixture also builds against the pre-optimization core. It measures
// the repeated neighborhood merge, not only the already-built frequency scan.
func BenchmarkCCV2CanonicalMerge(b *testing.B) {
	for _, shape := range []struct {
		name    string
		views   int
		overlap bool
	}{{"one", 1, false}, {"three_overlap", 3, true}, {"five_disjoint", 5, false}} {
		b.Run(shape.name, func(b *testing.B) {
			views := make([]*ResolverEvidenceView, shape.views)
			for source := range views {
				id := source
				if shape.overlap {
					id = 0
				}
				views[source], _, _ = ccv2FrequencyPublication(b, id, 512)
			}
			union := MergeResolverEvidenceViews(views...)
			want := shape.views * 512
			if shape.overlap {
				want = 512
			}
			if len(union.canonical) != 2 || len(union.canonical[0].receivers) != want || len(union.canonical[1].receivers) != want || union.Len() != shape.views*1024 {
				b.Fatal("merge benchmark lacks its raw-versus-distinct population")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				union = MergeResolverEvidenceViews(views...)
			}
			b.StopTimer()
			if union.Len() != shape.views*1024 {
				b.Fatal("timed merge lost raw provenance")
			}
		})
	}
}
