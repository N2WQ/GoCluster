//go:build !qualification

package cluster

import (
	"testing"
	"time"

	"dxcluster/pathreliability"
	"dxcluster/stats"
)

// This measures warm location selection and its allocation cost, not storage
// waits, source parsing or whole-pipeline/production tail latency. Reset only
// mutable location fields so each iteration starts with populated old caches;
// never copy Spot's sync.Once or time a repeated already-cleared no-op.
func BenchmarkCorrectedOutputLocation(b *testing.B) {
	requirePathReportH3Mappings(b)
	db := correctedGridCTY(b)
	for _, name := range []string{"unchanged", "reported", "derived", "incomplete"} {
		b.Run(name, func(b *testing.B) {
			cache := newCallMetaCache(64, 0)
			want, derived := "", false
			switch name {
			case "unchanged":
				want = "FN31"
			case "reported":
				cache.UpdateGrid("K1ABD", "QF56AB", false)
				want = "QF56AB"
			case "derived":
				cache.UpdateGrid("K1ABD", "EM12", true)
				want, derived = "JN58", true
			}
			calls := 0
			p := &outputPipeline{metaCache: cache, gridLookup: func(call string) (string, bool, bool) {
				calls++
				return cache.LookupGrid(call, nil)
			}}
			s := correctedGridSource(time.Now().UTC(), false)
			if name != "unchanged" {
				s.DXCall, s.DXCallNorm = "K1ABD", "K1ABD"
			}
			ctx := outputSpotContext{spot: s, dxIdentityChanged: name != "unchanged", ctyDB: db}
			p.prepareOutputLocation(&ctx, true) // Populate CTY cache before timing.
			calls = 0
			oldFine := uint16(pathreliability.EncodeCell("FN31"))
			oldCoarse := uint16(pathreliability.EncodeCoarseCell("FN31"))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				s.DXMetadata.Grid, s.DXMetadata.GridDerived = "FN31", false
				s.DXGridNorm, s.DXGrid2 = "FN31", "FN"
				s.DXCellID, s.DXCoarseCellID = oldFine, oldCoarse
				p.prepareOutputLocation(&ctx, true)
			}
			b.StopTimer()
			wantCalls := b.N
			if name == "unchanged" {
				wantCalls = 0
			}
			if calls != wantCalls || s.DXMetadata.Grid != want || s.DXMetadata.GridDerived != derived || s.DXGridNorm != want ||
				s.DXCellID != uint16(pathreliability.EncodeCell(want)) || s.DXCoarseCellID != uint16(pathreliability.EncodeCoarseCell(want)) ||
				s.DEMetadata.Grid != "FN20" || s.DECellID == 0 || s.DECoarseCellID == 0 {
				b.Fatal("timed workload did not preserve the corrected location contract")
			}
		})
	}
}

func BenchmarkCorrectedOutputWithoutGridCounter(b *testing.B) {
	tr := stats.NewTracker()
	b.ReportAllocs()
	for range b.N {
		tr.IncrementCorrectedOutputWithoutGrid()
	}
	if tr.CorrectedOutputWithoutGrid() != uint64(b.N) {
		b.Fatal("counter lost admitted output")
	}
}
