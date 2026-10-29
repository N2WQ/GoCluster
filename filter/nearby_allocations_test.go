package filter

import (
	"fmt"
	"runtime"
	"testing"

	"dxcluster/pathreliability"
	"dxcluster/spot"
)

// Measure only the public conversions named explicitly by each fixture. Native
// h3-go and pure-Go backends have different costs. Neither matcher nor its
// private endpoint helper may determine this independent allowance.
func nearbyRequiredConversionAllocations(grids []string, coarse bool) float64 {
	var cellSink pathreliability.CellID
	allocs := testing.AllocsPerRun(100, func() {
		for _, grid := range grids {
			if coarse {
				cellSink = pathreliability.EncodeCoarseCell(grid)
			} else {
				cellSink = pathreliability.EncodeCell(grid)
			}
		}
	})
	runtime.KeepAlive(cellSink)
	return allocs
}

func TestNearbyMatchingAllocations(t *testing.T) {
	requireH3Mappings(t)
	f := nearbyEndpointFilter(t)
	context := nearbyEndpointContext()
	near, far := pathreliability.EncodeCell("FN31"), pathreliability.EncodeCell("EM10")
	if near == pathreliability.InvalidCell || far == pathreliability.InvalidCell || near == far ||
		pathreliability.EncodeCoarseCell("FN31") == pathreliability.EncodeCoarseCell("EM10") {
		t.Fatal("allocation fixture lacks mapped distinct near/far cells")
	}
	type fixture struct {
		name, dx, de     string
		dxCache, deCache pathreliability.CellID
		conversions      []string
		want             bool
	}
	fine := []fixture{
		{"dx", "FN31", "", 0, 0, []string{"FN31", ""}, true},
		{"de", "", "FN31", 0, 0, []string{"", "FN31"}, true},
		{"both", "FN31", "EM10", 0, 0, []string{"FN31", "EM10"}, true},
		{"both-reversed", "EM10", "FN31", 0, 0, []string{"EM10", "FN31"}, true},
		{"dx-cached", "EM10", "EM10", near, 0, []string{"EM10"}, true},
		{"de-cached", "EM10", "EM10", 0, near, []string{"EM10"}, true},
		{"fully-cached", "EM10", "EM10", near, far, nil, true},
		{"fully-cached-reversed", "EM10", "EM10", far, near, nil, true},
	}
	coarse := []fixture{
		{"dx", "FN31", "", far, near, []string{"FN31", ""}, true},
		{"de", "", "FN31", near, far, []string{"", "FN31"}, true},
		{"both", "FN31", "EM10", far, far, []string{"FN31", "EM10"}, true},
		{"both-reversed", "EM10", "FN31", far, far, []string{"EM10", "FN31"}, true},
		{"fine-cache-not-used", "EM10", "EM10", near, near, []string{"EM10", "EM10"}, false},
	}
	for _, band := range []struct {
		frequency float64
		coarse    bool
	}{{14020, false}, {1820, true}, {3520, true}, {5355, true}} {
		cases := fine
		if band.coarse {
			cases = coarse
		}
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%.0f/%s", band.frequency, tc.name), func(t *testing.T) {
				s := spot.NewSpotNormalized("W6ABC", "N0XYZ", band.frequency, "CW")
				s.DXMetadata.Grid, s.DEMetadata.Grid = tc.dx, tc.de
				s.DXCellID, s.DECellID = uint16(tc.dxCache), uint16(tc.deCache)
				if got := f.MatchesWithContext(s, context); got != tc.want {
					t.Fatalf("allocation control Matches=%v, want %v", got, tc.want)
				}
				baseline := nearbyRequiredConversionAllocations(tc.conversions, band.coarse)
				if len(tc.conversions) == 0 && baseline != 0 {
					t.Fatal("fully cached fine allowance must be zero")
				}
				var matched bool
				allocs := testing.AllocsPerRun(100, func() { matched = f.MatchesWithContext(s, context) })
				if matched != tc.want || allocs > baseline {
					t.Fatalf("Matches=%v allocations=%g; want %v with at most %g conversion allocations", matched, allocs, tc.want, baseline)
				}
				// Keep a single pointer so the injected allocation must escape,
				// without retaining one object per measurement iteration.
				var allocationSink *byte
				extra := testing.AllocsPerRun(100, func() {
					matched = f.MatchesWithContext(s, context)
					allocationSink = new(byte)
					*allocationSink = 1
				})
				if *allocationSink != 1 || matched != tc.want || extra <= baseline {
					t.Fatalf("known extra allocation was not detected: %g <= %g", extra, baseline)
				}
				t.Logf("required conversions=%g matcher=%g injected=%g", baseline, allocs, extra)
			})
		}
	}
}
