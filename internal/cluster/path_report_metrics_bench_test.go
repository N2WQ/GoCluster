package cluster

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func BenchmarkPathReportMetricsObserve(b *testing.B) {
	if err := pathreliability.InitH3MappingsFromDir("../../data/h3"); err != nil {
		b.Skipf("InitH3Mappings unavailable: %v", err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	spots := make([]*spot.Spot, 1024)
	for i := range spots {
		spots[i] = &spot.Spot{
			DECall:     fmt.Sprintf("K%dABC", i),
			DECallNorm: fmt.Sprintf("K%dABC", i),
			BandNorm:   "20m",
			Time:       now,
			SourceType: spot.SourcePSKReporter,
			DEMetadata: spot.CallMetadata{Grid: fmt.Sprintf("FN%02d", i%100)},
			DXMetadata: spot.CallMetadata{Grid: fmt.Sprintf("EM%02d", (i*7)%100)},
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	metrics := newPathReportMetrics()
	for i := 0; i < b.N; i++ {
		metrics.Observe(spots[i&(len(spots)-1)], now)
	}
}
