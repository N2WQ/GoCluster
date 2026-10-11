package cluster

import (
	"runtime"
	"testing"

	"dxcluster/cty"
	"dxcluster/spot"
)

func BenchmarkOutputPipelinePrepareSpotContext(b *testing.B) {
	pipeline := &outputPipeline{
		ctyLookup: func() *cty.CTYDatabase { return nil },
	}
	src := spot.NewSpot("K1ABC", "W1XYZ-1", 14074.0, "FT8")
	src.Comment = "CQ TEST"
	src.SourceType = spot.SourceManual
	src.DXMetadata.Grid = "EN61"
	src.DEMetadata.Grid = "FN31"
	src.EnsureNormalized()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, ok := pipeline.prepareSpotContext(src)
		if !ok {
			b.Fatal("expected spot context")
		}
		runtime.KeepAlive(ctx)
	}
}

func BenchmarkOutputPipelineEmitSpotOwnership(b *testing.B) {
	src := spot.NewSpot("K1ABC", "W1XYZ-1", 14074.0, "FT8")
	src.Comment = "CQ TEST"
	src.SourceType = spot.SourceManual
	src.DXMetadata.Grid = "EN61"
	src.DEMetadata.Grid = "FN31"
	src.DECallStripped = "W1XYZ"
	src.DECallNormStripped = "W1XYZ"
	src.EnsureNormalized()
	_ = src.FormatDXCluster()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		shared := src.Clone().SealForAsync()
		shared.ID = uint64(i + 1)
		archiveSnapshot := shared
		telnetSnapshot := shared
		peerComment := src.Comment
		runtime.KeepAlive(shared)
		runtime.KeepAlive(archiveSnapshot)
		runtime.KeepAlive(telnetSnapshot)
		runtime.KeepAlive(peerComment)
	}
}
