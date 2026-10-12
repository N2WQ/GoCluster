//go:build !qualification

// File role: Measures the ordinary resolver stage after selected-loss capture,
// including one bounded observation/drain, with no retained override context.
package cluster

import (
	"testing"
	"time"

	"dxcluster/spot"
	"dxcluster/telnet"
)

func BenchmarkCorrectionStabilizerSelectedLossStage(b *testing.B) {
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 8, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 4,
		EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
		HysteresisWindows: 1, MaxEditDistance: 1, DistanceModelCW: "plain", DistanceModelRTTY: "plain",
	})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		b.Fatal(err)
	}
	cfg := correctionLossConfig()
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	at := time.Now().UTC()
	for _, receiver := range []string{"N0AAA", "N0BBB"} {
		if !r.Enqueue(spot.ResolverEvidence{Key: key, DXCall: "K1ABC", Spotter: receiver,
			FrequencyKHz: 14020, ObservedAt: at, RecencyWindow: 2 * time.Minute}) {
			b.Fatal("fixture refused healthy evidence")
		}
	}
	d.Drain()
	d.Sweep(at.Add(time.Millisecond))
	p := &outputPipeline{signalResolver: r, correctionCfg: cfg,
		telnet: telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 1}, nil)}
	s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
	s.SourceType = spot.SourceManual
	probe := outputSpotContext{spot: s, modeUpper: "CW"}
	if !p.applyResolverStage(&probe, nil) || s.Confidence != "V" || !s.ConfidencePercentOK ||
		s.ConfidencePercent != 100 || probe.stabilizerEvidenceLoss || probe.resolverSelectionOverride != nil {
		b.Fatal("healthy ordinary stage must qualify and keep its selection local")
	}
	d.Drain()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := outputSpotContext{spot: s, modeUpper: "CW"}
		if !p.applyResolverStage(&ctx, nil) || ctx.stabilizerEvidenceLoss || ctx.resolverSelectionOverride != nil {
			b.Fatal("timed ordinary stage changed selected-loss ownership")
		}
		d.Drain() // Avoid benchmarking a permanently full/refused observation queue.
	}
}
