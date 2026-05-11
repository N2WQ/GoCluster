package telnet

import (
	"testing"

	"dxcluster/pathreliability"
)

func TestPathPredictionStatsSnapshotSplit(t *testing.T) {
	s := &Server{}

	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceCombined, Weight: 2}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, Weight: 0, InsufficientReason: pathreliability.InsufficientNoSample}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, Weight: 0.25, InsufficientReason: pathreliability.InsufficientLowCount}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, Weight: 0.25, InsufficientReason: pathreliability.InsufficientLowReceiver}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, Weight: 0.25, InsufficientReason: pathreliability.InsufficientLowWeight}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, Weight: 0, InsufficientReason: pathreliability.InsufficientStale}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceCombined, Weight: 2, CapLimited: true, CapWouldBlock: true}, false, false)
	s.recordPathPrediction(pathreliability.Result{
		Source: pathreliability.SourceCombined,
		Weight: 2,
		CapShadow: pathreliability.ReceiverCapShadowSummary{
			Count: pathreliability.ReceiverShadowCapCandidateCount,
			Candidates: [pathreliability.ReceiverShadowCapCandidateCount]pathreliability.ReceiverCapShadowCandidate{
				{MaxEffectiveCount: 5, Pass: true, P50Pass: true, P50Class: "LOW"},
				{MaxEffectiveCount: 6, LowReceiver: true, Block: true, P50Pass: true, P50Class: "HIGH"},
				{MaxEffectiveCount: 8, LowWeight: true, Block: true},
			},
		},
		Class: "MEDIUM",
	}, false, false)

	stats := s.PathPredictionStatsSnapshot()
	if stats.Total != 8 {
		t.Fatalf("expected total=8, got %d", stats.Total)
	}
	if stats.Combined != 3 {
		t.Fatalf("expected combined=3, got %d", stats.Combined)
	}
	if stats.Insufficient != 5 {
		t.Fatalf("expected insufficient=5, got %d", stats.Insufficient)
	}
	if stats.NoSample != 1 {
		t.Fatalf("expected no_sample=1, got %d", stats.NoSample)
	}
	if stats.LowCount != 1 {
		t.Fatalf("expected low_count=1, got %d", stats.LowCount)
	}
	if stats.LowReceiver != 1 {
		t.Fatalf("expected low_receiver=1, got %d", stats.LowReceiver)
	}
	if stats.LowWeight != 1 {
		t.Fatalf("expected low_weight=1, got %d", stats.LowWeight)
	}
	if stats.Stale != 1 {
		t.Fatalf("expected stale=1, got %d", stats.Stale)
	}
	if stats.CapLimited != 1 || stats.CapWouldBlock != 1 {
		t.Fatalf("expected cap stats 1/1, got limited=%d wouldBlock=%d", stats.CapLimited, stats.CapWouldBlock)
	}
	if stats.CapShadow.Pass[0] != 1 || stats.CapShadow.LowReceiver[1] != 1 || stats.CapShadow.LowWeight[2] != 1 || stats.CapShadow.Block[1] != 1 || stats.CapShadow.Block[2] != 1 {
		t.Fatalf("unexpected cap-shadow stats: %+v", stats.CapShadow)
	}
	if stats.CapP50Shadow.PassLow[0] != 1 || stats.CapP50Shadow.PassHigh[1] != 1 || stats.CapP50Shadow.Weaker[0] != 1 || stats.CapP50Shadow.Stronger[1] != 1 || stats.CapP50Shadow.ToInsufficient[2] != 1 {
		t.Fatalf("unexpected cap-p50-shadow stats: %+v", stats.CapP50Shadow)
	}

	after := s.PathPredictionStatsSnapshot()
	if after.Total != 0 || after.Combined != 0 || after.Insufficient != 0 || after.NoSample != 0 || after.LowCount != 0 || after.LowReceiver != 0 || after.LowWeight != 0 || after.Stale != 0 || after.CapLimited != 0 || after.CapWouldBlock != 0 || after.OverrideR != 0 || after.OverrideG != 0 {
		t.Fatalf("expected zeroed snapshot, got %+v", after)
	}
	if after.CapShadow.Pass[0] != 0 || after.CapShadow.LowReceiver[1] != 0 || after.CapShadow.LowWeight[2] != 0 || after.CapShadow.Block[1] != 0 || after.CapShadow.Block[2] != 0 {
		t.Fatalf("expected zeroed cap-shadow snapshot, got %+v", after.CapShadow)
	}
	if after.CapP50Shadow.PassLow[0] != 0 || after.CapP50Shadow.PassHigh[1] != 0 || after.CapP50Shadow.Weaker[0] != 0 || after.CapP50Shadow.Stronger[1] != 0 || after.CapP50Shadow.ToInsufficient[2] != 0 {
		t.Fatalf("expected zeroed cap-p50-shadow snapshot, got %+v", after.CapP50Shadow)
	}
}

func BenchmarkRecordPathPrediction(b *testing.B) {
	s := &Server{}
	res := pathreliability.Result{Source: pathreliability.SourceCombined, Weight: 2}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.recordPathPrediction(res, false, false)
	}
}
