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
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, Weight: 0.25, InsufficientReason: pathreliability.InsufficientLowWeight}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, Weight: 0, InsufficientReason: pathreliability.InsufficientStale}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceCombined, Weight: 2, CapLimited: true, CapWouldBlock: true}, false, false)

	stats := s.PathPredictionStatsSnapshot()
	if stats.Total != 6 {
		t.Fatalf("expected total=6, got %d", stats.Total)
	}
	if stats.Combined != 2 {
		t.Fatalf("expected combined=2, got %d", stats.Combined)
	}
	if stats.Insufficient != 4 {
		t.Fatalf("expected insufficient=4, got %d", stats.Insufficient)
	}
	if stats.NoSample != 1 {
		t.Fatalf("expected no_sample=1, got %d", stats.NoSample)
	}
	if stats.LowCount != 1 {
		t.Fatalf("expected low_count=1, got %d", stats.LowCount)
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

	after := s.PathPredictionStatsSnapshot()
	if after.Total != 0 || after.Combined != 0 || after.Insufficient != 0 || after.NoSample != 0 || after.LowCount != 0 || after.LowWeight != 0 || after.Stale != 0 || after.CapLimited != 0 || after.CapWouldBlock != 0 || after.OverrideR != 0 || after.OverrideG != 0 {
		t.Fatalf("expected zeroed snapshot, got %+v", after)
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
