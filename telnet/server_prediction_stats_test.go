package telnet

import (
	"testing"

	"dxcluster/pathreliability"
	"dxcluster/spot"
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

func TestPathP50DiagStatsSnapshotBuckets(t *testing.T) {
	s := &Server{}
	cfg := pathreliability.DefaultConfig()
	s.recordPathP50Diag(pathreliability.Result{
		Glyph:     cfg.GlyphSymbols.Medium,
		MeanDB:    -11,
		HasMeanDB: true,
		P50DB:     -15,
		HasP50:    true,
		P50Glyph:  cfg.GlyphSymbols.Medium,
		Count:     19,
	}, &spot.Spot{SourceType: spot.SourceRBN, SourceNode: "RBN"}, "20m", "CW", cfg)
	s.recordPathP50Diag(pathreliability.Result{
		Glyph:     cfg.GlyphSymbols.Unlikely,
		MeanDB:    -20,
		HasMeanDB: true,
		P50DB:     -12,
		HasP50:    true,
		P50Glyph:  cfg.GlyphSymbols.Medium,
		Count:     3,
	}, &spot.Spot{SourceType: spot.SourceFT8}, "40m", "FT8", cfg)
	s.recordPathP50Diag(pathreliability.Result{
		Glyph:     cfg.GlyphSymbols.High,
		MeanDB:    -1,
		HasMeanDB: true,
		P50DB:     -13,
		HasP50:    true,
		P50Glyph:  cfg.GlyphSymbols.Low,
		Count:     525,
	}, &spot.Spot{SourceType: spot.SourcePSKReporter}, "15m", "RTTY", cfg)
	s.recordPathP50Diag(pathreliability.Result{
		Glyph:     cfg.GlyphSymbols.High,
		MeanDB:    1,
		HasMeanDB: true,
		P50DB:     -7,
		HasP50:    true,
		P50Glyph:  cfg.GlyphSymbols.Medium,
		Count:     150,
	}, &spot.Spot{SourceType: spot.SourceManual}, "6m", "SSB", cfg)
	s.recordPathP50Diag(pathreliability.Result{Glyph: cfg.GlyphSymbols.High, MeanDB: -11, HasMeanDB: true, Count: 7}, &spot.Spot{SourceType: spot.SourcePeer}, "80m", "CW", cfg)

	stats := s.PathP50DiagStatsSnapshot()
	if stats.Observed != 5 || stats.Missing != 1 {
		t.Fatalf("unexpected observed/missing: %+v", stats)
	}
	if stats.Delta[pathP50DiagDeltaLeNeg6] != 1 ||
		stats.Delta[pathP50DiagDeltaNeg5Pos5] != 1 ||
		stats.Delta[pathP50DiagDelta6To11] != 1 ||
		stats.Delta[pathP50DiagDeltaGe12] != 1 {
		t.Fatalf("unexpected delta buckets: %+v", stats.Delta)
	}
	if stats.N[pathP50DiagNLt17] != 1 ||
		stats.N[pathP50DiagN17To99] != 1 ||
		stats.N[pathP50DiagN100To499] != 1 ||
		stats.N[pathP50DiagNGe500] != 1 {
		t.Fatalf("unexpected n buckets: %+v", stats.N)
	}
	if stats.Shadow.Same != 1 || stats.Shadow.MeanGT != 2 || stats.Shadow.P50GT != 1 {
		t.Fatalf("unexpected shadow comparison counters: %+v", stats.Shadow)
	}
	if stats.Shadow.SevLow != 1 || stats.Shadow.SevNone != 1 {
		t.Fatalf("unexpected severe counters: %+v", stats.Shadow)
	}
	if stats.Shadow.N[pathP50DiagNLt17] != 1 ||
		stats.Shadow.N[pathP50DiagN17To99] != 1 ||
		stats.Shadow.N[pathP50DiagN100To499] != 1 ||
		stats.Shadow.N[pathP50DiagNGe500] != 1 {
		t.Fatalf("unexpected shadow n buckets: %+v", stats.Shadow.N)
	}
	if stats.Shadow.Band[pathP50ShadowBand20m] != 1 || stats.Shadow.Band[pathP50ShadowBand40m] != 1 || stats.Shadow.Band[pathP50ShadowBand15m] != 1 || stats.Shadow.Band[pathP50ShadowBand6m] != 1 {
		t.Fatalf("unexpected shadow band buckets: %+v", stats.Shadow.Band)
	}
	if stats.Shadow.Mode[pathP50ShadowModeCW] != 1 || stats.Shadow.Mode[pathP50ShadowModeFT] != 1 || stats.Shadow.Mode[pathP50ShadowModeRTTY] != 1 || stats.Shadow.Mode[pathP50ShadowModePhone] != 1 {
		t.Fatalf("unexpected shadow mode buckets: %+v", stats.Shadow.Mode)
	}
	if stats.Shadow.Source[pathP50ShadowSourceRBN] != 1 || stats.Shadow.Source[pathP50ShadowSourceRBNFT] != 1 || stats.Shadow.Source[pathP50ShadowSourcePSK] != 1 || stats.Shadow.Source[pathP50ShadowSourceHuman] != 1 {
		t.Fatalf("unexpected shadow source buckets: %+v", stats.Shadow.Source)
	}
	highLow := pathP50ShadowGlyphHigh*pathP50ShadowGlyphCount + pathP50ShadowGlyphLow
	if stats.Shadow.Pair[highLow] != 1 {
		t.Fatalf("expected high/low pair, got %+v", stats.Shadow.Pair)
	}
	after := s.PathP50DiagStatsSnapshot()
	if after.Observed != 0 || after.Missing != 0 || after.Shadow.Same != 0 || after.Shadow.MeanGT != 0 || after.Shadow.P50GT != 0 {
		t.Fatalf("expected snapshot reset, got %+v", after)
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

func BenchmarkRecordPathP50Diag(b *testing.B) {
	s := &Server{}
	cfg := pathreliability.DefaultConfig()
	sp := &spot.Spot{SourceType: spot.SourceRBN, SourceNode: "RBN"}
	res := pathreliability.Result{
		Glyph:     cfg.GlyphSymbols.Medium,
		MeanDB:    -11,
		HasMeanDB: true,
		P50DB:     -15,
		HasP50:    true,
		P50Glyph:  cfg.GlyphSymbols.Low,
		Count:     119,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.recordPathP50Diag(res, sp, "20m", "CW", cfg)
	}
}
