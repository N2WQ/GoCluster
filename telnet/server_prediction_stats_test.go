package telnet

import (
	"testing"
	"time"

	"dxcluster/pathreliability"
)

func TestPathPredictionStatsSnapshotSplit(t *testing.T) {
	s := &Server{
		pathClosedFallback: &statsPathClosedFallback{
			stats: pathreliability.VOACAPFallbackStats{
				Queued:        2,
				RunSuccess:    1,
				CacheHit:      3,
				NoCurrentHour: 4,
			},
		},
	}

	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceCombined, Weight: 2}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, Weight: 0, InsufficientReason: pathreliability.InsufficientNoSample}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, Weight: 0.25, InsufficientReason: pathreliability.InsufficientLowCount}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, Weight: 0.25, InsufficientReason: pathreliability.InsufficientLowReceiver}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, Weight: 0.25, InsufficientReason: pathreliability.InsufficientLowWeight}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, Weight: 0, InsufficientReason: pathreliability.InsufficientStale}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceCombined, Weight: 2, CapLimited: true, CapWouldBlock: true}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceVOACAPClosed}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceVOACAPAligned}, false, false)
	s.vStageClosed.Add(1)
	s.vStageClosedNoP50.Add(1)
	s.vStageClosedWithSparseP50.Add(4)
	s.vStageClosedSparseHigh.Add(1)
	s.vStageClosedSparseMedium.Add(1)
	s.vStageClosedSparseLow.Add(1)
	s.vStageClosedSparseUnlikely.Add(1)
	s.vStageAligned.Add(2)
	s.vStageNoP50.Add(3)
	s.vStageMismatch.Add(4)
	s.vP50CompareChecked.Add(5)
	s.vP50CompareCacheHit.Add(4)
	s.vP50CompareCacheMiss.Add(1)
	s.vP50CompareSameClass.Add(2)
	s.vP50CompareP50Stronger.Add(2)
	s.vP50CompareVOACAPStronger.Add(1)
	s.vP50CompareEqualSNR.Add(1)
	s.vP50CompareClosedP50High.Add(1)
	s.vP50CompareClosedP50Med.Add(1)
	s.vP50CompareClosedP50Low.Add(1)
	s.vP50CompareClosedP50Unlk.Add(1)
	s.vP50CompareDeltaAbs0To3.Add(1)
	s.vP50CompareDeltaAbs4To9.Add(1)
	s.vP50CompareDeltaAbs10To19.Add(1)
	s.vP50CompareDeltaAbs20Plus.Add(1)

	stats := s.PathPredictionStatsSnapshot()
	if stats.Total != 9 {
		t.Fatalf("expected total=9, got %d", stats.Total)
	}
	if stats.Combined != 2 {
		t.Fatalf("expected combined=2, got %d", stats.Combined)
	}
	if stats.VOACAPClosed != 1 {
		t.Fatalf("expected voacap closed=1, got %d", stats.VOACAPClosed)
	}
	if stats.VOACAPAligned != 1 {
		t.Fatalf("expected voacap aligned=1, got %d", stats.VOACAPAligned)
	}
	if stats.VOACAPFallback.Queued != 2 || stats.VOACAPFallback.RunSuccess != 1 || stats.VOACAPFallback.CacheHit != 3 || stats.VOACAPFallback.NoCurrentHour != 4 {
		t.Fatalf("unexpected fallback stats: %+v", stats.VOACAPFallback)
	}
	if stats.VOACAPFallbackClosedCandidate != 1 || stats.VOACAPFallbackAlignedCandidate != 2 || stats.VOACAPFallbackOpenNoP50 != 3 || stats.VOACAPFallbackClassMismatch != 4 {
		t.Fatalf("unexpected fallback stage stats: %+v", stats)
	}
	if stats.VOACAPFallbackClosedNoP50 != 1 ||
		stats.VOACAPFallbackClosedSparseP50 != 4 ||
		stats.VOACAPFallbackClosedSparseHigh != 1 ||
		stats.VOACAPFallbackClosedSparseMed != 1 ||
		stats.VOACAPFallbackClosedSparseLow != 1 ||
		stats.VOACAPFallbackClosedSparseUnlk != 1 {
		t.Fatalf("unexpected closed fallback split stats: %+v", stats)
	}
	if stats.VOACAPP50CompareChecked != 5 ||
		stats.VOACAPP50CompareCacheHit != 4 ||
		stats.VOACAPP50CompareCacheMiss != 1 ||
		stats.VOACAPP50CompareSameClass != 2 ||
		stats.VOACAPP50CompareP50Stronger != 2 ||
		stats.VOACAPP50CompareVOACAPStronger != 1 ||
		stats.VOACAPP50CompareEqualSNR != 1 ||
		stats.VOACAPP50CompareClosedP50High != 1 ||
		stats.VOACAPP50CompareClosedP50Med != 1 ||
		stats.VOACAPP50CompareClosedP50Low != 1 ||
		stats.VOACAPP50CompareClosedP50Unlk != 1 ||
		stats.VOACAPP50CompareDeltaAbs0To3 != 1 ||
		stats.VOACAPP50CompareDeltaAbs4To9 != 1 ||
		stats.VOACAPP50CompareDeltaAbs10To19 != 1 ||
		stats.VOACAPP50CompareDeltaAbs20Plus != 1 {
		t.Fatalf("unexpected VOACAP p50 compare stats: %+v", stats)
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

	after := s.PathPredictionStatsSnapshot()
	if after.Total != 0 || after.Combined != 0 || after.VOACAPClosed != 0 || after.VOACAPAligned != 0 || after.VOACAPFallback.HasActivity() || after.VOACAPFallbackClosedCandidate != 0 || after.VOACAPFallbackClosedNoP50 != 0 || after.VOACAPFallbackClosedSparseP50 != 0 || after.VOACAPFallbackClosedSparseHigh != 0 || after.VOACAPFallbackClosedSparseMed != 0 || after.VOACAPFallbackClosedSparseLow != 0 || after.VOACAPFallbackClosedSparseUnlk != 0 || after.VOACAPFallbackAlignedCandidate != 0 || after.VOACAPFallbackOpenNoP50 != 0 || after.VOACAPFallbackClassMismatch != 0 || after.VOACAPP50CompareChecked != 0 || after.VOACAPP50CompareCacheHit != 0 || after.VOACAPP50CompareCacheMiss != 0 || after.VOACAPP50CompareSameClass != 0 || after.VOACAPP50CompareP50Stronger != 0 || after.VOACAPP50CompareVOACAPStronger != 0 || after.VOACAPP50CompareEqualSNR != 0 || after.VOACAPP50CompareClosedP50High != 0 || after.VOACAPP50CompareClosedP50Med != 0 || after.VOACAPP50CompareClosedP50Low != 0 || after.VOACAPP50CompareClosedP50Unlk != 0 || after.VOACAPP50CompareDeltaAbs0To3 != 0 || after.VOACAPP50CompareDeltaAbs4To9 != 0 || after.VOACAPP50CompareDeltaAbs10To19 != 0 || after.VOACAPP50CompareDeltaAbs20Plus != 0 || after.Insufficient != 0 || after.NoSample != 0 || after.LowCount != 0 || after.LowReceiver != 0 || after.LowWeight != 0 || after.Stale != 0 || after.CapLimited != 0 || after.CapWouldBlock != 0 || after.OverrideR != 0 || after.OverrideG != 0 {
		t.Fatalf("expected zeroed snapshot, got %+v", after)
	}
}

func TestPathResultWithClosedFallbackStageStats(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	cfg.MinObservationCount = 2
	cfg.GlyphSymbols.Closed = "!"
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})
	req := pathreliability.VOACAPClosedRequest{Band: "20m", Mode: "FT8"}
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)

	tests := []struct {
		name              string
		base              pathreliability.Result
		forecastSNR       int
		wantSource        pathreliability.PredictionSource
		wantClosed        int64
		wantClosedNoP50   int64
		wantClosedSparse  int64
		wantClosedHigh    int64
		wantClosedMed     int64
		wantClosedLow     int64
		wantClosedUnlk    int64
		wantAligned       int64
		wantOpenNoP50     int64
		wantClassMismatch int64
	}{
		{
			name:            "closed",
			base:            pathreliability.Result{Source: pathreliability.SourceInsufficient},
			forecastSNR:     -34,
			wantSource:      pathreliability.SourceVOACAPClosed,
			wantClosed:      1,
			wantClosedNoP50: 1,
		},
		{
			name:             "closed with sparse high p50",
			base:             pathreliability.Result{Source: pathreliability.SourceInsufficient, HasP50: true, P50DB: -12},
			forecastSNR:      -34,
			wantSource:       pathreliability.SourceVOACAPClosed,
			wantClosed:       1,
			wantClosedSparse: 1,
			wantClosedHigh:   1,
		},
		{
			name:             "closed with sparse medium p50",
			base:             pathreliability.Result{Source: pathreliability.SourceInsufficient, HasP50: true, P50DB: -15},
			forecastSNR:      -34,
			wantSource:       pathreliability.SourceVOACAPClosed,
			wantClosed:       1,
			wantClosedSparse: 1,
			wantClosedMed:    1,
		},
		{
			name:             "closed with sparse low p50",
			base:             pathreliability.Result{Source: pathreliability.SourceInsufficient, HasP50: true, P50DB: -19},
			forecastSNR:      -34,
			wantSource:       pathreliability.SourceVOACAPClosed,
			wantClosed:       1,
			wantClosedSparse: 1,
			wantClosedLow:    1,
		},
		{
			name:             "closed with sparse unlikely p50",
			base:             pathreliability.Result{Source: pathreliability.SourceInsufficient, HasP50: true, P50DB: -24},
			forecastSNR:      -34,
			wantSource:       pathreliability.SourceVOACAPClosed,
			wantClosed:       1,
			wantClosedSparse: 1,
			wantClosedUnlk:   1,
		},
		{
			name:          "open no sparse p50",
			base:          pathreliability.Result{Source: pathreliability.SourceInsufficient},
			forecastSNR:   -15,
			wantSource:    pathreliability.SourceInsufficient,
			wantOpenNoP50: 1,
		},
		{
			name:              "class mismatch",
			base:              pathreliability.Result{Source: pathreliability.SourceInsufficient, HasP50: true, P50DB: -15},
			forecastSNR:       -19,
			wantSource:        pathreliability.SourceInsufficient,
			wantClassMismatch: 1,
		},
		{
			name:        "aligned",
			base:        pathreliability.Result{Source: pathreliability.SourceInsufficient, HasP50: true, P50DB: -15},
			forecastSNR: -15,
			wantSource:  pathreliability.SourceVOACAPAligned,
			wantAligned: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				pathPredictor: predictor,
				pathClosedFallback: fakePathClosedFallback{
					forecast: pathreliability.VOACAPCachedForecast{
						Record: pathreliability.VOACAPHourlyForecast{FT8SNRDB: tt.forecastSNR, HourUTC: 20, FrequencyMHz: 14.1},
						SSN:    112,
					},
					ok: true,
				},
			}
			got := s.pathResultWithClosedFallback(tt.base, req, now)
			if got.Source != tt.wantSource {
				t.Fatalf("source = %v, want %v; result=%+v", got.Source, tt.wantSource, got)
			}
			stats := s.PathPredictionStatsSnapshot()
			if stats.VOACAPFallbackClosedCandidate != tt.wantClosed ||
				stats.VOACAPFallbackClosedNoP50 != tt.wantClosedNoP50 ||
				stats.VOACAPFallbackClosedSparseP50 != tt.wantClosedSparse ||
				stats.VOACAPFallbackClosedSparseHigh != tt.wantClosedHigh ||
				stats.VOACAPFallbackClosedSparseMed != tt.wantClosedMed ||
				stats.VOACAPFallbackClosedSparseLow != tt.wantClosedLow ||
				stats.VOACAPFallbackClosedSparseUnlk != tt.wantClosedUnlk ||
				stats.VOACAPFallbackAlignedCandidate != tt.wantAligned ||
				stats.VOACAPFallbackOpenNoP50 != tt.wantOpenNoP50 ||
				stats.VOACAPFallbackClassMismatch != tt.wantClassMismatch {
				t.Fatalf("unexpected stage stats: %+v", stats)
			}
		})
	}
}

func TestPathResultWithClosedFallbackUsesEffectiveVOACAPSNR(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	cfg.GlyphSymbols.Closed = "!"
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})
	req := pathreliability.VOACAPClosedRequest{
		Band:                  "20m",
		Mode:                  "FT8",
		ReceiveNoisePenaltyDB: 5,
	}
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	forecast := pathreliability.VOACAPCachedForecast{
		Record: pathreliability.VOACAPHourlyForecast{
			FT8SNRDB:          -17,
			HourUTC:           20,
			FrequencyMHz:      14.1,
			ReceiveFT8SNRDB:   -10,
			TransmitFT8SNRDB:  -20,
			HasDirectionalSNR: true,
		},
		EffectiveFT8SNRDB:     -17,
		HasEffectiveFT8SNRDB:  true,
		ReceiveNoisePenaltyDB: 5,
		SSN:                   112,
	}
	s := &Server{
		pathPredictor: predictor,
		pathClosedFallback: fakePathClosedFallback{
			forecast: forecast,
			ok:       true,
		},
	}

	got := s.pathResultWithClosedFallback(pathreliability.Result{
		Source: pathreliability.SourceInsufficient,
		HasP50: true,
		P50DB:  -17,
	}, req, now)
	if got.Source != pathreliability.SourceVOACAPAligned {
		t.Fatalf("expected sparse p50 to align with effective VOACAP SNR, got %+v", got)
	}
	if got.VOACAPFT8SNRDB != -17 {
		t.Fatalf("diagnostic VOACAP SNR = %d, want -17", got.VOACAPFT8SNRDB)
	}
}

func TestPathResultWithClosedFallbackComparesSufficientP50AgainstCachedVOACAP(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	cfg.GlyphSymbols.Closed = "!"
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})
	req := pathreliability.VOACAPClosedRequest{Band: "20m", Mode: "FT8"}
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)

	s := &Server{
		pathPredictor: predictor,
		pathClosedFallback: cacheOnlyPathClosedFallback{
			forecast: pathreliability.VOACAPCachedForecast{
				Record: pathreliability.VOACAPHourlyForecast{FT8SNRDB: -34, HourUTC: 20, FrequencyMHz: 14.1},
				SSN:    112,
			},
			ok: true,
		},
	}
	base := pathreliability.Result{Source: pathreliability.SourceCombined, HasP50: true, P50DB: -12}
	got := s.pathResultWithClosedFallback(base, req, now)
	if got.Source != pathreliability.SourceCombined {
		t.Fatalf("comparison must not change emitted source, got %+v", got)
	}
	stats := s.PathPredictionStatsSnapshot()
	if stats.VOACAPP50CompareChecked != 1 ||
		stats.VOACAPP50CompareCacheHit != 1 ||
		stats.VOACAPP50CompareCacheMiss != 0 ||
		stats.VOACAPP50CompareP50Stronger != 1 ||
		stats.VOACAPP50CompareClosedP50High != 1 ||
		stats.VOACAPP50CompareDeltaAbs20Plus != 1 {
		t.Fatalf("unexpected p50 compare stats for closed/high disagreement: %+v", stats)
	}
	if stats.VOACAPFallbackClosedCandidate != 0 || stats.VOACAPClosed != 0 {
		t.Fatalf("comparison must not count fallback emissions or stages: %+v", stats)
	}

	s = &Server{
		pathPredictor: predictor,
		pathClosedFallback: cacheOnlyPathClosedFallback{
			forecast: pathreliability.VOACAPCachedForecast{
				Record: pathreliability.VOACAPHourlyForecast{FT8SNRDB: -15, HourUTC: 20, FrequencyMHz: 14.1},
				SSN:    112,
			},
			ok: true,
		},
	}
	base = pathreliability.Result{Source: pathreliability.SourceCombined, HasP50: true, P50DB: -15}
	got = s.pathResultWithClosedFallback(base, req, now)
	if got.Source != pathreliability.SourceCombined {
		t.Fatalf("comparison must not change emitted source, got %+v", got)
	}
	stats = s.PathPredictionStatsSnapshot()
	if stats.VOACAPP50CompareChecked != 1 ||
		stats.VOACAPP50CompareCacheHit != 1 ||
		stats.VOACAPP50CompareSameClass != 1 ||
		stats.VOACAPP50CompareEqualSNR != 1 ||
		stats.VOACAPP50CompareDeltaAbs0To3 != 1 {
		t.Fatalf("unexpected p50 compare stats for matching class: %+v", stats)
	}

	s = &Server{
		pathPredictor:      predictor,
		pathClosedFallback: cacheOnlyPathClosedFallback{},
	}
	got = s.pathResultWithClosedFallback(base, req, now)
	if got.Source != pathreliability.SourceCombined {
		t.Fatalf("cache miss comparison must not change emitted source, got %+v", got)
	}
	stats = s.PathPredictionStatsSnapshot()
	if stats.VOACAPP50CompareChecked != 1 ||
		stats.VOACAPP50CompareCacheHit != 0 ||
		stats.VOACAPP50CompareCacheMiss != 1 {
		t.Fatalf("unexpected p50 compare stats for cache miss: %+v", stats)
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

type statsPathClosedFallback struct {
	stats pathreliability.VOACAPFallbackStats
}

func (f *statsPathClosedFallback) CheckForecast(pathreliability.VOACAPClosedRequest, time.Time) (pathreliability.VOACAPCachedForecast, bool) {
	return pathreliability.VOACAPCachedForecast{}, false
}

func (f *statsPathClosedFallback) StatsSnapshot() pathreliability.VOACAPFallbackStats {
	stats := f.stats
	f.stats = pathreliability.VOACAPFallbackStats{}
	return stats
}

type cacheOnlyPathClosedFallback struct {
	forecast pathreliability.VOACAPCachedForecast
	ok       bool
}

func (f cacheOnlyPathClosedFallback) CheckForecast(pathreliability.VOACAPClosedRequest, time.Time) (pathreliability.VOACAPCachedForecast, bool) {
	return pathreliability.VOACAPCachedForecast{}, false
}

func (f cacheOnlyPathClosedFallback) CheckCachedForecast(pathreliability.VOACAPClosedRequest, time.Time) (pathreliability.VOACAPCachedForecast, bool) {
	return f.forecast, f.ok
}
