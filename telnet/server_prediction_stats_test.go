package telnet

import (
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/solarweather"
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
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceVOACAPSparseUpgrade}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceVOACAPOpen}, false, false)
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
	s.vStageSparseUpgrade.Add(5)
	s.vStageOpenNoP50REL.Add(6)
	s.vStageReliabilityMissing.Add(7)
	s.vStageReliabilityBelow.Add(8)
	s.vStageReliabilityMultiTier.Add(9)
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
	if stats.Total != 11 {
		t.Fatalf("expected total=11, got %d", stats.Total)
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
	if stats.VOACAPSparseUpgrade != 1 {
		t.Fatalf("expected voacap sparse upgrade=1, got %d", stats.VOACAPSparseUpgrade)
	}
	if stats.VOACAPOpen != 1 {
		t.Fatalf("expected voacap open=1, got %d", stats.VOACAPOpen)
	}
	if stats.VOACAPFallback.Queued != 2 || stats.VOACAPFallback.RunSuccess != 1 || stats.VOACAPFallback.CacheHit != 3 || stats.VOACAPFallback.NoCurrentHour != 4 {
		t.Fatalf("unexpected fallback stats: %+v", stats.VOACAPFallback)
	}
	if stats.VOACAPFallbackClosedCandidate != 1 || stats.VOACAPFallbackAlignedCandidate != 2 || stats.VOACAPFallbackOpenNoP50 != 3 || stats.VOACAPFallbackClassMismatch != 4 {
		t.Fatalf("unexpected fallback stage stats: %+v", stats)
	}
	if stats.VOACAPFallbackSparseUpgrade != 5 ||
		stats.VOACAPFallbackOpenNoP50REL != 6 ||
		stats.VOACAPFallbackReliabilityMissing != 7 ||
		stats.VOACAPFallbackReliabilityBelow != 8 ||
		stats.VOACAPFallbackReliabilityMultiTier != 9 {
		t.Fatalf("unexpected REL fallback stage stats: %+v", stats)
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
	if after.Total != 0 || after.Combined != 0 || after.VOACAPClosed != 0 || after.VOACAPAligned != 0 || after.VOACAPSparseUpgrade != 0 || after.VOACAPOpen != 0 || after.VOACAPFallback.HasActivity() || after.VOACAPFallbackClosedCandidate != 0 || after.VOACAPFallbackClosedNoP50 != 0 || after.VOACAPFallbackClosedSparseP50 != 0 || after.VOACAPFallbackClosedSparseHigh != 0 || after.VOACAPFallbackClosedSparseMed != 0 || after.VOACAPFallbackClosedSparseLow != 0 || after.VOACAPFallbackClosedSparseUnlk != 0 || after.VOACAPFallbackAlignedCandidate != 0 || after.VOACAPFallbackOpenNoP50 != 0 || after.VOACAPFallbackClassMismatch != 0 || after.VOACAPFallbackSparseUpgrade != 0 || after.VOACAPFallbackOpenNoP50REL != 0 || after.VOACAPFallbackReliabilityMissing != 0 || after.VOACAPFallbackReliabilityBelow != 0 || after.VOACAPFallbackReliabilityMultiTier != 0 || after.VOACAPP50CompareChecked != 0 || after.VOACAPP50CompareCacheHit != 0 || after.VOACAPP50CompareCacheMiss != 0 || after.VOACAPP50CompareSameClass != 0 || after.VOACAPP50CompareP50Stronger != 0 || after.VOACAPP50CompareVOACAPStronger != 0 || after.VOACAPP50CompareEqualSNR != 0 || after.VOACAPP50CompareClosedP50High != 0 || after.VOACAPP50CompareClosedP50Med != 0 || after.VOACAPP50CompareClosedP50Low != 0 || after.VOACAPP50CompareClosedP50Unlk != 0 || after.VOACAPP50CompareDeltaAbs0To3 != 0 || after.VOACAPP50CompareDeltaAbs4To9 != 0 || after.VOACAPP50CompareDeltaAbs10To19 != 0 || after.VOACAPP50CompareDeltaAbs20Plus != 0 || after.Insufficient != 0 || after.NoSample != 0 || after.LowCount != 0 || after.LowReceiver != 0 || after.LowWeight != 0 || after.Stale != 0 || after.CapLimited != 0 || after.CapWouldBlock != 0 || after.OverrideR != 0 || after.OverrideG != 0 {
		t.Fatalf("expected zeroed snapshot, got %+v", after)
	}
}

func TestPathPredictionStatsSnapshotBeaconCounters(t *testing.T) {
	s := &Server{}

	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceCombined, BeaconRX: true}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceInsufficient, BeaconRX: true, InsufficientReason: pathreliability.InsufficientLowCount, Weight: 0.25}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceVOACAPClosed, BeaconRX: true}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceVOACAPAligned, BeaconRX: true}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceVOACAPSparseUpgrade, BeaconRX: true}, false, false)
	s.recordPathPrediction(pathreliability.Result{Source: pathreliability.SourceVOACAPOpen, BeaconRX: true}, false, false)

	stats := s.PathPredictionStatsSnapshot()
	if stats.Total != 6 || stats.Combined != 1 || stats.Insufficient != 1 || stats.VOACAPClosed != 1 || stats.VOACAPAligned != 1 || stats.VOACAPSparseUpgrade != 1 || stats.VOACAPOpen != 1 {
		t.Fatalf("unexpected aggregate beacon stats: %+v", stats)
	}
	if stats.BeaconRX != 1 ||
		stats.BeaconRXInsufficient != 1 ||
		stats.BeaconRXLowCount != 1 ||
		stats.BeaconRXVOACAPClosed != 1 ||
		stats.BeaconRXVOACAPAligned != 1 ||
		stats.BeaconRXVOACAPSparseUpgrade != 1 ||
		stats.BeaconRXVOACAPOpen != 1 {
		t.Fatalf("unexpected beacon-specific stats: %+v", stats)
	}
}

func TestPathResultWithClosedFallbackTraceRecordsSparseP50VOACAP(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	cfg.GlyphSymbols.Closed = "!"
	cfg.VOACAPFallback.ReliabilityGatedOpenEnabled = true
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})
	req := pathreliability.VOACAPClosedRequest{Band: "20m", Mode: "FT8"}
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)

	t.Run("delayed no p50", func(t *testing.T) {
		s := &Server{
			pathPredictor: predictor,
			pathClosedFallback: detailedPathClosedFallback{
				check: pathreliability.VOACAPForecastCheck{
					Status:    pathreliability.VOACAPForecastCheckDelayWait,
					CacheMiss: true,
				},
			},
		}
		got, trace := s.pathResultWithClosedFallbackTrace(pathreliability.Result{
			Source:             pathreliability.SourceInsufficient,
			InsufficientReason: pathreliability.InsufficientNoSample,
		}, req, now)
		if got.Source != pathreliability.SourceInsufficient {
			t.Fatalf("delayed lookup must not change result: %+v", got)
		}
		s.recordSparseP50VOACAPTrace(trace)
		stats := s.PathPredictionStatsSnapshot().SparseP50VOACAP
		if stats.Total != 1 || stats.NoP50 != 1 || stats.CacheMissTotal != 1 || stats.Delayed != 1 {
			t.Fatalf("unexpected delayed sparse stats: %+v", stats)
		}
	})

	t.Run("closed very low sparse p50", func(t *testing.T) {
		s := &Server{
			pathPredictor: predictor,
			pathClosedFallback: detailedPathClosedFallback{
				check: pathreliability.VOACAPForecastCheck{
					Status: pathreliability.VOACAPForecastCheckReady,
					Forecast: pathreliability.VOACAPCachedForecast{
						Record: pathreliability.VOACAPHourlyForecast{FT8SNRDB: -34, HourUTC: 20, FrequencyMHz: 14.1},
						SSN:    112,
					},
				},
			},
		}
		got, trace := s.pathResultWithClosedFallbackTrace(pathreliability.Result{
			Source:             pathreliability.SourceInsufficient,
			HasP50:             true,
			P50DB:              -19,
			ObservationCount:   2,
			InsufficientReason: pathreliability.InsufficientLowCount,
		}, req, now)
		if got.Source != pathreliability.SourceVOACAPClosed {
			t.Fatalf("expected closed fallback result, got %+v", got)
		}
		s.recordSparseP50VOACAPTrace(trace)
		stats := s.PathPredictionStatsSnapshot().SparseP50VOACAP
		if stats.Total != 1 || stats.VeryLowCount != 1 || stats.CacheHit != 1 || stats.Closed != 1 {
			t.Fatalf("unexpected closed sparse stats: %+v", stats)
		}
	})

	t.Run("rel fail no p50", func(t *testing.T) {
		s := &Server{
			pathPredictor: predictor,
			pathClosedFallback: detailedPathClosedFallback{
				check: pathreliability.VOACAPForecastCheck{
					Status:   pathreliability.VOACAPForecastCheckReady,
					Forecast: forecastWithReqSNRReliability(-19, 0.60),
				},
			},
		}
		got, trace := s.pathResultWithClosedFallbackTrace(pathreliability.Result{
			Source: pathreliability.SourceInsufficient,
		}, req, now)
		if got.Source != pathreliability.SourceInsufficient {
			t.Fatalf("REL-failed no-p50 forecast must remain insufficient: %+v", got)
		}
		s.recordSparseP50VOACAPTrace(trace)
		stats := s.PathPredictionStatsSnapshot().SparseP50VOACAP
		if stats.Total != 1 || stats.OpenRELFail != 1 || stats.RELBelowFloor != 1 {
			t.Fatalf("unexpected REL-fail sparse stats: %+v", stats)
		}
	})

	t.Run("invalid request reason", func(t *testing.T) {
		s := &Server{
			pathPredictor: predictor,
			pathClosedFallback: detailedPathClosedFallback{
				check: pathreliability.VOACAPForecastCheck{
					Status:        pathreliability.VOACAPForecastCheckInvalidRequest,
					InvalidReason: pathreliability.VOACAPInvalidDXGrid,
				},
			},
		}
		got, trace := s.pathResultWithClosedFallbackTrace(pathreliability.Result{
			Source:             pathreliability.SourceInsufficient,
			InsufficientReason: pathreliability.InsufficientNoSample,
		}, req, now)
		if got.Source != pathreliability.SourceInsufficient {
			t.Fatalf("invalid VOACAP request must not change result: %+v", got)
		}
		s.recordSparseP50VOACAPTrace(trace)
		stats := s.PathPredictionStatsSnapshot().SparseP50VOACAP
		if stats.Total != 1 || stats.NoP50 != 1 || stats.InvalidRequest != 1 || stats.InvalidDXGrid != 1 {
			t.Fatalf("unexpected invalid sparse stats: %+v", stats)
		}
	})
}

func TestPathResultWithClosedFallbackTraceUsesNative160WhenVOACAPUnavailable(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	cfg.Native160Fallback.Enabled = true
	cfg.Native160Fallback.DisplayEnabled = true
	predictor := pathreliability.NewPredictor(cfg, []string{"160m"})
	req := pathreliability.VOACAPClosedRequest{
		UserGrid: "FN31",
		DXGrid:   "QF56",
		Band:     "160m",
		Mode:     "FT8",
	}
	now := time.Date(2026, time.June, 18, 12, 0, 0, 0, time.UTC)
	base := pathreliability.Result{
		Glyph:              cfg.GlyphSymbols.Insufficient,
		Source:             pathreliability.SourceInsufficient,
		InsufficientReason: pathreliability.InsufficientNoSample,
	}

	t.Run("no voacap fallback", func(t *testing.T) {
		s := &Server{pathPredictor: predictor}
		got, trace := s.pathResultWithClosedFallbackTrace(base, req, now)
		if got.Source != pathreliability.SourceNative160 || got.Class != filter.PathClassLow {
			t.Fatalf("expected native 160 LOW fallback, got %+v", got)
		}
		if !trace.Active {
			t.Fatalf("expected sparse VOACAP trace to remain active")
		}
		s.recordPathPrediction(got, false, false)
		stats := s.PathPredictionStatsSnapshot()
		if stats.Native160Low != 1 || stats.Native160.Candidate != 1 || stats.Native160.Emitted != 1 || stats.Native160.Low != 1 {
			t.Fatalf("unexpected native 160 stats: %+v", stats)
		}
	})

	t.Run("no voacap fallback native closed", func(t *testing.T) {
		s := &Server{pathPredictor: predictor}
		closedReq := req
		closedReq.DXGrid = "FN20"
		got, trace := s.pathResultWithClosedFallbackTrace(base, closedReq, time.Date(2026, time.June, 18, 16, 0, 0, 0, time.UTC))
		if got.Source != pathreliability.SourceNative160 || got.Class != filter.PathClassClosed {
			t.Fatalf("expected native 160 CLOSED fallback, got %+v", got)
		}
		if pathClassFromPrediction(pathPrediction{result: got}) != filter.PathClassClosed {
			t.Fatalf("native closed must map to PATH CLOSED, got %+v", got)
		}
		if !trace.Active {
			t.Fatalf("expected sparse VOACAP trace to remain active")
		}
		s.recordPathPrediction(got, false, false)
		stats := s.PathPredictionStatsSnapshot()
		if stats.Native160Closed != 1 ||
			stats.Native160.Candidate != 1 ||
			stats.Native160.Emitted != 1 ||
			stats.Native160.Closed != 1 ||
			stats.Native160.DarkLEClosed != 1 ||
			stats.Native160.NotDark != 0 {
			t.Fatalf("unexpected native 160 closed stats: %+v", stats)
		}
	})

	t.Run("voacap ready keeps precedence", func(t *testing.T) {
		s := &Server{
			pathPredictor: predictor,
			pathClosedFallback: detailedPathClosedFallback{
				check: pathreliability.VOACAPForecastCheck{
					Status: pathreliability.VOACAPForecastCheckReady,
					Forecast: pathreliability.VOACAPCachedForecast{
						Record: pathreliability.VOACAPHourlyForecast{FT8SNRDB: -34, HourUTC: 12, FrequencyMHz: 1.9},
						SSN:    112,
					},
				},
			},
		}
		got, _ := s.pathResultWithClosedFallbackTrace(base, req, now)
		if got.Source != pathreliability.SourceVOACAPClosed {
			t.Fatalf("ready VOACAP closed result must keep precedence, got %+v", got)
		}
		if got.Native160Checked {
			t.Fatalf("native 160 must not run when VOACAP is ready: %+v", got)
		}
	})
}

func TestPathGlyphFromPredictionDoesNotSolarOverrideNativeClosed(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	cfg.GlyphSymbols.Closed = "#"
	server := &Server{
		solarWeather: solarweather.NewManager(solarweather.Config{Enabled: true}, nil),
	}
	prediction := pathPrediction{
		result: pathreliability.Result{
			Source: pathreliability.SourceNative160,
			Class:  filter.PathClassClosed,
			Glyph:  cfg.GlyphSymbols.Closed,
		},
	}
	if got := server.pathGlyphFromPrediction(prediction); got != cfg.GlyphSymbols.Closed {
		t.Fatalf("native closed glyph = %q, want closed glyph %q", got, cfg.GlyphSymbols.Closed)
	}
	if stats := server.PathPredictionStatsSnapshot(); stats.OverrideR != 0 || stats.OverrideG != 0 {
		t.Fatalf("native closed must not enter R/G override path, got stats %+v", stats)
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

func TestPathResultWithClosedFallbackBeaconUsesReceiveLegVOACAP(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	cfg.VOACAPFallback.ReliabilityGatedOpenEnabled = true
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})
	req := pathreliability.VOACAPClosedRequest{
		Band:                  "20m",
		Mode:                  "FT8",
		ReceiveNoisePenaltyDB: 5,
	}
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)
	forecast := pathreliability.VOACAPCachedForecast{
		Record: pathreliability.VOACAPHourlyForecast{
			FT8SNRDB:                        -34,
			HourUTC:                         20,
			FrequencyMHz:                    14.1,
			ReceiveFT8SNRDB:                 -10,
			TransmitFT8SNRDB:                -34,
			HasDirectionalSNR:               true,
			ReceiveReqSNRReliability:        0.84,
			TransmitReqSNRReliability:       0.10,
			HasDirectionalReqSNRReliability: true,
		},
		EffectiveFT8SNRDB:     -34,
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
		Source:   pathreliability.SourceInsufficient,
		BeaconRX: true,
	}, req, now)
	if got.Source != pathreliability.SourceVOACAPOpen || !got.BeaconRX || got.Class != "MEDIUM" {
		t.Fatalf("expected beacon receive-leg VOACAP open MEDIUM result, got %+v", got)
	}
	if got.VOACAPFT8SNRDB != -15 || got.P50DB != -15 {
		t.Fatalf("expected receive-leg SNR diagnostics -15, got %+v", got)
	}
	if !got.VOACAPHasReqSNRReliability || got.VOACAPReqSNRReliability != 0.84 {
		t.Fatalf("expected receive-leg REL diagnostics, got %+v", got)
	}
	stats := s.PathPredictionStatsSnapshot()
	if stats.VOACAPFallbackOpenNoP50 != 1 || stats.VOACAPFallbackOpenNoP50REL != 1 {
		t.Fatalf("unexpected receive-leg fallback stats: %+v", stats)
	}
}

func TestPathResultWithClosedFallbackSkipsP50CompareForBeaconRX(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
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
	base := pathreliability.Result{Source: pathreliability.SourceCombined, BeaconRX: true, HasP50: true, P50DB: -12}
	got := s.pathResultWithClosedFallback(base, req, now)
	if got.Source != pathreliability.SourceCombined || !got.BeaconRX {
		t.Fatalf("beacon sufficient p50 should remain unchanged, got %+v", got)
	}
	stats := s.PathPredictionStatsSnapshot()
	if stats.VOACAPP50CompareChecked != 0 || stats.VOACAPP50CompareCacheHit != 0 || stats.VOACAPP50CompareCacheMiss != 0 {
		t.Fatalf("beacon RX sufficient p50 must not enter blended compare stats: %+v", stats)
	}
}

func TestPathResultWithClosedFallbackReliabilityGatedOpenAndSparseUpgrade(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	cfg.VOACAPFallback.ReliabilityGatedOpenEnabled = true
	cfg.VOACAPFallback.ReliabilitySparseUpgradeEnabled = true
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})
	req := pathreliability.VOACAPClosedRequest{Band: "20m", Mode: "FT8"}
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)

	s := &Server{
		pathPredictor: predictor,
		pathClosedFallback: fakePathClosedFallback{
			forecast: forecastWithReqSNRReliability(-19, 0.75),
			ok:       true,
		},
	}
	got := s.pathResultWithClosedFallback(pathreliability.Result{
		Source: pathreliability.SourceInsufficient,
	}, req, now)
	if got.Source != pathreliability.SourceVOACAPOpen || got.Class != "LOW" || got.Glyph != cfg.GlyphSymbols.Low {
		t.Fatalf("expected REL-gated no-p50 open LOW result, got %+v", got)
	}
	if got.HasP50 {
		t.Fatalf("VOACAP-only open result must not claim p50 evidence: %+v", got)
	}
	if !got.VOACAPHasReqSNRReliability || got.VOACAPReqSNRReliability != 0.75 {
		t.Fatalf("expected VOACAP REL diagnostics, got %+v", got)
	}
	stats := s.PathPredictionStatsSnapshot()
	if stats.VOACAPFallbackOpenNoP50 != 1 || stats.VOACAPFallbackOpenNoP50REL != 1 {
		t.Fatalf("unexpected no-p50 REL stage stats: %+v", stats)
	}

	s = &Server{
		pathPredictor: predictor,
		pathClosedFallback: fakePathClosedFallback{
			forecast: forecastWithReqSNRReliability(-15, 0.84),
			ok:       true,
		},
	}
	got = s.pathResultWithClosedFallback(pathreliability.Result{
		Source: pathreliability.SourceInsufficient,
		HasP50: true,
		P50DB:  -19,
	}, req, now)
	if got.Source != pathreliability.SourceVOACAPSparseUpgrade || got.Class != "MEDIUM" || got.Glyph != cfg.GlyphSymbols.Medium {
		t.Fatalf("expected REL-gated sparse p50 upgrade to MEDIUM, got %+v", got)
	}
	if !got.HasP50 || got.P50DB != -19 {
		t.Fatalf("sparse upgrade must retain original sparse p50 evidence, got %+v", got)
	}
	stats = s.PathPredictionStatsSnapshot()
	if stats.VOACAPFallbackSparseUpgrade != 1 || stats.VOACAPFallbackClassMismatch != 0 {
		t.Fatalf("unexpected sparse upgrade stage stats: %+v", stats)
	}
}

func TestPathResultWithClosedFallbackReliabilityGateBlocksOpenAndUpgrade(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	cfg.VOACAPFallback.ReliabilityGatedOpenEnabled = true
	cfg.VOACAPFallback.ReliabilitySparseUpgradeEnabled = true
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})
	req := pathreliability.VOACAPClosedRequest{Band: "20m", Mode: "FT8"}
	now := time.Date(2026, time.June, 8, 20, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		base          pathreliability.Result
		forecast      pathreliability.VOACAPCachedForecast
		wantMissing   int64
		wantBelow     int64
		wantMultiTier int64
		wantMismatch  int64
		wantNoP50     int64
	}{
		{
			name:        "no p50 missing REL",
			base:        pathreliability.Result{Source: pathreliability.SourceInsufficient},
			forecast:    forecastWithoutReqSNRReliability(-19),
			wantMissing: 1,
			wantNoP50:   1,
		},
		{
			name:      "no p50 below REL",
			base:      pathreliability.Result{Source: pathreliability.SourceInsufficient},
			forecast:  forecastWithReqSNRReliability(-19, 0.60),
			wantBelow: 1,
			wantNoP50: 1,
		},
		{
			name:          "sparse p50 multi tier upgrade shadow only",
			base:          pathreliability.Result{Source: pathreliability.SourceInsufficient, HasP50: true, P50DB: -19},
			forecast:      forecastWithReqSNRReliability(-12, 0.95),
			wantMultiTier: 1,
			wantMismatch:  1,
		},
		{
			name:         "sparse p50 one tier below REL",
			base:         pathreliability.Result{Source: pathreliability.SourceInsufficient, HasP50: true, P50DB: -19},
			forecast:     forecastWithReqSNRReliability(-15, 0.70),
			wantBelow:    1,
			wantMismatch: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				pathPredictor: predictor,
				pathClosedFallback: fakePathClosedFallback{
					forecast: tt.forecast,
					ok:       true,
				},
			}
			got := s.pathResultWithClosedFallback(tt.base, req, now)
			if got.Source != pathreliability.SourceInsufficient {
				t.Fatalf("expected blocked candidate to remain insufficient, got %+v", got)
			}
			stats := s.PathPredictionStatsSnapshot()
			if stats.VOACAPFallbackReliabilityMissing != tt.wantMissing ||
				stats.VOACAPFallbackReliabilityBelow != tt.wantBelow ||
				stats.VOACAPFallbackReliabilityMultiTier != tt.wantMultiTier ||
				stats.VOACAPFallbackClassMismatch != tt.wantMismatch ||
				stats.VOACAPFallbackOpenNoP50 != tt.wantNoP50 {
				t.Fatalf("unexpected blocked REL stats: %+v", stats)
			}
		})
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

type detailedPathClosedFallback struct {
	check pathreliability.VOACAPForecastCheck
}

func (f detailedPathClosedFallback) CheckForecast(pathreliability.VOACAPClosedRequest, time.Time) (pathreliability.VOACAPCachedForecast, bool) {
	if f.check.Status != pathreliability.VOACAPForecastCheckReady {
		return pathreliability.VOACAPCachedForecast{}, false
	}
	return f.check.Forecast, true
}

func (f detailedPathClosedFallback) CheckForecastDetailed(pathreliability.VOACAPClosedRequest, time.Time) pathreliability.VOACAPForecastCheck {
	return f.check
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

func forecastWithReqSNRReliability(snr int, rel float64) pathreliability.VOACAPCachedForecast {
	return pathreliability.VOACAPCachedForecast{
		Record: pathreliability.VOACAPHourlyForecast{
			FT8SNRDB:                        snr,
			HourUTC:                         20,
			FrequencyMHz:                    14.1,
			ReceiveReqSNRReliability:        rel,
			TransmitReqSNRReliability:       rel,
			HasDirectionalReqSNRReliability: true,
		},
		SSN: 112,
	}
}

func forecastWithoutReqSNRReliability(snr int) pathreliability.VOACAPCachedForecast {
	return pathreliability.VOACAPCachedForecast{
		Record: pathreliability.VOACAPHourlyForecast{
			FT8SNRDB:     snr,
			HourUTC:      20,
			FrequencyMHz: 14.1,
		},
		SSN: 112,
	}
}
