package cluster

import (
	"math"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"dxcluster/telnet"
)

func TestPathIngestMetricsEffectiveObservation(t *testing.T) {
	requirePathReportH3Mappings(t)
	arrival := time.Date(2026, time.October, 13, 12, 59, 50, 0, time.UTC)
	s := pathTimestampSpot("FT8", arrival.Add(20*time.Second))
	original := s.Time
	cells := hydrateSpotPathCells(s)
	metrics := newPathReportMetrics()
	metrics.ObserveWithCells(s, arrival, cells.deCoarse, cells.dxCoarse)
	metrics.ObserveWithCells(s, time.Time{}, cells.deCoarse, cells.dxCoarse)
	hour, receivers, pairs := metrics.HourlyCounts(arrival)
	if hour != "2026-10-13 12" || receivers["20m"] != 1 || pairs["20m"] != 1 {
		t.Fatalf("accepted metrics used wrong time/population: hour=%s receivers=%v pairs=%v", hour, receivers, pairs)
	}
	if got := metrics.SnapshotSources()["PSK"]; got != 1 {
		t.Fatalf("zero effective timestamp entered source metrics: count=%d", got)
	}
	if s.Time != original {
		t.Fatalf("metrics changed source time: got=%s want=%s", s.Time, original)
	}
	// The convenience wrapper retains its existing source-time behavior.
	metrics.Observe(s, arrival)
	if hour, _, _ := metrics.HourlyCounts(arrival); hour != "2026-10-13 13" {
		t.Fatalf("Observe wrapper stopped using source time: hour=%s", hour)
	}
}

func TestPathIngestPathOnlyObservationTimes(t *testing.T) {
	requirePathReportH3Mappings(t)
	arrival := time.Date(2026, time.October, 13, 12, 59, 50, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		observed time.Time
		arrived  time.Time
		accepted bool
		clamped  uint64
		wantHour string
	}{
		{"observed", arrival.Add(-5 * time.Second), arrival, true, 0, "2026-10-13 12"},
		{"future_before_boundary", arrival.Add(120*time.Second - time.Nanosecond), arrival, true, 1, "2026-10-13 12"},
		{"future_at_boundary", arrival.Add(120 * time.Second), arrival, true, 1, "2026-10-13 12"},
		{"future_after_boundary", arrival.Add(120*time.Second + time.Nanosecond), arrival, false, 0, ""},
		{"missing_observation", time.Time{}, arrival, true, 0, "2026-10-13 12"},
		{"missing_arrival", arrival, time.Time{}, false, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			predictor := pathTimestampPredictor()
			metrics := newPathReportMetrics()
			stats := &pathOnlyStats{}
			s := pathTimestampSpot("WSPR", tc.observed)
			processPSKRPathOnlySpot(s, tc.arrived, predictor, metrics, stats, config.SpotPolicy{}, pathTimestampAllowedBands())
			if s.Time != tc.observed {
				t.Fatalf("path-only admission changed Spot.Time: got=%s want=%s", s.Time, tc.observed)
			}
			admission := predictor.AdmissionStats()
			if admission.ClampedFuture != tc.clamped || admission.RejectedWeight != 0 {
				t.Fatalf("unexpected report admission counters: %+v", admission)
			}
			snap := snapshotPathOnly(stats)
			if !tc.accepted {
				if predictor.TotalBuckets() != 0 || len(metrics.SnapshotSources()) != 0 || admission.RejectedTime != 1 {
					t.Fatalf("rejected timestamp entered path learning: buckets=%d admission=%+v", predictor.TotalBuckets(), admission)
				}
				if snap.updates != 0 || snap.drops != 1 || snap.badTime != 1 || diffPathOnly(snap, pathOnlySnapshot{}).badTime != 1 {
					t.Fatalf("timestamp rejection missing from path-only stats: %+v", snap)
				}
				return
			}
			if admission.RejectedTime != 0 || snap.updates != 1 || snap.drops != 0 || predictor.TotalBuckets() != 2 {
				t.Fatalf("accepted report population wrong: stats=%+v admission=%+v buckets=%d", snap, admission, predictor.TotalBuckets())
			}
			hour, receivers, pairs := metrics.HourlyCounts(arrival)
			if hour != tc.wantHour || receivers["20m"] != 1 || pairs["20m"] != 1 || metrics.SnapshotSources()["PSK"] != 1 {
				t.Fatalf("accepted path-only metric timestamp/population wrong: hour=%s receivers=%v pairs=%v", hour, receivers, pairs)
			}
			if result := pathTimestampPrediction(predictor, s, arrival); result.RawCount != 1 {
				t.Fatalf("fine/coarse admission duplicated raw report: %+v", result)
			}
		})
	}
}

func TestPathIngestPathOnlyChannel(t *testing.T) {
	requirePathReportH3Mappings(t)
	predictor := pathTimestampPredictor()
	metrics := newPathReportMetrics()
	stats := &pathOnlyStats{}
	before := time.Now().UTC()
	reports := []*spot.Spot{
		pathTimestampSpot("WSPR", before.Add(-5*time.Second)),
		pathTimestampSpot("WSPR", before.Add(60*time.Second)),
		pathTimestampSpot("WSPR", before.Add(time.Hour)),
		pathTimestampSpot("WSPR", time.Time{}),
	}
	channel := make(chan *spot.Spot, len(reports)+1)
	channel <- nil
	for _, s := range reports {
		channel <- s
	}
	close(channel)
	processPSKRPathOnlyChannel(channel, predictor, metrics, stats, config.SpotPolicy{}, pathTimestampAllowedBands())
	after := time.Now().UTC()
	if after.Sub(before) >= 60*time.Second {
		t.Fatal("channel fixture outlasted its future-clamp input interval")
	}
	admission := predictor.AdmissionStats()
	if admission.ClampedFuture != 1 || admission.RejectedTime != 1 || admission.RejectedWeight != 0 {
		t.Fatalf("actual channel loop did not use independent arrival once/report: %+v", admission)
	}
	snap := snapshotPathOnly(stats)
	if snap.updates != 3 || snap.drops != 1 || snap.badTime != 1 || metrics.SnapshotSources()["PSK"] != 3 {
		t.Fatalf("actual channel loop learned/reported wrong population: %+v", snap)
	}
	if reports[0].Time != before.Add(-5*time.Second) || reports[1].Time != before.Add(60*time.Second) || reports[2].Time != before.Add(time.Hour) || !reports[3].Time.IsZero() {
		t.Fatal("actual channel loop rewrote source timestamps")
	}
	if result := pathTimestampPrediction(predictor, reports[0], after); result.RawCount != 3 {
		t.Fatalf("actual channel loop raw report count=%d, want 3", result.RawCount)
	}
}

func TestPathIngestRegularPreservesDelivery(t *testing.T) {
	requirePathReportH3Mappings(t)
	for _, kind := range []string{"observed", "clamped", "rejected", "missing"} {
		t.Run(kind, func(t *testing.T) {
			predictor := pathTimestampPredictor()
			metrics := newPathReportMetrics()
			pipeline, writer, server := pathTimestampDeliveryPipeline(t, predictor, metrics)
			before := time.Now().UTC()
			observed := before.Add(-5 * time.Second)
			switch kind {
			case "clamped":
				observed = before.Add(60 * time.Second)
			case "rejected":
				observed = before.Add(time.Hour)
			case "missing":
				observed = time.Time{}
			}
			s := pathTimestampSpot("FT8", observed)
			ctx := outputSpotContext{spot: s}
			if !pipeline.prepareFanoutSpot(&ctx) {
				t.Fatal("path timestamp admission suppressed ordinary fanout")
			}
			after := time.Now().UTC()
			admission := predictor.AdmissionStats()
			accepted := kind != "rejected"
			if (admission.ClampedFuture == 1) != (kind == "clamped") || (admission.RejectedTime == 1) != !accepted {
				t.Fatalf("regular ingress did not capture independent arrival: %+v", admission)
			}
			if accepted {
				if result := pathTimestampPrediction(predictor, s, after); result.RawCount != 1 || metrics.SnapshotSources()["PSK"] != 1 {
					t.Fatalf("accepted ordinary report missing or duplicated: %+v", result)
				}
			} else if predictor.TotalBuckets() != 0 || len(metrics.SnapshotSources()) != 0 {
				t.Fatal("invalid future ordinary report entered path learning")
			}
			pipeline.emitSpot(&ctx, outputDeliveryPlan{telnetDeliverNow: true, allowFast: true, allowMed: true, allowSlow: true})
			recent := pipeline.buf.GetRecent(1)
			if len(recent) != 1 || recent[0].Time != observed || readArchiveQueuedSpot(t, writer).Time != observed || readTelnetBroadcastSpot(t, server).Time != observed || s.Time != observed {
				t.Fatal("path timestamp handling changed or suppressed downstream source timestamp")
			}
		})
	}
}

func TestPathIngestAdmissionLogLine(t *testing.T) {
	predictor := pathTimestampPredictor()
	arrival := time.Date(2026, time.October, 13, 12, 0, 0, 0, time.UTC)
	previous := predictor.AdmissionStats()
	for _, tc := range []struct {
		observed time.Time
		weight   float64
	}{
		{arrival.Add(time.Second), 1},
		{arrival.Add(121 * time.Second), 1},
		{arrival, math.Inf(1)},
	} {
		predictor.UpdateWithReceiverHash(pathreliability.BucketCombined, 1, 2, 3, 4, "20m", -12, tc.weight, pathreliability.ObservationTimes{ObservedAt: tc.observed, ArrivedAt: arrival}, false, 1)
	}
	current := predictor.AdmissionStats()
	if line := pathAdmissionLogLine(current, previous); line != "Path admission (5m): future_clamped=1 rejected_time=1 rejected_weight=1" {
		t.Fatalf("report-level interval diagnostic: %q", line)
	}
	if line := pathAdmissionLogLine(current, current); line != "" {
		t.Fatalf("unchanged cumulative counters emitted a duplicate interval: %q", line)
	}
}

func pathTimestampPredictor() *pathreliability.Predictor {
	cfg := pathreliability.DefaultConfig()
	cfg.ReceiverContributionMode = pathreliability.ReceiverContributionOff
	cfg.MinEffectiveWeight = 0.01
	cfg.MinObservationCount = 1
	return pathreliability.NewPredictor(cfg, []string{"20m"})
}

func pathTimestampSpot(mode string, observed time.Time) *spot.Spot {
	s := spot.NewSpot("K1ABC", "W1AW", 14074, mode)
	s.Time = observed
	s.HasReport = true
	s.Report = -12
	s.SourceType = spot.SourcePSKReporter
	s.DEMetadata.Grid = "FN20"
	s.DXMetadata.Grid = "EM12"
	s.InvalidateMetadataCache()
	s.EnsureNormalized()
	return s
}

func pathTimestampAllowedBands() map[string]struct{} {
	return map[string]struct{}{"20m": {}}
}

func pathTimestampPrediction(predictor *pathreliability.Predictor, s *spot.Spot, query time.Time) pathreliability.Result {
	cells := hydrateSpotPathCells(s)
	return predictor.Predict(cells.deFine, cells.dxFine, cells.deCoarse, cells.dxCoarse, "20m", "FT8", 0, query)
}

func pathTimestampDeliveryPipeline(t *testing.T, predictor *pathreliability.Predictor, metrics *pathReportMetrics) (*outputPipeline, *archive.Writer, *telnet.Server) {
	t.Helper()
	writer, err := archive.NewWriter(config.ArchiveConfig{DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 1})
	if err != nil {
		t.Fatalf("new archive fixture: %v", err)
	}
	t.Cleanup(writer.Stop)
	server := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 1}, nil)
	pipeline := &outputPipeline{
		buf:           buffer.NewRingBuffer(4),
		archiveWriter: writer,
		telnet:        server,
		pathPredictor: predictor,
		pathReport:    metrics,
		allowedBands:  pathTimestampAllowedBands(),
	}
	return pipeline, writer, server
}
