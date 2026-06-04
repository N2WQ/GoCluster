package cluster

import (
	"testing"
	"time"

	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestPathReportMetricsCountsAndHourReset(t *testing.T) {
	requirePathReportH3Mappings(t)

	now := time.Date(2026, 6, 4, 10, 15, 0, 0, time.UTC)
	metrics := newPathReportMetrics()
	first := pathReportMetricsSpot("K1ABC", "FN20", "EM12", spot.SourcePSKReporter, now)
	second := pathReportMetricsSpot("W1AW", "FN31", "DM04", spot.SourceRBN, now)

	metrics.Observe(first, now)
	metrics.Observe(first, now)
	metrics.Observe(second, now)

	sources := metrics.SnapshotSources()
	if sources["PSK"] != 2 || sources["RBN"] != 1 {
		t.Fatalf("source counts PSK=%d RBN=%d, want 2/1", sources["PSK"], sources["RBN"])
	}
	resetSources := metrics.SnapshotSources()
	if resetSources["PSK"] != 0 || resetSources["RBN"] != 0 {
		t.Fatalf("source counts after reset PSK=%d RBN=%d, want 0/0", resetSources["PSK"], resetSources["RBN"])
	}

	hourKey, spotters, pairs := metrics.HourlyCounts(now)
	if hourKey != "2026-06-04 10" {
		t.Fatalf("hourKey=%q, want 2026-06-04 10", hourKey)
	}
	if spotters["20m"] != 2 || pairs["20m"] != 2 {
		t.Fatalf("hour counts spotters=%d pairs=%d, want 2/2", spotters["20m"], pairs["20m"])
	}

	nextHour := now.Add(time.Hour)
	metrics.Observe(pathReportMetricsSpot("K1ABC", "FN20", "EM12", spot.SourcePSKReporter, nextHour), nextHour)
	hourKey, spotters, pairs = metrics.HourlyCounts(nextHour)
	if hourKey != "2026-06-04 11" {
		t.Fatalf("next hourKey=%q, want 2026-06-04 11", hourKey)
	}
	if spotters["20m"] != 1 || pairs["20m"] != 1 {
		t.Fatalf("next hour counts spotters=%d pairs=%d, want 1/1", spotters["20m"], pairs["20m"])
	}
}

func requirePathReportH3Mappings(t *testing.T) {
	t.Helper()
	if err := pathreliability.InitH3MappingsFromDir("../../data/h3"); err != nil {
		t.Skipf("InitH3Mappings unavailable: %v", err)
	}
	if pathreliability.EncodeCoarseCell("FN20") == pathreliability.InvalidCell {
		t.Skip("InitH3Mappings did not provide expected coarse cell")
	}
}

func pathReportMetricsSpot(deCall, deGrid, dxGrid string, source spot.SourceType, ts time.Time) *spot.Spot {
	return &spot.Spot{
		DECall:     deCall,
		DECallNorm: deCall,
		BandNorm:   "20m",
		Time:       ts,
		SourceType: source,
		DEMetadata: spot.CallMetadata{Grid: deGrid},
		DXMetadata: spot.CallMetadata{Grid: dxGrid},
	}
}
