// File role: Owns propagation-report aggregates for path prediction reporting.
// Crawler notes: Start here when investigating path source-mix logs, hourly
// unique spotter/grid-pair counts, or allocation in path reporting.
// Related docs: pathreliability/README.md, docs/decisions/ADR-0139-active-path-p50-histogram-lane-retention.md.
// Related tests: internal/cluster/path_report_metrics_test.go, internal/cluster/path_report_metrics_bench_test.go.
package cluster

import (
	"strings"
	"sync"
	"time"

	"dxcluster/pathreliability"
	"dxcluster/spot"
)

// pathReportMetrics tracks per-spot aggregates for reporting.
// It maintains per-hour unique spotters/grid pairs and per-5m source mix.
type pathReportMetrics struct {
	mu            sync.Mutex
	hourKey       string
	hourStartUnix int64
	spotters      map[string]map[string]struct{}
	gridPairs     map[string]map[uint32]struct{}
	sourceCounts  map[string]uint64
}

func newPathReportMetrics() *pathReportMetrics {
	return &pathReportMetrics{
		spotters:     make(map[string]map[string]struct{}),
		gridPairs:    make(map[string]map[uint32]struct{}),
		sourceCounts: make(map[string]uint64),
	}
}

func (m *pathReportMetrics) Observe(s *spot.Spot, now time.Time) {
	if m == nil || s == nil {
		return
	}
	ts := s.Time
	if ts.IsZero() {
		ts = now
	}
	ts = ts.UTC()
	hourStart := ts.Truncate(time.Hour)
	hourStartUnix := hourStart.Unix()

	band := strings.TrimSpace(s.BandNorm)
	if band == "" {
		band = strings.TrimSpace(s.Band)
	}
	if band == "" {
		band = spot.FreqToBand(s.Frequency)
	}
	band = strings.TrimSpace(spot.NormalizeBand(band))
	if band == "" || band == "???" {
		return
	}

	deCall := strings.TrimSpace(s.DECallNorm)
	if deCall == "" {
		deCall = strings.TrimSpace(s.DECall)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.hourKey == "" || m.hourStartUnix != hourStartUnix {
		m.hourKey = hourStart.Format("2006-01-02 15")
		m.hourStartUnix = hourStartUnix
		m.spotters = make(map[string]map[string]struct{})
		m.gridPairs = make(map[string]map[uint32]struct{})
	}

	if label := sourceBucket(s.SourceType); label != "" {
		m.sourceCounts[label]++
	}

	if deCall != "" {
		set := m.spotters[band]
		if set == nil {
			set = make(map[string]struct{})
			m.spotters[band] = set
		}
		set[deCall] = struct{}{}
	}

	deCoarse := pathreliability.EncodeCoarseCell(s.DEMetadata.Grid)
	dxCoarse := pathreliability.EncodeCoarseCell(s.DXMetadata.Grid)
	if deCoarse != pathreliability.InvalidCell && dxCoarse != pathreliability.InvalidCell {
		set := m.gridPairs[band]
		if set == nil {
			set = make(map[uint32]struct{})
			m.gridPairs[band] = set
		}
		set[pathReportGridPairKey(deCoarse, dxCoarse)] = struct{}{}
	}
}

func (m *pathReportMetrics) SnapshotSources() map[string]uint64 {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]uint64, len(m.sourceCounts))
	for k, v := range m.sourceCounts {
		out[k] = v
	}
	for k := range m.sourceCounts {
		m.sourceCounts[k] = 0
	}
	return out
}

func (m *pathReportMetrics) HourlyCounts(now time.Time) (string, map[string]int, map[string]int) {
	if m == nil {
		return "", nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hourKey == "" {
		hourStart := now.UTC().Truncate(time.Hour)
		m.hourKey = hourStart.Format("2006-01-02 15")
		m.hourStartUnix = hourStart.Unix()
	}
	spotters := make(map[string]int, len(m.spotters))
	for band, set := range m.spotters {
		spotters[band] = len(set)
	}
	pairs := make(map[string]int, len(m.gridPairs))
	for band, set := range m.gridPairs {
		pairs[band] = len(set)
	}
	return m.hourKey, spotters, pairs
}

func pathReportGridPairKey(deCoarse, dxCoarse pathreliability.CellID) uint32 {
	return uint32(deCoarse)<<16 | uint32(dxCoarse)
}

func sourceBucket(source spot.SourceType) string {
	switch source {
	case spot.SourceRBN:
		return "RBN"
	case spot.SourceFT8, spot.SourceFT4:
		return "RBN-FT"
	case spot.SourcePSKReporter:
		return "PSK"
	case spot.SourceManual:
		return "HUMAN"
	case spot.SourceUpstream:
		return "UPSTREAM"
	case spot.SourcePeer:
		return "PEER"
	default:
		return "OTHER"
	}
}
