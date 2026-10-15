package telnet

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

// This consumer oracle funds the real regional store once. Its 21 weak versus
// 16 local strong reports produce LOW; the removed overlapping blend was HIGH.
func TestS6PopulationSelectionDisplayFilterAndDiag(t *testing.T) {
	requireH3Mappings(t)
	cfg, err := pathreliability.LoadFile(filepath.Join("..", "data", "config", "path_reliability.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinObservationCount != 21 || cfg.BeaconMinObservationCount != 11 || cfg.ReceiverMaxEffectiveCount != 8 {
		t.Fatal("shipped support policy changed")
	}
	cfg.VOACAPFallback.Enabled, cfg.Native160Fallback.Enabled = false, false
	now := time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
	rx, tx := pathreliability.EncodeCell("FN31"), pathreliability.EncodeCell("IO91")
	rc, tc := pathreliability.EncodeCoarseCell("FN31"), pathreliability.EncodeCoarseCell("IO91")
	if rx == tx || rc == tc {
		t.Fatal("fixture requires distinct directions")
	}
	for _, beacon := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "beacon"}[beacon], func(t *testing.T) {
			p := pathreliability.NewPredictor(cfg, []string{"20m"})
			for i := 0; i < 37; i++ {
				fr, ft, db := pathreliability.InvalidCell, pathreliability.InvalidCell, -20.0
				if i < 16 {
					fr, ft, db = rx, tx, -10
				}
				admission := p.UpdateWithReceiverHash(pathreliability.BucketCombined, fr, ft, rc, tc, "20m", db, 1, pathreliability.ObservationTimes{ObservedAt: now, ArrivedAt: now}, false, uint64(1+i%6))
				if !admission.Accepted() {
					t.Fatalf("report %d rejected: %+v", i, admission)
				}
			}
			s := &Server{pathPredictor: p, pathDisplay: true, noiseModel: cfg.NoiseModel(), nowFn: func() time.Time { return now }}
			c := &Client{filter: filter.NewFilter(), spotChan: make(chan *spotEnvelope, 1), grid: "FN31", gridCell: rx, noiseClass: "QUIET"}
			c.filter.SetMode("FT8", true)
			c.filter.SetPathClass(filter.PathClassLow, true)
			c.setDiagMode(diagModePath)
			sp := spot.NewSpot("DX1AA", "DE1AA", 14074, "FT8")
			sp.BandNorm, sp.DXCellID, sp.DXMetadata.Grid, sp.IsBeacon = "20m", uint16(tx), "IO91", beacon
			env := deliverPathPredictionReuseSpot(t, s, c, sp)
			if env.pathPrediction == nil {
				t.Fatal("PATH LOW admission has no prediction")
			}
			got := env.pathPrediction.result
			wantWeight := 18.5
			if beacon {
				wantWeight = 37
			}
			if got.Source != pathreliability.SourceCombined || got.Class != "LOW" || got.Glyph != "<" || !got.HasP50 || got.P50DB != -19.5 || got.ObservationCount != 37 || got.Weight != wantWeight || got.ReceiverCount != 6 || !got.HasObservedSamplingTime || got.ObservedSamplingUnix != now.Unix() {
				t.Fatalf("regional consumer result: %+v", got)
			}
			line := s.formatSpotEnvelopeForClient(c, env)
			if len(strings.TrimRight(line, "\r\n")) != 78 || !strings.Contains(line, "n37|") {
				t.Fatalf("regional DIAG or fixed width changed: %q", line)
			}
			c.filter.SetPathClass(filter.PathClassLow, false)
			c.filter.SetPathClass(filter.PathClassHigh, true)
			s.deliverJob(broadcastJob{spot: sp, clients: []*Client{c}, allowFast: true, allowMed: true, allowSlow: true})
			select {
			case rejected := <-c.spotChan:
				t.Fatalf("PATH HIGH admitted regional LOW: %+v", rejected)
			default:
			}
		})
	}
}
