//go:build !qualification

package cluster

import (
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/spot"
)

func TestCCV2HarmonicPresenceDelivered(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, tc := range []struct {
		name                  string
		fundamental, incoming bool
	}{
		{"missing_incoming", true, false}, {"missing_fundamental", false, true}, {"both_measured", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAmbiguityDeliveryFixture(t, nil, config.CallCorrectionConfig{})
			hd := spot.NewHarmonicDetector(spot.HarmonicSettings{Enabled: true, RecencyWindow: time.Minute,
				MaxHarmonicMultiple: 4, FrequencyToleranceHz: 25, MinReportDelta: 6})
			at := time.Now().UTC().Truncate(time.Second)
			fundamental := newDeliveryTestSpot("K1ABC", "N0BBB", at.Add(-time.Second))
			fundamental.Frequency, fundamental.Report, fundamental.HasReport = 7010, 20, tc.fundamental
			if drop, _, _, _ := hd.ShouldDrop(fundamental, at); drop {
				t.Fatal("fundamental dropped")
			}
			f.pipeline.harmonicDetector, f.pipeline.harmonicCfg.Enabled = hd, true
			s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
			s.SourceType, s.Report, s.HasReport = spot.SourceManual, 5, tc.incoming
			if !tc.incoming {
				s.Report = 0
			}
			f.pipeline.processSpotBody(s, nil)
			want := 1
			if tc.fundamental && tc.incoming {
				want = 0
			}
			if f.ring.GetCount() != want {
				t.Fatalf("harmonic output count=%d want %d", f.ring.GetCount(), want)
			}
			// A later positive handoff is a FIFO witness for both actual sockets.
			control := newDeliveryTestSpot("K1CTL", "N0AAA", at.Add(time.Millisecond))
			control.SourceType = spot.SourceManual
			f.pipeline.processSpotBody(control, nil)
			for _, client := range []struct{ diag bool }{{false}, {true}} {
				conn, reader := f.normalConn, f.normalReader
				if client.diag {
					conn, reader = f.diagConn, f.diagReader
				}
				if want == 1 && !strings.Contains(correctionEvidenceReadDX(t, conn, reader), "K1ABC") {
					t.Fatal("missing SNR report did not reach TCP")
				}
				if !strings.Contains(correctionEvidenceReadDX(t, conn, reader), "K1CTL") {
					t.Fatal("FIFO control did not reach TCP")
				}
			}
			f.writer.Stop()
			reopened, err := archive.NewWriter(f.archiveCfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(reopened.Stop)
			rows, err := reopened.Recent(4)
			if err != nil || len(rows) != want+1 {
				t.Fatalf("reopened rows=%d want %d error=%v", len(rows), want+1, err)
			}
			for _, row := range rows {
				if row.DXCall == "K1ABC" && row.HasReport != tc.incoming {
					t.Fatal("archive invented SNR presence")
				}
			}
		})
	}
}
