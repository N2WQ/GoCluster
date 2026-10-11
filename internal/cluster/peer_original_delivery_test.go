//go:build !qualification

// File role: Proves local correction and frequency averaging are independent of
// actual original-content peer receipt, including persisted local output.
package cluster

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/peer"
	"dxcluster/spot"
)

func TestB0OriginalPeerDeliveredAfterLocalCorrection(t *testing.T) {
	temporalDeliveryDirectory(t)
	at := time.Now().UTC().Truncate(time.Second)
	r, _, _ := temporalDeliveryPopulation(t, at.Add(-time.Second), 2*time.Minute, "K1ABC")
	f := newAmbiguityDeliveryFixture(t, r, confidenceAttributionConfig())
	p := f.pipeline
	p.freqAvg = spot.NewFrequencyAverager()
	p.spotPolicy.FrequencyAveragingMinReports = 2
	p.spotPolicy.FrequencyAveragingToleranceHz = 300
	p.freqAvg.Average("K1ABC", 14020.32, at, time.Minute, .3)
	s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
	s.SourceType = spot.SourceManual
	s.Frequency = 14020.12
	s.Comment = "  CW\t~CQ~  "
	s.SpotterIP = "2001:0DB8:0:0:0:0:0:1"
	// Independent constants are fixed before the production pipeline mutates s.
	originalFields := []string{"14020.12", "K1ABD", at.Format("02-Jan-2006"), at.Format("1504Z"),
		"  CW\t~CQ~  ", "N0AAA", "N0CALL-1", "2001:0DB8:0:0:0:0:0:1"}
	p.processSpot(s, nil)
	if p.tracker.CallCorrections() != 1 || p.tracker.FrequencyCorrections() != 1 {
		t.Fatalf("fixture must actually change local identity and frequency: calls=%d frequency=%d",
			p.tracker.CallCorrections(), p.tracker.FrequencyCorrections())
	}
	for _, dst := range []bool{false, true} {
		conn, reader := f.normalConn, f.normalReader
		if dst {
			conn, reader = f.diagConn, f.diagReader
		}
		wire := correctionEvidenceReadDX(t, conn, reader)
		if !strings.Contains(wire, "14020.22") || len(wire) != 80 || strings.TrimSpace(wire[27:39]) != "K1ABC" || wire[71] != 'C' {
			t.Errorf("actual local TCP corrected identity/frequency/glyph: %q", wire)
		}
	}
	frame, err := peer.ParseFrame(peerAdmissionReadSpot(t, f.peer))
	if err != nil || frame.Type != "PC61" || !reflect.DeepEqual(frame.Fields, originalFields) {
		t.Errorf("actual peer receipt must preserve independent original tuple: frame=%+v err=%v want=%q", frame, err, originalFields)
	}
	assertLocal := func(label string, rows []*spot.Spot) {
		t.Helper()
		if len(rows) != 1 || rows[0].DXCall != "K1ABC" || rows[0].Frequency != 14020.22 || rows[0].Confidence != "C" {
			t.Errorf("%s must retain local corrected output: %+v", label, rows)
		}
	}
	assertLocal("ring", f.ring.GetRecent(2))
	f.writer.Stop()
	reopened, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err := reopened.Recent(2)
	if err != nil {
		t.Fatal(err)
	}
	assertLocal("reopened archive", rows)
}
