package rbn

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/skew"
)

func TestCWBurstIngressSourceAndClock(t *testing.T) {
	arrival := time.Date(2026, 11, 21, 5, 31, 17, 430000000, time.UTC)
	for _, tc := range []struct {
		payload string
		cq      bool
	}{
		{"CW 5 dB 24 WPM CQ", true}, {"CW 5 dB CQ", true}, {"CW 5 dB 24 WPM BEACON", false},
		{"RTTY 5 dB 45 BPS CQ", false},
	} {
		c := NewClient("example.invalid", 0, "N0FT", "TEST", nil, false, 2)
		c.parseSpotAt("DX de K1ABC: 3547.0 OZ4ADX "+tc.payload+" 0531Z", arrival)
		s := mustReadSpot(t, c)
		if tc.cq {
			if !s.RBNCQReceivedAt.Equal(arrival) || s.RBNCQRawKHz != 3547 || s.Clone().RBNCQReceivedAt != arrival {
				t.Fatalf("CQ %s arrival=%v raw=%v", tc.payload, s.RBNCQReceivedAt, s.RBNCQRawKHz)
			}
		} else if !s.RBNCQReceivedAt.IsZero() || s.RBNCQRawKHz != 0 {
			t.Fatalf("unexpected CQ evidence for %s", tc.payload)
		}
	}
}

func TestTaggedReceiverSkewFeedsResolverAndPreservesRawCQ(t *testing.T) {
	table, err := skew.NewTable([]skew.Entry{{Callsign: "LB9KJ-1", CorrectionFactor: 1.000014694}})
	if err != nil {
		t.Fatal(err)
	}
	store := skew.NewStore()
	store.Set(table)
	arrival := time.Date(2026, 11, 21, 5, 31, 17, 430000000, time.UTC)
	const lineTail = " OZ4ADX CW 5 dB 24 WPM CQ 0531Z"
	cfg := config.CallCorrectionConfig{Enabled: true, FrequencyToleranceHz: 400}
	for _, tc := range []struct {
		name, receiver string
		minimal        bool
		frequency      float64
		bucket         int64
	}{
		{"tagged", "LB9KJ-1-#", false, 14020.21, 35051},
		{"exact", "LB9KJ-1", false, 14020.21, 35051},
		{"other numeric receiver", "LB9KJ-2-#", false, 14020, 35050},
		{"human", "LB9KJ-1-#", true, 14020, 35050},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient("example.invalid", 0, "N0FT", "TEST", store, false, 2)
			if tc.minimal {
				c.UseMinimalParser()
			}
			c.parseSpotAt("DX de "+tc.receiver+": 14020.0"+lineTail, arrival)
			s := mustReadSpot(t, c)
			if s.DECallNorm != tc.receiver || s.DXCallNorm != "OZ4ADX" || s.Frequency != tc.frequency {
				t.Fatalf("parsed receiver=%q DX=%q frequency=%.2f, want receiver=%q DX=OZ4ADX frequency=%.2f",
					s.DECallNorm, s.DXCallNorm, s.Frequency, tc.receiver, tc.frequency)
			}
			if tc.minimal {
				return
			}
			if s.RBNCQRawKHz != 14020 || !s.RBNCQReceivedAt.Equal(arrival) {
				t.Fatalf("raw CQ evidence changed: frequency=%.2f arrival=%v", s.RBNCQRawKHz, s.RBNCQReceivedAt)
			}
			evidence, found := correctionflow.BuildResolverEvidenceSnapshot(s, cfg, nil, arrival)
			if !found || evidence.Key.Bucket != tc.bucket || evidence.Spotter != tc.receiver || evidence.FrequencyKHz != tc.frequency {
				t.Fatalf("unexpected resolver evidence: found=%t evidence=%+v", found, evidence)
			}
		})
	}

	// Primary dedup floors to whole kHz, so exercise a different boundary.
	c := NewClient("example.invalid", 0, "N0FT", "TEST", store, false, 2)
	c.parseSpotAt("DX de LB9KJ-1-#: 14019.9"+lineTail, arrival)
	corrected := mustReadSpot(t, c)
	raw := corrected.Clone()
	raw.Frequency = corrected.RBNCQRawKHz
	if corrected.Frequency != 14020.11 || raw.Frequency != 14019.9 || corrected.DedupeKey() == raw.DedupeKey() {
		t.Fatalf("expected skew to cross a primary dedup frequency boundary: raw=%.2f corrected=%.2f", raw.Frequency, corrected.Frequency)
	}
}
