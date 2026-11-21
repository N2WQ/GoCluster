package rbn

import (
	"testing"
	"time"
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
