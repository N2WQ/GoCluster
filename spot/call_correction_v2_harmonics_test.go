package spot

import (
	"testing"
	"time"
)

func TestCCV2HarmonicPresence(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		first, next             bool
		firstReport, nextReport int
		elapsed                 time.Duration
		frequency               float64
		want                    bool
	}{
		{"both_absent", false, false, 20, 0, time.Second, 14000, false},
		{"fundamental_absent", false, true, 20, 0, time.Second, 14000, false},
		{"incoming_absent", true, false, 20, 0, time.Second, 14000, false},
		{"measured_zero_incoming", true, true, 20, 0, time.Second, 14000, true},
		{"measured_zero_fundamental", true, true, 0, -10, time.Second, 14000, true},
		{"delta_below", true, true, 10, 5, time.Second, 14000, false},
		{"delta_equal", true, true, 11, 5, time.Second, 14000, true},
		{"before_expiry", true, true, 20, 0, time.Minute - time.Nanosecond, 14000, true},
		{"expiry", true, true, 20, 0, time.Minute, 14000, false},
		{"stepped_below", true, true, 10, 1, time.Second, 21000, false},
		{"stepped_above", true, true, 10, 0, time.Second, 21000, true},
		{"nonmultiple", true, true, 20, 0, time.Second, 14100, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hd := NewHarmonicDetector(HarmonicSettings{Enabled: true, RecencyWindow: time.Minute,
				MaxHarmonicMultiple: 4, FrequencyToleranceHz: 25, MinReportDelta: 6, MinReportDeltaStep: 3.5})
			at := time.Unix(1700000000, 0).UTC()
			fundamental := &Spot{DXCall: "K1ABC", DECall: "N0AAA", Mode: "CW", Frequency: 7000,
				Time: at, Report: tc.firstReport, HasReport: tc.first}
			if drop, _, _, _ := hd.ShouldDrop(fundamental, at); drop {
				t.Fatal("first report dropped")
			}
			if !tc.first && (len(hd.entries) != 0 || len(hd.lastSeen) != 0 || len(hd.expiry) != 0) {
				t.Fatal("missing fundamental seeded retained comparison state")
			}
			// Preserve the explicitly chosen cross-receiver and cross-mode policy.
			next := &Spot{DXCall: "K1ABC", DECall: "N0BBB", Mode: "RTTY", Frequency: tc.frequency,
				Time: at.Add(tc.elapsed), Report: tc.nextReport, HasReport: tc.next}
			if drop, _, _, _ := hd.ShouldDrop(next, next.Time); drop != tc.want {
				t.Fatalf("drop=%t, want %t", drop, tc.want)
			}
			wantEntries := 0
			if tc.first {
				wantEntries = 1
			}
			if !tc.next && len(hd.entries["K1ABC"]) != wantEntries {
				t.Fatal("missing incoming report changed retained fundamental count")
			}
		})
	}
}
