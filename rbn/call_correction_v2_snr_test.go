package rbn

import (
	"strings"
	"testing"
)

func TestCCV2LegacySNRConsumers(t *testing.T) {
	for _, tc := range []struct {
		payload string
		report  int
		present bool
	}{
		{"CW -1250dB", 0, false}, {"CW 1.5dB", 0, false},
		{"CW -200 dB", -200, true}, {"CW 0dB", 0, true},
		{"1200Z-14dB CW", -14, true},
	} {
		t.Run(tc.payload, func(t *testing.T) {
			c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 2)
			c.parseSpot("DX de K1ABC: 3547.0 OZ4ADX " + tc.payload + " CQ")
			// The existing RBN source policy discards missing and zero reports.
			// The shared parser still distinguishes present zero for other users.
			if !tc.present || tc.report == 0 {
				select {
				case s := <-c.spotChan:
					t.Fatalf("RBN source admitted an unsupported report: %+v", s)
				default:
				}
				return
			}
			s := mustReadSpot(t, c)
			if s.HasReport != tc.present || s.Report != tc.report || s.Mode != "CW" {
				t.Fatalf("legacy wire report=%d present=%t mode=%s", s.Report, s.HasReport, s.Mode)
			}
			if tc.present && !strings.Contains(s.FormatDXClusterCW(false), "dB") {
				t.Fatal("measured SNR disappeared from formatted wire")
			}
			if tc.payload == "1200Z-14dB CW" && s.Time.UTC().Format("1504Z") != "1200Z" {
				t.Fatal("fused source timestamp was not retained")
			}
		})
	}
}
