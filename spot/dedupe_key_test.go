package spot

import (
	"encoding/hex"
	"testing"
	"time"
)

// These literals describe the pre-v15 wire-independent key layout, not the
// implementation of DedupeKey. Padding, truncation and minute/kHz equivalences
// remain intentional even though equality no longer relies on a short hash.
func TestPrimaryDedupeKeyLiteralVectors(t *testing.T) {
	base := func() *Spot {
		return &Spot{Time: time.Unix(61, 0), Frequency: 14074.9, DECall: " w1xyz ", DXCall: " k1abc "}
	}
	want := "3c00000000000000" + "fa360000" + "573158595a" + "00000000000000000000" + "4b31414243" + "00000000000000000000"
	tests := []struct {
		name string
		spot *Spot
		want string
	}{
		{"normalized", base(), want},
		{"same_minute_khz", &Spot{Time: time.Unix(119, 999), Frequency: 14074.1, DECall: "W1XYZ", DXCall: "K1ABC"}, want},
		{"next_minute", &Spot{Time: time.Unix(120, 0), Frequency: 14074.9, DECall: "W1XYZ", DXCall: "K1ABC"}, "7800000000000000" + want[16:]},
		{"next_khz", &Spot{Time: time.Unix(61, 0), Frequency: 14075, DECall: "W1XYZ", DXCall: "K1ABC"}, want[:16] + "fb360000" + want[24:]},
		{"fixed_call_width", &Spot{Time: time.Unix(61, 0), Frequency: 14074, DECallNorm: "ABCDEFGHIJKLMNOX", DXCallNorm: "PQRSTUVWXYZ1234Y"}, "3c00000000000000fa3600004142434445464748494a4b4c4d4e4f505152535455565758595a31323334"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := tt.spot.DedupeKey()
			if got := hex.EncodeToString(key[:]); got != tt.want {
				t.Fatalf("key=%s want=%s", got, tt.want)
			}
		})
	}
}

func TestHash32CompatibilityVectors(t *testing.T) {
	// Historical primary collision retained from Q1 c1cf35ca7b5a41e6925886802c900508.
	for _, tt := range []struct {
		call, at  string
		frequency float64
	}{
		{"DL1PPQQJJRR", "2026-10-02T17:33:00Z", 14220},
		{"DL1QQEEWWTT", "2026-10-02T17:34:00Z", 14041.8},
	} {
		at, err := time.Parse(time.RFC3339, tt.at)
		if err != nil {
			t.Fatal(err)
		}
		s := &Spot{DXCall: tt.call, DECall: "DL1AAA", Frequency: tt.frequency, Time: at}
		if got := s.Hash32(); got != 0xad34e59e {
			t.Fatalf("%s hash=%08x", tt.call, got)
		}
	}
}

func BenchmarkPrimaryDedupeKey(b *testing.B) {
	s := NewSpot("K1ABC", "W1XYZ", 14074, "FT8")
	s.Time = time.Unix(60, 0)
	b.ReportAllocs()
	for b.Loop() {
		if s.DedupeKey()[0] != 60 {
			b.Fatal("key changed")
		}
	}
}
