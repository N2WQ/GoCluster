package rbn

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestRBNOriginalCWSpeedProjection(t *testing.T) {
	tests := []struct {
		name, payload, canonical, display string
		speeds                            []string
		available                         bool
	}{
		{"normal", "22 WPM CQ", "", "", []string{"22 WPM"}, true},
		{"multiple", "22 WPM 30 WPM POTA CQ", "22 WPM 30 WPM POTA", "POTA", []string{"22 WPM", "30 WPM"}, true},
		{"nonadjacent", "22 TEST WPM CQ", "22 WPM TEST", "22 TEST WPM", nil, true},
		{"different number", "22 30 WPM CQ", "22 WPM 30", "22", []string{"30 WPM"}, true},
		{"zero", "0 wpm CQ", "", "", []string{"0 WPM"}, true},
		{"maximum", "200 WPM CQ", "", "", []string{"200 WPM"}, true},
		{"over range", "201 WPM CQ", "", "201 WPM", nil, true},
		{"signed", "+22 WPM CQ", "+22 WPM", "+22 WPM", nil, true},
		{"decimal", "22.5 WPM CQ", "22.5 WPM", "22.5 WPM", nil, true},
		{"decimal suffix", "22. WPM CQ", "", "22. WPM", nil, true},
		{"absent", "POTA CQ", "POTA", "", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 10)
			c.parseSpot("DX de K1ABC: 3547.0 OZ4ADX CW 5 dB " + tc.payload + " 0318Z")
			s := mustReadSpot(t, c)
			if s.Comment != tc.canonical || s.CWDisplayAvailable != tc.available || s.CWDisplayComment != tc.display || !reflect.DeepEqual(s.CWSpeeds, tc.speeds) {
				t.Fatalf("canonical=%q available=%v projection=%q speeds=%q", s.Comment, s.CWDisplayAvailable, s.CWDisplayComment, s.CWSpeeds)
			}
		})
	}
}

func FuzzRBNCWSpeedProjection(f *testing.F) {
	f.Add(uint8(0), true, true)
	f.Add(uint8(200), false, true)
	f.Fuzz(func(t *testing.T, raw uint8, adjacent, second bool) {
		value := int(raw) % 201
		var payload string
		var want []string
		if adjacent {
			payload = fmt.Sprintf("%d WPM", value)
			want = append(want, fmt.Sprintf("%d WPM", value))
		} else {
			payload = fmt.Sprintf("%d TEST WPM", value)
		}
		if second {
			payload += " 30 WPM"
			want = append(want, "30 WPM")
		}
		c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 2)
		c.parseSpot("DX de K1ABC: 3547.0 OZ4ADX CW 5 dB " + payload + " CQ 0318Z")
		s := mustReadSpot(t, c)
		if !s.CWDisplayAvailable || !reflect.DeepEqual(s.CWSpeeds, want) {
			t.Fatalf("input=%q speeds=%q want=%q", payload, s.CWSpeeds, want)
		}
	})
}

func TestRBNMultipleCWSpeedsBoundedByInput(t *testing.T) {
	const count = 125
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 10)
	line := "DX de K1ABC: 3547.0 OZ4ADX CW 5 dB " + strings.Repeat("1 WPM ", count) + "POTA CQ 0318Z"
	if len(line) > rbnMaxLineLength {
		t.Fatal("test line exceeds feed limit")
	}
	c.parseSpot(line)
	s := mustReadSpot(t, c)
	if len(s.CWSpeeds) != count || s.CWDisplayComment != "POTA" {
		t.Fatalf("speeds=%d projection=%q", len(s.CWSpeeds), s.CWDisplayComment)
	}
}
