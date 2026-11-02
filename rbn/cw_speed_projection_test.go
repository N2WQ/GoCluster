package rbn

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"dxcluster/spot"
)

// commentField examines the complete fixed-width mode/comment region, without
// comparing the formatter against another invocation of itself.
func commentField(line string) string {
	return strings.TrimSpace(line[39 : spot.CurrentDXClusterLayout().TailStartColumn-1])
}

func TestRBNProjectionRetainsCanonicalCleanupDecisions(t *testing.T) {
	tests := []struct {
		name, payload, canonical, projection, off, on string
		report                                        int
		speeds                                        []string
	}{
		{"signed before report", "CW +22 WPM 5 dB CQ", "+22 WPM", "+22 WPM", "CW 5 dB +22 WPM", "CW 5 dB +22 WPM", 5, nil},
		{"signed value is report", "CW +22 5 dB WPM CQ", "", "5 WPM", "CW 22 dB 5 WPM", "CW 22 dB 5 WPM", 22, nil},
		{"valid before report", "CW 22 WPM 5 dB CQ", "", "", "CW 5 dB", "CW 5 dB 22 WPM", 5, []string{"22 WPM"}},
		{"report and speed overlap", "CW 20 WPM 30 WPM 5 dB CQ", "20 WPM WPM 5", "5", "CW 30 dB 5", "CW 30 dB 5 20 WPM", 30, []string{"20 WPM", "30 WPM"}},
		{"removed CQ is not adjacency", "CW 22 CQ WPM 5 dB", "", "22 WPM", "CW 5 dB 22 WPM", "CW 5 dB 22 WPM", 5, nil},
		{"punctuated number", "CW 22. WPM 5 dB CQ", "", "22. WPM", "CW 5 dB 22. WPM", "CW 5 dB 22. WPM", 5, nil},
		{"punctuated unit", "CW 22 WPM, 5 dB CQ", "", "22 WPM,", "CW 5 dB 22 WPM,", "CW 5 dB 22 WPM,", 5, nil},
		{"peeled time prefix", "CW 0000Z22 WPM 5 dB CQ", "", "22 WPM", "CW 5 dB 22 WPM", "CW 5 dB 22 WPM", 5, nil},
		{"peeled time on unit", "CW 22 0000ZWPM 5 dB CQ", "", "22 WPM", "CW 5 dB 22 WPM", "CW 5 dB 22 WPM", 5, nil},
		{"peeled time on signed unit", "CW +22 0000ZWPM 5 dB CQ", "+22 WPM", "+22 WPM", "CW 5 dB +22 WPM", "CW 5 dB +22 WPM", 5, nil},
		{"peeled time keeps punctuation", "CW 5 dB 22 WPM CQ 0000ZPOTA!", "22 WPM POTA", "POTA!", "CW 5 dB POTA!", "CW 5 dB POTA! 22 WPM", 5, []string{"22 WPM"}},
		{"peeled time keeps leading punctuation", "CW 5 dB 22 WPM CQ .0000ZPOTA!", "22 WPM POTA", ".POTA!", "CW 5 dB .POTA!", "CW 5 dB .POTA! 22 WPM", 5, []string{"22 WPM"}},
		{"peeled time keeps numeric punctuation", "CW 5 dB 0000Z22. WPM CQ", "", "22. WPM", "CW 5 dB 22. WPM", "CW 5 dB 22. WPM", 5, nil},
		{"nonadjacent", "CW 22 TEST WPM 5 dB CQ", "22 WPM TEST", "22 TEST WPM", "CW 5 dB 22 TEST WPM", "CW 5 dB 22 TEST WPM", 5, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 2)
			c.parseSpot("DX de K1ABC: 3547.0 OZ4ADX " + tc.payload + " 0000Z")
			s := mustReadSpot(t, c)
			if !s.HasReport || s.Report != tc.report || s.Comment != tc.canonical || !s.CWDisplayAvailable || s.CWDisplayComment != tc.projection || !reflect.DeepEqual(s.CWSpeeds, tc.speeds) {
				t.Fatalf("report=%d hasReport=%v canonical=%q available=%v projection=%q speeds=%q", s.Report, s.HasReport, s.Comment, s.CWDisplayAvailable, s.CWDisplayComment, s.CWSpeeds)
			}
			if got := s.Time.UTC().Format("1504Z"); got != "0000Z" {
				t.Errorf("time=%q want 0000Z", got)
			}
			if got := commentField(s.FormatDXClusterCW(false)); got != tc.off {
				t.Errorf("OFF comment=%q want %q", got, tc.off)
			}
			if got := commentField(s.FormatDXClusterCW(true)); got != tc.on {
				t.Errorf("ON comment=%q want %q", got, tc.on)
			}
		})
	}
}

func TestRBNProjectionCanonicalOuterUnicodeWhitespace(t *testing.T) {
	for _, tc := range []struct {
		name, payload, canonical, display, off, on string
		speeds                                     []string
	}{
		{"trailing", "CW 5 dB 22 WPM 0000Z POTA\u2003 CQ", "22 WPM POTA", "POTA", "CW 5 dB POTA", "CW 5 dB POTA 22 WPM", []string{"22 WPM"}},
		{"leading", "\u2003CW 5 dB 22 WPM 0000Z CQ", "", "", "CW 5 dB", "CW 5 dB 22 WPM", []string{"22 WPM"}},
		{"unit trailing whitespace is not raw speed", "CW 5 dB 22 WPM\u2003 CQ", "", "22 WPM", "CW 5 dB 22 WPM", "CW 5 dB 22 WPM", nil},
		{"punctuation followed by whitespace", "CW 0000Z 5 dB 22 WPM,\u2003 CQ", "", "22 WPM,", "CW 5 dB 22 WPM,", "CW 5 dB 22 WPM,", nil},
		{"time-prefixed unit with punctuation and whitespace", "CW 5 dB 22 0000ZWPM,\u2003 CQ", "", "22 WPM,", "CW 5 dB 22 WPM,", "CW 5 dB 22 WPM,", nil},
		{"multiple punctuation and lowercase", "CW 0000Z 5 dB 22 wpm,;!\u2003 CQ", "", "22 wpm,;!", "CW 5 dB 22 wpm,;!", "CW 5 dB 22 wpm,;!", nil},
		{"leading whitespace and punctuation", "\u2003,WPM CW 5 dB 22 CQ", "WPM 22", ",WPM 22", "CW 5 dB ,WPM 22", "CW 5 dB ,WPM 22", nil},
		{"leading punctuation and time-prefixed unit", "\u2003,0000ZWPM CW 5 dB 22 CQ", "WPM 22", ",WPM 22", "CW 5 dB ,WPM 22", "CW 5 dB ,WPM 22", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 2)
			c.parseSpot("DX de K1ABC: 3547.0 OZ4ADX " + tc.payload)
			s := mustReadSpot(t, c)
			if !s.HasReport || s.Report != 5 || s.Comment != tc.canonical || !s.CWDisplayAvailable || s.CWDisplayComment != tc.display || !reflect.DeepEqual(s.CWSpeeds, tc.speeds) {
				t.Fatalf("report=%d canonical=%q display=%q speeds=%q", s.Report, s.Comment, s.CWDisplayComment, s.CWSpeeds)
			}
			if got := commentField(s.FormatDXClusterCW(false)); got != tc.off {
				t.Errorf("OFF=%q want %q", got, tc.off)
			}
			if got := commentField(s.FormatDXClusterCW(true)); got != tc.on {
				t.Errorf("ON=%q want %q", got, tc.on)
			}
		})
	}
}

func TestRBNProjectionUnicodeScannerOffset(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 2)
	c.parseSpot("DX de K1ABC: 3547.0 OZ4ADX CW 5 dB 22 ıııı FOO WPMX CQ 0000Z")
	s := mustReadSpot(t, c)
	if !s.HasReport || s.Report != 5 || s.Comment != "22 WPM ıııı WPMX" || !s.CWDisplayAvailable || s.CWDisplayComment != "22 ıııı FOO WPMX" || len(s.CWSpeeds) != 0 {
		t.Fatalf("report=%d canonical=%q available=%v display=%q speeds=%q", s.Report, s.Comment, s.CWDisplayAvailable, s.CWDisplayComment, s.CWSpeeds)
	}
	for _, show := range []bool{false, true} {
		if got := commentField(s.FormatDXClusterCW(show)); !strings.Contains(got, "FOO") {
			t.Errorf("show=%v lost ordinary text: %q", show, got)
		}
	}

	control := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 2)
	control.parseSpot("DX de K1ABC: 3547.0 OZ4ADX CW 5 dB 22 FOO WPMX CQ 0000Z")
	if got := mustReadSpot(t, control); got.CWDisplayAvailable || got.Comment != "22 FOO WPMX" {
		t.Fatalf("ASCII control changed: %+v", got)
	}

	unicodeControl := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 2)
	unicodeControl.parseSpot("DX de K1ABC: 3547.0 OZ4ADX CW 5 dB 22 ÿ POTA WPMX, CQ 0000Z")
	if got := mustReadSpot(t, unicodeControl); !got.CWDisplayAvailable || got.Comment != "22 ÿ POTA WPMX" || got.CWDisplayComment != "22 ÿ POTA WPMX," || len(got.CWSpeeds) != 0 {
		t.Fatalf("conservative Unicode control: canonical=%q display=%q speeds=%q", got.Comment, got.CWDisplayComment, got.CWSpeeds)
	}
}

func TestRBNProjectionConservativeInteriorToken(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 2)
	c.parseSpot("DX de K1ABC: 3547.0 OZ4ADX CW 5 dB 22 WPM,\u2003! POTA 30 WPM CQ 0000Z")
	s := mustReadSpot(t, c)
	if !s.HasReport || s.Report != 5 || s.Comment != "22 WPM WPM, POTA 30" || !s.CWDisplayAvailable || s.CWDisplayComment != "22 WPM,\u2003! POTA" || !reflect.DeepEqual(s.CWSpeeds, []string{"30 WPM"}) {
		t.Fatalf("report=%d canonical=%q display=%q speeds=%q", s.Report, s.Comment, s.CWDisplayComment, s.CWSpeeds)
	}
}

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
	f.Add(uint8(0), true, true, true)
	f.Add(uint8(200), false, true, false)
	f.Add(uint8(22), true, false, false)
	f.Fuzz(func(t *testing.T, raw uint8, adjacent, second, reportFirst bool) {
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
		report, ordinary := 5, "POTA"
		if !adjacent {
			ordinary = fmt.Sprintf("%d TEST WPM POTA", value)
		}
		body := "CW 5 dB " + payload + " POTA CQ 0318Z"
		if !reportFirst {
			body = "CW " + payload + " 5 dB POTA CQ 0318Z"
			if second {
				report = 30
				ordinary = strings.Replace(ordinary, "POTA", "5 POTA", 1)
			}
		}
		c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 2)
		c.parseSpot("DX de K1ABC: 3547.0 OZ4ADX " + body)
		s := mustReadSpot(t, c)
		if !s.CWDisplayAvailable || !reflect.DeepEqual(s.CWSpeeds, want) || !s.HasReport || s.Report != report || s.CWDisplayComment != ordinary {
			t.Fatalf("input=%q report=%d ordinary=%q speeds=%q want report=%d ordinary=%q speeds=%q", body, s.Report, s.CWDisplayComment, s.CWSpeeds, report, ordinary, want)
		}
	})
}

func FuzzRBNCWSpeedRawPunctuation(f *testing.F) {
	f.Add(uint8(22), uint8(0), false)
	f.Add(uint8(0), uint8(1), true)
	f.Add(uint8(200), uint8(2), false)
	f.Fuzz(func(t *testing.T, raw, form uint8, reportFirst bool) {
		value := int(raw) % 201
		var speedLike string
		switch form % 3 {
		case 0:
			speedLike = fmt.Sprintf("+%d WPM", value)
		case 1:
			speedLike = fmt.Sprintf("%d. WPM", value)
		default:
			speedLike = fmt.Sprintf("%d WPM,", value)
		}
		body := "CW 5 dB " + speedLike + " POTA CQ 0000Z"
		if !reportFirst {
			body = "CW " + speedLike + " 5 dB POTA CQ 0000Z"
		}
		c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 2)
		c.parseSpot("DX de K1ABC: 3547.0 OZ4ADX " + body)
		s := mustReadSpot(t, c)
		if !s.HasReport || s.Report != 5 || !s.CWDisplayAvailable || len(s.CWSpeeds) != 0 || s.CWDisplayComment != speedLike+" POTA" {
			t.Fatalf("input=%q report=%d display=%q speeds=%q", body, s.Report, s.CWDisplayComment, s.CWSpeeds)
		}
	})
}

func FuzzRBNCWPunctuationWhitespace(f *testing.F) {
	f.Add(uint8(22), uint8(0), uint8(0), false)
	f.Add(uint8(200), uint8(4), uint8(3), true)
	f.Fuzz(func(t *testing.T, raw, punctuationIndex, whitespaceIndex uint8, timeOnUnit bool) {
		punctuation := []string{",", ";", ":", "!", ".", ",;!"}[int(punctuationIndex)%6]
		whitespace := []string{"\u2003", "\u00a0", "\u202f", "\u3000"}[int(whitespaceIndex)%4]
		value := int(raw) % 201
		unit := "WPM" + punctuation + whitespace
		body := fmt.Sprintf("CW 0000Z 5 dB %d %s CQ", value, unit)
		if timeOnUnit {
			body = fmt.Sprintf("CW 5 dB %d 0000Z%s CQ", value, unit)
		}
		c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 2)
		c.parseSpot("DX de K1ABC: 3547.0 OZ4ADX " + body)
		s := mustReadSpot(t, c)
		want := fmt.Sprintf("%d WPM%s", value, punctuation)
		if !s.HasReport || s.Report != 5 || s.Comment != "" || !s.CWDisplayAvailable || len(s.CWSpeeds) != 0 || s.CWDisplayComment != want {
			t.Fatalf("input=%q report=%d canonical=%q display=%q speeds=%q want=%q", body, s.Report, s.Comment, s.CWDisplayComment, s.CWSpeeds, want)
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

func BenchmarkRBNCWSpeedIngest(b *testing.B) {
	for _, tc := range []struct {
		name, input, mode, canonical, display string
		available                             bool
	}{
		{"no-wpm", "CW 5 dB POTA CQ 0318Z", "CW", "POTA", "", false},
		{"ft8-no-wpm", "FT8 +5 dB POTA CQ 0318Z", "FT8", "POTA", "", false},
		{"speed-before-report", "CW 22 WPM 5 dB POTA CQ 0318Z", "CW", "22 WPM POTA", "POTA", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 1)
			line := "DX de K1ABC: 3547.0 OZ4ADX " + tc.input
			c.parseSpot(line)
			select {
			case s := <-c.spotChan:
				if s.Mode != tc.mode || !s.HasReport || s.Report != 5 || s.Comment != tc.canonical || s.CWDisplayAvailable != tc.available || s.CWDisplayComment != tc.display {
					b.Fatalf("incorrect parsed spot: %+v", s)
				}
			default:
				b.Fatal("spot not admitted")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				c.parseSpot(line)
				<-c.spotChan
			}
		})
	}
}
