package spot

import (
	"strings"
	"testing"
	"time"
)

func TestCWSpeedDisplayProjectionAndSnapshot(t *testing.T) {
	s := NewSpot("K1ABC", "W1XYZ", 14030, "CW")
	s.Report, s.HasReport = 5, true
	s.Comment = "22 WPM 30 WPM POTA"
	s.CWDisplayAvailable, s.CWDisplayComment = true, "POTA"
	s.CWSpeeds = []string{"22 WPM", "30 WPM"}
	_ = s.FormatDXCluster()
	clone := s.Clone()
	s.CWSpeeds[0] = "44 WPM"
	if clone.CWSpeeds[0] != "22 WPM" {
		t.Fatal("clone did not own speed labels")
	}
	off, on := clone.FormatDXClusterCW(false), clone.FormatDXClusterCW(true)
	if !strings.Contains(off, "POTA") || strings.Contains(off, "WPM") || !strings.Contains(on, "POTA 22 WPM") || strings.Contains(on, "30 WP") {
		t.Fatalf("off=%q on=%q", off, on)
	}
	if clone.Comment != "22 WPM 30 WPM POTA" {
		t.Fatal("display changed canonical comment")
	}
}

func TestCWSpeedCloneWithReplacedCommentDropsProjection(t *testing.T) {
	s := NewSpot("K1ABC", "W1XYZ", 14030, "CW")
	s.Comment = "22 WPM POTA"
	s.CWDisplayAvailable, s.CWDisplayComment = true, "POTA"
	s.CWSpeeds = []string{"22 WPM"}
	_ = s.FormatDXCluster()
	clone := s.CloneWithComment("replacement")
	if clone.CWDisplayAvailable || clone.CWDisplayComment != "" || len(clone.CWSpeeds) != 0 {
		t.Fatalf("replacement retained old projection: %+v", clone)
	}
	for _, show := range []bool{false, true} {
		if line := clone.FormatDXClusterCW(show); !strings.Contains(line, "replacement") || strings.Contains(line, "WPM") || strings.Contains(line, "POTA") {
			t.Fatalf("show=%v rendered stale comment: %q", show, line)
		}
	}
	if !s.CWDisplayAvailable || s.CWDisplayComment != "POTA" || len(s.CWSpeeds) != 1 {
		t.Fatal("replacement mutated source")
	}
}

func TestCWSpeedDisplayCacheEquivalence(t *testing.T) {
	setDXClusterLineLength(t, 78)
	tests := []struct {
		name, canonical, projection, beaconComment, off, on string
		beacon, reuse                                       bool
	}{
		{name: "empty", off: "CW", on: "CW 22 WPM", reuse: true},
		{name: "same comment", canonical: "POTA", projection: "POTA", off: "CW POTA", on: "CW POTA 22 WPM", reuse: true},
		{name: "different comment", canonical: "22 WPM POTA", projection: "POTA", off: "CW POTA", on: "CW POTA 22 WPM"},
		{name: "sanitized equal", canonical: "A\tB", projection: "A B", off: "CW A B", on: "CW A B 22 WPM", reuse: true},
		{name: "sanitized different", canonical: "A\tB", projection: "A C", off: "CW A C", on: "CW A C 22 WPM"},
		{name: "unicode sanitized", canonical: "AéB", projection: "A?B", off: "CW A?B", on: "CW A?B 22 WPM", reuse: true},
		{name: "default beacon", beacon: true, off: "CW BEACON", on: "CW BEACON 22 WPM", reuse: true},
		{name: "live ncdxf beacon", beacon: true, beaconComment: BeaconCommentNCDXF, off: "CW NCDXF BEACON", on: "CW NCDXF BEACON 22 WPM", reuse: true},
		{name: "reopened ncdxf beacon", canonical: BeaconCommentNCDXF, beacon: true, off: "CW NCDXF BEACON", on: "CW NCDXF BEACON 22 WPM", reuse: true},
		{name: "reopened default beacon", canonical: BeaconCommentDefault, beacon: true, beaconComment: BeaconCommentNCDXF, off: "CW BEACON", on: "CW BEACON 22 WPM", reuse: true},
		{name: "different beacon fallback", canonical: "POTA", beacon: true, off: "CW BEACON", on: "CW BEACON 22 WPM"},
	}
	for _, tc := range tests {
		for _, warm := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/cold", true: "/warm"}[warm], func(t *testing.T) {
				s := NewSpot("K1ABC", "W1XYZ", 14030, "CW")
				s.Comment, s.CWDisplayComment = tc.canonical, tc.projection
				s.CWDisplayAvailable, s.IsBeacon, s.BeaconComment = true, tc.beacon, tc.beaconComment
				s.CWSpeeds = []string{"22 WPM"}
				if warm {
					_ = s.FormatDXCluster()
				}
				field := func(line string) string {
					return strings.TrimSpace(line[commentColumn:currentDXClusterLayout().tailStartIdx])
				}
				if got := field(s.FormatDXClusterCW(false)); got != tc.off {
					t.Fatalf("OFF comment = %q, want %q", got, tc.off)
				}
				if !warm && (s.formatted != "") != tc.reuse {
					t.Fatalf("canonical cache populated = %v, want %v", s.formatted != "", tc.reuse)
				}
				if got := field(s.FormatDXClusterCW(true)); got != tc.on {
					t.Fatalf("ON comment = %q, want %q", got, tc.on)
				}
				if got := field(s.FormatDXClusterCW(false)); got != tc.off {
					t.Fatalf("OFF after ON = %q, want %q", got, tc.off)
				}
			})
		}
	}
}

func TestCWSpeedDisplayNeverTruncatesLabel(t *testing.T) {
	setDXClusterLineLength(t, 78)
	s := NewSpot("K1ABC", "W1XYZ", 14030, "CW")
	s.Time = time.Date(2026, time.November, 10, 22, 22, 0, 0, time.UTC)
	s.HasReport, s.Report = true, 5
	s.CWDisplayAvailable = true
	s.CWDisplayComment = strings.Repeat("A", 200)
	s.CWSpeeds = []string{"22 WPM"}
	line := s.FormatDXClusterCW(true)
	if !strings.Contains(line, "2222Z") {
		t.Fatalf("missing fixed timestamp: %q", line)
	}
	if got, want := strings.TrimSpace(line[commentColumn:currentDXClusterLayout().tailStartIdx]), "CW 5 dB AAAAAAAAAAAAAAA"; got != want {
		t.Fatalf("comment = %q, want %q (line %q)", got, want, line)
	}
}

func TestCWSpeedDisplayBeaconFallback(t *testing.T) {
	s := NewSpot("K1ABC", "W1XYZ", 14030, "CW")
	s.IsBeacon, s.CWDisplayAvailable = true, true
	s.CWSpeeds = []string{"0 WPM"}
	if got := s.FormatDXClusterCW(true); !strings.Contains(got, "BEACON 0 WPM") {
		t.Fatalf("lost beacon or zero speed: %q", got)
	}
}
