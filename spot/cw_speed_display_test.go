package spot

import (
	"strings"
	"testing"
)

func TestCWSpeedDisplayProjectionAndSnapshot(t *testing.T) {
	s := NewSpot("K1ABC", "W1XYZ", 14030, "CW")
	s.Report, s.HasReport = 5, true
	s.Comment = "22 WPM 30 WPM POTA"
	s.CWDisplayAvailable, s.CWDisplayComment = true, "POTA"
	s.CWSpeeds = []string{"22 WPM", "30 WPM"}
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

func TestCWSpeedDisplayNeverTruncatesLabel(t *testing.T) {
	s := NewSpot("K1ABC", "W1XYZ", 14030, "CW")
	s.HasReport, s.Report = true, 5
	s.CWDisplayAvailable = true
	s.CWDisplayComment = strings.Repeat("A", 200)
	s.CWSpeeds = []string{"22 WPM"}
	line := s.FormatDXClusterCW(true)
	if strings.Contains(line, "22") || strings.Contains(line, "WPM") {
		t.Fatalf("speed shown despite full comment: %q", line)
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
