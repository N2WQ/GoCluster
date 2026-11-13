package rbn

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestMinimalParserPreservesFusedCommentTime(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 10)
	c.UseMinimalParser()

	ts := time.Now().UTC().Format("1504Z")
	line := fmt.Sprintf("DX de GM5G: 28421.9 GM5G USB ARRL 10m Contest %sIO87 %s", ts, ts)
	c.parseSpot(line)

	var s *spot.Spot
	select {
	case s = <-c.spotChan:
	default:
		t.Fatalf("expected a parsed spot")
	}

	if got := s.Time.UTC().Format("1504Z"); got != ts {
		t.Fatalf("expected parsed time %s, got %q", ts, got)
	}
	if s.Comment != "ARRL 10m Contest "+ts+"IO87" {
		t.Fatalf("expected entire fused comment time and grid preserved, got %q", s.Comment)
	}
	if s.Mode != "USB" {
		t.Fatalf("expected explicit mode USB, got %q", s.Mode)
	}
	if s.SourceType != spot.SourceUpstream || !s.IsHuman {
		t.Fatalf("expected upstream/human spot, got SourceType=%s IsHuman=%v", s.SourceType, s.IsHuman)
	}
}

func TestMinimalParserWireTimestamp(t *testing.T) {
	for _, tc := range []struct {
		name, tail string
		valid      bool
	}{
		{"midnight", "0000Z", true}, {"last_minute", "2359Z", true},
		{"missing", "", false}, {"invalid_hour", "2400Z", false},
		{"invalid_minute", "1260Z", false}, {"lowercase", "1200z", false},
		{"punctuation", "1200Z,", false}, {"wrapped", "(1200Z)", false},
		{"fused_grid", "1200ZFN20", false}, {"following_grid", "1200Z FN20", false},
		{"earlier_time_only", "invalid", false}, {"nondigit", "12A0Z", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 1)
			c.UseMinimalParser()
			// The earlier comment time must neither override a valid wire time
			// nor rescue a missing/invalid terminal field.
			before := time.Now().UTC()
			c.parseSpot("DX de W1AW: 14074 K1ABC FT8 -14 QRT 1800Z POTA " + tc.tail)
			after := time.Now().UTC()
			select {
			case got := <-c.spotChan:
				if !tc.valid {
					t.Fatalf("unusable terminal timestamp enqueued %+v", got)
				}
				wantBefore := parseTimeFromRBNAt(tc.tail, before)
				wantAfter := parseTimeFromRBNAt(tc.tail, after)
				if got.Time.Before(wantBefore) || got.Time.After(wantAfter) || got.Comment != "QRT 1800Z POTA" || !got.HasReport || got.Report != -14 || !got.IsHuman {
					t.Fatalf("human timestamp/comment/report mismatch: %+v", got)
				}
			default:
				if tc.valid {
					t.Fatal("valid terminal timestamp did not enqueue")
				}
			}
			if !tc.valid && c.lastSpotAt.Load() != 0 {
				t.Fatal("rejected timestamp updated accepted-spot liveness")
			}
		})
	}
}

func TestMinimalParserHumanCommentRouting(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 1)
	c.UseMinimalParser()
	c.parseSpot("DX de W1AW: 14025 K1ABC CW UP 2 QRT 1800Z CWT 0000Z")
	got := mustReadSpot(t, c)
	if got.HasReport || got.Comment != "UP 2 QRT 1800Z CWT" || got.Mode != "CW" || got.Time.UTC().Format("1504Z") != "0000Z" {
		t.Fatalf("minimal parser retained legacy human cleanup: %+v", got)
	}
	c.parseSpot("DX de W1AW: 14074 K1ABC FT8 ... -14 0000Z")
	got = mustReadSpot(t, c)
	if got.HasReport || got.Comment != "-14" || got.Mode != "FT8" {
		t.Fatalf("structural cleanup manufactured report adjacency: %+v", got)
	}
}

func TestMinimalParserExtractsLowercaseDBReport(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 10)
	c.UseMinimalParser()

	line := "DX de EB3WH: 7074.0 PD5XMAS FT8 -12 db SES XMAS AWARD 1801Z"
	c.parseSpot(line)

	var s *spot.Spot
	select {
	case s = <-c.spotChan:
	default:
		t.Fatalf("expected a parsed spot")
	}

	if s.Mode != "FT8" {
		t.Fatalf("expected mode FT8, got %q", s.Mode)
	}
	if !s.HasReport || s.Report != -12 {
		t.Fatalf("expected SNR -12 dB, got HasReport=%v Report=%d", s.HasReport, s.Report)
	}
	if strings.Contains(strings.ToLower(s.Comment), "db") {
		t.Fatalf("expected dB token stripped from comment, got %q", s.Comment)
	}
}

func TestMinimalParserPreservesExplicitFT2Mode(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, false, 10)
	c.UseMinimalParser()

	line := "DX de EB3WH: 14080.0 PD5XMAS FT2 -12 dB SES XMAS AWARD 1801Z"
	c.parseSpot(line)

	var s *spot.Spot
	select {
	case s = <-c.spotChan:
	default:
		t.Fatalf("expected a parsed spot")
	}

	if s.Mode != "FT2" {
		t.Fatalf("expected mode FT2, got %q", s.Mode)
	}
	if s.ModeNorm != "FT2" {
		t.Fatalf("expected FT2 ModeNorm, got %q", s.ModeNorm)
	}
	if s.ModeProvenance != spot.ModeProvenanceCommentExplicit {
		t.Fatalf("expected comment explicit mode provenance, got %q", s.ModeProvenance)
	}
}
