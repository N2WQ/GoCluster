package rbn

import (
	"strings"
	"testing"

	"dxcluster/spot"
)

func TestMinimalParserStripsFusedTimeAndKeepsRemainder(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, nil, false, 10)
	c.UseMinimalParser()

	line := "DX de GM5G: 28421.9 GM5G USB ARRL 10m Contest 1708ZIO87 1708Z"
	c.parseSpot(line)

	var s *spot.Spot
	select {
	case s = <-c.spotChan:
	default:
		t.Fatalf("expected a parsed spot")
	}

	if got := s.Time.UTC().Format("1504Z"); got != "1708Z" {
		t.Fatalf("expected parsed time 1708Z, got %q", got)
	}
	if strings.Contains(s.Comment, "1708Z") {
		t.Fatalf("expected comment to exclude time token, got %q", s.Comment)
	}
	if !strings.Contains(s.Comment, "IO87") {
		t.Fatalf("expected grid tail to remain, got %q", s.Comment)
	}
	if s.Mode != "USB" {
		t.Fatalf("expected explicit mode USB, got %q", s.Mode)
	}
	if s.SourceType != spot.SourceUpstream || !s.IsHuman {
		t.Fatalf("expected upstream/human spot, got SourceType=%s IsHuman=%v", s.SourceType, s.IsHuman)
	}
}

func TestMinimalParserExtractsLowercaseDBReport(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, nil, false, 10)
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
