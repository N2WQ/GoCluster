package spot

import (
	"strings"
	"testing"
)

func TestParseSpotCommentRTTYBPS(t *testing.T) {
	comment := "RTTY 45 BPS TEST"
	result := ParseSpotComment(comment, 14070.0)

	if result.Mode != "RTTY" {
		t.Fatalf("expected mode RTTY, got %q", result.Mode)
	}
	if result.HasReport {
		t.Fatalf("expected HasReport=false, got true with Report=%d", result.Report)
	}
	if result.Comment != "45 BPS TEST" {
		t.Fatalf("expected comment to preserve BPS speed, got %q", result.Comment)
	}
}

func TestParseSpotCommentStripsTimeAndReport(t *testing.T) {
	comment := "FT8 -12 dB 1708ZIO87 CQ"
	result := ParseSpotComment(comment, 7074.0)

	if result.Mode != "FT8" {
		t.Fatalf("expected mode FT8, got %q", result.Mode)
	}
	if !result.HasReport || result.Report != -12 {
		t.Fatalf("expected report -12 dB, got HasReport=%v Report=%d", result.HasReport, result.Report)
	}
	if result.TimeToken != "1708Z" {
		t.Fatalf("expected time token 1708Z, got %q", result.TimeToken)
	}
	if strings.Contains(result.Comment, "1708Z") || strings.Contains(strings.ToLower(result.Comment), "db") {
		t.Fatalf("expected time/report tokens stripped, got %q", result.Comment)
	}
	if result.Comment != "IO87 CQ" {
		t.Fatalf("expected cleaned comment 'IO87 CQ', got %q", result.Comment)
	}
}

func TestParseSpotCommentNormalizesSSBByFrequency(t *testing.T) {
	comment := "SSB CQ"
	result := ParseSpotComment(comment, 7074.0)

	if result.Mode != "LSB" {
		t.Fatalf("expected mode LSB on 40m, got %q", result.Mode)
	}
	if result.Comment != "CQ" {
		t.Fatalf("expected comment CQ, got %q", result.Comment)
	}
}

func TestParseSpotCommentBareNumericBecomesReportForDigital(t *testing.T) {
	comment := "FT8 -04 73"
	result := ParseSpotComment(comment, 14074.0)

	if result.Mode != "FT8" {
		t.Fatalf("expected mode FT8, got %q", result.Mode)
	}
	if !result.HasReport || result.Report != -4 {
		t.Fatalf("expected bare numeric to become report -4, got HasReport=%v Report=%d", result.HasReport, result.Report)
	}
	if strings.Contains(result.Comment, "-04") {
		t.Fatalf("expected numeric stripped from comment, got %q", result.Comment)
	}
	if !strings.Contains(result.Comment, "73") {
		t.Fatalf("expected 73 to remain in comment, got %q", result.Comment)
	}
}
