package rbn

import (
	"strings"
	"testing"

	"dxcluster/spot"
)

func TestACParserExtractsModeSNRAndGrid(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, nil, false, 10)
	line := "DX de DL1YAW: 144360.0 S51AT JO41DX<MS>JN75 MSK144 +5 dB 1800Z"
	c.parseSpot(line)

	var s *spot.Spot
	select {
	case s = <-c.spotChan:
	default:
		t.Fatalf("expected a parsed spot")
	}

	if s.Mode != "MSK144" {
		t.Fatalf("expected mode MSK144, got %q", s.Mode)
	}
	if !s.HasReport || s.Report != 5 {
		t.Fatalf("expected SNR +5 dB, got HasReport=%v Report=%d", s.HasReport, s.Report)
	}
	if got := s.Time.UTC().Format("1504Z"); got != "1800Z" {
		t.Fatalf("expected time 1800Z, got %q", got)
	}
	if !strings.Contains(s.Comment, "JO41DX<MS>JN75") {
		t.Fatalf("expected grid fragment to remain in comment, got %q", s.Comment)
	}
	if strings.Contains(strings.ToLower(s.Comment), "db") {
		t.Fatalf("expected dB token stripped from comment, got %q", s.Comment)
	}
}

func TestACParserInfersModeFromBandTable(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, nil, false, 10)
	line := "DX de K0DG: 28015.1 K7SS WA 1912Z"
	c.parseSpot(line)

	var s *spot.Spot
	select {
	case s = <-c.spotChan:
	default:
		t.Fatalf("expected a parsed spot")
	}

	if s.Mode != "CW" {
		t.Fatalf("expected inferred mode CW, got %q", s.Mode)
	}
	if s.HasReport {
		t.Fatalf("did not expect a report, got %v", s.Report)
	}
	if strings.TrimSpace(s.Comment) != "WA" {
		t.Fatalf("expected comment WA, got %q", s.Comment)
	}
}

func TestACParserUsesVoiceAllocationFor10m(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, nil, false, 10)
	line := "DX de KC9IMA: 28319.0 KC9IMA ARRL 10-Meter Contest 1912Z"
	c.parseSpot(line)

	var s *spot.Spot
	select {
	case s = <-c.spotChan:
	default:
		t.Fatalf("expected a parsed spot")
	}

	if s.Mode != "USB" {
		t.Fatalf("expected inferred mode USB, got %q", s.Mode)
	}
	if strings.TrimSpace(s.Comment) != "ARRL 10-Meter Contest" {
		t.Fatalf("expected contest name in comment, got %q", s.Comment)
	}
}

func TestACParserDigitalReport(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, nil, false, 10)
	line := "DX de PE2JMR: 50280.0 I2RNJ MSK144 +9 dB RX 1912Z"
	c.parseSpot(line)

	var s *spot.Spot
	select {
	case s = <-c.spotChan:
	default:
		t.Fatalf("expected a parsed spot")
	}

	if s.Mode != "MSK144" {
		t.Fatalf("expected mode MSK144, got %q", s.Mode)
	}
	if !s.HasReport || s.Report != 9 {
		t.Fatalf("expected SNR +9 dB, got HasReport=%v Report=%d", s.HasReport, s.Report)
	}
	if strings.TrimSpace(s.Comment) != "RX" {
		t.Fatalf("expected comment RX, got %q", s.Comment)
	}
}

func TestACParserInlineSNRWithoutSpace(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, nil, false, 10)
	line := "DX de EA1AHP: 14074.0 CX3VB FT8 -13dB from GF27 2062Hz 1943Z"
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
	if !s.HasReport || s.Report != -13 {
		t.Fatalf("expected SNR -13 dB, got HasReport=%v Report=%d", s.HasReport, s.Report)
	}
	if !strings.Contains(s.Comment, "GF27") || !strings.Contains(s.Comment, "2062Hz") {
		t.Fatalf("expected comment to keep text, got %q", s.Comment)
	}
}

func TestACParserPositiveSNRNoComment(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, nil, false, 10)
	line := "DX de UA3RF: 144360.0 R1BPV MSK144 +2 dB 1943Z"
	c.parseSpot(line)

	var s *spot.Spot
	select {
	case s = <-c.spotChan:
	default:
		t.Fatalf("expected a parsed spot")
	}

	if s.Mode != "MSK144" {
		t.Fatalf("expected mode MSK144, got %q", s.Mode)
	}
	if !s.HasReport || s.Report != 2 {
		t.Fatalf("expected SNR +2 dB, got HasReport=%v Report=%d", s.HasReport, s.Report)
	}
	if strings.TrimSpace(s.Comment) != "" {
		t.Fatalf("expected empty comment, got %q", s.Comment)
	}
}

func TestACParserParsesMSKSNRWithTrailingComment(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, nil, false, 10)
	line := "DX de IK8PV: 144360.0 DL1OBF MSK144 +7 dB HRD"
	c.parseSpot(line)

	var s *spot.Spot
	select {
	case s = <-c.spotChan:
	default:
		t.Fatalf("expected a parsed spot")
	}

	if s.Mode != "MSK144" {
		t.Fatalf("expected mode MSK144, got %q", s.Mode)
	}
	if !s.HasReport || s.Report != 7 {
		t.Fatalf("expected SNR +7 dB, got HasReport=%v Report=%d", s.HasReport, s.Report)
	}
	if strings.TrimSpace(s.Comment) != "HRD" {
		t.Fatalf("expected comment HRD, got %q", s.Comment)
	}
}

func TestACParserParsesMSKSNRWithSignificantComment(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, nil, false, 10)
	line := "DX de I3EPV: 144360.0 DJ9MG MSK144 +15 dB"
	c.parseSpot(line)

	var s *spot.Spot
	select {
	case s = <-c.spotChan:
	default:
		t.Fatalf("expected a parsed spot")
	}

	if s.Mode != "MSK144" {
		t.Fatalf("expected mode MSK144, got %q", s.Mode)
	}
	if !s.HasReport || s.Report != 15 {
		t.Fatalf("expected SNR +15 dB, got HasReport=%v Report=%d", s.HasReport, s.Report)
	}
	if strings.TrimSpace(s.Comment) != "" {
		t.Fatalf("expected empty comment, got %q", s.Comment)
	}
}

func TestACParserSetsHasReportFalseWhenNoSNR(t *testing.T) {
	c := NewClient("example.com", 0, "N0FT", "UPSTREAM", nil, nil, false, 10)
	line := "DX de PB0MD: 144360.0 S50TA MSK144 HRD"
	c.parseSpot(line)

	var s *spot.Spot
	select {
	case s = <-c.spotChan:
	default:
		t.Fatalf("expected a parsed spot")
	}

	if s.HasReport {
		t.Fatalf("expected HasReport=false when no SNR, got true with Report=%d", s.Report)
	}
}
