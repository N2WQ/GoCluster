package peer

import (
	"strings"
	"testing"
)

func TestParsePC11IgnoresCommentTimeToken(t *testing.T) {
	line := "PC11^14074.0^K1ABC^23-Dec-2025^2001Z^CQ 1800Z TEST^W1XYZ^N1ORIG^H3^"
	frame, err := ParseFrame(line)
	if err != nil {
		t.Fatalf("ParseFrame error: %v", err)
	}
	spot, err := parseSpotFromFrame(frame, "FALLBACK")
	if err != nil {
		t.Fatalf("parseSpotFromFrame error: %v", err)
	}
	if got := spot.Time.UTC().Format("1504Z"); got != "2001Z" {
		t.Fatalf("expected PC11 time 2001Z, got %q", got)
	}
	if strings.Contains(spot.Comment, "1800Z") {
		t.Fatalf("expected comment time token stripped, got %q", spot.Comment)
	}
	if spot.Comment != "CQ TEST" {
		t.Fatalf("expected cleaned comment 'CQ TEST', got %q", spot.Comment)
	}
}

func TestParseSpotCanonicalizesDXNumericSSID(t *testing.T) {
	frame, err := ParseFrame("PC11^14074.0^K1ABC-2^23-Dec-2025^2001Z^CQ TEST^W1XYZ^N1ORIG^H3^")
	if err != nil {
		t.Fatalf("ParseFrame error: %v", err)
	}
	spotEntry, err := parseSpotFromFrame(frame, "FALLBACK")
	if err != nil {
		t.Fatalf("parseSpotFromFrame error: %v", err)
	}
	if spotEntry.DXCall != "K1ABC" || spotEntry.DXCallNorm != "K1ABC" {
		t.Fatalf("expected canonical DX K1ABC, got DXCall=%q DXCallNorm=%q", spotEntry.DXCall, spotEntry.DXCallNorm)
	}
}

func TestParseSpotterStripsOnlyTerminalSkimmerMarker(t *testing.T) {
	tests := []struct {
		name string
		line string
		want string
	}{
		{
			name: "pc11 plain skimmer marker",
			line: "PC11^14074.0^K1ABC^23-Dec-2025^2001Z^CQ TEST^W1XYZ-#^N1ORIG^H3^",
			want: "W1XYZ",
		},
		{
			name: "pc61 numeric ssid preserved before marker",
			line: "PC61^14074.0^K1ABC^23-Dec-2025^2001Z^CQ TEST^W1XYZ-1-#^N1ORIG^203.0.113.7^H3^",
			want: "W1XYZ-1",
		},
		{
			name: "pc26 delegates marker stripping through pc11 parser",
			line: "PC26^7074.0^DX1ABC^24-Dec-2025^1501Z^TEST COMMENT^SP1OT-#^N1ORIG^ ^H5^",
			want: "SP1OT",
		},
		{
			name: "numeric ssid without marker preserved",
			line: "PC11^14074.0^K1ABC^23-Dec-2025^2001Z^CQ TEST^W1XYZ-1^N1ORIG^H3^",
			want: "W1XYZ-1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := ParseFrame(tt.line)
			if err != nil {
				t.Fatalf("ParseFrame error: %v", err)
			}
			spotEntry, err := parseSpotFromFrame(frame, "FALLBACK")
			if err != nil {
				t.Fatalf("parseSpotFromFrame error: %v", err)
			}
			if spotEntry.DECall != tt.want || spotEntry.DECallNorm != tt.want {
				t.Fatalf("expected DECall=%q DECallNorm=%q, got DECall=%q DECallNorm=%q", tt.want, tt.want, spotEntry.DECall, spotEntry.DECallNorm)
			}
		})
	}
}

func TestParsePC26UsesCorrectFieldsAndIgnoresPlaceholder(t *testing.T) {
	line := "PC26^7074.0^DX1ABC^24-Dec-2025^1501Z^TEST COMMENT^SP1OT^N1ORIG^ ^H5^"
	frame, err := ParseFrame(line)
	if err != nil {
		t.Fatalf("ParseFrame error: %v", err)
	}
	spotEntry, err := parseSpotFromFrame(frame, "FALLBACK")
	if err != nil {
		t.Fatalf("parseSpotFromFrame error: %v", err)
	}
	if spotEntry.DXCall != "DX1ABC" || spotEntry.DECall != "SP1OT" {
		t.Fatalf("unexpected calls: dx=%s de=%s", spotEntry.DXCall, spotEntry.DECall)
	}
	if got := spotEntry.Time.UTC().Format("1504Z"); got != "1501Z" {
		t.Fatalf("expected time 1501Z, got %s", got)
	}
	if spotEntry.SourceNode != "N1ORIG" {
		t.Fatalf("expected origin N1ORIG, got %s", spotEntry.SourceNode)
	}
}
