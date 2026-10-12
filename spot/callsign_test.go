package spot

import "testing"

func TestNormalizeCallsignReplacesDot(t *testing.T) {
	input := "W6.UT5UF"
	want := "W6/UT5UF"
	if got := NormalizeCallsign(input); got != want {
		t.Fatalf("NormalizeCallsign(%q) = %q, want %q", input, got, want)
	}
}

func TestNormalizeCallsignTrimsTrailingSlash(t *testing.T) {
	input := "K1ABC/"
	want := "K1ABC"
	if got := NormalizeCallsign(input); got != want {
		t.Fatalf("NormalizeCallsign(%q) = %q, want %q", input, got, want)
	}
}

func TestNormalizeCallsignStripsPortableSuffix(t *testing.T) {
	input := "K1ABC/P"
	want := "K1ABC"
	if got := NormalizeCallsign(input); got != want {
		t.Fatalf("NormalizeCallsign(%q) = %q, want %q", input, got, want)
	}
}

func TestNormalizeSpotDXCallsignStripsOnlyTrailingNumericSSID(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "CALL-2", want: "CALL"},
		{in: "CALL-15", want: "CALL"},
		{in: "CALL-ABC", want: "CALL-ABC"},
		{in: "CALL-2/P", want: "CALL"},
		{in: "CALL/P-2", want: "CALL"},
		{in: "W6/CALL-2", want: "W6/CALL"},
		{in: "CALL/B", want: "CALL/B"},
	}
	for _, tt := range tests {
		if got := NormalizeSpotDXCallsign(tt.in); got != tt.want {
			t.Fatalf("NormalizeSpotDXCallsign(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestIsValidCallsignRejectsNonDigitWithSlash(t *testing.T) {
	if IsValidCallsign("KWS/NM") {
		t.Fatalf("IsValidCallsign should reject KWS/NM because it lacks digits")
	}
}

func TestIsValidCallsignRequiresDigitAfterSlash(t *testing.T) {
	if IsValidCallsign("ABC/DEF") {
		t.Fatalf("IsValidCallsign should reject ABC/DEF because it lacks digits")
	}
}

func TestIsValidCallsignLengthBounds(t *testing.T) {
	valid := "K1ABCDEF/GHIJKL" // 15 chars, contains a digit.
	if !IsValidCallsign(valid) {
		t.Fatalf("IsValidCallsign should accept max-length callsign %q", valid)
	}
	invalid := "K1ABCDEF/GHIJKLM" // 16 chars.
	if IsValidCallsign(invalid) {
		t.Fatalf("IsValidCallsign should reject overlong callsign %q", invalid)
	}
}

func TestIsValidCallsignRequiresIdentitySegment(t *testing.T) {
	valid := []string{
		"K1ABC",
		"DL6LD",
		"4U1UN",
		"P5/N1K",
		"K1ABC/B",
		"W6TEST-1",
		"W1AW/5",
		"JA1CTC.P",
	}
	for _, call := range valid {
		if !IsValidCallsign(call) {
			t.Fatalf("IsValidCallsign should accept call-like identity %q", call)
		}
	}

	invalid := []string{
		"SET",
		"FT8",
		"FT4",
		"NOFT8",
		"PSK31",
		"SET/FT8",
		"SET/NOFT8",
		"K1/FT8",
	}
	for _, call := range invalid {
		if IsValidCallsign(call) {
			t.Fatalf("IsValidCallsign should reject command/mode token %q", call)
		}
	}
}

var benchmarkCallsignValidSink bool

func BenchmarkIsValidNormalizedCallsign(b *testing.B) {
	calls := []string{
		"K1ABC",
		"DL6LD",
		"4U1UN",
		"P5/N1K",
		"K1ABC/B",
		"W6TEST-1",
		"SET/NOFT8",
		"PSK31",
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, call := range calls {
			benchmarkCallsignValidSink = IsValidNormalizedCallsign(call)
		}
	}
}
