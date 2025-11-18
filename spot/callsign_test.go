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

func TestIsValidCallsignRejectsNonDigitWithSlash(t *testing.T) {
	if IsValidCallsign("KWS/NM") {
		t.Fatalf("IsValidCallsign should reject KWS/NM because it lacks digits")
	}
}

func TestIsValidCallsignAcceptsDotSuffix(t *testing.T) {
	if !IsValidCallsign("JA1CTC.P") {
		t.Fatalf("IsValidCallsign should accept JA1CTC.P after normalization")
	}
}

func TestIsValidCallsignRequiresDigitAfterSlash(t *testing.T) {
	if IsValidCallsign("ABC/DEF") {
		t.Fatalf("IsValidCallsign should reject ABC/DEF because it lacks digits")
	}
}
