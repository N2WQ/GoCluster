package config

import "testing"

func TestRawPeeringCallV12Grammar(t *testing.T) {
	// Literal results come from the pinned DXUtil::is_callsign grammar, not
	// from the login normalizer. Raw validity and stable identity are separate.
	for _, call := range []string{"K1ABC", "K1ABC-00", "K1ABC-01", "K1ABC/P", "EA8/K1ABC", "EA8/K1ABC/MM", "W1AW/P", "K1ABC-01/P", "1XX99/1YY99999ABCDEFGH-99/ABCDEFG/AM"} {
		if !IsRawPeeringCall(call) {
			t.Errorf("raw-valid call rejected: %q", call)
		}
	}
	for _, call := range []string{"", "K1ABC/", "/K1ABC", "K1ABC-000", "k1abc", " K1ABC", "K1ABC ", "K1ABC\t", "K1ABC\n", "K1ABC\x00", "K1ABC\u00a0", "EA8/K1ABC/MM-00", "K1ABC/P-01", "NODE", "123", "K1ABC^", "K1ABC/ABCDEFGH"} {
		if IsRawPeeringCall(call) {
			t.Errorf("raw-invalid call accepted: %q", call)
		}
	}
}

func BenchmarkRawPeeringCallV12(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if !IsRawPeeringCall("K1ABC") {
			b.Fatal("canonical call rejected")
		}
	}
}
