package rbn

import (
	"math"
	"testing"
)

func TestFindFrequencyFieldStandard(t *testing.T) {
	parts := []string{"DX", "de", "W3LPL-#:", "14074.0", "K1ABC", "FT8", "-5", "dB", "CQ", "1234Z"}
	idx, freq, ok := findFrequencyField(parts)
	if !ok {
		t.Fatalf("expected to locate frequency field")
	}
	if idx != 3 {
		t.Fatalf("expected index 3, got %d", idx)
	}
	if math.Abs(freq-14074.0) > 1e-6 {
		t.Fatalf("expected freq 14074.0, got %f", freq)
	}
}

func TestFindFrequencyFieldSkipsRepeatedSpotter(t *testing.T) {
	parts := []string{"DX", "de", "JJ1QLT-#:", "JJ1QLT", "22", "7038.3", "JA1ABC", "FT8", "-4", "dB", "CQ", "2359Z"}
	idx, freq, ok := findFrequencyField(parts)
	if !ok {
		t.Fatalf("expected to locate frequency field")
	}
	if idx != 5 {
		t.Fatalf("expected index 5 when skipping extra tokens, got %d", idx)
	}
	if math.Abs(freq-7038.3) > 1e-6 {
		t.Fatalf("expected freq 7038.3, got %f", freq)
	}
}

func TestFindFrequencyFieldMissing(t *testing.T) {
	parts := []string{"DX", "de", "K1ABC:", "NOTAFREQ", "DATA"}
	if idx, _, ok := findFrequencyField(parts); ok {
		t.Fatalf("expected no frequency, but got index %d", idx)
	}
}

func TestSplitSpotterTokenWithAttachedFrequency(t *testing.T) {
	parts := []string{"DX", "de", "JI1HFJ-#:1294068.2", "JN1KWR", "CW"}
	call, updated := splitSpotterToken(parts)
	if call != "JI1HFJ-#" {
		t.Fatalf("expected call without colon, got %s", call)
	}
	if len(updated) != len(parts)+1 {
		t.Fatalf("expected slice to grow, old=%d new=%d", len(parts), len(updated))
	}
	if updated[3] != "1294068.2" {
		t.Fatalf("expected inserted frequency token, got %s", updated[3])
	}
}

func TestSplitSpotterTokenStandard(t *testing.T) {
	parts := []string{"DX", "de", "W3LPL-#:", "14074.0"}
	call, updated := splitSpotterToken(parts)
	if call != "W3LPL-#" {
		t.Fatalf("expected standard call, got %s", call)
	}
	if len(updated) != len(parts) {
		t.Fatalf("expected slice length unchanged, got %d vs %d", len(updated), len(parts))
	}
}
