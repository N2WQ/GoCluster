package rbn

import (
	"testing"

	"dxcluster/spot"
)

func TestExtractCallAndFreqFromGluedToken(t *testing.T) {
	raw := "JI1HFJ-#:1294068.2"
	tok := spotToken{
		raw:       raw,
		clean:     raw,
		trimStart: 0,
		trimEnd:   len(raw),
	}
	call, freq, ok := extractCallAndFreq(tok)
	if !ok {
		t.Fatalf("expected frequency to be detected")
	}
	if call != "JI1HFJ-#" {
		t.Fatalf("expected call JI1HFJ-#, got %s", call)
	}
	if freq != 1294068.2 {
		t.Fatalf("expected freq 1294068.2, got %f", freq)
	}
}

func TestFinalizeModeDefaultsForVHFWhenUnknown(t *testing.T) {
	mode := spot.FinalizeMode("", 144350.0)
	if mode != "USB" {
		t.Fatalf("expected USB fallback for VHF, got %q", mode)
	}
}

func TestFinalizeModeNormalizesSSB(t *testing.T) {
	if got := spot.NormalizeVoiceMode("SSB", 7000); got != "LSB" {
		t.Fatalf("expected SSB on 40m to normalize to LSB, got %q", got)
	}
	if got := spot.NormalizeVoiceMode("SSB", 14074); got != "USB" {
		t.Fatalf("expected SSB on 20m to normalize to USB, got %q", got)
	}
}
