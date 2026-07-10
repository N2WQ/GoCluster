package rbn

import (
	"testing"
	"time"

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

func TestRegionalClassifierLeavesVHFBlankWhenUnknown(t *testing.T) {
	sp := spot.NewSpot("K1ABC", "W1AAA", 144350.0, "")
	got := spot.ClassifyRegionalMode(sp)
	if got.Mode != "" || got.Provenance != spot.ModeProvenanceRegionalUnknownBlank {
		t.Fatalf("expected blank unknown-region VHF outcome, got (%q, %q)", got.Mode, got.Provenance)
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

func TestParseTimeFromRBNClampsFuture(t *testing.T) {
	now := time.Date(2026, 1, 23, 22, 0, 0, 0, time.UTC)
	got := parseTimeFromRBNAt("2359Z", now)
	if !got.Equal(now) {
		t.Fatalf("expected future time to clamp to now %v, got %v", now, got)
	}
}

func TestParseTimeFromRBNAllowsSmallFutureSkew(t *testing.T) {
	now := time.Date(2026, 1, 23, 22, 0, 0, 0, time.UTC)
	got := parseTimeFromRBNAt("2201Z", now)
	want := time.Date(2026, 1, 23, 22, 1, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("expected small future skew to be accepted (%v), got %v", want, got)
	}
}

func TestParseTimeFromRBNKeepsSameDay(t *testing.T) {
	now := time.Date(2026, 1, 23, 22, 0, 0, 0, time.UTC)
	got := parseTimeFromRBNAt("0006Z", now)
	want := time.Date(2026, 1, 23, 0, 6, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("expected same-day parse (%v), got %v", want, got)
	}
}

func TestParseTimeFromRBNInvalidFormatFallsBack(t *testing.T) {
	now := time.Date(2026, 1, 23, 22, 0, 0, 0, time.UTC)
	got := parseTimeFromRBNAt("bad", now)
	if !got.Equal(now) {
		t.Fatalf("expected invalid format to return now %v, got %v", now, got)
	}
}
