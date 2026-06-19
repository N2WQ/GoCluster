package pathreliability

import (
	"testing"
	"time"
)

func TestNative160FallbackResultEmitsConservativeClass(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Native160Fallback.Enabled = true
	cfg.Native160Fallback.DisplayEnabled = true
	req := VOACAPClosedRequest{
		UserGrid: "FN31",
		DXGrid:   "QF56",
		Band:     "160m",
		Mode:     "FT8",
	}
	now := time.Date(2026, time.June, 18, 12, 0, 0, 0, time.UTC)
	base := Result{Glyph: cfg.GlyphSymbols.Insufficient, Source: SourceInsufficient, InsufficientReason: InsufficientNoSample}
	got := Native160FallbackResult(base, cfg, req, now)
	if !got.Native160Checked || got.Native160Unknown {
		t.Fatalf("expected checked known native 160m result: %+v", got)
	}
	if got.Source != SourceNative160 {
		t.Fatalf("source = %v, want native 160m", got.Source)
	}
	if got.Class != classLow || got.Glyph != cfg.GlyphSymbols.Low {
		t.Fatalf("class/glyph = %q/%q, want LOW/%q", got.Class, got.Glyph, cfg.GlyphSymbols.Low)
	}
	if got.Native160CivilDarkFraction < cfg.Native160Fallback.LowMinCivilDarkFraction {
		t.Fatalf("dark fraction %.3f below LOW threshold", got.Native160CivilDarkFraction)
	}
}

func TestNative160FallbackResultEmitsClosedForDaylitPath(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Native160Fallback.Enabled = true
	cfg.Native160Fallback.DisplayEnabled = true
	req := VOACAPClosedRequest{
		UserGrid: "FN31",
		DXGrid:   "FN20",
		Band:     "160m",
		Mode:     "FT8",
	}
	now := time.Date(2026, time.June, 18, 16, 0, 0, 0, time.UTC)
	base := Result{Glyph: cfg.GlyphSymbols.Insufficient, Source: SourceInsufficient, InsufficientReason: InsufficientNoSample}
	got := Native160FallbackResult(base, cfg, req, now)
	if !got.Native160Checked || got.Native160Unknown {
		t.Fatalf("expected checked known native 160m result: %+v", got)
	}
	if got.Source != SourceNative160 || got.Class != classClosed || got.Glyph != cfg.GlyphSymbols.Closed {
		t.Fatalf("class/glyph = %v/%q/%q, want native CLOSED/%q", got.Source, got.Class, got.Glyph, cfg.GlyphSymbols.Closed)
	}
	if got.Native160CivilDarkFraction > cfg.Native160Fallback.ClosedMaxCivilDarkFraction {
		t.Fatalf("dark fraction %.3f above CLOSED threshold %.3f", got.Native160CivilDarkFraction, cfg.Native160Fallback.ClosedMaxCivilDarkFraction)
	}
}

func TestNative160FallbackResultLeavesMiddleBandBlank(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Native160Fallback.Enabled = true
	cfg.Native160Fallback.DisplayEnabled = true
	cfg.Native160Fallback.ClosedMaxCivilDarkFraction = 0.25
	cfg.Native160Fallback.UnlikelyMinCivilDarkFraction = 0.95
	cfg.Native160Fallback.LowMinCivilDarkFraction = 1.0
	req := VOACAPClosedRequest{
		UserGrid: "FN31",
		DXGrid:   "QF56",
		Band:     "160m",
		Mode:     "FT8",
	}
	now := time.Date(2026, time.June, 18, 12, 0, 0, 0, time.UTC)
	base := Result{Glyph: cfg.GlyphSymbols.Insufficient, Source: SourceInsufficient, InsufficientReason: InsufficientNoSample}
	got := Native160FallbackResult(base, cfg, req, now)
	if !got.Native160Checked || got.Native160Unknown {
		t.Fatalf("expected checked known native 160m result: %+v", got)
	}
	if got.Native160CivilDarkFraction <= cfg.Native160Fallback.ClosedMaxCivilDarkFraction ||
		got.Native160CivilDarkFraction >= cfg.Native160Fallback.UnlikelyMinCivilDarkFraction {
		t.Fatalf("test path dark fraction %.3f is not in the deliberate blank band", got.Native160CivilDarkFraction)
	}
	if got.Source != SourceInsufficient || got.Native160Emitted {
		t.Fatalf("middle band must stay blank/insufficient, got %+v", got)
	}
}

func TestNative160FallbackResultEmitsUnlikelyBetweenThresholds(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Native160Fallback.Enabled = true
	cfg.Native160Fallback.DisplayEnabled = true
	cfg.Native160Fallback.ClosedMaxCivilDarkFraction = 0.25
	cfg.Native160Fallback.UnlikelyMinCivilDarkFraction = 0.50
	cfg.Native160Fallback.LowMinCivilDarkFraction = 1.0
	req := VOACAPClosedRequest{
		UserGrid: "FN31",
		DXGrid:   "QF56",
		Band:     "160m",
		Mode:     "FT8",
	}
	now := time.Date(2026, time.June, 18, 12, 0, 0, 0, time.UTC)
	base := Result{Glyph: cfg.GlyphSymbols.Insufficient, Source: SourceInsufficient, InsufficientReason: InsufficientNoSample}
	got := Native160FallbackResult(base, cfg, req, now)
	if got.Source != SourceNative160 || got.Class != classUnlikely || got.Glyph != cfg.GlyphSymbols.Unlikely {
		t.Fatalf("expected native UNLIKELY fallback, got %+v", got)
	}
}

func TestNative160FallbackResultPreservesSufficientP50(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Native160Fallback.Enabled = true
	cfg.Native160Fallback.DisplayEnabled = true
	base := Result{Glyph: cfg.GlyphSymbols.High, Class: classHigh, Source: SourceCombined, HasP50: true, P50DB: -10}
	got := Native160FallbackResult(base, cfg, VOACAPClosedRequest{UserGrid: "FN31", DXGrid: "QF56", Band: "160m"}, time.Date(2026, time.June, 18, 12, 0, 0, 0, time.UTC))
	if got.Source != SourceCombined || got.Glyph != base.Glyph || got.Native160Checked {
		t.Fatalf("sufficient p50 must remain unchanged: %+v", got)
	}
}

func TestNative160FallbackResultDoesNotEmitWhenDisplayDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Native160Fallback.Enabled = true
	cfg.Native160Fallback.DisplayEnabled = false
	base := Result{Glyph: cfg.GlyphSymbols.Insufficient, Source: SourceInsufficient, InsufficientReason: InsufficientNoSample}
	got := Native160FallbackResult(base, cfg, VOACAPClosedRequest{UserGrid: "FN31", DXGrid: "QF56", Band: "160m"}, time.Date(2026, time.June, 18, 12, 0, 0, 0, time.UTC))
	if !got.Native160Checked || !got.Native160DisplayDisabled {
		t.Fatalf("expected checked display-disabled native 160m result: %+v", got)
	}
	if got.Source != SourceInsufficient {
		t.Fatalf("display-disabled fallback must not emit, got %+v", got)
	}
}

func TestNative160FallbackResultIgnoresOtherBands(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Native160Fallback.Enabled = true
	cfg.Native160Fallback.DisplayEnabled = true
	base := Result{Glyph: cfg.GlyphSymbols.Insufficient, Source: SourceInsufficient, InsufficientReason: InsufficientNoSample}
	got := Native160FallbackResult(base, cfg, VOACAPClosedRequest{UserGrid: "FN31", DXGrid: "QF56", Band: "80m"}, time.Date(2026, time.June, 18, 12, 0, 0, 0, time.UTC))
	if got.Native160Checked || got.Source != SourceInsufficient {
		t.Fatalf("other bands must remain untouched: %+v", got)
	}
}
