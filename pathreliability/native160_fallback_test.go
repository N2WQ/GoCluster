package pathreliability

import (
	"testing"
	"time"

	"dxcluster/internal/solarpath"
)

func TestNative160FallbackResultEmitsConservativeClass(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Native160Fallback.Enabled = true
	cfg.Native160Fallback.DisplayEnabled = true
	req := VOACAPClosedRequest{
		UserGrid: "FN31",
		DXGrid:   "FN20",
		Band:     "160m",
		Mode:     "FT8",
	}
	now := time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
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
	if got.Native160UserDaylight || got.Native160DXDaylight || got.Native160UserTwilight || got.Native160DXTwilight {
		t.Fatalf("expected both endpoints civil-dark for LOW path, got %+v", got)
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
	if !got.Native160UserDaylight && !got.Native160DXDaylight {
		t.Fatalf("expected endpoint daylight veto, got %+v", got)
	}
}

func TestNative160FallbackClassLeavesMiddleBandBlank(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Native160Fallback.ClosedMaxCivilDarkFraction = 0.25
	cfg.Native160Fallback.UnlikelyMinCivilDarkFraction = 0.95
	cfg.Native160Fallback.LowMinCivilDarkFraction = 1.0
	if got := native160FallbackClass(Result{}, cfg, 0.50); got != "" {
		t.Fatalf("middle band class = %q, want blank", got)
	}
}

func TestNative160FallbackClassEmitsUnlikelyBetweenThresholds(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Native160Fallback.ClosedMaxCivilDarkFraction = 0.25
	cfg.Native160Fallback.UnlikelyMinCivilDarkFraction = 0.50
	cfg.Native160Fallback.LowMinCivilDarkFraction = 1.0
	if got := native160FallbackClass(Result{}, cfg, 0.75); got != classUnlikely {
		t.Fatalf("class = %q, want UNLIKELY", got)
	}
}

func TestNative160FallbackClassEndpointStateWinsBeforePathDarkness(t *testing.T) {
	cfg := DefaultConfig()
	tests := []struct {
		name string
		res  Result
		want string
	}{
		{
			name: "user daylight closes dark path",
			res:  Result{Native160UserDaylight: true},
			want: classClosed,
		},
		{
			name: "dx daylight closes dark path",
			res:  Result{Native160DXDaylight: true},
			want: classClosed,
		},
		{
			name: "user twilight caps dark path at unlikely",
			res:  Result{Native160UserTwilight: true},
			want: classUnlikely,
		},
		{
			name: "dx twilight caps dark path at unlikely",
			res:  Result{Native160DXTwilight: true},
			want: classUnlikely,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := native160FallbackClass(tt.res, cfg, 1.0); got != tt.want {
				t.Fatalf("class = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNative160EndpointSolarState(t *testing.T) {
	sun := solarpath.LatLonToVec(0, 0).Normalize()
	tests := []struct {
		name         string
		point        solarpath.Vec3
		wantDaylight bool
		wantTwilight bool
	}{
		{
			name:         "above horizon",
			point:        solarpath.LatLonToVec(0, 0).Normalize(),
			wantDaylight: true,
		},
		{
			name:         "civil twilight below horizon",
			point:        solarpath.LatLonToVec(0, 92).Normalize(),
			wantTwilight: true,
		},
		{
			name:  "civil dark",
			point: solarpath.LatLonToVec(0, 100).Normalize(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDaylight, gotTwilight := native160EndpointSolarState(tt.point, sun, 6)
			if gotDaylight != tt.wantDaylight || gotTwilight != tt.wantTwilight {
				t.Fatalf("state daylight=%v twilight=%v, want daylight=%v twilight=%v", gotDaylight, gotTwilight, tt.wantDaylight, tt.wantTwilight)
			}
		})
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
