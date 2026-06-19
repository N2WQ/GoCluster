// File role: Owns native 160m solar-darkness fallback result construction.
// Crawler notes: This fallback is conservative and never replaces sufficient
// p50 or a usable current-hour VOACAP fallback result.
// Related docs: docs/decisions/ADR-0194-native-160m-solar-darkness-fallback.md;
// docs/decisions/ADR-0195-native-160m-closed-solar-proxy.md.
package pathreliability

import (
	"strings"
	"time"

	"dxcluster/internal/solarpath"
)

// Native160FallbackResult evaluates the experimental solar-darkness fallback.
// It never changes sufficient p50 results and emits only CLOSED/LOW/UNLIKELY
// proxy classes from civil-dark path geometry.
func Native160FallbackResult(base Result, cfg Config, req VOACAPClosedRequest, now time.Time) Result {
	if base.Source != SourceInsufficient || !cfg.Native160Fallback.Enabled || normalizeBand(req.Band) != "160m" {
		return base
	}
	res := base
	res.Native160Checked = true
	res.Native160CivilTwilightDeg = cfg.Native160Fallback.CivilTwilightDegrees
	res.Native160ClosedMaxDarkFrac = cfg.Native160Fallback.ClosedMaxCivilDarkFraction

	userLat, userLon, ok := GridCenterLatLon(strings.TrimSpace(req.UserGrid))
	if !ok {
		res.Native160Unknown = true
		return res
	}
	dxLat, dxLon, ok := GridCenterLatLon(strings.TrimSpace(req.DXGrid))
	if !ok {
		res.Native160Unknown = true
		return res
	}

	exposure := solarpath.SolarExposure(
		solarpath.LatLonToVec(userLat, userLon).Normalize(),
		solarpath.LatLonToVec(dxLat, dxLon).Normalize(),
		solarpath.SunVectorECEF(now),
		cfg.Native160Fallback.CivilTwilightDegrees,
		solarpath.Tolerances{
			CrossNormTiny: 1e-12,
			DSmallRad:     1e-6,
			DAntipodalRad: solarpath.DegreesToRadians(180) - 1e-6,
		},
	)
	res.Native160DaylightFraction = exposure.DaylightFraction
	res.Native160CivilDarkFraction = exposure.CivilDarkFraction
	if exposure.Unknown {
		res.Native160Unknown = true
		return res
	}

	var class string
	switch {
	case exposure.CivilDarkFraction <= cfg.Native160Fallback.ClosedMaxCivilDarkFraction:
		class = classClosed
	case exposure.CivilDarkFraction >= cfg.Native160Fallback.LowMinCivilDarkFraction:
		class = classLow
	case exposure.CivilDarkFraction >= cfg.Native160Fallback.UnlikelyMinCivilDarkFraction:
		class = classUnlikely
	default:
		return res
	}
	if !cfg.Native160Fallback.DisplayEnabled {
		res.Native160DisplayDisabled = true
		return res
	}
	res.Source = SourceNative160
	res.Class = class
	res.Glyph = glyphForClass(class, cfg)
	res.Native160Emitted = true
	return res
}
