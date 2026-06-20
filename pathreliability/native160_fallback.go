// File role: Owns native 160m solar-darkness fallback result construction.
// Crawler notes: This fallback is conservative and never replaces sufficient
// p50 or a usable current-hour VOACAP fallback result.
// Related docs: docs/decisions/ADR-0196-native-160m-solar-darkness-fallback.md;
// docs/decisions/ADR-0197-native-160m-closed-solar-proxy.md.
package pathreliability

import (
	"strings"
	"time"

	"dxcluster/internal/solarpath"
)

// Native160FallbackResult evaluates the experimental solar-darkness fallback.
// It never changes sufficient p50 results and emits only CLOSED/LOW/UNLIKELY
// proxy classes from endpoint state first, then civil-dark path geometry.
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
	userVec := solarpath.LatLonToVec(userLat, userLon).Normalize()
	dxLat, dxLon, ok := GridCenterLatLon(strings.TrimSpace(req.DXGrid))
	if !ok {
		res.Native160Unknown = true
		return res
	}
	dxVec := solarpath.LatLonToVec(dxLat, dxLon).Normalize()
	sun := solarpath.SunVectorECEF(now)
	res.Native160UserDaylight, res.Native160UserTwilight = native160EndpointSolarState(
		userVec,
		sun,
		cfg.Native160Fallback.CivilTwilightDegrees,
	)
	res.Native160DXDaylight, res.Native160DXTwilight = native160EndpointSolarState(
		dxVec,
		sun,
		cfg.Native160Fallback.CivilTwilightDegrees,
	)

	exposure := solarpath.SolarExposure(
		userVec,
		dxVec,
		sun,
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

	class := native160FallbackClass(res, cfg, exposure.CivilDarkFraction)
	if class == "" {
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

func native160FallbackClass(res Result, cfg Config, civilDarkFraction float64) string {
	switch {
	case res.Native160UserDaylight || res.Native160DXDaylight:
		return classClosed
	case res.Native160UserTwilight || res.Native160DXTwilight:
		return classUnlikely
	case civilDarkFraction <= cfg.Native160Fallback.ClosedMaxCivilDarkFraction:
		return classClosed
	case civilDarkFraction >= cfg.Native160Fallback.LowMinCivilDarkFraction:
		return classLow
	case civilDarkFraction >= cfg.Native160Fallback.UnlikelyMinCivilDarkFraction:
		return classUnlikely
	default:
		return ""
	}
}

func native160EndpointSolarState(point, sun solarpath.Vec3, civilTwilightDeg float64) (daylight, twilight bool) {
	dot := point.Dot(sun)
	if dot > solarpath.SunElevationThreshold(0) {
		return true, false
	}
	if dot > solarpath.LitThresholdForTwilight(civilTwilightDeg) {
		return false, true
	}
	return false, false
}
