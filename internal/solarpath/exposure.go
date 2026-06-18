// File role: Owns exact great-circle solar exposure calculations.
// Crawler notes: Start here for daylight and civil-darkness fractions used by
// solarweather gates and native 160m fallback.
// Related docs: docs/decisions/ADR-0194-native-160m-solar-darkness-fallback.md.
package solarpath

import "math"

// Tolerances bounds numerical decisions for great-circle path geometry.
type Tolerances struct {
	CrossNormTiny float64
	DSmallRad     float64
	DAntipodalRad float64
}

// Exposure carries exact path fractions for solar daylight and civil darkness.
type Exposure struct {
	DaylightFraction  float64
	CivilLitFraction  float64
	CivilDarkFraction float64
	NearTerminator    bool
	Unknown           bool
}

// SunElevationThreshold returns the dot-product threshold for a sun elevation.
func SunElevationThreshold(elevationDeg float64) float64 {
	return math.Sin(DegreesToRadians(elevationDeg))
}

// LitThresholdForTwilight returns the threshold for "lit above -twilight".
func LitThresholdForTwilight(twilightDeg float64) float64 {
	if twilightDeg <= 0 {
		return 0
	}
	return SunElevationThreshold(-twilightDeg)
}

// SolarExposure returns exact daylight and civil-dark fractions for a path.
func SolarExposure(a, b, sun Vec3, civilTwilightDeg float64, tol Tolerances) Exposure {
	daylight, nearTerm, daylightUnknown := FractionAboveThreshold(a, b, sun, 0, tol)
	civilLit, _, civilUnknown := FractionAboveThreshold(a, b, sun, LitThresholdForTwilight(civilTwilightDeg), tol)
	return Exposure{
		DaylightFraction:  daylight,
		CivilLitFraction:  civilLit,
		CivilDarkFraction: 1 - civilLit,
		NearTerminator:    nearTerm,
		Unknown:           daylightUnknown || civilUnknown,
	}
}

// FractionAboveThreshold solves the fraction of the shorter great-circle arc
// from a to b where axis dot point is strictly greater than threshold.
func FractionAboveThreshold(a, b, axis Vec3, threshold float64, tol Tolerances) (fraction float64, nearBoundary bool, unknown bool) {
	D := AngleBetween(a, b)
	if D <= tol.DSmallRad {
		if a.Dot(axis) > threshold {
			return 1, false, false
		}
		return 0, false, false
	}
	if D >= tol.DAntipodalRad {
		return 0, false, true
	}
	n := a.Cross(b)
	if n.Norm() <= tol.CrossNormTiny {
		return 0, false, true
	}
	n = n.Normalize()
	u := a.Sub(n.Mul(a.Dot(n))).Normalize()
	v := n.Cross(u)
	thetaA := math.Atan2(v.Dot(a), u.Dot(a))
	thetaB := math.Atan2(v.Dot(b), u.Dot(b))
	delta := wrapToPi(thetaB - thetaA)
	if delta == 0 {
		return 0, false, true
	}
	start := thetaA
	end := thetaA + delta
	if end < start {
		start, end = end, start
	}

	A := axis.Dot(u)
	B := axis.Dot(v)
	M := math.Hypot(A, B)
	if M <= tol.CrossNormTiny {
		switch {
		case 0 > threshold:
			return 1, false, false
		case math.Abs(threshold) <= tol.CrossNormTiny:
			return 0.5, true, false
		default:
			return 0, false, false
		}
	}
	if threshold <= -M {
		return 1, false, false
	}
	if threshold >= M {
		return 0, false, false
	}

	alpha := math.Acos(threshold / M)
	center := math.Atan2(B, A)
	overlap := 0.0
	for _, shift := range []float64{-twoPi, 0, twoPi} {
		overlap += overlapLength(start, end, center-alpha+shift, center+alpha+shift)
	}
	return Clamp(overlap/math.Abs(delta), 0, 1), false, false
}
