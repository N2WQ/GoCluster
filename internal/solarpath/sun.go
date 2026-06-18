// File role: Owns subsolar point and ECEF sun-vector calculation.
// Crawler notes: Used by shared solar exposure math, R/G gates, and native
// 160m darkness fallback.
// Related docs: docs/decisions/ADR-0194-native-160m-solar-darkness-fallback.md.
package solarpath

import (
	"math"
	"time"
)

// SubsolarPoint returns the subsolar latitude/longitude in degrees.
func SubsolarPoint(t time.Time) (latDeg, lonDeg float64) {
	utc := t.UTC()
	jd := julianDay(utc)
	T := (jd - 2451545.0) / 36525.0

	L0 := math.Mod(280.46646+T*(36000.76983+T*0.0003032), 360.0)
	M := 357.52911 + T*(35999.05029-0.0001537*T)
	e := 0.016708634 - T*(0.000042037+0.0000001267*T)

	C := math.Sin(DegreesToRadians(M))*(1.914602-T*(0.004817+0.000014*T)) +
		math.Sin(DegreesToRadians(2*M))*(0.019993-0.000101*T) +
		math.Sin(DegreesToRadians(3*M))*0.000289

	trueLong := L0 + C
	omega := 125.04 - 1934.136*T
	lambda := trueLong - 0.00569 - 0.00478*math.Sin(DegreesToRadians(omega))

	eps0 := 23.0 + (26.0+(21.448-T*(46.815+T*(0.00059-0.001813*T)))/60.0)/60.0
	eps := eps0 + 0.00256*math.Cos(DegreesToRadians(omega))

	sinDecl := math.Sin(DegreesToRadians(eps)) * math.Sin(DegreesToRadians(lambda))
	decl := math.Asin(sinDecl)

	y := math.Tan(DegreesToRadians(eps) / 2.0)
	y *= y

	eqTime := 4 * RadiansToDegrees(
		y*math.Sin(2*DegreesToRadians(L0))-
			2*e*math.Sin(DegreesToRadians(M))+
			4*e*y*math.Sin(DegreesToRadians(M))*math.Cos(2*DegreesToRadians(L0))-
			0.5*y*y*math.Sin(4*DegreesToRadians(L0))-
			1.25*e*e*math.Sin(2*DegreesToRadians(M)),
	)

	minutes := float64(utc.Hour()*60+utc.Minute()) + float64(utc.Second())/60.0 + float64(utc.Nanosecond())/6e10
	lonDeg = (720 - (minutes + eqTime)) / 4
	lonDeg = wrapLongitude(lonDeg)
	latDeg = RadiansToDegrees(decl)
	return latDeg, lonDeg
}

// SunVectorECEF returns a unit vector from Earth center toward the sun in ECEF.
func SunVectorECEF(t time.Time) Vec3 {
	lat, lon := SubsolarPoint(t)
	return LatLonToVec(lat, lon).Normalize()
}

func wrapLongitude(lonDeg float64) float64 {
	lon := math.Mod(lonDeg+180.0, 360.0)
	if lon < 0 {
		lon += 360.0
	}
	return lon - 180.0
}

func julianDay(t time.Time) float64 {
	y, m, d := t.Date()
	if m <= 2 {
		y -= 1
		m += 12
	}
	A := y / 100
	B := 2 - A + A/4
	day := float64(d) + (float64(t.Hour()) / 24.0) + (float64(t.Minute()) / 1440.0) + (float64(t.Second()) / 86400.0) + (float64(t.Nanosecond()) / 8.64e13)
	jd := float64(int(365.25*float64(y+4716))) + float64(int(30.6001*float64(m+1))) + day + float64(B) - 1524.5
	return jd
}
