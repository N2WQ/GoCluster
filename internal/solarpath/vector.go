// File role: Owns shared allocation-free vector helpers for solar path geometry.
// Crawler notes: Start here for Vec3 math used by R/G daylight gates and native
// 160m darkness fallback.
// Related docs: docs/decisions/ADR-0196-native-160m-solar-darkness-fallback.md.
package solarpath

import "math"

const (
	pi     = math.Pi
	twoPi  = 2 * math.Pi
	radDeg = 180 / math.Pi
	degRad = math.Pi / 180
)

// Vec3 is a small value vector in Earth-centered Earth-fixed coordinates.
type Vec3 struct {
	X float64
	Y float64
	Z float64
}

// Dot returns the scalar dot product.
func (v Vec3) Dot(o Vec3) float64 {
	return v.X*o.X + v.Y*o.Y + v.Z*o.Z
}

// Cross returns the vector cross product.
func (v Vec3) Cross(o Vec3) Vec3 {
	return Vec3{
		X: v.Y*o.Z - v.Z*o.Y,
		Y: v.Z*o.X - v.X*o.Z,
		Z: v.X*o.Y - v.Y*o.X,
	}
}

// Add returns v + o.
func (v Vec3) Add(o Vec3) Vec3 {
	return Vec3{X: v.X + o.X, Y: v.Y + o.Y, Z: v.Z + o.Z}
}

// Sub returns v - o.
func (v Vec3) Sub(o Vec3) Vec3 {
	return Vec3{X: v.X - o.X, Y: v.Y - o.Y, Z: v.Z - o.Z}
}

// Mul returns v scaled by k.
func (v Vec3) Mul(k float64) Vec3 {
	return Vec3{X: v.X * k, Y: v.Y * k, Z: v.Z * k}
}

// Norm returns the Euclidean vector length.
func (v Vec3) Norm() float64 {
	return math.Sqrt(v.Dot(v))
}

// Normalize returns a unit vector, or a zero vector for zero-length input.
func (v Vec3) Normalize() Vec3 {
	n := v.Norm()
	if n == 0 {
		return Vec3{}
	}
	return v.Mul(1 / n)
}

// DegreesToRadians converts degrees to radians.
func DegreesToRadians(deg float64) float64 {
	return deg * degRad
}

// RadiansToDegrees converts radians to degrees.
func RadiansToDegrees(rad float64) float64 {
	return rad * radDeg
}

// LatLonToVec returns the unit-sphere ECEF vector for latitude/longitude.
func LatLonToVec(latDeg, lonDeg float64) Vec3 {
	lat := DegreesToRadians(latDeg)
	lon := DegreesToRadians(lonDeg)
	clat := math.Cos(lat)
	return Vec3{
		X: clat * math.Cos(lon),
		Y: clat * math.Sin(lon),
		Z: math.Sin(lat),
	}
}

// AngleBetween returns the central angle between unit vectors.
func AngleBetween(a, b Vec3) float64 {
	dot := Clamp(a.Dot(b), -1, 1)
	return math.Acos(dot)
}

// Slerp returns the spherical interpolation point from a to b.
func Slerp(a, b Vec3, t float64) Vec3 {
	dot := Clamp(a.Dot(b), -1, 1)
	omega := math.Acos(dot)
	if omega == 0 {
		return a
	}
	sinOmega := math.Sin(omega)
	if sinOmega == 0 {
		return a
	}
	factor1 := math.Sin((1-t)*omega) / sinOmega
	factor2 := math.Sin(t*omega) / sinOmega
	return a.Mul(factor1).Add(b.Mul(factor2))
}

// Clamp bounds val to [min, max].
func Clamp(val, min, max float64) float64 {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

func wrapToPi(x float64) float64 {
	y := math.Mod(x+pi, twoPi)
	if y < 0 {
		y += twoPi
	}
	y -= pi
	if y == -pi {
		return pi
	}
	return y
}

func overlapLength(aStart, aEnd, bStart, bEnd float64) float64 {
	if aEnd < aStart {
		aStart, aEnd = aEnd, aStart
	}
	if bEnd < bStart {
		bStart, bEnd = bEnd, bStart
	}
	start := aStart
	if bStart > start {
		start = bStart
	}
	end := aEnd
	if bEnd < end {
		end = bEnd
	}
	if end <= start {
		return 0
	}
	return end - start
}
