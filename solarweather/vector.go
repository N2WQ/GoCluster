package solarweather

import (
	"math"

	"dxcluster/internal/solarpath"
)

const (
	pi     = math.Pi
	twoPi  = 2 * math.Pi
	radDeg = 180 / math.Pi
	degRad = math.Pi / 180
)

type Vec3 = solarpath.Vec3

func clamp(val, min, max float64) float64 {
	return solarpath.Clamp(val, min, max)
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

func degToRad(deg float64) float64 {
	return solarpath.DegreesToRadians(deg)
}

func radToDeg(rad float64) float64 {
	return solarpath.RadiansToDegrees(rad)
}

func latLonToVec(latDeg, lonDeg float64) Vec3 {
	return solarpath.LatLonToVec(latDeg, lonDeg)
}

func angleBetween(a, b Vec3) float64 {
	return solarpath.AngleBetween(a, b)
}

func slerp(a, b Vec3, t float64) Vec3 {
	return solarpath.Slerp(a, b, t)
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
