package solarweather

import (
	"time"

	"dxcluster/internal/solarpath"
)

// SubsolarPoint returns the subsolar latitude/longitude in degrees (ECEF frame).
func SubsolarPoint(t time.Time) (latDeg, lonDeg float64) {
	return solarpath.SubsolarPoint(t)
}

// SunVectorECEF returns a unit vector pointing from Earth center toward the sun in ECEF.
func SunVectorECEF(t time.Time) Vec3 {
	return solarpath.SunVectorECEF(t)
}
