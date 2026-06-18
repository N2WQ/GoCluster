package solarpath

import (
	"math"
	"testing"
	"time"
)

func TestFractionAboveThresholdSplitsHorizon(t *testing.T) {
	tol := testTolerances()
	sun := LatLonToVec(0, 0).Normalize()
	a := LatLonToVec(0, 45).Normalize()
	b := LatLonToVec(0, 135).Normalize()
	got, near, unknown := FractionAboveThreshold(a, b, sun, 0, tol)
	if unknown || near {
		t.Fatalf("unexpected flags near=%v unknown=%v", near, unknown)
	}
	if math.Abs(got-0.5) > 1e-9 {
		t.Fatalf("fraction = %.12f, want 0.5", got)
	}
}

func TestFractionAboveThresholdTerminatorArcReportsNear(t *testing.T) {
	tol := testTolerances()
	sun := LatLonToVec(0, 0).Normalize()
	a := LatLonToVec(-45, 90).Normalize()
	b := LatLonToVec(45, 90).Normalize()
	got, near, unknown := FractionAboveThreshold(a, b, sun, 0, tol)
	if unknown || !near {
		t.Fatalf("expected near terminator, near=%v unknown=%v", near, unknown)
	}
	if math.Abs(got-0.5) > 1e-9 {
		t.Fatalf("fraction = %.12f, want 0.5", got)
	}
}

func TestSolarExposureCivilDark(t *testing.T) {
	tol := testTolerances()
	sun := LatLonToVec(0, 0).Normalize()
	a := LatLonToVec(0, 170).Normalize()
	b := LatLonToVec(0, -170).Normalize()
	got := SolarExposure(a, b, sun, 6, tol)
	if got.Unknown {
		t.Fatalf("unexpected unknown exposure")
	}
	if got.CivilDarkFraction < 0.99 {
		t.Fatalf("civil dark fraction = %.3f, want nearly all dark", got.CivilDarkFraction)
	}
}

func BenchmarkSolarExposure(b *testing.B) {
	tol := testTolerances()
	a := LatLonToVec(41.5, -72.7).Normalize()
	c := LatLonToVec(-33.9, 151.2).Normalize()
	times := make([]time.Time, 1024)
	base := time.Date(2026, time.June, 18, 0, 0, 0, 0, time.UTC)
	suns := make([]Vec3, len(times))
	for i := range times {
		times[i] = base.Add(time.Duration(i) * time.Minute)
		suns[i] = SunVectorECEF(times[i])
	}
	b.ReportAllocs()
	var sink Exposure
	for i := 0; i < b.N; i++ {
		sink = SolarExposure(a, c, suns[i&1023], 6, tol)
	}
	_ = sink
}

func testTolerances() Tolerances {
	return Tolerances{
		CrossNormTiny: 1e-12,
		DSmallRad:     1e-6,
		DAntipodalRad: math.Pi - 1e-6,
	}
}
