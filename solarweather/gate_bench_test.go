package solarweather

import (
	"math"
	"testing"
	"time"

	"dxcluster/pathreliability"
)

var phase1GateDecisionSink GateDecision
var phase1FloatSink float64
var phase1BoolSink bool
var phase1VecSink Vec3
var phase1ExposureSink phase1SolarExposure

func BenchmarkSolarWeatherPathGates(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.normalize()

	userVec, ok := gridVector("FN31")
	if !ok {
		b.Fatal("invalid user grid")
	}
	dxVec, ok := gridVector("QF56")
	if !ok {
		b.Fatal("invalid DX grid")
	}
	times := phase1SolarWeatherBenchmarkTimes()
	sunVecs := make([]Vec3, len(times))
	dipoleVecs := make([]Vec3, len(times))
	for i, now := range times {
		sunVecs[i] = SunVectorECEF(now)
		dipoleVecs[i] = DipoleAxisECEF(now)
	}

	input := PathInput{
		UserGrid: "FN31",
		DXGrid:   "QF56",
		UserCell: pathreliability.CellID(1),
		DXCell:   pathreliability.CellID(2),
		Band:     "160m",
	}

	b.Run("evaluate_no_cache", func(b *testing.B) {
		b.ReportAllocs()
		var decision GateDecision
		for i := 0; i < b.N; i++ {
			idx := i & 1023
			decision = EvaluateGates(times[idx], cfg, dipoleVecs[idx], sunVecs[idx], 8, true, input, nil)
		}
		phase1GateDecisionSink = decision
	})

	b.Run("evaluate_cache_hit", func(b *testing.B) {
		b.ReportAllocs()
		cache := NewGateCache(cfg.GateCache)
		var decision GateDecision
		for i := 0; i < b.N; i++ {
			idx := i & 1023
			decision = EvaluateGates(times[idx], cfg, dipoleVecs[idx], sunVecs[idx], 8, true, input, cache)
		}
		phase1GateDecisionSink = decision
	})

	b.Run("daylight_fraction_only", func(b *testing.B) {
		b.ReportAllocs()
		var frac float64
		var near bool
		var unknown bool
		for i := 0; i < b.N; i++ {
			frac, near, unknown = daylightFraction(userVec, dxVec, sunVecs[i&1023], cfg)
		}
		phase1FloatSink = frac
		phase1BoolSink = near || unknown
	})

	b.Run("high_lat_fraction_only", func(b *testing.B) {
		b.ReportAllocs()
		var frac float64
		var maxAbs float64
		lEdge := highLatEdge(cfg, 8, true)
		for i := 0; i < b.N; i++ {
			frac, maxAbs = highLatFractionDipole(userVec, dxVec, dipoleVecs[i&1023], lEdge, cfg)
		}
		phase1FloatSink = frac + maxAbs
	})

	b.Run("sun_vector_only", func(b *testing.B) {
		b.ReportAllocs()
		var sun Vec3
		for i := 0; i < b.N; i++ {
			sun = SunVectorECEF(times[i&1023])
		}
		phase1VecSink = sun
	})
}

func BenchmarkSolarWeatherOverrideGlyph(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.normalize()
	now := time.Date(2026, time.June, 18, 12, 0, 0, 0, time.UTC)
	input := PathInput{
		UserGrid: "FN31",
		DXGrid:   "QF56",
		UserCell: pathreliability.CellID(1),
		DXCell:   pathreliability.CellID(2),
		Band:     "160m",
	}

	b.Run("manager_no_active", func(b *testing.B) {
		m := NewManager(cfg, nil)
		m.dipole = DipoleAxisECEF(now)
		b.ReportAllocs()
		var glyph string
		var kind OverrideKind
		for i := 0; i < b.N; i++ {
			glyph, kind = m.OverrideGlyph(now, input)
		}
		if kind != OverrideNone {
			phase1BoolSink = glyph != ""
		}
	})

	b.Run("manager_active_g4_cache_hit", func(b *testing.B) {
		m := phase1ActiveG4Manager(cfg, now)
		m.OverrideGlyph(now, input)
		b.ReportAllocs()
		var glyph string
		var kind OverrideKind
		for i := 0; i < b.N; i++ {
			glyph, kind = m.OverrideGlyph(now, input)
		}
		phase1BoolSink = glyph != "" || kind != OverrideNone
	})

	b.Run("manager_active_g4_no_gate_cache", func(b *testing.B) {
		m := phase1ActiveG4Manager(cfg, now)
		m.gateCache = nil
		b.ReportAllocs()
		var glyph string
		var kind OverrideKind
		for i := 0; i < b.N; i++ {
			glyph, kind = m.OverrideGlyph(now, input)
		}
		phase1BoolSink = glyph != "" || kind != OverrideNone
	})
}

func BenchmarkSolarWeatherDarknessAlternatives(b *testing.B) {
	cfgHorizon := DefaultConfig()
	cfgHorizon.Enabled = true
	cfgHorizon.Sun.TwilightDegrees = 0
	cfgHorizon.normalize()

	cfgCivil := cfgHorizon
	cfgCivil.Sun.TwilightDegrees = 6

	userVec, ok := gridVector("FN31")
	if !ok {
		b.Fatal("invalid user grid")
	}
	dxVec, ok := gridVector("QF56")
	if !ok {
		b.Fatal("invalid DX grid")
	}
	times := phase1SolarWeatherBenchmarkTimes()
	sunVecs := make([]Vec3, len(times))
	for i, now := range times {
		sunVecs[i] = SunVectorECEF(now)
	}

	b.Run("analytic_horizon_and_civil", func(b *testing.B) {
		b.ReportAllocs()
		var exposure phase1SolarExposure
		for i := 0; i < b.N; i++ {
			exposure = phase1AnalyticExposure(userVec, dxVec, sunVecs[i&1023], cfgHorizon, cfgCivil)
		}
		phase1ExposureSink = exposure
	})

	b.Run("sample9_vector_dot", func(b *testing.B) {
		b.ReportAllocs()
		var exposure phase1SolarExposure
		for i := 0; i < b.N; i++ {
			exposure = phase1SampleExposure(userVec, dxVec, sunVecs[i&1023], 9)
		}
		phase1ExposureSink = exposure
	})

	b.Run("sample9_vector_dot_with_sun", func(b *testing.B) {
		b.ReportAllocs()
		var exposure phase1SolarExposure
		for i := 0; i < b.N; i++ {
			idx := i & 1023
			exposure = phase1SampleExposure(userVec, dxVec, SunVectorECEF(times[idx]), 9)
		}
		phase1ExposureSink = exposure
	})
}

func TestPhase1AnalyticVsSample9Corpus(t *testing.T) {
	cfgHorizon := DefaultConfig()
	cfgHorizon.Enabled = true
	cfgHorizon.Sun.TwilightDegrees = 0
	cfgHorizon.normalize()

	cfgCivil := cfgHorizon
	cfgCivil.Sun.TwilightDegrees = 6

	paths := []struct {
		name string
		a    string
		b    string
	}{
		{name: "local_us", a: "FN31", b: "FN32"},
		{name: "us_east_to_us_west", a: "FN31", b: "CM87"},
		{name: "us_east_to_europe", a: "FN31", b: "JN18"},
		{name: "us_east_to_australia", a: "FN31", b: "QF56"},
		{name: "europe_to_japan", a: "IO91", b: "PM95"},
		{name: "alaska_to_new_zealand", a: "BP51", b: "RF72"},
		{name: "equatorial_long", a: "JJ00", b: "RJ90"},
		{name: "near_polar_long", a: "AR09", b: "RR99"},
	}
	times := phase1SeasonalHourlyTimes()

	stats := phase1CompareStats{}
	for _, path := range paths {
		a, ok := gridVector(path.a)
		if !ok {
			t.Fatalf("invalid grid %s", path.a)
		}
		b, ok := gridVector(path.b)
		if !ok {
			t.Fatalf("invalid grid %s", path.b)
		}
		for _, now := range times {
			sun := SunVectorECEF(now)
			analytic := phase1AnalyticExposure(a, b, sun, cfgHorizon, cfgCivil)
			sampled := phase1SampleExposure(a, b, sun, 9)
			stats.observe(analytic, sampled)
		}
	}

	t.Logf("cases=%d", stats.cases)
	t.Logf("daylight fraction abs error sample9 vs analytic horizon: mean=%.3f max=%.3f", stats.meanDaylightAbs(), stats.maxDaylightAbs)
	t.Logf("dark fraction abs error sample9 vs analytic civil darkness: mean=%.3f max=%.3f", stats.meanDarkAbs(), stats.maxDarkAbs)
	t.Logf("dark decision mismatches sample9 vs analytic: threshold>=0.50 %d/%d, >=0.75 %d/%d, >=0.90 %d/%d",
		stats.darkMismatch50, stats.cases,
		stats.darkMismatch75, stats.cases,
		stats.darkMismatch90, stats.cases)
	t.Logf("analytic unknown cases=%d", stats.unknown)
}

func phase1ActiveG4Manager(cfg Config, now time.Time) *Manager {
	m := NewManager(cfg, nil)
	m.dipole = DipoleAxisECEF(now)
	m.state.Kp = 8
	m.state.KpTime = now
	m.state.KpUpdatedAt = now
	return m
}

type phase1SolarExposure struct {
	DaylightFraction float64
	CivilLitFraction float64
	DarkFraction     float64
	MinElevationDeg  float64
	MaxElevationDeg  float64
	Unknown          bool
}

type phase1CompareStats struct {
	cases          int
	unknown        int
	sumDaylightAbs float64
	maxDaylightAbs float64
	sumDarkAbs     float64
	maxDarkAbs     float64
	darkMismatch50 int
	darkMismatch75 int
	darkMismatch90 int
}

func (s *phase1CompareStats) observe(analytic, sampled phase1SolarExposure) {
	s.cases++
	if analytic.Unknown {
		s.unknown++
		return
	}
	daylightAbs := math.Abs(sampled.DaylightFraction - analytic.DaylightFraction)
	if daylightAbs > s.maxDaylightAbs {
		s.maxDaylightAbs = daylightAbs
	}
	s.sumDaylightAbs += daylightAbs

	darkAbs := math.Abs(sampled.DarkFraction - analytic.DarkFraction)
	if darkAbs > s.maxDarkAbs {
		s.maxDarkAbs = darkAbs
	}
	s.sumDarkAbs += darkAbs

	if phase1AtLeast(sampled.DarkFraction, 0.50) != phase1AtLeast(analytic.DarkFraction, 0.50) {
		s.darkMismatch50++
	}
	if phase1AtLeast(sampled.DarkFraction, 0.75) != phase1AtLeast(analytic.DarkFraction, 0.75) {
		s.darkMismatch75++
	}
	if phase1AtLeast(sampled.DarkFraction, 0.90) != phase1AtLeast(analytic.DarkFraction, 0.90) {
		s.darkMismatch90++
	}
}

func (s phase1CompareStats) meanDaylightAbs() float64 {
	if s.cases == s.unknown {
		return 0
	}
	return s.sumDaylightAbs / float64(s.cases-s.unknown)
}

func (s phase1CompareStats) meanDarkAbs() float64 {
	if s.cases == s.unknown {
		return 0
	}
	return s.sumDarkAbs / float64(s.cases-s.unknown)
}

func phase1AtLeast(value, threshold float64) bool {
	return value+1e-12 >= threshold
}

func phase1AnalyticExposure(a, b, sun Vec3, horizonCfg, civilCfg Config) phase1SolarExposure {
	daylight, daylightUnknown := phase1AnalyticLitFraction(a, b, sun, 0, horizonCfg)
	civilThreshold := -math.Sin(degToRad(civilCfg.Sun.TwilightDegrees))
	civilLit, civilUnknown := phase1AnalyticLitFraction(a, b, sun, civilThreshold, civilCfg)
	return phase1SolarExposure{
		DaylightFraction: daylight,
		CivilLitFraction: civilLit,
		DarkFraction:     1 - civilLit,
		Unknown:          daylightUnknown || civilUnknown,
	}
}

func phase1AnalyticLitFraction(a, b, axis Vec3, threshold float64, cfg Config) (float64, bool) {
	D := angleBetween(a, b)
	if D <= cfg.Daylight.DSmallRad {
		if a.Dot(axis) > threshold {
			return 1, false
		}
		return 0, false
	}
	if D >= cfg.Daylight.DAntipodalRad {
		return 0, true
	}
	n := a.Cross(b)
	if n.Norm() <= cfg.Daylight.CrossNormTiny {
		return 0, true
	}
	n = n.Normalize()
	u := a.Sub(n.Mul(a.Dot(n))).Normalize()
	v := n.Cross(u)
	thetaA := math.Atan2(v.Dot(a), u.Dot(a))
	thetaB := math.Atan2(v.Dot(b), u.Dot(b))
	delta := wrapToPi(thetaB - thetaA)
	if delta == 0 {
		return 0, true
	}
	start := thetaA
	end := thetaA + delta
	if end < start {
		start, end = end, start
	}

	A := axis.Dot(u)
	B := axis.Dot(v)
	M := math.Hypot(A, B)
	if M <= cfg.Daylight.CrossNormTiny {
		if 0 > threshold {
			return 1, false
		}
		return 0, false
	}
	if threshold <= -M {
		return 1, false
	}
	if threshold >= M {
		return 0, false
	}

	alpha := math.Acos(threshold / M)
	center := math.Atan2(B, A)
	overlap := 0.0
	for _, shift := range []float64{-twoPi, 0, twoPi} {
		overlap += overlapLength(start, end, center-alpha+shift, center+alpha+shift)
	}
	return clamp(overlap/math.Abs(delta), 0, 1), false
}

func phase1SampleExposure(a, b, sun Vec3, samples int) phase1SolarExposure {
	if samples < 2 {
		samples = 2
	}
	civilThreshold := -math.Sin(degToRad(6))
	exposure := phase1SolarExposure{
		MinElevationDeg: math.Inf(1),
		MaxElevationDeg: math.Inf(-1),
	}
	daylight := 0
	civilLit := 0
	dark := 0
	for i := 0; i < samples; i++ {
		fraction := float64(i) / float64(samples-1)
		point := slerp(a, b, fraction).Normalize()
		dot := clamp(point.Dot(sun), -1, 1)
		elevation := radToDeg(math.Asin(dot))
		if elevation < exposure.MinElevationDeg {
			exposure.MinElevationDeg = elevation
		}
		if elevation > exposure.MaxElevationDeg {
			exposure.MaxElevationDeg = elevation
		}
		switch {
		case dot > 0:
			daylight++
			civilLit++
		case dot > civilThreshold:
			civilLit++
		default:
			dark++
		}
	}
	exposure.DaylightFraction = float64(daylight) / float64(samples)
	exposure.CivilLitFraction = float64(civilLit) / float64(samples)
	exposure.DarkFraction = float64(dark) / float64(samples)
	return exposure
}

func phase1SolarWeatherBenchmarkTimes() []time.Time {
	base := time.Date(2026, time.June, 18, 0, 0, 0, 0, time.UTC)
	times := make([]time.Time, 1024)
	for i := range times {
		times[i] = base.Add(time.Duration(i) * time.Minute)
	}
	return times
}

func phase1SeasonalHourlyTimes() []time.Time {
	days := []time.Time{
		time.Date(2026, time.March, 20, 0, 0, 0, 0, time.UTC),
		time.Date(2026, time.June, 21, 0, 0, 0, 0, time.UTC),
		time.Date(2026, time.September, 22, 0, 0, 0, 0, time.UTC),
		time.Date(2026, time.December, 21, 0, 0, 0, 0, time.UTC),
	}
	times := make([]time.Time, 0, len(days)*24)
	for _, day := range days {
		for hour := 0; hour < 24; hour++ {
			times = append(times, day.Add(time.Duration(hour)*time.Hour))
		}
	}
	return times
}
