package solarweather

import (
	"math"
	"time"

	"dxcluster/pathreliability"
	"dxcluster/strutil"
)

type PathInput struct {
	UserGrid string
	DXGrid   string
	UserCell pathreliability.CellID
	DXCell   pathreliability.CellID
	Band     string
}

type GateDecision struct {
	Daylight        bool
	HighLat         bool
	DaylightUnknown bool
	NearTerminator  bool
	DaylightFrac    float64
	HighLatFrac     float64
	MaxAbsMlatDeg   float64
}

func EvaluateGates(now time.Time, cfg Config, dipole Vec3, sun Vec3, kp float64, kpValid bool, input PathInput, cache *GateCache) GateDecision {
	userVec, ok := gridVector(input.UserGrid)
	if !ok {
		return GateDecision{}
	}
	dxVec, ok := gridVector(input.DXGrid)
	if !ok {
		return GateDecision{}
	}
	key := makePathKey(input, cfg)
	prev := gateState{}
	if cache != nil {
		if cached, ok := cache.Get(key, now); ok {
			prev = cached
		}
	}

	dayFrac, nearTerm, dayUnknown := daylightFraction(userVec, dxVec, sun, cfg)
	dayNext := applyDaylightHysteresis(prev.daylight, dayFrac, nearTerm, dayUnknown, cfg)

	lEdge := highLatEdge(cfg, kp, kpValid)
	highFrac, maxAbs := highLatFractionDipole(userVec, dxVec, dipole, lEdge, cfg)
	highNext := applyHighLatHysteresis(prev.highLat, maxAbs, highFrac, lEdge, cfg)

	nextState := gateState{daylight: dayNext, highLat: highNext, lastSeen: now}
	if cache != nil {
		cache.Put(key, nextState, now)
	}

	return GateDecision{
		Daylight:        dayNext,
		HighLat:         highNext,
		DaylightUnknown: dayUnknown,
		NearTerminator:  nearTerm,
		DaylightFrac:    dayFrac,
		HighLatFrac:     highFrac,
		MaxAbsMlatDeg:   maxAbs,
	}
}

func makePathKey(input PathInput, cfg Config) pathKey {
	userCell := uint16(input.UserCell)
	dxCell := uint16(input.DXCell)
	userGrid := ""
	dxGrid := ""
	if input.UserCell == pathreliability.InvalidCell {
		userGrid = normalizeGridKey(input.UserGrid)
	}
	if input.DXCell == pathreliability.InvalidCell {
		dxGrid = normalizeGridKey(input.DXGrid)
	}
	band := ""
	if cfg.PathKeyIncludeBand != nil && *cfg.PathKeyIncludeBand {
		if normalized, ok := normalizeBand(input.Band); ok {
			band = normalized
		}
	}
	return pathKey{
		userCell: userCell,
		dxCell:   dxCell,
		userGrid: userGrid,
		dxGrid:   dxGrid,
		band:     band,
	}
}

func normalizeGridKey(grid string) string {
	g := strutil.NormalizeUpper(grid)
	if len(g) >= 6 {
		return g[:6]
	}
	if len(g) >= 4 {
		return g[:4]
	}
	return ""
}

func gridVector(grid string) (Vec3, bool) {
	lat, lon, ok := pathreliability.GridCenterLatLon(grid)
	if !ok {
		return Vec3{}, false
	}
	return latLonToVec(lat, lon).Normalize(), true
}

func daylightFraction(a, b, sun Vec3, cfg Config) (float64, bool, bool) {
	D := angleBetween(a, b)
	if D <= cfg.Daylight.DSmallRad {
		return sunlitTest(a, sun, cfg), false, false
	}
	if D >= cfg.Daylight.DAntipodalRad {
		return 0, false, true
	}
	cross := a.Cross(b)
	if cross.Norm() <= cfg.Daylight.CrossNormTiny {
		return 0, false, true
	}
	pathNormal := cross.Normalize()
	crossTerm := pathNormal.Cross(sun)
	if crossTerm.Norm() <= cfg.Daylight.CrossNormTiny {
		if cfg.Sun.TwilightDegrees > 0 {
			return 1.0, false, false
		}
		return 0.5, true, false
	}
	intersect := crossTerm.Normalize()
	candidates := []Vec3{intersect, intersect.Mul(-1)}
	intersections := make([]Vec3, 0, 2)
	eps := math.Max(cfg.Daylight.EpsBaseRad, cfg.Daylight.EpsScale*D)
	for _, c := range candidates {
		d1 := angleBetween(a, c)
		d2 := angleBetween(c, b)
		if math.Abs((d1+d2)-D) <= eps {
			intersections = append(intersections, c)
		}
	}
	if len(intersections) == 0 {
		mid := slerp(a, b, 0.5).Normalize()
		if sunlitTest(mid, sun, cfg) > 0 {
			return 1.0, false, false
		}
		return 0.0, false, false
	}
	if len(intersections) == 1 {
		c := intersections[0]
		d1 := angleBetween(a, c)
		segments := [][2]Vec3{{a, c}, {c, b}}
		lengths := []float64{d1, D - d1}
		lit := 0.0
		for i, seg := range segments {
			if lengths[i] <= 0 {
				continue
			}
			mid := slerp(seg[0], seg[1], 0.5).Normalize()
			if sunlitTest(mid, sun, cfg) > 0 {
				lit += lengths[i]
			}
		}
		if D <= 0 {
			return 0, false, false
		}
		return clamp(lit/D, 0, 1), false, false
	}
	c1 := intersections[0]
	c2 := intersections[1]
	d1 := angleBetween(a, c1)
	d2 := angleBetween(a, c2)
	if d2 < d1 {
		c1, c2 = c2, c1
		d1, d2 = d2, d1
	}
	segments := [][2]Vec3{
		{a, c1},
		{c1, c2},
		{c2, b},
	}
	lengths := []float64{d1, d2 - d1, D - d2}
	lit := 0.0
	for i, seg := range segments {
		if lengths[i] <= 0 {
			continue
		}
		mid := slerp(seg[0], seg[1], 0.5).Normalize()
		if sunlitTest(mid, sun, cfg) > 0 {
			lit += lengths[i]
		}
	}
	if D <= 0 {
		return 0, false, false
	}
	return clamp(lit/D, 0, 1), false, false
}

func sunlitTest(p, sun Vec3, cfg Config) float64 {
	threshold := 0.0
	if cfg.Sun.TwilightDegrees > 0 {
		threshold = -math.Sin(degToRad(cfg.Sun.TwilightDegrees))
	}
	if p.Dot(sun) > threshold {
		return 1
	}
	return 0
}

func applyDaylightHysteresis(prev bool, frac float64, nearTerminator bool, unknown bool, cfg Config) bool {
	if unknown {
		return false
	}
	if nearTerminator && cfg.Sun.NearTerminatorHold {
		return prev
	}
	if prev {
		return frac > cfg.Sun.DaylightExit
	}
	return frac >= cfg.Sun.DaylightEnter
}

func highLatEdge(cfg Config, kp float64, kpValid bool) float64 {
	if !cfg.HighLat.UseKpBoundary {
		return cfg.HighLat.FixedLEdgeDeg
	}
	// L_edge(Kp) = clamp(48, 66 - 2*Kp, 66)
	if !kpValid {
		return cfg.HighLat.FixedLEdgeDeg
	}
	raw := cfg.HighLat.LEdgeMaxDeg - cfg.HighLat.LEdgeSlopeDegPerK*kp
	return clamp(raw, cfg.HighLat.LEdgeMinDeg, cfg.HighLat.LEdgeMaxDeg)
}

func highLatFractionDipole(a, b, dipole Vec3, lEdgeDeg float64, cfg Config) (float64, float64) {
	D := angleBetween(a, b)
	if D <= cfg.Daylight.DSmallRad || D >= cfg.Daylight.DAntipodalRad {
		return 0, 0
	}
	n := a.Cross(b).Normalize()
	u := a.Sub(n.Mul(a.Dot(n))).Normalize()
	v := n.Cross(u)

	thetaA := math.Atan2(v.Dot(a), u.Dot(a))
	thetaB := math.Atan2(v.Dot(b), u.Dot(b))
	delta := wrapToPi(thetaB - thetaA)
	if delta == 0 {
		return 0, 0
	}
	intervalStart := thetaA
	intervalEnd := thetaA + delta

	mPerp := dipole.Sub(n.Mul(dipole.Dot(n)))
	M := mPerp.Norm()
	if M <= cfg.HighLat.MTiny {
		return 0, 0
	}
	mHat := mPerp.Normalize()
	theta0 := math.Atan2(v.Dot(mHat), u.Dot(mHat))

	k := math.Sin(degToRad(lEdgeDeg))
	if k <= 0 {
		return 1.0, radToDeg(math.Asin(clamp(M, 0, 1)))
	}
	if k > M {
		maxAbs := maxAbsDotOnInterval(intervalStart, intervalEnd, theta0, M)
		return 0, radToDeg(math.Asin(clamp(maxAbs, 0, 1)))
	}

	alpha := math.Acos(k / M)

	overlap := 0.0
	centers := []float64{theta0, theta0 + math.Pi}
	for _, center := range centers {
		arcStart := center - alpha
		arcEnd := center + alpha
		for _, shift := range []float64{-twoPi, 0, twoPi} {
			overlap += overlapLength(intervalStart, intervalEnd, arcStart+shift, arcEnd+shift)
		}
	}

	frac := overlap / math.Abs(delta)
	maxAbs := maxAbsDotOnInterval(intervalStart, intervalEnd, theta0, M)
	return clamp(frac, 0, 1), radToDeg(math.Asin(clamp(maxAbs, 0, 1)))
}

func maxAbsDotOnInterval(start, end, theta0, M float64) float64 {
	if end < start {
		start, end = end, start
	}
	maxAbs := math.Abs(M * math.Cos(start-theta0))
	candidate := math.Abs(M * math.Cos(end-theta0))
	if candidate > maxAbs {
		maxAbs = candidate
	}
	for _, center := range []float64{theta0, theta0 + math.Pi} {
		for _, shift := range []float64{-twoPi, 0, twoPi} {
			t := center + shift
			if t >= start && t <= end {
				val := math.Abs(M * math.Cos(t-theta0))
				if val > maxAbs {
					maxAbs = val
				}
			}
		}
	}
	return maxAbs
}

func applyHighLatHysteresis(prev bool, maxAbsDeg, frac, lEdgeDeg float64, cfg Config) bool {
	enterMax := lEdgeDeg + cfg.HighLat.EnterMaxAbsOffset
	exitMax := lEdgeDeg + cfg.HighLat.ExitMaxAbsOffset
	if prev {
		return !(maxAbsDeg <= exitMax && frac <= cfg.HighLat.ExitFrac)
	}
	return maxAbsDeg >= enterMax || frac >= cfg.HighLat.EnterFrac
}
