package solarweather

import (
	"math"
	"time"

	"dxcluster/internal/solarpath"
	"dxcluster/pathreliability"
	"dxcluster/strutil"
)

// PathInput carries the operator-visible path coordinates for a solar override
// check. Cell IDs are preferred when available; grids are retained as a fallback
// for diagnostics and older callers.
type PathInput struct {
	UserGrid string
	DXGrid   string
	UserCell pathreliability.CellID
	DXCell   pathreliability.CellID
	Band     string
}

// GateDecision exposes the intermediate daylight/high-latitude decisions so a
// support agent can explain why an R/G event did or did not affect a path.
type GateDecision struct {
	Daylight        bool
	HighLat         bool
	DaylightUnknown bool
	NearTerminator  bool
	DaylightFrac    float64
	HighLatFrac     float64
	MaxAbsMlatDeg   float64
}

// EvaluateGates combines geometry, Kp-derived high-latitude boundary, and
// hysteresis into a stable per-path decision. Hysteresis is cached per path so
// telnet glyphs do not flap near thresholds.
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

// makePathKey chooses compact cell IDs when available and falls back to
// normalized grids only when cell encoding is unavailable.
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

// daylightFraction estimates how much of the great-circle path is sunlit. It
// returns unknown for antipodal or degenerate paths rather than forcing a false
// precision into support output.
func daylightFraction(a, b, sun Vec3, cfg Config) (float64, bool, bool) {
	threshold := solarpath.LitThresholdForTwilight(cfg.Sun.TwilightDegrees)
	return solarpath.FractionAboveThreshold(a, b, sun, threshold, solarpath.Tolerances{
		CrossNormTiny: cfg.Daylight.CrossNormTiny,
		DSmallRad:     cfg.Daylight.DSmallRad,
		DAntipodalRad: cfg.Daylight.DAntipodalRad,
	})
}

// applyDaylightHysteresis keeps daylight gating stable near the terminator where
// tiny timing or grid changes could otherwise flip the override repeatedly.
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

// highLatEdge converts Kp into the moving geomagnetic latitude boundary used by
// G-event overrides, falling back to the fixed boundary when Kp is stale.
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

// highLatFractionDipole measures how much of the path crosses high magnetic
// latitude. It uses the dipole vector so support output matches the same model
// used for override selection.
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

// applyHighLatHysteresis keeps high-latitude decisions sticky enough for telnet
// readability while still releasing once both max latitude and path fraction
// fall below exit thresholds.
func applyHighLatHysteresis(prev bool, maxAbsDeg, frac, lEdgeDeg float64, cfg Config) bool {
	enterMax := lEdgeDeg + cfg.HighLat.EnterMaxAbsOffset
	exitMax := lEdgeDeg + cfg.HighLat.ExitMaxAbsOffset
	if prev {
		return !(maxAbsDeg <= exitMax && frac <= cfg.HighLat.ExitFrac)
	}
	return maxAbsDeg >= enterMax || frac >= cfg.HighLat.EnterFrac
}
