// File role: Allocation-free canonical great-circle SP/LP sampling, Meeus solar
// ephemeris reuse, IGRF-14 dipole auroral geometry, ocean-mask lookup, and v4
// 31-feature builder.
// Crawler notes: Mirrors hf-prop-lab/src/geometry.py and src/ocean_mask.py in
// float64 before casting to [31]float32 for MLP inference.
package propprior

import (
	"math"
	"time"

	"dxcluster/internal/solarpath"
)

const (
	earthRadiusKM      = 6371.0
	numArcPoints       = 21
	midArcIdx          = numArcPoints / 2 // 10 (f = 0.50)
	seaStartIdx        = 3                // f = 0.15
	seaEndIdx          = 17               // f = 0.85 (15 interior reflection points)
	seaPointCount      = 15.0
	interiorPointCount = 19.0
	auroralLatFloorDeg = 45.0
	dipoleNorthLatDeg  = 80.8
	dipoleNorthLonDeg  = -72.8
	degToRad           = math.Pi / 180.0
	radToDeg           = 180.0 / math.Pi
	twoPi              = 2.0 * math.Pi
	singularitySmall   = 1e-7
	singularityAnti    = math.Pi - 1e-4
)

var (
	dipoleNorthLatRad = dipoleNorthLatDeg * degToRad
	dipoleNorthLonRad = dipoleNorthLonDeg * degToRad
	sinPhiP           = math.Sin(dipoleNorthLatRad)
	cosPhiP           = math.Cos(dipoleNorthLatRad)
	arcFractions      = [numArcPoints]float64{
		0.00, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30,
		0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65,
		0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00,
	}
)

type solarContext struct {
	subLonDeg float64
	sinDecl   float64
	cosDecl   float64
	sinDoy    float64
	cosDoy    float64
}

type pathGeometry struct {
	p1      float64
	l1      float64
	deltaSP float64
	azSP    float64
	deltaLP float64
	azLP    float64
}

func computeSolarContext(t time.Time) solarContext {
	utc := t.UTC()
	subLatDeg, subLonDeg := solarpath.SubsolarPoint(utc)
	declRad := subLatDeg * degToRad
	doyAng := (twoPi / 365.25) * float64(utc.YearDay())
	return solarContext{
		subLonDeg: subLonDeg,
		sinDecl:   math.Sin(declRad),
		cosDecl:   math.Cos(declRad),
		sinDoy:    math.Sin(doyAng),
		cosDoy:    math.Cos(doyAng),
	}
}

func canonicalizeEndpoints(lat1, lon1, lat2, lon2 float64) (float64, float64, float64, float64) {
	if lat1 > lat2 || (lat1 == lat2 && lon1 > lon2) {
		return lat2, lon2, lat1, lon1
	}
	return lat1, lon1, lat2, lon2
}

func computeGreatCircleSPLP(lat1Deg, lon1Deg, lat2Deg, lon2Deg float64) pathGeometry {
	cLat1, cLon1, cLat2, cLon2 := canonicalizeEndpoints(lat1Deg, lon1Deg, lat2Deg, lon2Deg)
	p1 := cLat1 * degToRad
	l1 := cLon1 * degToRad
	p2 := cLat2 * degToRad
	l2 := cLon2 * degToRad

	dp := p2 - p1
	dl := l2 - l1
	sinDP2 := math.Sin(dp * 0.5)
	sinDL2 := math.Sin(dl * 0.5)
	h := clampMaxOne(sinDP2*sinDP2+math.Cos(p1)*math.Cos(p2)*sinDL2*sinDL2, 0.0)
	deltaSP := 2.0 * math.Atan2(math.Sqrt(h), math.Sqrt(1.0-h))

	azSP := math.Atan2(
		math.Sin(dl)*math.Cos(p2),
		math.Cos(p1)*math.Sin(p2)-math.Sin(p1)*math.Cos(p2)*math.Cos(dl),
	)
	if deltaSP < singularitySmall || deltaSP > singularityAnti {
		azSP = 0.0
	}

	deltaLP := twoPi - deltaSP
	azLP := wrapTwoPi(azSP + math.Pi)
	return pathGeometry{
		p1:      p1,
		l1:      l1,
		deltaSP: deltaSP,
		azSP:    azSP,
		deltaLP: deltaLP,
		azLP:    azLP,
	}
}

// buildArcFeatures populates the exact 31-feature v4 vector for a single SP or LP
// arc in float64 before casting to float32. Performs zero heap allocations.
func buildArcFeatures(
	out *[NumFeatures]float32,
	mask *[OceanMaskBytes]byte,
	p1, l1, delta, az float64,
	logFreq float64,
	solar solarContext,
	sw SpaceWeatherFeatures,
) {
	sinP1 := math.Sin(p1)
	cosP1 := math.Cos(p1)
	sinAz := math.Sin(az)
	cosAz := math.Cos(az)

	auroralThresh := 67.0 - 2.0*sw.Hp30Lag1
	if auroralThresh < auroralLatFloorDeg {
		auroralThresh = auroralLatFloorDeg
	}

	var (
		latMin, latMax         float64
		midLatDeg, midLonRad   float64
		maxAbsGM, sumAbsGM     float64
		auroralCount, seaCount int
		cosZ0, cosZ20, cosZMid float64
		cosZIntMin, cosZIntMax float64
		daylightCosIntSum      float64
		sinHAMid               float64
		sinHAIntSum            float64
		sinHAIntMin, sinHAIntMax float64
	)

	for k := range numArcPoints {
		theta := delta * arcFractions[k]
		sinTh := math.Sin(theta)
		cosTh := math.Cos(theta)

		sinPF := clampMaxOne(sinP1*cosTh+cosP1*sinTh*cosAz, -1.0)
		pf := math.Asin(sinPF)
		cosPF := math.Cos(pf)
		lf := wrapPi(l1 + math.Atan2(sinAz*sinTh*cosP1, cosTh-sinP1*sinPF))

		latDeg := pf * radToDeg
		lonDeg := lf * radToDeg

		if k == 0 {
			latMin = latDeg
			latMax = latDeg
		} else {
			if latDeg < latMin {
				latMin = latDeg
			}
			if latDeg > latMax {
				latMax = latDeg
			}
		}

		sinGM := clampMaxOne(sinPF*sinPhiP+cosPF*cosPhiP*math.Cos(lf-dipoleNorthLonRad), -1.0)
		absGM := math.Abs(math.Asin(sinGM) * radToDeg)
		if absGM > maxAbsGM {
			maxAbsGM = absGM
		}
		sumAbsGM += absGM
		if absGM >= auroralThresh {
			auroralCount++
		}

		if k >= seaStartIdx && k <= seaEndIdx && lookupOceanMaskBit(mask, latDeg, lonDeg) {
			seaCount++
		}

		hourAngleRad := (lonDeg - solar.subLonDeg) * degToRad
		cosZ := clampMaxOne(sinPF*solar.sinDecl+cosPF*solar.cosDecl*math.Cos(hourAngleRad), -1.0)
		sinHA := math.Sin(hourAngleRad)

		switch k {
		case 0:
			cosZ0 = cosZ
		case numArcPoints - 1:
			cosZ20 = cosZ
		}
		if k == midArcIdx {
			midLatDeg = latDeg
			midLonRad = lf
			cosZMid = cosZ
			sinHAMid = sinHA
		}
		if k >= 1 && k <= numArcPoints-2 {
			if k == 1 {
				cosZIntMin = cosZ
				cosZIntMax = cosZ
				sinHAIntMin = sinHA
				sinHAIntMax = sinHA
			} else {
				if cosZ < cosZIntMin {
					cosZIntMin = cosZ
				}
				if cosZ > cosZIntMax {
					cosZIntMax = cosZ
				}
				if sinHA < sinHAIntMin {
					sinHAIntMin = sinHA
				}
				if sinHA > sinHAIntMax {
					sinHAIntMax = sinHA
				}
			}
			if cosZ > 0 {
				daylightCosIntSum += cosZ
			}
			sinHAIntSum += sinHA
		}
	}

	cosZEndMin, cosZEndMax := cosZ0, cosZ20
	if cosZEndMin > cosZEndMax {
		cosZEndMin, cosZEndMax = cosZEndMax, cosZEndMin
	}
	dstRecovery := sw.DstNow - sw.Dst24hMin
	if dstRecovery < 0 {
		dstRecovery = 0
	}

	dArcKM := earthRadiusKM * delta
	out[0] = float32(logFreq)
	out[1] = float32(dArcKM / 20000.0)
	out[2] = float32(midLatDeg / 90.0)
	out[3] = float32(math.Sin(midLonRad))
	out[4] = float32(math.Cos(midLonRad))
	out[5] = float32((latMax - latMin) / 180.0)
	out[6] = float32(maxAbsGM / 90.0)
	out[7] = float32((sumAbsGM / float64(numArcPoints)) / 90.0)
	out[8] = float32(float64(auroralCount) / float64(numArcPoints))
	out[9] = float32(float64(seaCount) / seaPointCount)
	out[10] = float32(cosZEndMin)
	out[11] = float32(cosZEndMax)
	out[12] = float32(cosZMid)
	out[13] = float32(cosZIntMin)
	out[14] = float32(cosZIntMax)
	out[15] = float32(daylightCosIntSum / interiorPointCount)
	out[16] = float32(sinHAMid)
	out[17] = float32(sinHAIntSum / interiorPointCount)
	out[18] = float32(sinHAIntMin)
	out[19] = float32(sinHAIntMax)
	out[20] = float32(solar.sinDoy)
	out[21] = float32(solar.cosDoy)
	out[22] = float32(sw.SFI72hMean)
	out[23] = float32(sw.SFINow - sw.SFI72hMean)
	out[24] = float32(sw.Hp30Lag1)
	out[25] = float32(sw.Hp3012hMax)
	out[26] = float32(sw.Hp3024hMean)
	out[27] = float32(sw.BzSouth3h)
	out[28] = float32(sw.Vsw3h)
	out[29] = float32(sw.Dst24hMin)
	out[30] = float32(dstRecovery)
}

func lookupOceanMaskBit(mask *[OceanMaskBytes]byte, latDeg, lonDeg float64) bool {
	row := int(math.Floor((latDeg + 90.0) * 2.0))
	if row < 0 {
		row = 0
	} else if row >= OceanMaskRows {
		row = OceanMaskRows - 1
	}
	lonWrapped := math.Mod(lonDeg+180.0, 360.0)
	if lonWrapped < 0 {
		lonWrapped += 360.0
	}
	col := int(math.Floor(lonWrapped*2.0)) % OceanMaskCols
	if col < 0 {
		col += OceanMaskCols
	}
	bitIdx := row*OceanMaskCols + col
	return (mask[bitIdx>>3]>>(uint(bitIdx)&7))&1 != 0
}

// wrapPi wraps an angle in radians strictly into [-pi, pi), matching NumPy
// np.mod(rad + pi, 2*pi) - pi.
func wrapPi(rad float64) float64 {
	r := math.Mod(rad+math.Pi, twoPi)
	if r < 0 {
		r += twoPi
	}
	r -= math.Pi
	if r >= math.Pi {
		return -math.Pi
	}
	return r
}

func wrapTwoPi(rad float64) float64 {
	r := math.Mod(rad, twoPi)
	if r < 0 {
		r += twoPi
	}
	if r >= twoPi {
		return 0
	}
	return r
}

func clampMaxOne(v, lo float64) float64 {
	if v < lo {
		return lo
	}
	if v > 1.0 {
		return 1.0
	}
	return v
}
