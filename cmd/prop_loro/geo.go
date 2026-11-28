// File role: Exact spherical great-circle distance, IGRF-14 dipole polar path
// classification, NA<->JA corridor detection, and path-midpoint solar day/night
// classification using internal/solarpath.
package main

import (
	"math"
	"time"

	"dxcluster/internal/solarpath"
)

const (
	geoEarthRadiusKM      = 6371.0
	geoDegToRad           = math.Pi / 180.0
	geoRadToDeg           = 180.0 / math.Pi
	geoDipoleNorthLatDeg  = 80.8
	geoDipoleNorthLonDeg  = -72.8
	geoPolarGeomagThresh  = 60.0
	geoLongHaulKM         = 6000.0
	geoArcSamples         = 21
	geoCivilTwilightCosZ  = -0.10452846326765347 // -sin(6 deg)
)

var (
	geoSinPhiP = math.Sin(geoDipoleNorthLatDeg * geoDegToRad)
	geoCosPhiP = math.Cos(geoDipoleNorthLatDeg * geoDegToRad)
	geoLonP    = geoDipoleNorthLonDeg * geoDegToRad
)

type pathSolarRegime string

const (
	solarDay      pathSolarRegime = "day"
	solarTwilight pathSolarRegime = "twilight"
	solarNight    pathSolarRegime = "night"
)

type pathFeatures struct {
	distKM       float64
	maxAbsGeomag float64
	midLatDeg    float64
	midLonDeg    float64
	geomagBin    string
	isLongHaul   bool
	isPolar      bool
	isNAJA       bool
	isNAEU       bool
	isNorthNAEEU bool
	isMidNAEEU   bool
	isNASACA     bool
}

var highBandOrder = []string{"15m", "12m", "10m"}

func computePathFeatures(lat1Deg, lon1Deg, lat2Deg, lon2Deg float64) pathFeatures {
	p1 := lat1Deg * geoDegToRad
	l1 := lon1Deg * geoDegToRad
	p2 := lat2Deg * geoDegToRad
	l2 := lon2Deg * geoDegToRad

	dp := p2 - p1
	dl := l2 - l1
	sinDP2 := math.Sin(dp * 0.5)
	sinDL2 := math.Sin(dl * 0.5)
	h := sinDP2*sinDP2 + math.Cos(p1)*math.Cos(p2)*sinDL2*sinDL2
	if h < 0 {
		h = 0
	} else if h > 1 {
		h = 1
	}
	delta := 2.0 * math.Atan2(math.Sqrt(h), math.Sqrt(1.0-h))
	distKM := geoEarthRadiusKM * delta

	az := math.Atan2(
		math.Sin(dl)*math.Cos(p2),
		math.Cos(p1)*math.Sin(p2)-math.Sin(p1)*math.Cos(p2)*math.Cos(dl),
	)
	if delta < 1e-7 || delta > math.Pi-1e-4 {
		az = 0
	}

	sinP1, cosP1 := math.Sin(p1), math.Cos(p1)
	sinAz, cosAz := math.Sin(az), math.Cos(az)
	maxAbsGM := 0.0
	midLatDeg, midLonDeg := lat1Deg, lon1Deg

	for k := range geoArcSamples {
		frac := float64(k) / float64(geoArcSamples-1)
		theta := delta * frac
		sinTh, cosTh := math.Sin(theta), math.Cos(theta)
		sinPF := sinP1*cosTh + cosP1*sinTh*cosAz
		if sinPF < -1 {
			sinPF = -1
		} else if sinPF > 1 {
			sinPF = 1
		}
		pf := math.Asin(sinPF)
		cosPF := math.Cos(pf)
		lf := l1 + math.Atan2(sinAz*sinTh*cosP1, cosTh-sinP1*sinPF)
		if k == geoArcSamples/2 {
			midLatDeg = pf * geoRadToDeg
			midLonDeg = wrapLonDeg(lf * geoRadToDeg)
		}
		sinGM := sinPF*geoSinPhiP + cosPF*geoCosPhiP*math.Cos(lf-geoLonP)
		if sinGM < -1 {
			sinGM = -1
		} else if sinGM > 1 {
			sinGM = 1
		}
		absGM := math.Abs(math.Asin(sinGM) * geoRadToDeg)
		if absGM > maxAbsGM {
			maxAbsGM = absGM
		}
	}

	return pathFeatures{
		distKM:       distKM,
		maxAbsGeomag: maxAbsGM,
		midLatDeg:    midLatDeg,
		midLonDeg:    midLonDeg,
		geomagBin:    geomagCliffBin(maxAbsGM),
		isLongHaul:   distKM >= geoLongHaulKM,
		isPolar:      maxAbsGM >= geoPolarGeomagThresh,
		isNAJA:       isNAJACorridor(lat1Deg, lon1Deg, lat2Deg, lon2Deg),
		isNAEU:       isNAEUCorridor(lat1Deg, lon1Deg, lat2Deg, lon2Deg),
		isNorthNAEEU: isNorthNAEEUCorridor(lat1Deg, lon1Deg, lat2Deg, lon2Deg),
		isMidNAEEU:   isMidNAEEUCorridor(lat1Deg, lon1Deg, lat2Deg, lon2Deg),
		isNASACA:     isNASACACorridor(lat1Deg, lon1Deg, lat2Deg, lon2Deg),
	}
}

func geomagCliffBin(maxAbsGM float64) string {
	switch {
	case maxAbsGM < 50.0:
		return "geomag_lt_50"
	case maxAbsGM < 55.0:
		return "geomag_50_55"
	case maxAbsGM < 60.0:
		return "geomag_55_60"
	case maxAbsGM < 65.0:
		return "geomag_60_65"
	case maxAbsGM < 70.0:
		return "geomag_65_70"
	default:
		return "geomag_ge_70"
	}
}

func isHighBand(band string) bool {
	return band == "15m" || band == "12m" || band == "10m"
}

func classifyMidpointSolar(midLatDeg, midLonDeg float64, at time.Time) pathSolarRegime {
	subLatDeg, subLonDeg := solarpath.SubsolarPoint(at.UTC())
	p := midLatDeg * geoDegToRad
	decl := subLatDeg * geoDegToRad
	ha := (midLonDeg - subLonDeg) * geoDegToRad
	cosZ := math.Sin(p)*math.Sin(decl) + math.Cos(p)*math.Cos(decl)*math.Cos(ha)
	switch {
	case cosZ > 0:
		return solarDay
	case cosZ <= geoCivilTwilightCosZ:
		return solarNight
	default:
		return solarTwilight
	}
}

func isNAJACorridor(lat1, lon1, lat2, lon2 float64) bool {
	return (inNorthAmericaBox(lat1, lon1) && inJapanBox(lat2, lon2)) ||
		(inJapanBox(lat1, lon1) && inNorthAmericaBox(lat2, lon2))
}

func inNorthAmericaBox(lat, lon float64) bool {
	return lat >= 24.0 && lat <= 62.0 && lon >= -145.0 && lon <= -52.0
}

func inJapanBox(lat, lon float64) bool {
	return lat >= 30.0 && lat <= 46.0 && lon >= 128.0 && lon <= 146.5
}

func isNAEUCorridor(lat1, lon1, lat2, lon2 float64) bool {
	return (inNorthAmericaBox(lat1, lon1) && inEuropeBox(lat2, lon2)) ||
		(inEuropeBox(lat1, lon1) && inNorthAmericaBox(lat2, lon2))
}

func isNorthNAEEUCorridor(lat1, lon1, lat2, lon2 float64) bool {
	return (inNorthNABox(lat1, lon1) && inEasternEuropeBox(lat2, lon2)) ||
		(inEasternEuropeBox(lat1, lon1) && inNorthNABox(lat2, lon2))
}

func isMidNAEEUCorridor(lat1, lon1, lat2, lon2 float64) bool {
	return (inMidNABox(lat1, lon1) && inEasternEuropeBox(lat2, lon2)) ||
		(inEasternEuropeBox(lat1, lon1) && inMidNABox(lat2, lon2))
}

func isNASACACorridor(lat1, lon1, lat2, lon2 float64) bool {
	return (inNorthAmericaBox(lat1, lon1) && inSACABox(lat2, lon2)) ||
		(inSACABox(lat1, lon1) && inNorthAmericaBox(lat2, lon2))
}

func inEuropeBox(lat, lon float64) bool {
	return lat >= 35.0 && lat <= 71.0 && lon >= -11.0 && lon <= 40.0
}

func inEasternEuropeBox(lat, lon float64) bool {
	return lat >= 39.0 && lat <= 55.0 && lon >= 18.0 && lon <= 40.0
}

// inNorthNABox covers VE3 / northeastern North America (lat >= 42N), strictly
// disjoint from inMidNABox (W3 / Mid-Atlantic, 34N <= lat < 42N).
func inNorthNABox(lat, lon float64) bool {
	return lat >= 42.0 && lat <= 56.0 && lon >= -92.0 && lon <= -58.0
}

func inMidNABox(lat, lon float64) bool {
	return lat >= 34.0 && lat < 42.0 && lon >= -84.0 && lon <= -70.0
}

// inSACABox covers Caribbean, Central, and South America (lat < 24N), strictly
// disjoint from inNorthAmericaBox (lat >= 24N).
func inSACABox(lat, lon float64) bool {
	return lat >= -56.0 && lat < 24.0 && lon >= -118.0 && lon <= -34.0
}

func wrapLonDeg(lon float64) float64 {
	lon = math.Mod(lon+180.0, 360.0)
	if lon < 0 {
		lon += 360.0
	}
	return lon - 180.0
}
