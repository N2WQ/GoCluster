package pathreliability

import (
	"fmt"
	"math"
	"testing"
	"time"
)

type phase1GeoPoint struct {
	lat float64
	lon float64
}

type phase1SolarFeatures struct {
	darkCount     int
	twilightCount int
	daylightCount int
	minElevation  float64
	maxElevation  float64
	darkFraction  float64
}

var phase1SolarFeatureSink phase1SolarFeatures

func BenchmarkNative160SolarPathSamples(b *testing.B) {
	userLat, userLon, ok := GridCenterLatLon("FN31")
	if !ok {
		b.Fatal("invalid user grid")
	}
	dxLat, dxLon, ok := GridCenterLatLon("QF56")
	if !ok {
		b.Fatal("invalid DX grid")
	}
	user := phase1GeoPoint{lat: userLat, lon: userLon}
	dx := phase1GeoPoint{lat: dxLat, lon: dxLon}
	times := phase1BenchmarkTimes()

	for _, samples := range []int{7, 9, 11} {
		b.Run(fmt.Sprintf("predecoded_%d", samples), func(b *testing.B) {
			b.ReportAllocs()
			var features phase1SolarFeatures
			for i := 0; i < b.N; i++ {
				features = phase1PathSolarFeatures(user, dx, times[i&1023], samples)
			}
			phase1SolarFeatureSink = features
		})
		b.Run(fmt.Sprintf("with_grid_decode_%d", samples), func(b *testing.B) {
			b.ReportAllocs()
			var features phase1SolarFeatures
			for i := 0; i < b.N; i++ {
				userLat, userLon, ok := GridCenterLatLon("FN31")
				if !ok {
					b.Fatal("invalid user grid")
				}
				dxLat, dxLon, ok := GridCenterLatLon("QF56")
				if !ok {
					b.Fatal("invalid DX grid")
				}
				features = phase1PathSolarFeatures(
					phase1GeoPoint{lat: userLat, lon: userLon},
					phase1GeoPoint{lat: dxLat, lon: dxLon},
					times[i&1023],
					samples,
				)
			}
			phase1SolarFeatureSink = features
		})
	}
}

func phase1BenchmarkTimes() []time.Time {
	base := time.Date(2026, time.June, 18, 0, 0, 0, 0, time.UTC)
	times := make([]time.Time, 1024)
	for i := range times {
		times[i] = base.Add(time.Duration(i) * time.Minute)
	}
	return times
}

func phase1PathSolarFeatures(user, dx phase1GeoPoint, now time.Time, samples int) phase1SolarFeatures {
	if samples < 2 {
		samples = 2
	}
	features := phase1SolarFeatures{
		minElevation: math.Inf(1),
		maxElevation: math.Inf(-1),
	}
	for i := 0; i < samples; i++ {
		fraction := float64(i) / float64(samples-1)
		point := phase1GreatCirclePoint(user, dx, fraction)
		elevation := phase1SolarElevationDegrees(point.lat, point.lon, now)
		switch {
		case elevation <= -6:
			features.darkCount++
		case elevation <= 0:
			features.twilightCount++
		default:
			features.daylightCount++
		}
		if elevation < features.minElevation {
			features.minElevation = elevation
		}
		if elevation > features.maxElevation {
			features.maxElevation = elevation
		}
	}
	features.darkFraction = float64(features.darkCount) / float64(samples)
	return features
}

func phase1GreatCirclePoint(a, b phase1GeoPoint, fraction float64) phase1GeoPoint {
	lat1 := phase1DegreesToRadians(a.lat)
	lon1 := phase1DegreesToRadians(a.lon)
	lat2 := phase1DegreesToRadians(b.lat)
	lon2 := phase1DegreesToRadians(b.lon)
	dLat := lat2 - lat1
	dLon := lon2 - lon1
	sinLat := math.Sin(dLat / 2)
	sinLon := math.Sin(dLon / 2)
	h := sinLat*sinLat + math.Cos(lat1)*math.Cos(lat2)*sinLon*sinLon
	if h > 1 {
		h = 1
	}
	d := 2 * math.Atan2(math.Sqrt(h), math.Sqrt(1-h))
	if d <= 1e-12 {
		return a
	}
	sinD := math.Sin(d)
	scaleA := math.Sin((1-fraction)*d) / sinD
	scaleB := math.Sin(fraction*d) / sinD
	x := scaleA*math.Cos(lat1)*math.Cos(lon1) + scaleB*math.Cos(lat2)*math.Cos(lon2)
	y := scaleA*math.Cos(lat1)*math.Sin(lon1) + scaleB*math.Cos(lat2)*math.Sin(lon2)
	z := scaleA*math.Sin(lat1) + scaleB*math.Sin(lat2)
	lat := math.Atan2(z, math.Sqrt(x*x+y*y))
	lon := math.Atan2(y, x)
	return phase1GeoPoint{lat: phase1RadiansToDegrees(lat), lon: phase1RadiansToDegrees(lon)}
}

func phase1SolarElevationDegrees(lat, lon float64, now time.Time) float64 {
	now = now.UTC()
	hour := float64(now.Hour()) + float64(now.Minute())/60 + float64(now.Second())/3600 + float64(now.Nanosecond())/3.6e12
	gamma := 2 * math.Pi / 365 * (float64(now.YearDay()-1) + (hour-12)/24)
	eqTime := 229.18 * (0.000075 +
		0.001868*math.Cos(gamma) -
		0.032077*math.Sin(gamma) -
		0.014615*math.Cos(2*gamma) -
		0.040849*math.Sin(2*gamma))
	declination := 0.006918 -
		0.399912*math.Cos(gamma) +
		0.070257*math.Sin(gamma) -
		0.006758*math.Cos(2*gamma) +
		0.000907*math.Sin(2*gamma) -
		0.002697*math.Cos(3*gamma) +
		0.00148*math.Sin(3*gamma)
	trueSolarMinutes := math.Mod(hour*60+eqTime+4*lon, 1440)
	if trueSolarMinutes < 0 {
		trueSolarMinutes += 1440
	}
	hourAngle := trueSolarMinutes/4 - 180
	if hourAngle < -180 {
		hourAngle += 360
	}
	latRad := phase1DegreesToRadians(lat)
	hourAngleRad := phase1DegreesToRadians(hourAngle)
	cosZenith := math.Sin(latRad)*math.Sin(declination) +
		math.Cos(latRad)*math.Cos(declination)*math.Cos(hourAngleRad)
	if cosZenith > 1 {
		cosZenith = 1
	} else if cosZenith < -1 {
		cosZenith = -1
	}
	return 90 - phase1RadiansToDegrees(math.Acos(cosZenith))
}

func phase1DegreesToRadians(degrees float64) float64 {
	return degrees * math.Pi / 180
}

func phase1RadiansToDegrees(radians float64) float64 {
	return radians * 180 / math.Pi
}
