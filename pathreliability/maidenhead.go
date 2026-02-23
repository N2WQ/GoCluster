package pathreliability

import "dxcluster/strutil"

const (
	fieldLonSize    = 20.0
	fieldLatSize    = 10.0
	squareLonSize   = 2.0
	squareLatSize   = 1.0
	subLonSize      = squareLonSize / 24.0
	subLatSize      = squareLatSize / 24.0
	subCenterLon    = subLonSize / 2.0
	subCenterLat    = subLatSize / 2.0
	squareCenterLon = squareLonSize / 2.0
	squareCenterLat = squareLatSize / 2.0
)

// GridCenterLatLon returns the center lat/lon for a 4-6 char Maidenhead grid.
func GridCenterLatLon(grid string) (lat float64, lon float64, ok bool) {
	g := strutil.NormalizeUpper(grid)
	if len(g) < 4 {
		return 0, 0, false
	}
	if len(g) == 5 {
		return 0, 0, false
	}
	a, b := g[0], g[1]
	if a < 'A' || a > 'R' || b < 'A' || b > 'R' {
		return 0, 0, false
	}
	fieldLon := float64(a-'A') * fieldLonSize
	fieldLat := float64(b-'A') * fieldLatSize
	d0, d1 := g[2], g[3]
	if d0 < '0' || d0 > '9' || d1 < '0' || d1 > '9' {
		return 0, 0, false
	}
	squareLon := float64(d0-'0') * squareLonSize
	squareLat := float64(d1-'0') * squareLatSize
	lon = -180.0 + fieldLon + squareLon
	lat = -90.0 + fieldLat + squareLat
	if len(g) >= 6 {
		s0, s1 := g[4], g[5]
		if s0 < 'A' || s0 > 'X' || s1 < 'A' || s1 > 'X' {
			return 0, 0, false
		}
		subLon := float64(s0-'A') * subLonSize
		subLat := float64(s1-'A') * subLatSize
		lon += subLon + subCenterLon
		lat += subLat + subCenterLat
		return lat, lon, true
	}
	lon += squareCenterLon
	lat += squareCenterLat
	return lat, lon, true
}

// gridCenterLatLon is retained for internal compatibility.
func gridCenterLatLon(grid string) (lat float64, lon float64, ok bool) {
	return GridCenterLatLon(grid)
}
