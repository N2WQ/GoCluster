package cty

import "math"

// Grid4FromPrefixInfo locates a CTY record's representative coordinate, not a
// verified station position. Country Files stores north-positive latitude and
// west-positive longitude; only this boundary changes longitude to east-positive.
// Raw PrefixInfo values and the generic geographic converter remain unchanged.
func Grid4FromPrefixInfo(info *PrefixInfo) (string, bool) {
	if info == nil {
		return "", false
	}
	return Grid4FromLatLon(info.Latitude, -info.Longitude)
}

// Grid4FromLatLon returns the 4-character Maidenhead grid for a lat/lon pair.
// It returns false when coordinates are out of range or non-finite.
func Grid4FromLatLon(lat, lon float64) (string, bool) {
	if math.IsNaN(lat) || math.IsNaN(lon) || math.IsInf(lat, 0) || math.IsInf(lon, 0) {
		return "", false
	}
	if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		return "", false
	}
	if lat == 90 {
		lat = 89.999999
	}
	if lon == 180 {
		lon = 179.999999
	}
	adjLon := lon + 180
	adjLat := lat + 90
	fieldLon := int(adjLon / 20)
	fieldLat := int(adjLat / 10)
	if fieldLon < 0 || fieldLon >= 18 || fieldLat < 0 || fieldLat >= 18 {
		return "", false
	}
	squareLon := int((adjLon - float64(fieldLon)*20) / 2)
	squareLat := int((adjLat - float64(fieldLat)*10) / 1)
	if squareLon < 0 || squareLon >= 10 || squareLat < 0 || squareLat >= 10 {
		return "", false
	}
	return string([]byte{
		byte('A' + fieldLon),
		byte('A' + fieldLat),
		byte('0' + squareLon),
		byte('0' + squareLat),
	}), true
}
