package spot

import "strings"

// BandInfo describes an amateur radio band by name and frequency range in kHz.
type BandInfo struct {
	Name string  // canonical band name (e.g., "20m", "70cm")
	Min  float64 // minimum frequency in kHz
	Max  float64 // maximum frequency in kHz
}

var bandTable = []BandInfo{
	{Name: "2200m", Min: 135.7, Max: 137.8},
	{Name: "630m", Min: 472, Max: 479},
	{Name: "160m", Min: 1800, Max: 2000},
	{Name: "80m", Min: 3500, Max: 4000},
	{Name: "60m", Min: 5330, Max: 5405},
	{Name: "40m", Min: 7000, Max: 7300},
	{Name: "30m", Min: 10100, Max: 10150},
	{Name: "20m", Min: 14000, Max: 14350},
	{Name: "17m", Min: 18068, Max: 18168},
	{Name: "15m", Min: 21000, Max: 21450},
	{Name: "12m", Min: 24890, Max: 24990},
	{Name: "10m", Min: 28000, Max: 29700},
	{Name: "6m", Min: 50000, Max: 54000},
	{Name: "2m", Min: 144000, Max: 148000},
	{Name: "1.25m", Min: 222000, Max: 225000},
	{Name: "70cm", Min: 420000, Max: 450000},
	{Name: "33cm", Min: 902000, Max: 928000},
	{Name: "23cm", Min: 1240000, Max: 1300000},
	{Name: "13cm", Min: 2300000, Max: 2310000},
}

var bandLookup = func() map[string]BandInfo {
	m := make(map[string]BandInfo, len(bandTable))
	for _, entry := range bandTable {
		normalized := NormalizeBand(entry.Name)
		if normalized == "" {
			continue
		}
		m[normalized] = entry
	}
	return m
}()

// NormalizeBand returns the canonical lowercase band identifier for the given label.
// It removes whitespace, converts meter/centimeter words to units, and appends "m" when
// the value looks like a bare number. The result is suitable for map lookups.
func NormalizeBand(label string) string {
	cleaned := strings.ToLower(strings.TrimSpace(label))
	if cleaned == "" {
		return ""
	}

	replacementPairs := []struct{ old, new string }{
		{"meters", "m"},
		{"meter", "m"},
		{"metres", "m"},
		{"metre", "m"},
		{"centimeters", "cm"},
		{"centimeter", "cm"},
		{"centimetres", "cm"},
		{"centimetre", "cm"},
	}
	for _, pair := range replacementPairs {
		cleaned = strings.ReplaceAll(cleaned, pair.old, pair.new)
	}

	cleaned = strings.ReplaceAll(cleaned, " ", "")
	if cleaned == "" {
		return ""
	}

	last := cleaned[len(cleaned)-1]
	if last >= '0' && last <= '9' {
		cleaned += "m"
	}

	return cleaned
}

// IsValidBand returns true if the provided label corresponds to a known band.
func IsValidBand(label string) bool {
	normalized := NormalizeBand(label)
	if normalized == "" {
		return false
	}
	_, ok := bandLookup[normalized]
	return ok
}

// SupportedBandNames returns the canonical names of all tracked bands.
func SupportedBandNames() []string {
	names := make([]string, len(bandTable))
	for i, entry := range bandTable {
		names[i] = entry.Name
	}
	return names
}

// FrequencyBounds returns the minimum and maximum frequencies covered by the band table.
func FrequencyBounds() (min, max float64) {
	if len(bandTable) == 0 {
		return 0, 0
	}
	min = bandTable[0].Min
	max = bandTable[len(bandTable)-1].Max
	return
}
