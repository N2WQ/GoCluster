package spot

import "strings"

// BandSegment describes one contiguous frequency allocation in kHz.
type BandSegment struct {
	Min float64 // minimum frequency in kHz
	Max float64 // maximum frequency in kHz
}

// BandInfo describes an amateur radio band by name and frequency range in kHz.
type BandInfo struct {
	Name     string        // canonical band name (e.g., "20m", "70cm")
	Min      float64       // minimum primary segment frequency in kHz
	Max      float64       // maximum primary segment frequency in kHz
	Segments []BandSegment // optional additional contiguous segments for the same band
}

var bandTable = []BandInfo{
	{Name: "2200m", Min: 135.7, Max: 137.8},
	{Name: "630m", Min: 472, Max: 479},
	{Name: "160m", Min: 1800, Max: 2000},
	{Name: "80m", Min: 3500, Max: 4000},
	{Name: "60m", Min: 5260, Max: 5405},
	{Name: "40m", Min: 7000, Max: 7300},
	{Name: "30m", Min: 10100, Max: 10150},
	{Name: "20m", Min: 14000, Max: 14350},
	{Name: "17m", Min: 18068, Max: 18168},
	{Name: "15m", Min: 21000, Max: 21450},
	{Name: "12m", Min: 24890, Max: 24990},
	{Name: "10m", Min: 28000, Max: 29700},
	{Name: "6m", Min: 50000, Max: 54000},
	{Name: "4m", Min: 70000, Max: 70650},
	{Name: "2m", Min: 144000, Max: 148000},
	{Name: "1.25m", Min: 220000, Max: 225000},
	{Name: "70cm", Min: 420000, Max: 450000},
	{Name: "33cm", Min: 902000, Max: 928000},
	{Name: "23cm", Min: 1240000, Max: 1300000},
	{Name: "13cm", Min: 2300000, Max: 2310000, Segments: []BandSegment{{Min: 2390000, Max: 2450000}}},
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

// Contains reports whether freq falls inside any segment for this band.
func (b BandInfo) Contains(freq float64) bool {
	if freq >= b.Min && freq <= b.Max {
		return true
	}
	for _, segment := range b.Segments {
		if freq >= segment.Min && freq <= segment.Max {
			return true
		}
	}
	return false
}

// NormalizeBand normalizes a band label into canonical lookup form.
// Key aspects: Lowercases, strips whitespace, normalizes units, and appends "m" for bare numbers.
// Upstream: Callers parsing band labels or user input.
// Downstream: strings.ReplaceAll and NormalizeBand lookup table.
// NormalizeBand returns the canonical lowercase band identifier for the given label.
// It removes whitespace, converts meter/centimeter words to units, and appends "m" when
// the value looks like a bare number. The result is suitable for map lookups.
func NormalizeBand(label string) string {
	cleaned := strings.ToLower(strings.TrimSpace(label))
	if cleaned == "" {
		return ""
	}

	//nolint:misspell // Accept UK/US unit spellings in user-provided labels.
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

// IsValidBand validates that a band label maps to a known band.
// Key aspects: Normalizes label and checks lookup table.
// Upstream: Band validation paths.
// Downstream: NormalizeBand and bandLookup.
// IsValidBand returns true if the provided label corresponds to a known band.
func IsValidBand(label string) bool {
	normalized := NormalizeBand(label)
	if normalized == "" {
		return false
	}
	_, ok := bandLookup[normalized]
	return ok
}

// BandContainsFrequency uses the common physical-band catalog and inclusive
// segment endpoints. Meta names are deliberately not physical band identifiers.
// The lookup reads immutable startup data and allocates no retained state.
func BandContainsFrequency(name string, frequencyKHz float64) bool {
	band, ok := bandLookup[NormalizeBand(name)]
	return ok && band.Contains(frequencyKHz)
}

// BandFrequencySegments returns an owned physical-band range snapshot in kHz.
// Separate segments preserve allocation gaps such as the gap within 13cm.
func BandFrequencySegments(name string) ([]BandSegment, bool) {
	band, ok := bandLookup[NormalizeBand(name)]
	if !ok {
		return nil, false
	}
	segments := make([]BandSegment, 1, 1+len(band.Segments))
	segments[0] = BandSegment{Min: band.Min, Max: band.Max}
	return append(segments, band.Segments...), true
}

// SupportedBandNames returns canonical names for all configured bands.
// Key aspects: Preserves the bandTable order.
// Upstream: UI/config validation paths.
// Downstream: bandTable iteration.
// SupportedBandNames returns the canonical names of all tracked bands.
func SupportedBandNames() []string {
	names := make([]string, len(bandTable))
	for i, entry := range bandTable {
		names[i] = entry.Name
	}
	return names
}

// FrequencyBounds returns min/max frequencies across all bands.
// Key aspects: Scans all primary and secondary band segments.
// Upstream: Frequency sanity checks and UI displays.
// Downstream: bandTable access.
// FrequencyBounds returns the minimum and maximum frequencies covered by the band table.
func FrequencyBounds() (min, max float64) {
	if len(bandTable) == 0 {
		return 0, 0
	}
	min = bandTable[0].Min
	max = bandTable[0].Max
	for _, entry := range bandTable {
		if entry.Min < min {
			min = entry.Min
		}
		if entry.Max > max {
			max = entry.Max
		}
		for _, segment := range entry.Segments {
			if segment.Min < min {
				min = segment.Min
			}
			if segment.Max > max {
				max = segment.Max
			}
		}
	}
	return
}
