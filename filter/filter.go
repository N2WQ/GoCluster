package filter

import (
	"strings"

	"dxcluster/spot"
)

// Filter represents a user's spot filtering preferences
type Filter struct {
	Bands     map[string]bool // Enabled bands (e.g., "20m" -> true)
	Modes     map[string]bool // Enabled modes (e.g., "CW" -> true)
	Callsigns []string        // Callsign patterns (e.g., "W1*", "LZ5VV")
	AllBands  bool            // If true, accept all bands
	AllModes  bool            // If true, accept all modes
}

// NewFilter creates a new filter with all bands and modes enabled
func NewFilter() *Filter {
	return &Filter{
		Bands:     make(map[string]bool),
		Modes:     make(map[string]bool),
		Callsigns: make([]string, 0),
		AllBands:  true, // Start with everything enabled
		AllModes:  true,
	}
}

// SetBand enables or disables a specific band
func (f *Filter) SetBand(band string, enabled bool) {
	band = strings.ToUpper(band)
	if enabled {
		f.Bands[band] = true
		f.AllBands = false // Once we set specific bands, we're not accepting all
	} else {
		delete(f.Bands, band)
	}
}

// SetMode enables or disables a specific mode
func (f *Filter) SetMode(mode string, enabled bool) {
	mode = strings.ToUpper(mode)
	if enabled {
		f.Modes[mode] = true
		f.AllModes = false // Once we set specific modes, we're not accepting all
	} else {
		delete(f.Modes, mode)
	}
}

// AddCallsignPattern adds a callsign pattern (e.g., "W1*", "LZ5VV")
func (f *Filter) AddCallsignPattern(pattern string) {
	pattern = strings.ToUpper(pattern)
	f.Callsigns = append(f.Callsigns, pattern)
}

// ClearCallsignPatterns removes all callsign filters
func (f *Filter) ClearCallsignPatterns() {
	f.Callsigns = make([]string, 0)
}

// ResetBands clears band filters and accepts all bands
func (f *Filter) ResetBands() {
	f.Bands = make(map[string]bool)
	f.AllBands = true
}

// ResetModes clears mode filters and accepts all modes
func (f *Filter) ResetModes() {
	f.Modes = make(map[string]bool)
	f.AllModes = true
}

// Reset clears all filters
func (f *Filter) Reset() {
	f.ResetBands()
	f.ResetModes()
	f.ClearCallsignPatterns()
}

// Matches returns true if the spot passes this filter
func (f *Filter) Matches(s *spot.Spot) bool {
	// Check band filter
	if !f.AllBands {
		if !f.Bands[s.Band] {
			return false
		}
	}

	// Check mode filter
	if !f.AllModes {
		if !f.Modes[s.Mode] {
			return false
		}
	}

	// Check callsign patterns (if any are set)
	if len(f.Callsigns) > 0 {
		matched := false
		for _, pattern := range f.Callsigns {
			if matchesCallsignPattern(s.DXCall, pattern) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	return true
}

// matchesCallsignPattern checks if a callsign matches a pattern
// Supports wildcards: W1* matches W1ABC, W1XYZ, etc.
func matchesCallsignPattern(callsign, pattern string) bool {
	callsign = strings.ToUpper(callsign)
	pattern = strings.ToUpper(pattern)

	// Exact match
	if callsign == pattern {
		return true
	}

	// Wildcard at end: W1* matches W1ABC, W1XYZ
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(callsign, prefix)
	}

	// Wildcard at start: *ABC matches W1ABC, K3ABC
	if strings.HasPrefix(pattern, "*") {
		suffix := strings.TrimPrefix(pattern, "*")
		return strings.HasSuffix(callsign, suffix)
	}

	// No match
	return false
}

// String returns a human-readable description of the filter
func (f *Filter) String() string {
	var parts []string

	if f.AllBands {
		parts = append(parts, "Bands: ALL")
	} else {
		bands := make([]string, 0, len(f.Bands))
		for band := range f.Bands {
			bands = append(bands, band)
		}
		if len(bands) > 0 {
			parts = append(parts, "Bands: "+strings.Join(bands, ", "))
		}
	}

	if f.AllModes {
		parts = append(parts, "Modes: ALL")
	} else {
		modes := make([]string, 0, len(f.Modes))
		for mode := range f.Modes {
			modes = append(modes, mode)
		}
		if len(modes) > 0 {
			parts = append(parts, "Modes: "+strings.Join(modes, ", "))
		}
	}

	if len(f.Callsigns) > 0 {
		parts = append(parts, "Callsigns: "+strings.Join(f.Callsigns, ", "))
	}

	if len(parts) == 0 {
		return "No active filters"
	}

	return strings.Join(parts, " | ")
}
