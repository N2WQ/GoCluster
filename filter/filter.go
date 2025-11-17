// Package filter implements per-client spot filtering for the DX Cluster Server.
//
// Filters allow users to customize which spots they receive based on:
//   - Band (e.g., 20m, 40m, 160m)
//   - Mode (e.g., CW, SSB, FT8, RTTY)
//   - Callsign patterns (e.g., W1*, LZ5VV, *ABC)
//
// Filter Logic:
//   - Multiple filters use AND logic (all must match)
//   - Default state: All bands and modes enabled (no filtering)
//   - Once a specific filter is set, only matching spots pass
//   - Callsign patterns support wildcards (* at start or end)
//
// Each telnet client has its own Filter instance, allowing personalized spot feeds.
// This reduces bandwidth for clients who only want specific spots (e.g., 20m CW only).
package filter

import (
	"strings"

	"dxcluster/spot"
)

// Filter represents a user's spot filtering preferences.
//
// The filter maintains three types of criteria that can be combined:
//  1. Band filters: Which amateur radio bands to accept (20m, 40m, etc.)
//  2. Mode filters: Which operating modes to accept (CW, SSB, FT8, etc.)
//  3. Callsign patterns: Which callsigns to accept (W1*, LZ5VV, etc.)
//
// Default Behavior:
//   - AllBands=true, AllModes=true: Accept everything (no filtering)
//   - Setting specific band/mode: Disables "all" flag, only accepts specified values
//   - Callsign patterns: Only applied if non-empty (no impact on band/mode filters)
//
// Filter Matching:
//   - All active filters must match (AND logic)
//   - Example: Band=20m + Mode=CW → Only 20m CW spots pass
//   - Example: Band=20m + Call=W1* → Only 20m spots from W1 callsigns pass
//
// Thread Safety:
//   - Each client has their own Filter instance (no sharing)
//   - No internal locking needed (single-threaded per client)
type Filter struct {
	Bands     map[string]bool // Enabled bands (e.g., "20m" → true, "40m" → true)
	Modes     map[string]bool // Enabled modes (e.g., "CW" → true, "FT8" → true)
	Callsigns []string        // Callsign patterns (e.g., ["W1*", "LZ5VV"])
	AllBands  bool            // If true, accept all bands (Bands map ignored)
	AllModes  bool            // If true, accept all modes (Modes map ignored)
}

// NewFilter creates a new filter with all bands and modes enabled (no filtering).
//
// Returns:
//   - *Filter: Initialized filter accepting all spots
//
// The default state allows all spots through until the user sets specific filters.
//
// Example:
//   filter := filter.NewFilter()
//   // All spots pass through initially
func NewFilter() *Filter {
	return &Filter{
		Bands:     make(map[string]bool),
		Modes:     make(map[string]bool),
		Callsigns: make([]string, 0),
		AllBands:  true, // Start with everything enabled (no filtering)
		AllModes:  true,
	}
}

// normalizeBand normalizes band names for consistent matching.
//
// Normalization rules:
//   - Convert to lowercase
//   - Trim whitespace
//   - Ensure 'm' suffix (20 → 20m, 20M → 20m)
//
// Parameters:
//   - band: Band string in any format (e.g., "20M", "20m", "20", " 40M ")
//
// Returns:
//   - string: Normalized band (e.g., "20m")
//
// Examples:
//   normalizeBand("20M")   → "20m"
//   normalizeBand("40")    → "40m"
//   normalizeBand(" 20m ") → "20m"
func normalizeBand(band string) string {
	band = strings.ToLower(strings.TrimSpace(band))
	// If it doesn't end with 'm', add it
	if !strings.HasSuffix(band, "m") {
		band = band + "m"
	}
	return band
}

// SetBand enables or disables filtering for a specific band.
//
// Parameters:
//   - band: Band to filter (e.g., "20M", "40m")
//   - enabled: true to accept this band, false to reject
//
// Behavior:
//   - When enabling first band: Disables AllBands flag (switches to whitelist mode)
//   - Multiple bands can be enabled (OR logic within bands)
//   - Disabling a band removes it from the filter
//
// Examples:
//   filter.SetBand("20M", true)  // Only accept 20m (disables all other bands)
//   filter.SetBand("40m", true)  // Now accept 20m OR 40m
//   filter.SetBand("20M", false) // Only accept 40m now
func (f *Filter) SetBand(band string, enabled bool) {
	band = normalizeBand(band)
	if enabled {
		f.Bands[band] = true
		f.AllBands = false // Once we set specific bands, we're not accepting all
	} else {
		delete(f.Bands, band)
	}
}

// SetMode enables or disables filtering for a specific mode.
//
// Parameters:
//   - mode: Mode to filter (e.g., "CW", "FT8", "SSB")
//   - enabled: true to accept this mode, false to reject
//
// Behavior:
//   - When enabling first mode: Disables AllModes flag (switches to whitelist mode)
//   - Multiple modes can be enabled (OR logic within modes)
//   - Disabling a mode removes it from the filter
//
// Examples:
//   filter.SetMode("CW", true)   // Only accept CW (disables all other modes)
//   filter.SetMode("FT8", true)  // Now accept CW OR FT8
//   filter.SetMode("CW", false)  // Only accept FT8 now
func (f *Filter) SetMode(mode string, enabled bool) {
	mode = strings.ToUpper(mode)
	if enabled {
		f.Modes[mode] = true
		f.AllModes = false // Once we set specific modes, we're not accepting all
	} else {
		delete(f.Modes, mode)
	}
}

// AddCallsignPattern adds a callsign pattern to the filter.
//
// Parameters:
//   - pattern: Callsign pattern with optional wildcards (e.g., "W1*", "LZ5VV", "*ABC")
//
// Pattern matching:
//   - Exact match: "LZ5VV" matches only LZ5VV
//   - Prefix wildcard: "W1*" matches W1ABC, W1XYZ, etc.
//   - Suffix wildcard: "*ABC" matches W1ABC, K3ABC, etc.
//
// Behavior:
//   - Multiple patterns can be added (OR logic)
//   - Patterns are case-insensitive (normalized to uppercase)
//
// Examples:
//   filter.AddCallsignPattern("W1*")    // Accept all W1 callsigns
//   filter.AddCallsignPattern("LZ5VV")  // Also accept LZ5VV
//   // Now accepts: W1ABC, W1XYZ, LZ5VV, etc.
func (f *Filter) AddCallsignPattern(pattern string) {
	pattern = strings.ToUpper(pattern)
	f.Callsigns = append(f.Callsigns, pattern)
}

// ClearCallsignPatterns removes all callsign filters.
//
// After calling this, callsign filtering is disabled and all callsigns are accepted
// (subject to band/mode filters).
//
// Example:
//   filter.ClearCallsignPatterns()
//   // All callsigns now pass through
func (f *Filter) ClearCallsignPatterns() {
	f.Callsigns = make([]string, 0)
}

// ResetBands clears all band filters and accepts all bands.
//
// Behavior:
//   - Clears the Bands map
//   - Sets AllBands = true
//   - Spots from any band will pass (subject to mode/callsign filters)
//
// Example:
//   filter.ResetBands()
//   // All bands now pass through
func (f *Filter) ResetBands() {
	f.Bands = make(map[string]bool)
	f.AllBands = true
}

// ResetModes clears all mode filters and accepts all modes.
//
// Behavior:
//   - Clears the Modes map
//   - Sets AllModes = true
//   - Spots from any mode will pass (subject to band/callsign filters)
//
// Example:
//   filter.ResetModes()
//   // All modes now pass through
func (f *Filter) ResetModes() {
	f.Modes = make(map[string]bool)
	f.AllModes = true
}

// Reset clears all filters and returns to default state (accept everything).
//
// Equivalent to calling:
//   - ResetBands()
//   - ResetModes()
//   - ClearCallsignPatterns()
//
// After reset, all spots pass through the filter.
//
// Example:
//   filter.Reset()
//   // Filter is now in default state (all spots pass)
func (f *Filter) Reset() {
	f.ResetBands()
	f.ResetModes()
	f.ClearCallsignPatterns()
}

// Matches returns true if the spot passes all active filters.
//
// Parameters:
//   - s: Spot to check against filters
//
// Returns:
//   - bool: true if spot passes all filters, false if any filter rejects it
//
// Filter Logic (AND):
//  1. Band filter: If AllBands=false, spot.Band must be in Bands map
//  2. Mode filter: If AllModes=false, spot.Mode must be in Modes map
//  3. Callsign filter: If patterns exist, spot.DXCall must match at least one
//
// Examples:
//   filter.SetBand("20m", true)
//   filter.SetMode("CW", true)
//   filter.Matches(spot_20m_CW)   → true
//   filter.Matches(spot_40m_CW)   → false (wrong band)
//   filter.Matches(spot_20m_SSB)  → false (wrong mode)
func (f *Filter) Matches(s *spot.Spot) bool {
	// Check band filter
	if !f.AllBands {
		spotBand := normalizeBand(s.Band)
		if !f.Bands[spotBand] {
			return false // Band not in enabled list
		}
	}

	// Check mode filter
	if !f.AllModes {
		if !f.Modes[s.Mode] {
			return false // Mode not in enabled list
		}
	}

	// Check callsign patterns (if any are set)
	if len(f.Callsigns) > 0 {
		matched := false
		for _, pattern := range f.Callsigns {
			if matchesCallsignPattern(s.DXCall, pattern) {
				matched = true
				break // At least one pattern matched (OR logic)
			}
		}
		if !matched {
			return false // No patterns matched
		}
	}

	return true // Passed all filters
}

// matchesCallsignPattern checks if a callsign matches a pattern with wildcards.
//
// Parameters:
//   - callsign: Actual callsign to match (e.g., "W1ABC")
//   - pattern: Pattern with optional wildcards (e.g., "W1*", "*ABC", "LZ5VV")
//
// Returns:
//   - bool: true if callsign matches the pattern
//
// Matching Rules:
//   - Exact match: "LZ5VV" matches "LZ5VV"
//   - Wildcard at end: "W1*" matches "W1ABC", "W1XYZ", etc.
//   - Wildcard at start: "*ABC" matches "W1ABC", "K3ABC", etc.
//   - Case-insensitive: "w1abc" matches "W1ABC"
//
// Examples:
//   matchesCallsignPattern("W1ABC", "W1*")   → true
//   matchesCallsignPattern("W1ABC", "*ABC")  → true
//   matchesCallsignPattern("W1ABC", "LZ5VV") → false
func matchesCallsignPattern(callsign, pattern string) bool {
	// Normalize both to uppercase for case-insensitive matching
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

// String returns a human-readable description of the active filters.
//
// Returns:
//   - string: Description of filter state (e.g., "Bands: 20m, 40m | Modes: CW, FT8")
//
// Output format depends on filter state:
//   - Default (no filters): "No active filters"
//   - Band filter: "Bands: 20m, 40m" or "Bands: ALL"
//   - Mode filter: "Modes: CW, FT8" or "Modes: ALL"
//   - Callsign filter: "Callsigns: W1*, LZ5VV"
//   - Empty filter: "Bands: NONE (no spots will pass)"
//
// This is used for the SHOW/FILTER command to display current filter state.
//
// Examples:
//   filter.String() → "No active filters"
//   (after SetBand("20m", true))
//   filter.String() → "Bands: 20m | Modes: ALL"
//   (after SetMode("CW", true))
//   filter.String() → "Bands: 20m | Modes: CW"
func (f *Filter) String() string {
	var parts []string

	// Describe band filter
	if f.AllBands {
		parts = append(parts, "Bands: ALL")
	} else {
		bands := make([]string, 0, len(f.Bands))
		for band := range f.Bands {
			bands = append(bands, band)
		}
		if len(bands) > 0 {
			parts = append(parts, "Bands: "+strings.Join(bands, ", "))
		} else {
			parts = append(parts, "Bands: NONE (no spots will pass)")
		}
	}

	// Describe mode filter
	if f.AllModes {
		parts = append(parts, "Modes: ALL")
	} else {
		modes := make([]string, 0, len(f.Modes))
		for mode := range f.Modes {
			modes = append(modes, mode)
		}
		if len(modes) > 0 {
			parts = append(parts, "Modes: "+strings.Join(modes, ", "))
		} else {
			parts = append(parts, "Modes: NONE (no spots will pass)")
		}
	}

	// Describe callsign patterns (if any)
	if len(f.Callsigns) > 0 {
		parts = append(parts, "Callsigns: "+strings.Join(f.Callsigns, ", "))
	}

	if len(parts) == 0 {
		return "No active filters"
	}

	return strings.Join(parts, " | ")
}
