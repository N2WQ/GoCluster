// Package filter implements per-client spot filtering for the DX Cluster Server.
//
// Filters allow users to customize which spots they receive based on:
//   - Band (e.g., 20m, 40m, 160m)
//   - Mode (e.g., CW, USB, FT8, RTTY)
//   - Callsign patterns (e.g., W1*, LZ5VV, *ABC) for DX and DE calls
//   - Source category (HUMAN vs SKIMMER/automated)
//
// Filter Logic:
//   - Multiple filters use AND logic (all must match)
//   - Default state: All bands and modes enabled (no filtering)
//   - Once a specific filter is set, only matching spots pass
//   - Callsign patterns support wildcards (* at start or end) for both DX and DE calls
//
// Each telnet client has its own Filter instance, allowing personalized spot feeds.
// This reduces bandwidth for clients who only want specific spots (e.g., 20m CW only).
package filter

import (
	"errors"
	"os"
	"sort"
	"strconv"
	"strings"

	"dxcluster/spot"
)

// SupportedModes lists the commonly used modes that users can enable/disable
// via the PASS MODE command. Exported so UI/commands can display them.
var SupportedModes = []string{
	"CW",
	"FT4",
	"FT8",
	"LSB",
	"USB",
	"RTTY",
	"MSK144",
	"PSK31",
}

func boolPtr(value bool) *bool {
	return &value
}

// SupportedSources enumerates how telnet users can filter on Spot.IsHuman.
//
// HUMAN means the spot was marked as coming from a human operator (Spot.IsHuman=true).
// SKIMMER represents everything else (Spot.IsHuman=false), including automated sources.
var SupportedSources = []string{"HUMAN", "SKIMMER"}

// SupportedContinents enumerates continent codes used in DX metadata.
var SupportedContinents = []string{"AF", "AN", "AS", "EU", "NA", "OC", "SA"}

const (
	minCQZone = 1
	maxCQZone = 40
)

// defaultModeSelection controls which modes are enabled when a new filter is created.
// The initial values match the curated CW/USB/LSB/RTTY set, but can be overridden.
var defaultModeSelection = []string{"CW", "LSB", "USB", "RTTY"}

// defaultSourceSelection controls which Spot.IsHuman categories a brand-new
// filter allows by default.
//
// A nil or empty slice means "ALL" (disable SOURCE filtering).
var defaultSourceSelection []string

var supportedModeSet = func() map[string]bool {
	m := make(map[string]bool)
	for _, s := range SupportedModes {
		m[strings.ToUpper(strings.TrimSpace(s))] = true
	}
	return m
}()

var supportedSourceSet = func() map[string]bool {
	m := make(map[string]bool, len(SupportedSources))
	for _, source := range SupportedSources {
		s := strings.ToUpper(strings.TrimSpace(source))
		if s == "" {
			continue
		}
		m[s] = true
	}
	return m
}()

var supportedContinentSet = func() map[string]bool {
	m := make(map[string]bool, len(SupportedContinents))
	for _, c := range SupportedContinents {
		m[c] = true
	}
	return m
}()

// SupportedConfidenceSymbols enumerates the glyphs emitted in the telnet
// stream that users can filter on.
var SupportedConfidenceSymbols = []string{"?", "S", "C", "P", "V", "B"}

var supportedConfidenceSymbolSet = func() map[string]bool {
	m := make(map[string]bool, len(SupportedConfidenceSymbols))
	for _, symbol := range SupportedConfidenceSymbols {
		m[symbol] = true
	}
	return m
}()

var confidenceSymbolScores = map[string]int{
	"?": 0,
	"S": 25,
	"B": 10,
	"P": 50,
	"V": 100,
	"C": 100,
}

// IsSupportedMode returns true if the given mode is in the supported list.
func IsSupportedMode(mode string) bool {
	mode = strings.ToUpper(strings.TrimSpace(mode))
	return supportedModeSet[mode]
}

// IsSupportedSource reports whether the label is one of the supported SOURCE categories.
func IsSupportedSource(source string) bool {
	source = strings.ToUpper(strings.TrimSpace(source))
	return supportedSourceSet[source]
}

func normalizeSource(source string) string {
	source = strings.ToUpper(strings.TrimSpace(source))
	if supportedSourceSet[source] {
		return source
	}
	return ""
}

// IsSupportedContinent returns true if the continent code is known.
func IsSupportedContinent(cont string) bool {
	cont = strings.ToUpper(strings.TrimSpace(cont))
	return supportedContinentSet[cont]
}

// IsSupportedZone returns true when the CQ zone falls in the valid range.
func IsSupportedZone(zone int) bool {
	return zone >= minCQZone && zone <= maxCQZone
}

// MinCQZone exposes the lower bound for CQ zones.
func MinCQZone() int {
	return minCQZone
}

// MaxCQZone exposes the upper bound for CQ zones.
func MaxCQZone() int {
	return maxCQZone
}

// SetDefaultModeSelection replaces the modes that brand-new filters enable by default.
// Passing an empty slice resets to the built-in CW/LSB/USB/RTTY set.
func SetDefaultModeSelection(modes []string) {
	if len(modes) == 0 {
		defaultModeSelection = []string{"CW", "LSB", "USB", "RTTY"}
		return
	}
	normalized := make([]string, 0, len(modes))
	for _, mode := range modes {
		candidate := strings.ToUpper(strings.TrimSpace(mode))
		if candidate == "" {
			continue
		}
		normalized = append(normalized, candidate)
	}
	if len(normalized) == 0 {
		defaultModeSelection = []string{"CW", "LSB", "USB", "RTTY"}
		return
	}
	defaultModeSelection = normalized
}

// SetDefaultSourceSelection replaces the SOURCE categories that brand-new
// filters allow by default.
//
// Supported categories are "HUMAN" and "SKIMMER". An empty slice (or any input
// that normalizes to both categories) disables SOURCE filtering (equivalent to
// allowing ALL sources).
func SetDefaultSourceSelection(sources []string) {
	if len(sources) == 0 {
		defaultSourceSelection = nil
		return
	}
	normalized := make([]string, 0, len(sources))
	seen := make(map[string]bool, len(SupportedSources))
	for _, source := range sources {
		candidate := strings.ToUpper(strings.TrimSpace(source))
		if candidate == "" {
			continue
		}
		if candidate == "ALL" {
			defaultSourceSelection = nil
			return
		}
		if !supportedSourceSet[candidate] {
			continue
		}
		if seen[candidate] {
			continue
		}
		normalized = append(normalized, candidate)
		seen[candidate] = true
	}
	// Treat "both categories" the same as "ALL" so default filters show the
	// conventional "Source: ALL" state.
	if len(normalized) == 0 || len(normalized) == len(SupportedSources) {
		defaultSourceSelection = nil
		return
	}
	defaultSourceSelection = normalized
}

// User data directory (relative to working dir)
// UserDataDir is the base directory for persisted per-user data.
// It is a variable to allow tests to redirect the path without touching real data.
var UserDataDir = "data/users"

// SaveUserFilter persists a user's Filter while preserving any existing
// per-user metadata (such as recent IP history).
// Callsign is uppercased for filename stability.
func SaveUserFilter(callsign string, f *Filter) error {
	if f == nil {
		return errors.New("nil filter")
	}
	record, err := LoadUserRecord(callsign)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if record == nil {
		record = &UserRecord{}
	}
	record.Filter = *f
	return SaveUserRecord(callsign, record)
}

// LoadUserFilter loads the saved Filter for a given callsign.
// Returns os.ErrNotExist if no saved file is found.
func LoadUserFilter(callsign string) (*Filter, error) {
	record, err := LoadUserRecord(callsign)
	if err != nil {
		return nil, err
	}
	filter := record.Filter
	return &filter, nil
}

// EnsureUserDataDir makes sure the directory for saved filters exists.
func EnsureUserDataDir() error {
	return os.MkdirAll(UserDataDir, 0o755)
}

// Filter represents a user's spot filtering preferences.
//
// The filter maintains several types of criteria that can be combined:
//  1. Band filters: Which amateur radio bands to accept (20m, 40m, 160m)
//  2. Mode filters: Which operating modes to accept (CW, USB, FT8, etc.)
//  3. Callsign patterns: Which DX/DE callsigns to accept (W1*, LZ5VV, etc.)
//  4. Confidence glyphs: Which consensus indicators (?, S, C, P, V, B) to accept.
//  5. Beacon inclusion: Whether DX calls ending in /B (beacons) should be delivered.
//  6. Source category: Whether to deliver HUMAN spots, SKIMMER (automated) spots, or both.
//
// Default Behavior:
//   - AllBands=true: accept every band
//   - AllModes=false with the curated default mode list pre-enabled
//   - Callsign patterns: Only applied if non-empty (no impact on band/mode filters)
//   - AllConfidence=true: accept every consensus glyph until specific ones are enabled
//   - IncludeBeacons=true: beacon spots are delivered unless explicitly disabled
//   - AllSources=true: accept both HUMAN and SKIMMER spots (unless overridden for new users)
//
// Thread Safety:
//   - Each client has their own Filter instance (no sharing)
//   - No internal locking needed (single-threaded per client)
type Filter struct {
	Bands                map[string]bool // Allowed bands (whitelist when non-empty)
	BlockBands           map[string]bool // Blocked bands (deny wins over allow)
	Modes                map[string]bool // Allowed modes
	BlockModes           map[string]bool // Blocked modes
	Sources              map[string]bool // Allowed source categories (HUMAN/SKIMMER)
	BlockSources         map[string]bool // Blocked source categories
	DXCallsigns          []string        `yaml:"callsigns,omitempty"`   // DX callsign patterns (e.g., ["W1*", "LZ5VV"])
	DECallsigns          []string        `yaml:"decallsigns,omitempty"` // DE callsign patterns
	AllBands             bool            // If true, accept all bands (except blocked)
	BlockAllBands        bool            // If true, reject all bands
	AllModes             bool            // If true, accept all modes (except blocked)
	BlockAllModes        bool            // If true, reject all modes
	AllSources           bool            // If true, accept all source categories (except blocked)
	BlockAllSources      bool            // If true, reject all sources
	Confidence           map[string]bool // Allowed confidence glyphs (whitelist when non-empty)
	BlockConfidence      map[string]bool // Blocked confidence glyphs
	AllConfidence        bool            // If true, accept all confidence glyphs (except blocked)
	BlockAllConfidence   bool            // If true, reject all confidence glyphs (except exempt modes)
	IncludeBeacons       *bool           `yaml:"include_beacons,omitempty"` // nil/true delivers beacons; false suppresses
	AllowWWV             *bool           `yaml:"allow_wwv,omitempty"`       // nil/true delivers WWV bulletins; false suppresses
	AllowWCY             *bool           `yaml:"allow_wcy,omitempty"`       // nil/true delivers WCY bulletins; false suppresses
	AllowAnnounce        *bool           `yaml:"allow_announce,omitempty"`  // nil/true delivers PC93 announcements; false suppresses
	DXContinents         map[string]bool // Allowed DX continents
	BlockDXContinents    map[string]bool // Blocked DX continents
	DEContinents         map[string]bool // Allowed DE continents
	BlockDEContinents    map[string]bool // Blocked DE continents
	AllDXContinents      bool            // If true, accept all DX continents (except blocked)
	BlockAllDXContinents bool            // If true, reject all DX continents
	AllDEContinents      bool            // If true, accept all DE continents (except blocked)
	BlockAllDEContinents bool            // If true, reject all DE continents
	DXZones              map[int]bool    // Allowed DX CQ zones (1-40)
	BlockDXZones         map[int]bool    // Blocked DX CQ zones
	DEZones              map[int]bool    // Allowed DE CQ zones (1-40)
	BlockDEZones         map[int]bool    // Blocked DE CQ zones
	AllDXZones           bool            // If true, accept all DX CQ zones (except blocked)
	BlockAllDXZones      bool            // If true, reject all DX CQ zones
	AllDEZones           bool            // If true, accept all DE CQ zones (except blocked)
	BlockAllDEZones      bool            // If true, reject all DE CQ zones
	DXGrid2Prefixes      map[string]bool // Allowed 2-character DX grids
	BlockDXGrid2         map[string]bool // Blocked 2-character DX grids
	DEGrid2Prefixes      map[string]bool // Allowed 2-character DE grids
	BlockDEGrid2         map[string]bool // Blocked 2-character DE grids
	AllDXGrid2           bool            // If true, accept all DX 2-character grids (except blocked)
	BlockAllDXGrid2      bool            // If true, reject all DX 2-character grids
	AllDEGrid2           bool            // If true, accept all DE 2-character grids (except blocked)
	BlockAllDEGrid2      bool            // If true, reject all DE 2-character grids
	DXDXCC               map[int]bool    // Allowed DX ADIF/DXCC codes (whitelist when non-empty)
	BlockDXDXCC          map[int]bool    // Blocked DX ADIF/DXCC codes
	DEDXCC               map[int]bool    // Allowed DE ADIF/DXCC codes
	BlockDEDXCC          map[int]bool    // Blocked DE ADIF/DXCC codes
	AllDXDXCC            bool            // If true, accept all DX ADIF codes (except blocked)
	BlockAllDXDXCC       bool            // If true, reject all DX ADIF codes
	AllDEDXCC            bool            // If true, accept all DE ADIF codes (except blocked)
	BlockAllDEDXCC       bool            // If true, reject all DE ADIF codes

	// LegacyMinConfidence captures the old percentage-based filter persisted to
	// YAML so we can migrate user data to the new glyph-based approach.
	LegacyMinConfidence int `yaml:"minconfidence,omitempty"`
}

// NewFilter creates a new filter with every band enabled plus the configured
// default mode and SOURCE selections.
//
// Returns:
//   - *Filter: Initialized filter accepting all bands and the default mode subset
//
// Example:
//
//	filter := filter.NewFilter()
func NewFilter() *Filter {
	f := &Filter{
		Bands:                make(map[string]bool),
		BlockBands:           make(map[string]bool),
		Modes:                make(map[string]bool),
		BlockModes:           make(map[string]bool),
		Sources:              make(map[string]bool),
		BlockSources:         make(map[string]bool),
		DXCallsigns:          make([]string, 0),
		DECallsigns:          make([]string, 0),
		Confidence:           make(map[string]bool),
		BlockConfidence:      make(map[string]bool),
		DXContinents:         make(map[string]bool),
		BlockDXContinents:    make(map[string]bool),
		DEContinents:         make(map[string]bool),
		BlockDEContinents:    make(map[string]bool),
		DXZones:              make(map[int]bool),
		BlockDXZones:         make(map[int]bool),
		DEZones:              make(map[int]bool),
		BlockDEZones:         make(map[int]bool),
		DXGrid2Prefixes:      make(map[string]bool),
		BlockDXGrid2:         make(map[string]bool),
		DEGrid2Prefixes:      make(map[string]bool),
		BlockDEGrid2:         make(map[string]bool),
		DXDXCC:               make(map[int]bool),
		BlockDXDXCC:          make(map[int]bool),
		DEDXCC:               make(map[int]bool),
		BlockDEDXCC:          make(map[int]bool),
		AllBands:             true,  // Start with all bands enabled
		BlockAllBands:        false, // No band is globally blocked
		AllModes:             false, // Default to the curated mode subset below
		BlockAllModes:        false,
		AllSources:           true, // Accept both HUMAN and SKIMMER spots unless narrowed
		BlockAllSources:      false,
		AllowWWV:             boolPtr(true),
		AllowWCY:             boolPtr(true),
		AllowAnnounce:        boolPtr(true),
		AllConfidence:        true, // Accept every confidence glyph until user sets one
		BlockAllConfidence:   false,
		AllDXContinents:      true,
		BlockAllDXContinents: false,
		AllDEContinents:      true,
		BlockAllDEContinents: false,
		AllDXZones:           true,
		BlockAllDXZones:      false,
		AllDEZones:           true,
		BlockAllDEZones:      false,
		AllDXGrid2:           true,
		BlockAllDXGrid2:      false,
		AllDEGrid2:           true,
		BlockAllDEGrid2:      false,
		AllDXDXCC:            true,
		BlockAllDXDXCC:       false,
		AllDEDXCC:            true,
		BlockAllDEDXCC:       false,
	}
	for _, mode := range defaultModeSelection {
		f.Modes[mode] = true
	}
	for _, source := range defaultSourceSelection {
		f.SetSource(source, true)
	}
	f.SetBeaconEnabled(true)
	return f
}

// SetBand enables or disables filtering for a specific band.
//
// Parameters:
//   - band: Band to filter (e.g., "20M", "40m")
//   - enabled: true to accept this band, false to reject
//
// Behavior:
//   - When enabled: adds to the allowlist and removes it from the blocklist
//   - When disabled: adds to the blocklist and removes it from the allowlist
//   - Blocklist takes precedence over allowlist during matching
//
// Examples:
//
//	filter.SetBand("20M", true)  // Allow 20m
//	filter.SetBand("40m", false) // Explicitly block 40m
func (f *Filter) SetBand(band string, enabled bool) {
	normalized := spot.NormalizeBand(band)
	if normalized == "" || !spot.IsValidBand(normalized) {
		return
	}
	if f.Bands == nil {
		f.Bands = make(map[string]bool)
	}
	if f.BlockBands == nil {
		f.BlockBands = make(map[string]bool)
	}
	if enabled {
		f.Bands[normalized] = true
		delete(f.BlockBands, normalized)
		f.BlockAllBands = false
		f.AllBands = len(f.Bands) == 0
		return
	}
	delete(f.Bands, normalized)
	f.BlockBands[normalized] = true
	f.BlockAllBands = false
	f.AllBands = len(f.Bands) == 0
}

// SetMode enables or disables filtering for a specific mode.
//
// Parameters:
//   - mode: Mode to filter (e.g., "CW", "FT8", "USB")
//   - enabled: true to accept this mode, false to reject
//
// Behavior:
//   - When enabling first mode: Disables AllModes flag (switches to whitelist mode)
//   - Multiple modes can be enabled (OR logic within modes)
//   - Disabling a mode removes it from the filter
//
// Examples:
//
//	filter.SetMode("CW", true)   // Only accept CW (disables all other modes)
//	filter.SetMode("FT8", true)  // Now accept CW OR FT8
//	filter.SetMode("CW", false)  // Only accept FT8 now
func (f *Filter) SetMode(mode string, enabled bool) {
	mode = strings.ToUpper(mode)
	if f.Modes == nil {
		f.Modes = make(map[string]bool)
	}
	if f.BlockModes == nil {
		f.BlockModes = make(map[string]bool)
	}
	if enabled {
		f.Modes[mode] = true
		delete(f.BlockModes, mode)
		f.BlockAllModes = false
		f.AllModes = len(f.Modes) == 0
		return
	}
	delete(f.Modes, mode)
	f.BlockModes[mode] = true
	f.BlockAllModes = false
	f.AllModes = len(f.Modes) == 0
}

// SetSource enables or disables filtering for the spot origin category.
//
// "HUMAN" refers to spots marked Spot.IsHuman=true, while "SKIMMER" refers to
// everything else (Spot.IsHuman=false).
func (f *Filter) SetSource(source string, enabled bool) {
	if f == nil {
		return
	}
	source = normalizeSource(source)
	if source == "" {
		return
	}
	if f.Sources == nil {
		f.Sources = make(map[string]bool)
	}
	if f.BlockSources == nil {
		f.BlockSources = make(map[string]bool)
	}
	if enabled {
		f.Sources[source] = true
		delete(f.BlockSources, source)
		f.BlockAllSources = false
		f.AllSources = len(f.Sources) == 0
		return
	}
	delete(f.Sources, source)
	f.BlockSources[source] = true
	f.BlockAllSources = false
	f.AllSources = len(f.Sources) == 0
}

// AddDXCallsignPattern adds a DX callsign pattern to the filter.
//
// Parameters:
//   - pattern: Callsign pattern with optional wildcards (e.g., "W1*", "LZ5VV", "*ABC")
//
// Behavior:
//   - Multiple patterns can be added (OR logic)
//   - Patterns are case-insensitive (normalized to uppercase)
func (f *Filter) AddDXCallsignPattern(pattern string) {
	pattern = strings.ToUpper(pattern)
	f.DXCallsigns = append(f.DXCallsigns, pattern)
}

// AddDECallsignPattern adds a DE/spotter callsign pattern to the filter.
func (f *Filter) AddDECallsignPattern(pattern string) {
	pattern = strings.ToUpper(pattern)
	f.DECallsigns = append(f.DECallsigns, pattern)
}

// SetDXContinent enables or disables filtering for a specific DX continent.
func (f *Filter) SetDXContinent(cont string, enabled bool) {
	cont = strings.ToUpper(strings.TrimSpace(cont))
	if !IsSupportedContinent(cont) {
		return
	}
	if f.DXContinents == nil {
		f.DXContinents = make(map[string]bool)
	}
	if f.BlockDXContinents == nil {
		f.BlockDXContinents = make(map[string]bool)
	}
	if enabled {
		f.DXContinents[cont] = true
		delete(f.BlockDXContinents, cont)
		f.BlockAllDXContinents = false
		f.AllDXContinents = len(f.DXContinents) == 0
		return
	}
	delete(f.DXContinents, cont)
	f.BlockDXContinents[cont] = true
	f.BlockAllDXContinents = false
	f.AllDXContinents = len(f.DXContinents) == 0
}

// SetDEContinent enables or disables filtering for a specific spotter continent.
func (f *Filter) SetDEContinent(cont string, enabled bool) {
	cont = strings.ToUpper(strings.TrimSpace(cont))
	if !IsSupportedContinent(cont) {
		return
	}
	if f.DEContinents == nil {
		f.DEContinents = make(map[string]bool)
	}
	if f.BlockDEContinents == nil {
		f.BlockDEContinents = make(map[string]bool)
	}
	if enabled {
		f.DEContinents[cont] = true
		delete(f.BlockDEContinents, cont)
		f.BlockAllDEContinents = false
		f.AllDEContinents = len(f.DEContinents) == 0
		return
	}
	delete(f.DEContinents, cont)
	f.BlockDEContinents[cont] = true
	f.BlockAllDEContinents = false
	f.AllDEContinents = len(f.DEContinents) == 0
}

// SetDXZone enables or disables filtering for a specific DX CQ zone (1-40).
func (f *Filter) SetDXZone(zone int, enabled bool) {
	if !IsSupportedZone(zone) {
		return
	}
	if f.DXZones == nil {
		f.DXZones = make(map[int]bool)
	}
	if f.BlockDXZones == nil {
		f.BlockDXZones = make(map[int]bool)
	}
	if enabled {
		f.DXZones[zone] = true
		delete(f.BlockDXZones, zone)
		f.BlockAllDXZones = false
		f.AllDXZones = len(f.DXZones) == 0
		return
	}
	delete(f.DXZones, zone)
	f.BlockDXZones[zone] = true
	f.BlockAllDXZones = false
	f.AllDXZones = len(f.DXZones) == 0
}

// SetDEZone enables or disables filtering for a specific spotter CQ zone (1-40).
func (f *Filter) SetDEZone(zone int, enabled bool) {
	if !IsSupportedZone(zone) {
		return
	}
	if f.DEZones == nil {
		f.DEZones = make(map[int]bool)
	}
	if f.BlockDEZones == nil {
		f.BlockDEZones = make(map[int]bool)
	}
	if enabled {
		f.DEZones[zone] = true
		delete(f.BlockDEZones, zone)
		f.BlockAllDEZones = false
		f.AllDEZones = len(f.DEZones) == 0
		return
	}
	delete(f.DEZones, zone)
	f.BlockDEZones[zone] = true
	f.BlockAllDEZones = false
	f.AllDEZones = len(f.DEZones) == 0
}

// SetDXDXCC enables or disables filtering for a specific DX ADIF/DXCC code.
func (f *Filter) SetDXDXCC(code int, enabled bool) {
	if code <= 0 {
		return
	}
	if f.DXDXCC == nil {
		f.DXDXCC = make(map[int]bool)
	}
	if f.BlockDXDXCC == nil {
		f.BlockDXDXCC = make(map[int]bool)
	}
	if enabled {
		f.DXDXCC[code] = true
		delete(f.BlockDXDXCC, code)
		f.BlockAllDXDXCC = false
		f.AllDXDXCC = len(f.DXDXCC) == 0
		return
	}
	delete(f.DXDXCC, code)
	f.BlockDXDXCC[code] = true
	f.BlockAllDXDXCC = false
	f.AllDXDXCC = len(f.DXDXCC) == 0
}

// SetDEDXCC enables or disables filtering for a specific DE ADIF/DXCC code.
func (f *Filter) SetDEDXCC(code int, enabled bool) {
	if code <= 0 {
		return
	}
	if f.DEDXCC == nil {
		f.DEDXCC = make(map[int]bool)
	}
	if f.BlockDEDXCC == nil {
		f.BlockDEDXCC = make(map[int]bool)
	}
	if enabled {
		f.DEDXCC[code] = true
		delete(f.BlockDEDXCC, code)
		f.BlockAllDEDXCC = false
		f.AllDEDXCC = len(f.DEDXCC) == 0
		return
	}
	delete(f.DEDXCC, code)
	f.BlockDEDXCC[code] = true
	f.BlockAllDEDXCC = false
	f.AllDEDXCC = len(f.DEDXCC) == 0
}

// ClearDXCallsignPatterns removes all DX callsign filters.
func (f *Filter) ClearDXCallsignPatterns() {
	f.DXCallsigns = make([]string, 0)
}

// ClearDECallsignPatterns removes all DE callsign filters.
func (f *Filter) ClearDECallsignPatterns() {
	f.DECallsigns = make([]string, 0)
}

// ClearCallsignPatterns clears both DX and DE callsign filters.
func (f *Filter) ClearCallsignPatterns() {
	f.ClearDXCallsignPatterns()
	f.ClearDECallsignPatterns()
}

// SetConfidenceSymbol enables or disables filtering for a specific confidence glyph.
//
// Parameters:
//   - symbol: Confidence glyph (e.g., "?", "P", "V")
//   - enabled: true to accept this glyph, false to reject it
//
// Behavior:
//   - Enabling any glyph disables AllConfidence (whitelist behavior)
//   - Disabling the last glyph reverts to accepting all confidence values
func (f *Filter) SetConfidenceSymbol(symbol string, enabled bool) {
	if f == nil {
		return
	}
	canonical := normalizeConfidenceSymbol(symbol)
	if canonical == "" {
		return
	}
	if f.Confidence == nil {
		f.Confidence = make(map[string]bool)
	}
	if f.BlockConfidence == nil {
		f.BlockConfidence = make(map[string]bool)
	}
	if enabled {
		f.Confidence[canonical] = true
		delete(f.BlockConfidence, canonical)
		f.BlockAllConfidence = false
		f.AllConfidence = len(f.Confidence) == 0
		return
	}
	delete(f.Confidence, canonical)
	f.BlockConfidence[canonical] = true
	f.BlockAllConfidence = false
	f.AllConfidence = len(f.Confidence) == 0
}

// ResetConfidence disables confidence-based filtering.
func (f *Filter) ResetConfidence() {
	f.Confidence = make(map[string]bool)
	f.BlockConfidence = make(map[string]bool)
	f.AllConfidence = true
	f.BlockAllConfidence = false
	f.LegacyMinConfidence = 0
}

// SetBeaconEnabled controls whether DX beacons (/B) are delivered.
func (f *Filter) SetBeaconEnabled(enabled bool) {
	if f == nil {
		return
	}
	value := enabled
	f.IncludeBeacons = &value
}

// BeaconsEnabled reports whether the filter currently allows beacon spots.
func (f *Filter) BeaconsEnabled() bool {
	if f == nil || f.IncludeBeacons == nil {
		return true
	}
	return *f.IncludeBeacons
}

// SetWWVEnabled controls whether WWV bulletins are delivered.
func (f *Filter) SetWWVEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowWWV = boolPtr(enabled)
}

// SetWCYEnabled controls whether WCY bulletins are delivered.
func (f *Filter) SetWCYEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowWCY = boolPtr(enabled)
}

// SetAnnounceEnabled controls whether PC93 announcements are delivered.
func (f *Filter) SetAnnounceEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowAnnounce = boolPtr(enabled)
}

// WWVEnabled reports whether the filter currently allows WWV bulletins.
func (f *Filter) WWVEnabled() bool {
	if f == nil || f.AllowWWV == nil {
		return true
	}
	return *f.AllowWWV
}

// WCYEnabled reports whether the filter currently allows WCY bulletins.
func (f *Filter) WCYEnabled() bool {
	if f == nil || f.AllowWCY == nil {
		return true
	}
	return *f.AllowWCY
}

// AnnounceEnabled reports whether the filter currently allows PC93 announcements.
func (f *Filter) AnnounceEnabled() bool {
	if f == nil || f.AllowAnnounce == nil {
		return true
	}
	return *f.AllowAnnounce
}

// AllowsBulletin reports whether the bulletin kind should be delivered.
// Supported kinds are WWV/WCY/ANNOUNCE (or PC23/PC73/PC93); unknown kinds default to true.
func (f *Filter) AllowsBulletin(kind string) bool {
	switch strings.ToUpper(strings.TrimSpace(kind)) {
	case "WWV", "PC23":
		return f.WWVEnabled()
	case "WCY", "PC73":
		return f.WCYEnabled()
	case "ANNOUNCE", "PC93":
		return f.AnnounceEnabled()
	default:
		return true
	}
}

// ResetBands clears all band filters and accepts all bands.
//
// Behavior:
//   - Clears the Bands map
//   - Sets AllBands = true
//   - Spots from any band will pass (subject to mode/callsign filters)
//
// Example:
//
//	filter.ResetBands()
//	// All bands now pass through
func (f *Filter) ResetBands() {
	f.Bands = make(map[string]bool)
	f.AllBands = true
	f.BlockBands = make(map[string]bool)
	f.BlockAllBands = false
}

// ResetModes clears all mode filters and accepts all modes.
//
// Behavior:
//   - Clears the Modes map
//   - Sets AllModes = true
//   - Spots from any mode will pass (subject to band/callsign filters)
//
// Example:
//
//	filter.ResetModes()
//	// All modes now pass through
func (f *Filter) ResetModes() {
	f.Modes = make(map[string]bool)
	f.AllModes = true
	f.BlockModes = make(map[string]bool)
	f.BlockAllModes = false
}

// ResetSources clears SOURCE filtering and allows both human and automated spots.
func (f *Filter) ResetSources() {
	f.Sources = make(map[string]bool)
	f.AllSources = true
	f.BlockSources = make(map[string]bool)
	f.BlockAllSources = false
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
//
//	filter.Reset()
//	// Filter is now in default state (all spots pass)
func (f *Filter) Reset() {
	f.ResetBands()
	f.ResetModes()
	f.ResetSources()
	f.ClearCallsignPatterns()
	f.ResetConfidence()
	f.ResetDXContinents()
	f.ResetDEContinents()
	f.ResetDXZones()
	f.ResetDEZones()
	f.ResetDXGrid2()
	f.ResetDEGrid2()
	f.ResetDXDXCC()
	f.ResetDEDXCC()
	f.SetBeaconEnabled(true)
	f.SetWWVEnabled(true)
	f.SetWCYEnabled(true)
	f.SetAnnounceEnabled(true)
}

// ResetDXContinents clears DX continent filters and accepts all.
func (f *Filter) ResetDXContinents() {
	f.DXContinents = make(map[string]bool)
	f.AllDXContinents = true
	f.BlockDXContinents = make(map[string]bool)
	f.BlockAllDXContinents = false
}

// ResetDEContinents clears spotter continent filters and accepts all.
func (f *Filter) ResetDEContinents() {
	f.DEContinents = make(map[string]bool)
	f.AllDEContinents = true
	f.BlockDEContinents = make(map[string]bool)
	f.BlockAllDEContinents = false
}

// ResetDXZones clears DX CQ zone filters and accepts all.
func (f *Filter) ResetDXZones() {
	f.DXZones = make(map[int]bool)
	f.AllDXZones = true
	f.BlockDXZones = make(map[int]bool)
	f.BlockAllDXZones = false
}

// ResetDEZones clears spotter CQ zone filters and accepts all.
func (f *Filter) ResetDEZones() {
	f.DEZones = make(map[int]bool)
	f.AllDEZones = true
	f.BlockDEZones = make(map[int]bool)
	f.BlockAllDEZones = false
}

// ResetDXDXCC clears DX ADIF/DXCC filters and accepts all.
func (f *Filter) ResetDXDXCC() {
	f.DXDXCC = make(map[int]bool)
	f.BlockDXDXCC = make(map[int]bool)
	f.AllDXDXCC = true
	f.BlockAllDXDXCC = false
}

// ResetDEDXCC clears DE ADIF/DXCC filters and accepts all.
func (f *Filter) ResetDEDXCC() {
	f.DEDXCC = make(map[int]bool)
	f.BlockDEDXCC = make(map[int]bool)
	f.AllDEDXCC = true
	f.BlockAllDEDXCC = false
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
//
//	filter.SetBand("20m", true)
//	filter.SetMode("CW", true)
//	filter.Matches(spot_20m_CW)   → true
//	filter.Matches(spot_40m_CW)   → false (wrong band)
//	filter.Matches(spot_20m_USB)  → false (wrong mode)
func (f *Filter) Matches(s *spot.Spot) bool {
	// Spot must be normalized upstream; this function treats the spot as immutable.
	if s != nil && s.IsBeacon && !f.BeaconsEnabled() {
		return false
	}

	modeUpper := s.ModeNorm
	if modeUpper == "" {
		modeUpper = strings.ToUpper(strings.TrimSpace(s.Mode))
	}

	bandNorm := s.BandNorm
	if bandNorm == "" {
		bandNorm = spot.NormalizeBand(s.Band)
	}

	dxCont := s.DXContinentNorm
	if dxCont == "" {
		dxCont = strings.ToUpper(strings.TrimSpace(s.DXMetadata.Continent))
	}
	deCont := s.DEContinentNorm
	if deCont == "" {
		deCont = strings.ToUpper(strings.TrimSpace(s.DEMetadata.Continent))
	}

	dxGrid2 := s.DXGrid2
	if dxGrid2 == "" {
		dxGrid2 = normalizeGrid2Token(s.DXMetadata.Grid)
	}
	deGrid2 := s.DEGrid2
	if deGrid2 == "" {
		deGrid2 = normalizeGrid2Token(s.DEMetadata.Grid)
	}

	// Band and mode filters: blocklist wins, allowlist optional.
	if !passesStringFilter(bandNorm, f.Bands, f.BlockBands, f.AllBands, f.BlockAllBands) {
		return false
	}
	if !passesStringFilter(modeUpper, f.Modes, f.BlockModes, f.AllModes, f.BlockAllModes) {
		return false
	}

	sourceLabel := "SKIMMER"
	if s.IsHuman {
		sourceLabel = "HUMAN"
	}
	if !passesStringFilter(sourceLabel, f.Sources, f.BlockSources, f.AllSources, f.BlockAllSources) {
		return false
	}

	// DX/DE continent filters.
	if !passesStringFilter(dxCont, f.DXContinents, f.BlockDXContinents, f.AllDXContinents, f.BlockAllDXContinents) {
		return false
	}
	if !passesStringFilter(deCont, f.DEContinents, f.BlockDEContinents, f.AllDEContinents, f.BlockAllDEContinents) {
		return false
	}

	// DX/DE CQ zones.
	if !passesIntFilter(s.DXMetadata.CQZone, f.DXZones, f.BlockDXZones, f.AllDXZones, f.BlockAllDXZones, IsSupportedZone) {
		return false
	}
	if !passesIntFilter(s.DEMetadata.CQZone, f.DEZones, f.BlockDEZones, f.AllDEZones, f.BlockAllDEZones, IsSupportedZone) {
		return false
	}

	// DX/DE ADIF (DXCC) filters.
	if !passesIntFilter(s.DXMetadata.ADIF, f.DXDXCC, f.BlockDXDXCC, f.AllDXDXCC, f.BlockAllDXDXCC, nil) {
		return false
	}
	if !passesIntFilter(s.DEMetadata.ADIF, f.DEDXCC, f.BlockDEDXCC, f.AllDEDXCC, f.BlockAllDEDXCC, nil) {
		return false
	}

	// 2-character grid filters.
	if !passesStringFilter(prefix2(dxGrid2), f.DXGrid2Prefixes, f.BlockDXGrid2, f.AllDXGrid2, f.BlockAllDXGrid2) {
		return false
	}
	if !passesStringFilter(prefix2(deGrid2), f.DEGrid2Prefixes, f.BlockDEGrid2, f.AllDEGrid2, f.BlockAllDEGrid2) {
		return false
	}

	// Check DX callsign patterns (if any are set)
	if len(f.DXCallsigns) > 0 {
		matched := false
		for _, pattern := range f.DXCallsigns {
			if matchesCallsignPattern(s.DXCall, pattern) {
				matched = true
				break // At least one pattern matched (OR logic)
			}
		}
		if !matched {
			return false // No patterns matched
		}
	}

	// Check DE callsign patterns (if any are set)
	if len(f.DECallsigns) > 0 {
		matched := false
		for _, pattern := range f.DECallsigns {
			if matchesCallsignPattern(s.DECall, pattern) {
				matched = true
				break // At least one pattern matched (OR logic)
			}
		}
		if !matched {
			return false // No patterns matched
		}
	}

	if !isConfidenceExemptMode(modeUpper) {
		symbol := normalizeConfidenceSymbol(s.Confidence)
		if !passesStringFilter(symbol, f.Confidence, f.BlockConfidence, f.AllConfidence, f.BlockAllConfidence) {
			return false
		}
	}

	return true // Passed all filters
}

// passesStringFilter evaluates a token against allow/block lists with deny-first semantics.
func passesStringFilter(token string, allow map[string]bool, block map[string]bool, allowAll, blockAll bool) bool {
	if blockAll {
		return false
	}
	token = safeTrimSpace(token)
	if len(block) > 0 && block[token] {
		return false
	}
	if !allowAll && len(allow) == 0 {
		return false
	}
	if len(allow) > 0 {
		if token == "" || !allow[token] {
			return false
		}
	}
	return true
}

// safeTrimSpace trims whitespace but guards against malformed string headers that could panic.
// If a panic occurs, it returns an empty string to fail closed.
func safeTrimSpace(s string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = ""
		}
	}()
	out = strings.TrimSpace(s)
	return
}

// passesIntFilter evaluates an integer token against allow/block lists with deny-first semantics.
func passesIntFilter(token int, allow map[int]bool, block map[int]bool, allowAll, blockAll bool, validator func(int) bool) bool {
	if blockAll {
		return false
	}
	if len(block) > 0 && block[token] {
		return false
	}
	if validator != nil && !validator(token) {
		if len(allow) > 0 || !allowAll {
			return false
		}
		// If allowlist is empty and allowAll is true, an unknown token passes unless explicitly blocked.
		return true
	}
	if !allowAll && len(allow) == 0 {
		return false
	}
	if len(allow) > 0 {
		if !allow[token] {
			return false
		}
	}
	return true
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
//
//	matchesCallsignPattern("W1ABC", "W1*")   → true
//	matchesCallsignPattern("W1ABC", "*ABC")  → true
//	matchesCallsignPattern("W1ABC", "LZ5VV") → false
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

// isConfidenceExemptMode reports whether confidence filtering should be skipped
// because the pipeline never assigns consensus/confidence glyphs to that mode.
func isConfidenceExemptMode(mode string) bool {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "FT8", "FT4":
		return true
	default:
		return false
	}
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
// This is used for the SHOW FILTER command to display current filter state.
//
// Examples:
//
//	filter.String() → "No active filters"
//	(after SetBand("20m", true))
//	filter.String() → "Bands: 20m | Modes: ALL"
//	(after SetMode("CW", true))
//	filter.String() → "Bands: 20m | Modes: CW"
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
		sort.Strings(bands)
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
		sort.Strings(modes)
		if len(modes) > 0 {
			parts = append(parts, "Modes: "+strings.Join(modes, ", "))
		} else {
			parts = append(parts, "Modes: NONE (no spots will pass)")
		}
	}

	// Describe source filter.
	{
		label := "ALL"
		if f.BlockAllSources || (f.BlockSources["HUMAN"] && f.BlockSources["SKIMMER"]) {
			label = "NONE (no spots will pass)"
		} else if !f.AllSources {
			sources := make([]string, 0, len(f.Sources))
			for source := range f.Sources {
				sources = append(sources, source)
			}
			sort.Strings(sources)
			if len(sources) > 0 {
				label = strings.Join(sources, ", ")
			} else {
				label = "NONE (no spots will pass)"
			}
		} else {
			// When allowing all sources, show the implicit selection if the user has
			// blocked exactly one category.
			if f.BlockSources["HUMAN"] && !f.BlockSources["SKIMMER"] {
				label = "SKIMMER"
			} else if f.BlockSources["SKIMMER"] && !f.BlockSources["HUMAN"] {
				label = "HUMAN"
			}
		}
		parts = append(parts, "Source: "+label)
	}

	// Describe callsign patterns (if any)
	if len(f.DXCallsigns) > 0 {
		parts = append(parts, "DXCallsigns: "+strings.Join(f.DXCallsigns, ", "))
	}
	if len(f.DECallsigns) > 0 {
		parts = append(parts, "DECallsigns: "+strings.Join(f.DECallsigns, ", "))
	}

	// Describe continent filters
	if f.AllDXContinents {
		parts = append(parts, "DXCont: ALL")
	} else {
		parts = append(parts, "DXCont: "+strings.Join(enabledContinents(f.DXContinents), ", "))
	}
	if f.AllDEContinents {
		parts = append(parts, "DECont: ALL")
	} else {
		parts = append(parts, "DECont: "+strings.Join(enabledContinents(f.DEContinents), ", "))
	}

	// Describe CQ zone filters
	if f.AllDXZones {
		parts = append(parts, "DXZone: ALL")
	} else {
		parts = append(parts, "DXZone: "+strings.Join(enabledZones(f.DXZones), ", "))
	}
	if f.AllDEZones {
		parts = append(parts, "DEZone: ALL")
	} else {
		parts = append(parts, "DEZone: "+strings.Join(enabledZones(f.DEZones), ", "))
	}

	// Describe confidence glyph filter
	if f.AllConfidence || len(f.Confidence) == 0 {
		parts = append(parts, "Confidence: ALL")
	} else {
		levels := f.EnabledConfidenceSymbols()
		if len(levels) > 0 {
			parts = append(parts, "Confidence: "+strings.Join(levels, ", "))
		} else {
			parts = append(parts, "Confidence: NONE (no spots will pass)")
		}
	}

	if f.BeaconsEnabled() {
		parts = append(parts, "Beacons: ON")
	} else {
		parts = append(parts, "Beacons: OFF")
	}
	if f.WWVEnabled() {
		parts = append(parts, "WWV: ON")
	} else {
		parts = append(parts, "WWV: OFF")
	}
	if f.WCYEnabled() {
		parts = append(parts, "WCY: ON")
	} else {
		parts = append(parts, "WCY: OFF")
	}
	if f.AnnounceEnabled() {
		parts = append(parts, "ANNOUNCE: ON")
	} else {
		parts = append(parts, "ANNOUNCE: OFF")
	}

	if f.AllDXGrid2 {
		parts = append(parts, "DXGrid2: ALL")
	} else {
		parts = append(parts, "DXGrid2: "+strings.Join(enabledGrid2(f.DXGrid2Prefixes), ", "))
	}
	if f.AllDEGrid2 {
		parts = append(parts, "DEGrid2: ALL")
	} else {
		parts = append(parts, "DEGrid2: "+strings.Join(enabledGrid2(f.DEGrid2Prefixes), ", "))
	}
	if f.AllDXDXCC {
		parts = append(parts, "DXDXCC: ALL")
	} else {
		parts = append(parts, "DXDXCC: "+strings.Join(enabledDXCC(f.DXDXCC), ", "))
	}
	if f.AllDEDXCC {
		parts = append(parts, "DEDXCC: ALL")
	} else {
		parts = append(parts, "DEDXCC: "+strings.Join(enabledDXCC(f.DEDXCC), ", "))
	}

	if len(parts) == 0 {
		return "No active filters"
	}

	return strings.Join(parts, " | ")
}

// EnabledConfidenceSymbols returns the currently whitelisted glyphs in display order.
func (f *Filter) EnabledConfidenceSymbols() []string {
	if f == nil || len(f.Confidence) == 0 {
		return nil
	}
	result := make([]string, 0, len(f.Confidence))
	seen := make(map[string]bool, len(f.Confidence))
	for _, symbol := range SupportedConfidenceSymbols {
		if f.Confidence[symbol] {
			result = append(result, symbol)
			seen[symbol] = true
		}
	}
	for symbol := range f.Confidence {
		if !seen[symbol] {
			result = append(result, symbol)
		}
	}
	return result
}

// ConfidenceSymbolEnabled reports whether the glyph is currently allowed.
func (f *Filter) ConfidenceSymbolEnabled(symbol string) bool {
	if f == nil || f.AllConfidence {
		return true
	}
	canonical := normalizeConfidenceSymbol(symbol)
	if canonical == "" {
		return false
	}
	return f.Confidence[canonical]
}

// enabledContinents returns sorted continent labels from the provided map.
func enabledContinents(m map[string]bool) []string {
	if len(m) == 0 {
		return []string{"NONE"}
	}
	out := make([]string, 0, len(m))
	for cont := range m {
		out = append(out, cont)
	}
	sort.Strings(out)
	return out
}

// enabledZones returns sorted CQ zone labels as strings.
func enabledZones(m map[int]bool) []string {
	if len(m) == 0 {
		return []string{"NONE"}
	}
	out := make([]int, 0, len(m))
	for zone := range m {
		out = append(out, zone)
	}
	sort.Ints(out)
	strs := make([]string, 0, len(out))
	for _, z := range out {
		strs = append(strs, strconv.Itoa(z))
	}
	return strs
}

// enabledGrid2 returns sorted 2-character grid prefixes from the provided map.
func enabledGrid2(m map[string]bool) []string {
	if len(m) == 0 {
		return []string{"NONE"}
	}
	out := make([]string, 0, len(m))
	for grid := range m {
		out = append(out, grid)
	}
	sort.Strings(out)
	return out
}

// enabledDXCC returns sorted ADIF/DXCC codes from the provided map.
func enabledDXCC(m map[int]bool) []string {
	if len(m) == 0 {
		return []string{"NONE"}
	}
	out := make([]int, 0, len(m))
	for code := range m {
		out = append(out, code)
	}
	sort.Ints(out)
	strs := make([]string, 0, len(out))
	for _, code := range out {
		strs = append(strs, strconv.Itoa(code))
	}
	return strs
}

// IsSupportedConfidenceSymbol returns true if the glyph is one of the known consensus indicators.
func IsSupportedConfidenceSymbol(symbol string) bool {
	normalized := strings.ToUpper(strings.TrimSpace(symbol))
	if normalized == "" {
		return false
	}
	return supportedConfidenceSymbolSet[normalized]
}

func normalizeConfidenceSymbol(label string) string {
	value := strings.TrimSpace(label)
	if value == "" {
		return "?"
	}
	upper := strings.ToUpper(value)
	if supportedConfidenceSymbolSet[upper] {
		return upper
	}
	trimmed := strings.TrimSuffix(upper, "%")
	if trimmed != upper {
		upper = trimmed
	}
	num, err := strconv.Atoi(upper)
	if err != nil {
		return ""
	}
	switch {
	case num <= 25:
		return "?"
	case num <= 75:
		return "P"
	default:
		return "V"
	}
}

func confidenceSymbolsForThreshold(threshold int) []string {
	if threshold < 0 {
		threshold = 0
	}
	if threshold > 100 {
		threshold = 100
	}
	result := make([]string, 0, len(SupportedConfidenceSymbols))
	for _, symbol := range SupportedConfidenceSymbols {
		score := confidenceSymbolScores[symbol]
		if score >= threshold {
			result = append(result, symbol)
		}
	}
	return result
}

func (f *Filter) migrateLegacyConfidence() {
	if f == nil {
		return
	}
	if f.Confidence == nil {
		f.Confidence = make(map[string]bool)
	}
	if f.LegacyMinConfidence > 0 {
		for _, symbol := range confidenceSymbolsForThreshold(f.LegacyMinConfidence) {
			f.Confidence[symbol] = true
		}
		if len(f.Confidence) > 0 {
			f.AllConfidence = false
		}
		f.LegacyMinConfidence = 0
	}
	if len(f.Confidence) == 0 {
		f.AllConfidence = true
	}
}

// normalizeDefaults repairs zero-value filters loaded from disk so missing fields
// revert to the permissive defaults instead of accidentally blocking traffic.
func (f *Filter) normalizeDefaults() {
	if f == nil {
		return
	}
	if f.Bands == nil {
		f.Bands = make(map[string]bool)
	}
	if f.BlockBands == nil {
		f.BlockBands = make(map[string]bool)
	}
	if f.Modes == nil {
		f.Modes = make(map[string]bool)
	}
	if f.BlockModes == nil {
		f.BlockModes = make(map[string]bool)
	}
	if f.Sources == nil {
		f.Sources = make(map[string]bool)
	}
	if f.BlockSources == nil {
		f.BlockSources = make(map[string]bool)
	}
	if f.Confidence == nil {
		f.Confidence = make(map[string]bool)
	}
	if f.BlockConfidence == nil {
		f.BlockConfidence = make(map[string]bool)
	}
	if f.DXContinents == nil {
		f.DXContinents = make(map[string]bool)
	}
	if f.BlockDXContinents == nil {
		f.BlockDXContinents = make(map[string]bool)
	}
	if f.DEContinents == nil {
		f.DEContinents = make(map[string]bool)
	}
	if f.BlockDEContinents == nil {
		f.BlockDEContinents = make(map[string]bool)
	}
	if f.DXZones == nil {
		f.DXZones = make(map[int]bool)
	}
	if f.BlockDXZones == nil {
		f.BlockDXZones = make(map[int]bool)
	}
	if f.DEZones == nil {
		f.DEZones = make(map[int]bool)
	}
	if f.BlockDEZones == nil {
		f.BlockDEZones = make(map[int]bool)
	}
	if f.DXGrid2Prefixes == nil {
		f.DXGrid2Prefixes = make(map[string]bool)
	}
	if f.BlockDXGrid2 == nil {
		f.BlockDXGrid2 = make(map[string]bool)
	}
	if f.DEGrid2Prefixes == nil {
		f.DEGrid2Prefixes = make(map[string]bool)
	}
	if f.BlockDEGrid2 == nil {
		f.BlockDEGrid2 = make(map[string]bool)
	}
	if f.DXDXCC == nil {
		f.DXDXCC = make(map[int]bool)
	}
	if f.BlockDXDXCC == nil {
		f.BlockDXDXCC = make(map[int]bool)
	}
	if f.DEDXCC == nil {
		f.DEDXCC = make(map[int]bool)
	}
	if f.BlockDEDXCC == nil {
		f.BlockDEDXCC = make(map[int]bool)
	}
	if f.AllowWWV == nil {
		f.AllowWWV = boolPtr(true)
	}
	if f.AllowWCY == nil {
		f.AllowWCY = boolPtr(true)
	}
	if f.AllowAnnounce == nil {
		f.AllowAnnounce = boolPtr(true)
	}

	if len(f.Bands) == 0 {
		f.AllBands = true
	}
	if len(f.Modes) == 0 {
		f.AllModes = true
	}
	if len(f.Sources) == 0 {
		f.AllSources = true
	}
	if len(f.Confidence) == 0 {
		f.AllConfidence = true
	}
	if len(f.DXContinents) == 0 {
		f.AllDXContinents = true
	}
	if len(f.DEContinents) == 0 {
		f.AllDEContinents = true
	}
	if len(f.DXZones) == 0 {
		f.AllDXZones = true
	}
	if len(f.DEZones) == 0 {
		f.AllDEZones = true
	}
	if len(f.DXGrid2Prefixes) == 0 {
		f.AllDXGrid2 = true
	}
	if len(f.DEGrid2Prefixes) == 0 {
		f.AllDEGrid2 = true
	}
	if len(f.DXDXCC) == 0 {
		f.AllDXDXCC = true
	}
	if len(f.DEDXCC) == 0 {
		f.AllDEDXCC = true
	}
}

// SetGrid2Prefix enables or disables a specific 2-character grid prefix.
// Tokens longer than two characters are truncated to the first two; invalid
// tokens are ignored.
func (f *Filter) SetDXGrid2Prefix(grid string, enabled bool) {
	token := normalizeGrid2Token(grid)
	if token == "" {
		return
	}
	if f.DXGrid2Prefixes == nil {
		f.DXGrid2Prefixes = make(map[string]bool)
	}
	if f.BlockDXGrid2 == nil {
		f.BlockDXGrid2 = make(map[string]bool)
	}
	if enabled {
		f.DXGrid2Prefixes[token] = true
		delete(f.BlockDXGrid2, token)
		f.BlockAllDXGrid2 = false
		f.AllDXGrid2 = len(f.DXGrid2Prefixes) == 0
		return
	}
	delete(f.DXGrid2Prefixes, token)
	f.BlockDXGrid2[token] = true
	f.BlockAllDXGrid2 = false
	f.AllDXGrid2 = len(f.DXGrid2Prefixes) == 0
}

// SetDEGrid2Prefix enables or disables a specific 2-character DE grid prefix.
func (f *Filter) SetDEGrid2Prefix(grid string, enabled bool) {
	token := normalizeGrid2Token(grid)
	if token == "" {
		return
	}
	if f.DEGrid2Prefixes == nil {
		f.DEGrid2Prefixes = make(map[string]bool)
	}
	if f.BlockDEGrid2 == nil {
		f.BlockDEGrid2 = make(map[string]bool)
	}
	if enabled {
		f.DEGrid2Prefixes[token] = true
		delete(f.BlockDEGrid2, token)
		f.BlockAllDEGrid2 = false
		f.AllDEGrid2 = len(f.DEGrid2Prefixes) == 0
		return
	}
	delete(f.DEGrid2Prefixes, token)
	f.BlockDEGrid2[token] = true
	f.BlockAllDEGrid2 = false
	f.AllDEGrid2 = len(f.DEGrid2Prefixes) == 0
}

// ResetDXGrid2 clears the DX 2-character grid whitelist and accepts all.
func (f *Filter) ResetDXGrid2() {
	f.DXGrid2Prefixes = make(map[string]bool)
	f.AllDXGrid2 = true
	f.BlockDXGrid2 = make(map[string]bool)
	f.BlockAllDXGrid2 = false
}

// ResetDEGrid2 clears the DE 2-character grid whitelist and accepts all.
func (f *Filter) ResetDEGrid2() {
	f.DEGrid2Prefixes = make(map[string]bool)
	f.AllDEGrid2 = true
	f.BlockDEGrid2 = make(map[string]bool)
	f.BlockAllDEGrid2 = false
}

func normalizeGrid2Token(grid string) string {
	grid = strings.ToUpper(strings.TrimSpace(grid))
	if grid == "" {
		return ""
	}
	if len(grid) > 2 {
		grid = grid[:2]
	}
	if len(grid) != 2 {
		return ""
	}
	return grid
}

// prefix2 returns the first two characters of an already-normalized grid prefix.
func prefix2(grid2 string) string {
	if len(grid2) < 2 {
		return ""
	}
	return grid2[:2]
}
