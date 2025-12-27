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

// Purpose: Return a pointer to the provided bool.
// Key aspects: Convenience helper for optional settings.
// Upstream: NewFilter and default normalization.
// Downstream: None.
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

// Purpose: Check whether a mode is supported for filtering.
// Key aspects: Normalizes the input before lookup.
// Upstream: Telnet filter parsing and validation.
// Downstream: supportedModeSet.
func IsSupportedMode(mode string) bool {
	mode = strings.ToUpper(strings.TrimSpace(mode))
	return supportedModeSet[mode]
}

// Purpose: Check whether a source label is supported for filtering.
// Key aspects: Normalizes the input before lookup.
// Upstream: Telnet filter parsing and validation.
// Downstream: supportedSourceSet.
func IsSupportedSource(source string) bool {
	source = strings.ToUpper(strings.TrimSpace(source))
	return supportedSourceSet[source]
}

// Purpose: Normalize a source label to a supported canonical value.
// Key aspects: Returns empty string when unsupported.
// Upstream: Filter.SetSource, SetDefaultSourceSelection.
// Downstream: supportedSourceSet.
func normalizeSource(source string) string {
	source = strings.ToUpper(strings.TrimSpace(source))
	if supportedSourceSet[source] {
		return source
	}
	return ""
}

// Purpose: Check whether a continent code is supported.
// Key aspects: Normalizes the input before lookup.
// Upstream: Telnet filter parsing and validation.
// Downstream: supportedContinentSet.
func IsSupportedContinent(cont string) bool {
	cont = strings.ToUpper(strings.TrimSpace(cont))
	return supportedContinentSet[cont]
}

// Purpose: Validate CQ zone range.
// Key aspects: Inclusive min/max bounds.
// Upstream: Filter validation and matching.
// Downstream: minCQZone/maxCQZone constants.
func IsSupportedZone(zone int) bool {
	return zone >= minCQZone && zone <= maxCQZone
}

// Purpose: Return the minimum supported CQ zone.
// Key aspects: Exposes constant for UI/validation.
// Upstream: Telnet filter formatting.
// Downstream: minCQZone constant.
func MinCQZone() int {
	return minCQZone
}

// Purpose: Return the maximum supported CQ zone.
// Key aspects: Exposes constant for UI/validation.
// Upstream: Telnet filter formatting.
// Downstream: maxCQZone constant.
func MaxCQZone() int {
	return maxCQZone
}

// Purpose: Configure the default mode whitelist for new filters.
// Key aspects: Normalizes inputs; empty slice resets to the built-in defaults.
// Upstream: Config load or admin overrides.
// Downstream: defaultModeSelection.
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

// Purpose: Configure the default source categories for new filters.
// Key aspects: Normalizes input; "ALL" or both categories disables source filtering.
// Upstream: Config load or admin overrides.
// Downstream: defaultSourceSelection.
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

// Purpose: Persist a user's filter while preserving metadata.
// Key aspects: Loads existing record to merge recent IP history.
// Upstream: Telnet client save flows.
// Downstream: LoadUserRecord, SaveUserRecord.
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

// Purpose: Load the saved filter for a callsign.
// Key aspects: Returns os.ErrNotExist when no record exists.
// Upstream: Telnet client login/restore.
// Downstream: LoadUserRecord.
func LoadUserFilter(callsign string) (*Filter, error) {
	record, err := LoadUserRecord(callsign)
	if err != nil {
		return nil, err
	}
	filter := record.Filter
	return &filter, nil
}

// Purpose: Ensure the per-user data directory exists.
// Key aspects: Creates the directory tree with default permissions.
// Upstream: Startup or save operations.
// Downstream: os.MkdirAll.
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

// Purpose: Construct a new filter with defaults applied.
// Key aspects: Starts with all bands, curated mode subset, and default sources.
// Upstream: Telnet client session initialization.
// Downstream: Filter setters and default selection state.
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

// Purpose: Allow or block a specific band.
// Key aspects: Normalizes band, updates allow/block lists with deny precedence.
// Upstream: Telnet PASS/REJECT BAND commands.
// Downstream: spot.NormalizeBand, spot.IsValidBand.
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

// Purpose: Allow or block a specific mode.
// Key aspects: Updates allow/block lists and AllModes/BlockAllModes flags.
// Upstream: Telnet PASS/REJECT MODE commands.
// Downstream: None.
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

// Purpose: Allow or block a specific source category.
// Key aspects: Normalizes to HUMAN/SKIMMER; updates allow/block flags.
// Upstream: Telnet PASS/REJECT SOURCE commands.
// Downstream: normalizeSource.
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

// Purpose: Add a DX callsign pattern to the allowlist.
// Key aspects: Stores uppercase patterns; OR semantics across patterns.
// Upstream: Telnet PASS DXCALL commands.
// Downstream: None.
func (f *Filter) AddDXCallsignPattern(pattern string) {
	pattern = strings.ToUpper(pattern)
	f.DXCallsigns = append(f.DXCallsigns, pattern)
}

// Purpose: Add a DE callsign pattern to the allowlist.
// Key aspects: Stores uppercase patterns; OR semantics across patterns.
// Upstream: Telnet PASS DECALL commands.
// Downstream: None.
func (f *Filter) AddDECallsignPattern(pattern string) {
	pattern = strings.ToUpper(pattern)
	f.DECallsigns = append(f.DECallsigns, pattern)
}

// Purpose: Allow or block a DX continent code.
// Key aspects: Normalizes to uppercase and updates allow/block flags.
// Upstream: Telnet PASS/REJECT DXCONT commands.
// Downstream: IsSupportedContinent.
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

// Purpose: Allow or block a DE continent code.
// Key aspects: Normalizes to uppercase and updates allow/block flags.
// Upstream: Telnet PASS/REJECT DECONT commands.
// Downstream: IsSupportedContinent.
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

// Purpose: Allow or block a DX CQ zone.
// Key aspects: Validates zone range and updates allow/block flags.
// Upstream: Telnet PASS/REJECT DXZONE commands.
// Downstream: IsSupportedZone.
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

// Purpose: Allow or block a DE CQ zone.
// Key aspects: Validates zone range and updates allow/block flags.
// Upstream: Telnet PASS/REJECT DEZONE commands.
// Downstream: IsSupportedZone.
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

// Purpose: Allow or block a DX ADIF/DXCC code.
// Key aspects: Updates allow/block lists with deny precedence.
// Upstream: Telnet PASS/REJECT DXDXCC commands.
// Downstream: None.
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

// Purpose: Allow or block a DE ADIF/DXCC code.
// Key aspects: Updates allow/block lists with deny precedence.
// Upstream: Telnet PASS/REJECT DEDXCC commands.
// Downstream: None.
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

// Purpose: Clear all DX callsign patterns.
// Key aspects: Resets the DX pattern slice to empty.
// Upstream: Telnet RESET/REJECT DXCALL ALL flows.
// Downstream: None.
func (f *Filter) ClearDXCallsignPatterns() {
	f.DXCallsigns = make([]string, 0)
}

// Purpose: Clear all DE callsign patterns.
// Key aspects: Resets the DE pattern slice to empty.
// Upstream: Telnet RESET/REJECT DECALL ALL flows.
// Downstream: None.
func (f *Filter) ClearDECallsignPatterns() {
	f.DECallsigns = make([]string, 0)
}

// Purpose: Clear both DX and DE callsign patterns.
// Key aspects: Delegates to the per-direction clear helpers.
// Upstream: Telnet RESET CALLSIGN filters.
// Downstream: ClearDXCallsignPatterns, ClearDECallsignPatterns.
func (f *Filter) ClearCallsignPatterns() {
	f.ClearDXCallsignPatterns()
	f.ClearDECallsignPatterns()
}

// Purpose: Allow or block a confidence glyph.
// Key aspects: Normalizes glyphs; updates allow/block lists and flags.
// Upstream: Telnet PASS/REJECT CONFIDENCE commands.
// Downstream: normalizeConfidenceSymbol.
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

// Purpose: Clear confidence filters and allow all glyphs.
// Key aspects: Resets allow/block maps and legacy threshold.
// Upstream: Telnet RESET CONFIDENCE flows.
// Downstream: None.
func (f *Filter) ResetConfidence() {
	f.Confidence = make(map[string]bool)
	f.BlockConfidence = make(map[string]bool)
	f.AllConfidence = true
	f.BlockAllConfidence = false
	f.LegacyMinConfidence = 0
}

// Purpose: Set whether beacon spots are delivered.
// Key aspects: Stores an explicit bool pointer to preserve tri-state.
// Upstream: Telnet PASS/REJECT BEACON commands.
// Downstream: BeaconsEnabled.
func (f *Filter) SetBeaconEnabled(enabled bool) {
	if f == nil {
		return
	}
	value := enabled
	f.IncludeBeacons = &value
}

// Purpose: Report whether beacon spots are allowed.
// Key aspects: Defaults to true when unset.
// Upstream: Filter.Matches.
// Downstream: None.
func (f *Filter) BeaconsEnabled() bool {
	if f == nil || f.IncludeBeacons == nil {
		return true
	}
	return *f.IncludeBeacons
}

// Purpose: Set whether WWV bulletins are delivered.
// Key aspects: Stores a bool pointer to preserve tri-state.
// Upstream: Telnet PASS/REJECT WWV commands.
// Downstream: WWVEnabled.
func (f *Filter) SetWWVEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowWWV = boolPtr(enabled)
}

// Purpose: Set whether WCY bulletins are delivered.
// Key aspects: Stores a bool pointer to preserve tri-state.
// Upstream: Telnet PASS/REJECT WCY commands.
// Downstream: WCYEnabled.
func (f *Filter) SetWCYEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowWCY = boolPtr(enabled)
}

// Purpose: Set whether PC93 announcements are delivered.
// Key aspects: Stores a bool pointer to preserve tri-state.
// Upstream: Telnet PASS/REJECT ANNOUNCE commands.
// Downstream: AnnounceEnabled.
func (f *Filter) SetAnnounceEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowAnnounce = boolPtr(enabled)
}

// Purpose: Report whether WWV bulletins are allowed.
// Key aspects: Defaults to true when unset.
// Upstream: AllowsBulletin.
// Downstream: None.
func (f *Filter) WWVEnabled() bool {
	if f == nil || f.AllowWWV == nil {
		return true
	}
	return *f.AllowWWV
}

// Purpose: Report whether WCY bulletins are allowed.
// Key aspects: Defaults to true when unset.
// Upstream: AllowsBulletin.
// Downstream: None.
func (f *Filter) WCYEnabled() bool {
	if f == nil || f.AllowWCY == nil {
		return true
	}
	return *f.AllowWCY
}

// Purpose: Report whether PC93 announcements are allowed.
// Key aspects: Defaults to true when unset.
// Upstream: AllowsBulletin.
// Downstream: None.
func (f *Filter) AnnounceEnabled() bool {
	if f == nil || f.AllowAnnounce == nil {
		return true
	}
	return *f.AllowAnnounce
}

// Purpose: Decide whether a bulletin kind should be delivered.
// Key aspects: Maps legacy PC codes to WWV/WCY/ANNOUNCE; defaults to true.
// Upstream: Telnet bulletin broadcast.
// Downstream: WWVEnabled, WCYEnabled, AnnounceEnabled.
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

// Purpose: Clear band filters and accept all bands.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET BAND or filter reset flows.
// Downstream: None.
func (f *Filter) ResetBands() {
	f.Bands = make(map[string]bool)
	f.AllBands = true
	f.BlockBands = make(map[string]bool)
	f.BlockAllBands = false
}

// Purpose: Clear mode filters and accept all modes.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET MODE or filter reset flows.
// Downstream: None.
func (f *Filter) ResetModes() {
	f.Modes = make(map[string]bool)
	f.AllModes = true
	f.BlockModes = make(map[string]bool)
	f.BlockAllModes = false
}

// Purpose: Clear source filters and allow all sources.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET SOURCE or filter reset flows.
// Downstream: None.
func (f *Filter) ResetSources() {
	f.Sources = make(map[string]bool)
	f.AllSources = true
	f.BlockSources = make(map[string]bool)
	f.BlockAllSources = false
}

// Purpose: Reset all filter criteria back to permissive defaults.
// Key aspects: Invokes the specific reset helpers for each filter domain.
// Upstream: Telnet RESET ALL commands or new-session defaults.
// Downstream: ResetBands, ResetModes, ResetSources, ClearCallsignPatterns, ResetConfidence, Reset* helpers.
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

// Purpose: Clear DX continent filters and accept all.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DXCONT flows.
// Downstream: None.
func (f *Filter) ResetDXContinents() {
	f.DXContinents = make(map[string]bool)
	f.AllDXContinents = true
	f.BlockDXContinents = make(map[string]bool)
	f.BlockAllDXContinents = false
}

// Purpose: Clear DE continent filters and accept all.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DECONT flows.
// Downstream: None.
func (f *Filter) ResetDEContinents() {
	f.DEContinents = make(map[string]bool)
	f.AllDEContinents = true
	f.BlockDEContinents = make(map[string]bool)
	f.BlockAllDEContinents = false
}

// Purpose: Clear DX CQ zone filters and accept all.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DXZONE flows.
// Downstream: None.
func (f *Filter) ResetDXZones() {
	f.DXZones = make(map[int]bool)
	f.AllDXZones = true
	f.BlockDXZones = make(map[int]bool)
	f.BlockAllDXZones = false
}

// Purpose: Clear DE CQ zone filters and accept all.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DEZONE flows.
// Downstream: None.
func (f *Filter) ResetDEZones() {
	f.DEZones = make(map[int]bool)
	f.AllDEZones = true
	f.BlockDEZones = make(map[int]bool)
	f.BlockAllDEZones = false
}

// Purpose: Clear DX ADIF/DXCC filters and accept all.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DXDXCC flows.
// Downstream: None.
func (f *Filter) ResetDXDXCC() {
	f.DXDXCC = make(map[int]bool)
	f.BlockDXDXCC = make(map[int]bool)
	f.AllDXDXCC = true
	f.BlockAllDXDXCC = false
}

// Purpose: Clear DE ADIF/DXCC filters and accept all.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DEDXCC flows.
// Downstream: None.
func (f *Filter) ResetDEDXCC() {
	f.DEDXCC = make(map[int]bool)
	f.BlockDEDXCC = make(map[int]bool)
	f.AllDEDXCC = true
	f.BlockAllDEDXCC = false
}

// Purpose: Evaluate whether a spot passes all active filters.
// Key aspects: Applies deny-first allow/block logic across bands, modes, sources,
// geography, grids, confidence, and callsign patterns.
// Upstream: Telnet broadcast workers.
// Downstream: passesStringFilter, passesIntFilter, matchesCallsignPattern.
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

// Purpose: Evaluate a string token against allow/block lists.
// Key aspects: Deny-first semantics; allow list optional when allowAll is true.
// Upstream: Filter.Matches.
// Downstream: safeTrimSpace.
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

// Purpose: Trim whitespace with panic protection.
// Key aspects: Fails closed by returning empty string if panic occurs.
// Upstream: passesStringFilter.
// Downstream: strings.TrimSpace.
func safeTrimSpace(s string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = ""
		}
	}()
	out = strings.TrimSpace(s)
	return
}

// Purpose: Evaluate an integer token against allow/block lists.
// Key aspects: Deny-first semantics; optional validator controls unknown values.
// Upstream: Filter.Matches.
// Downstream: validator callback.
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

// Purpose: Match a callsign against a simple wildcard pattern.
// Key aspects: Supports leading or trailing '*' only; case-insensitive.
// Upstream: Filter.Matches.
// Downstream: strings.HasPrefix/HasSuffix.
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

// Purpose: Report whether confidence filtering should be skipped for a mode.
// Key aspects: Exempts modes that never receive confidence glyphs.
// Upstream: Filter.Matches.
// Downstream: None.
func isConfidenceExemptMode(mode string) bool {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "FT8", "FT4":
		return true
	default:
		return false
	}
}

// Purpose: Render a human-readable summary of active filters.
// Key aspects: Describes allow/block state for each filter category.
// Upstream: Telnet SHOW FILTER command output.
// Downstream: enabled* helpers and filter flags.
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

// Purpose: Return whitelisted confidence glyphs in display order.
// Key aspects: Preserves supported glyph ordering, then appends unknown ones.
// Upstream: Filter.String and UI display.
// Downstream: SupportedConfidenceSymbols.
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

// Purpose: Report whether a confidence glyph is allowed.
// Key aspects: Defaults to allowed when AllConfidence is true.
// Upstream: Filter.Matches.
// Downstream: normalizeConfidenceSymbol.
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

// Purpose: Return sorted continent labels from a map.
// Key aspects: Returns "NONE" when the map is empty.
// Upstream: Filter.String.
// Downstream: sort.Strings.
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

// Purpose: Return sorted CQ zone labels as strings.
// Key aspects: Returns "NONE" when the map is empty.
// Upstream: Filter.String.
// Downstream: sort.Ints, strconv.Itoa.
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

// Purpose: Return sorted 2-character grid prefixes.
// Key aspects: Returns "NONE" when the map is empty.
// Upstream: Filter.String.
// Downstream: sort.Strings.
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

// Purpose: Return sorted ADIF/DXCC codes as strings.
// Key aspects: Returns "NONE" when the map is empty.
// Upstream: Filter.String.
// Downstream: sort.Ints, strconv.Itoa.
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

// Purpose: Check whether a confidence glyph is supported.
// Key aspects: Normalizes input before lookup.
// Upstream: Telnet filter validation.
// Downstream: supportedConfidenceSymbolSet.
func IsSupportedConfidenceSymbol(symbol string) bool {
	normalized := strings.ToUpper(strings.TrimSpace(symbol))
	if normalized == "" {
		return false
	}
	return supportedConfidenceSymbolSet[normalized]
}

// Purpose: Normalize confidence inputs (glyphs or percentages) to glyphs.
// Key aspects: Accepts '?/S/P/V/B/C' or numeric percentages.
// Upstream: Filter.SetConfidenceSymbol, ConfidenceSymbolEnabled.
// Downstream: supportedConfidenceSymbolSet, strconv.Atoi.
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

// Purpose: Convert a numeric threshold into allowed confidence glyphs.
// Key aspects: Clamps range 0..100 and includes glyphs >= threshold score.
// Upstream: Filter.migrateLegacyConfidence.
// Downstream: confidenceSymbolScores.
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

// Purpose: Migrate legacy percentage-based confidence to glyph-based filters.
// Key aspects: Populates glyph map and clears legacy field.
// Upstream: Filter.normalizeDefaults after loading from disk.
// Downstream: confidenceSymbolsForThreshold.
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

// Purpose: Repair zero-value filters loaded from disk.
// Key aspects: Ensures maps/pointers exist and default flags are permissive.
// Upstream: LoadUserRecord or YAML decoding flow.
// Downstream: migrateLegacyConfidence, boolPtr.
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

// Purpose: Allow or block a DX 2-character grid prefix.
// Key aspects: Normalizes to two characters; updates allow/block maps.
// Upstream: Telnet PASS/REJECT DXGRID2 commands.
// Downstream: normalizeGrid2Token.
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

// Purpose: Allow or block a DE 2-character grid prefix.
// Key aspects: Normalizes to two characters; updates allow/block maps.
// Upstream: Telnet PASS/REJECT DEGRID2 commands.
// Downstream: normalizeGrid2Token.
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

// Purpose: Clear DX grid filters and accept all grids.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DXGRID2 flows.
// Downstream: None.
func (f *Filter) ResetDXGrid2() {
	f.DXGrid2Prefixes = make(map[string]bool)
	f.AllDXGrid2 = true
	f.BlockDXGrid2 = make(map[string]bool)
	f.BlockAllDXGrid2 = false
}

// Purpose: Clear DE grid filters and accept all grids.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DEGRID2 flows.
// Downstream: None.
func (f *Filter) ResetDEGrid2() {
	f.DEGrid2Prefixes = make(map[string]bool)
	f.AllDEGrid2 = true
	f.BlockDEGrid2 = make(map[string]bool)
	f.BlockAllDEGrid2 = false
}

// Purpose: Normalize a grid token to a 2-character prefix.
// Key aspects: Uppercases, trims, and rejects invalid lengths.
// Upstream: SetDXGrid2Prefix, SetDEGrid2Prefix, Filter.Matches.
// Downstream: strings helpers.
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

// Purpose: Return the first two characters of a normalized grid prefix.
// Key aspects: Returns empty string if too short.
// Upstream: Filter.Matches.
// Downstream: None.
func prefix2(grid2 string) string {
	if len(grid2) < 2 {
		return ""
	}
	return grid2[:2]
}
