// Package filter implements per-client spot filtering for the DX Cluster Server.
//
// Filters allow users to customize which spots they receive based on:
//   - Band (e.g., 20m, 40m, 160m)
//   - Mode (e.g., CW, USB, JS8, SSTV, FT8, RTTY)
//   - Callsign patterns (e.g., W1*, LZ5VV, *ABC) for DX and DE calls
//   - Source category (HUMAN vs SKIMMER/automated)
//   - Path reliability class (HIGH/MEDIUM/LOW/UNLIKELY/INSUFFICIENT)
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
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"dxcluster/pathreliability"
	"dxcluster/spot"
	"dxcluster/strutil"
)

// SupportedModes lists the commonly used modes that users can enable/disable
// via the PASS MODE command. Exported so UI/commands can display them.
var SupportedModes = []string{
	"CW",
	"FT4",
	"FT8",
	"JS8",
	"LSB",
	"USB",
	"RTTY",
	"MSK144",
	"PSK",
	"SSTV",
}

// Purpose: Return a pointer to the provided bool.
// Key aspects: Convenience helper for optional settings.
// Upstream: NewFilter and default normalization.
// Downstream: None.
func boolPtr(value bool) *bool {
	return &value
}

// canonicalModeToken collapses PSK variants into the PSK family while keeping
// other modes uppercase/trimmed.
func canonicalModeToken(mode string) string {
	mode = strutil.NormalizeUpper(mode)
	if canonical, _, ok := spot.CanonicalPSKMode(mode); ok {
		return canonical
	}
	return mode
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
		m[strutil.NormalizeUpper(s)] = true
	}
	return m
}()

var supportedSourceSet = func() map[string]bool {
	m := make(map[string]bool, len(SupportedSources))
	for _, source := range SupportedSources {
		s := strutil.NormalizeUpper(source)
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

const (
	PathClassHigh         = "HIGH"
	PathClassMedium       = "MEDIUM"
	PathClassLow          = "LOW"
	PathClassUnlikely     = "UNLIKELY"
	PathClassInsufficient = "INSUFFICIENT"
)

// SupportedPathClasses enumerates the path reliability class labels that users can filter on.
var SupportedPathClasses = []string{
	PathClassHigh,
	PathClassMedium,
	PathClassLow,
	PathClassUnlikely,
	PathClassInsufficient,
}

var supportedPathClassSet = func() map[string]bool {
	m := make(map[string]bool, len(SupportedPathClasses))
	for _, class := range SupportedPathClasses {
		key := strutil.NormalizeUpper(class)
		if key == "" {
			continue
		}
		m[key] = true
	}
	return m
}()

// IsSupportedMode reports whether a mode is supported for filtering.
// Key aspects: Normalizes the input before lookup.
// Upstream: Telnet filter parsing and validation.
// Downstream: supportedModeSet.
func IsSupportedMode(mode string) bool {
	mode = canonicalModeToken(mode)
	return supportedModeSet[mode]
}

// IsSupportedSource reports whether a source label is supported for filtering.
// Key aspects: Normalizes the input before lookup.
// Upstream: Telnet filter parsing and validation.
// Downstream: supportedSourceSet.
func IsSupportedSource(source string) bool {
	source = strutil.NormalizeUpper(source)
	return supportedSourceSet[source]
}

// Purpose: Normalize a source label to a supported canonical value.
// Key aspects: Returns empty string when unsupported.
// Upstream: Filter.SetSource, SetDefaultSourceSelection.
// Downstream: supportedSourceSet.
func normalizeSource(source string) string {
	source = strutil.NormalizeUpper(source)
	if supportedSourceSet[source] {
		return source
	}
	return ""
}

// IsSupportedContinent reports whether a continent code is supported.
// Key aspects: Normalizes the input before lookup.
// Upstream: Telnet filter parsing and validation.
// Downstream: supportedContinentSet.
func IsSupportedContinent(cont string) bool {
	cont = strutil.NormalizeUpper(cont)
	return supportedContinentSet[cont]
}

// IsSupportedZone reports whether a CQ zone is within the supported range.
// Key aspects: Inclusive min/max bounds.
// Upstream: Filter validation and matching.
// Downstream: minCQZone/maxCQZone constants.
func IsSupportedZone(zone int) bool {
	return zone >= minCQZone && zone <= maxCQZone
}

// MinCQZone returns the minimum supported CQ zone.
// Key aspects: Exposes constant for UI/validation.
// Upstream: Telnet filter formatting.
// Downstream: minCQZone constant.
func MinCQZone() int {
	return minCQZone
}

// MaxCQZone returns the maximum supported CQ zone.
// Key aspects: Exposes constant for UI/validation.
// Upstream: Telnet filter formatting.
// Downstream: maxCQZone constant.
func MaxCQZone() int {
	return maxCQZone
}

// SetDefaultModeSelection configures the default mode whitelist for new filters.
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
		candidate := canonicalModeToken(mode)
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

// SetDefaultSourceSelection configures the default source categories for new filters.
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
		candidate := strutil.NormalizeUpper(source)
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

// SaveUserFilter persists a user's filter while preserving metadata.
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

// LoadUserFilter loads the saved filter for a callsign.
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

// EnsureUserDataDir ensures the per-user data directory exists.
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
//  2. Mode filters: Which operating modes to accept (CW, USB, JS8, SSTV, FT8, etc.)
//  3. Callsign patterns: Allow/block DX/DE callsigns (W1*, LZ5VV, etc.)
//  4. Confidence glyphs: Which consensus indicators (?, S, C, P, V, B) to accept.
//  5. Path reliability class: Which prediction classes (HIGH/MEDIUM/LOW/UNLIKELY/INSUFFICIENT) to accept.
//  6. Beacon inclusion: Whether DX calls ending in /B (beacons) should be delivered.
//  7. Source category: Whether to deliver HUMAN spots, SKIMMER (automated) spots, or both.
//
// Default Behavior:
//   - AllBands=true: accept every band
//   - AllModes=false with the curated default mode list pre-enabled
//   - Callsign patterns: Allowlist applies only if non-empty; blocklist always denies
//   - AllConfidence=true: accept every consensus glyph until specific ones are enabled
//   - AllPathClasses=true: accept every path class until specific ones are enabled
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
	DXCallsigns          []string        `yaml:"callsigns,omitempty"` // DX callsign patterns (e.g., ["W1*", "LZ5VV"])
	BlockDXCallsigns     []string        `yaml:"block_callsigns,omitempty"`
	DECallsigns          []string        `yaml:"decallsigns,omitempty"` // DE callsign patterns
	BlockDECallsigns     []string        `yaml:"block_decallsigns,omitempty"`
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
	PathClasses          map[string]bool // Allowed path reliability classes
	BlockPathClasses     map[string]bool // Blocked path reliability classes
	AllPathClasses       bool            // If true, accept all path classes (except blocked)
	BlockAllPathClasses  bool            // If true, reject all path classes
	IncludeBeacons       *bool           `yaml:"include_beacons,omitempty"` // nil/true delivers beacons; false suppresses
	AllowWWV             *bool           `yaml:"allow_wwv,omitempty"`       // nil/true delivers WWV bulletins; false suppresses
	AllowWCY             *bool           `yaml:"allow_wcy,omitempty"`       // nil/true delivers WCY bulletins; false suppresses
	AllowAnnounce        *bool           `yaml:"allow_announce,omitempty"`  // nil/true delivers PC93 announcements; false suppresses
	AllowSelf            *bool           `yaml:"allow_self,omitempty"`      // nil/true delivers self DX-call spots; false suppresses
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

	// NearbyEnabled toggles H3 nearby matching that bypasses location filters.
	// This state is persisted per user.
	NearbyEnabled bool `yaml:"nearby_enabled,omitempty"`
	// NearbySnapshot stores the prior location filter state to restore on disable.
	NearbySnapshot *NearbyLocationSnapshot `yaml:"-"`
	// NearbyUserFine stores the user's H3 res-2 cell for nearby comparisons.
	NearbyUserFine pathreliability.CellID `yaml:"-"`
	// NearbyUserCoarse stores the user's H3 res-1 cell for nearby comparisons.
	NearbyUserCoarse pathreliability.CellID `yaml:"-"`

	// LegacyMinConfidence captures the old percentage-based filter persisted to
	// YAML so we can migrate user data to the new glyph-based approach.
	LegacyMinConfidence int `yaml:"minconfidence,omitempty"`
}

// NearbyLocationSnapshot stores location filter state for restoration when
// PASS NEARBY is disabled. It is not persisted.
type NearbyLocationSnapshot struct {
	DXContinents         map[string]bool
	BlockDXContinents    map[string]bool
	DEContinents         map[string]bool
	BlockDEContinents    map[string]bool
	AllDXContinents      bool
	BlockAllDXContinents bool
	AllDEContinents      bool
	BlockAllDEContinents bool

	DXZones         map[int]bool
	BlockDXZones    map[int]bool
	DEZones         map[int]bool
	BlockDEZones    map[int]bool
	AllDXZones      bool
	BlockAllDXZones bool
	AllDEZones      bool
	BlockAllDEZones bool

	DXGrid2Prefixes map[string]bool
	BlockDXGrid2    map[string]bool
	DEGrid2Prefixes map[string]bool
	BlockDEGrid2    map[string]bool
	AllDXGrid2      bool
	BlockAllDXGrid2 bool
	AllDEGrid2      bool
	BlockAllDEGrid2 bool

	DXDXCC         map[int]bool
	BlockDXDXCC    map[int]bool
	DEDXCC         map[int]bool
	BlockDEDXCC    map[int]bool
	AllDXDXCC      bool
	BlockAllDXDXCC bool
	AllDEDXCC      bool
	BlockAllDEDXCC bool
}

// NewFilter constructs a new filter with defaults applied.
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
		BlockDXCallsigns:     make([]string, 0),
		DECallsigns:          make([]string, 0),
		BlockDECallsigns:     make([]string, 0),
		Confidence:           make(map[string]bool),
		BlockConfidence:      make(map[string]bool),
		PathClasses:          make(map[string]bool),
		BlockPathClasses:     make(map[string]bool),
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
		AllowSelf:            boolPtr(true),
		AllConfidence:        true, // Accept every confidence glyph until user sets one
		BlockAllConfidence:   false,
		AllPathClasses:       true, // Accept every path class until user sets one
		BlockAllPathClasses:  false,
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
		NearbyEnabled:        false,
		NearbySnapshot:       nil,
		NearbyUserFine:       pathreliability.InvalidCell,
		NearbyUserCoarse:     pathreliability.InvalidCell,
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

func ensureBoolMap[K comparable](m *map[K]bool) {
	if *m == nil {
		*m = make(map[K]bool)
	}
}

// applyAllowBlockToggle centralizes deny-first allow/block mutation for map-backed filters.
// Invariant: allowAll reflects whether explicit allow entries exist after mutation.
func applyAllowBlockToggle[K comparable](allow *map[K]bool, block *map[K]bool, token K, enabled bool, allowAll *bool, blockAll *bool) {
	ensureBoolMap(allow)
	ensureBoolMap(block)
	if enabled {
		(*allow)[token] = true
		delete(*block, token)
		*blockAll = false
		*allowAll = len(*allow) == 0
		return
	}
	delete(*allow, token)
	(*block)[token] = true
	*blockAll = false
	*allowAll = len(*allow) == 0
}

// SetBand allows or blocks a specific band.
// Key aspects: Normalizes band, updates allow/block lists with deny precedence.
// Upstream: Telnet PASS/REJECT BAND commands.
// Downstream: spot.NormalizeBand, spot.IsValidBand.
func (f *Filter) SetBand(band string, enabled bool) {
	normalized := spot.NormalizeBand(band)
	if normalized == "" || !spot.IsValidBand(normalized) {
		return
	}
	applyAllowBlockToggle(&f.Bands, &f.BlockBands, normalized, enabled, &f.AllBands, &f.BlockAllBands)
}

// SetMode allows or blocks a specific mode.
// Key aspects: Updates allow/block lists and AllModes/BlockAllModes flags.
// Upstream: Telnet PASS/REJECT MODE commands.
// Downstream: None.
func (f *Filter) SetMode(mode string, enabled bool) {
	mode = canonicalModeToken(mode)
	applyAllowBlockToggle(&f.Modes, &f.BlockModes, mode, enabled, &f.AllModes, &f.BlockAllModes)
}

// SetSource allows or blocks a specific source category.
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
	applyAllowBlockToggle(&f.Sources, &f.BlockSources, source, enabled, &f.AllSources, &f.BlockAllSources)
}

// AddDXCallsignPattern adds a DX callsign pattern to the allowlist.
// Key aspects: Stores uppercase patterns; OR semantics across patterns.
// Upstream: Telnet PASS DXCALL commands.
// Downstream: None.
func (f *Filter) AddDXCallsignPattern(pattern string) {
	pattern = normalizeCallsignPattern(pattern)
	if pattern == "" {
		return
	}
	f.DXCallsigns = addPatternUnique(f.DXCallsigns, pattern)
	f.BlockDXCallsigns = removePattern(f.BlockDXCallsigns, pattern)
}

// AddDECallsignPattern adds a DE callsign pattern to the allowlist.
// Key aspects: Stores uppercase patterns; OR semantics across patterns.
// Upstream: Telnet PASS DECALL commands.
// Downstream: None.
func (f *Filter) AddDECallsignPattern(pattern string) {
	pattern = normalizeCallsignPattern(pattern)
	if pattern == "" {
		return
	}
	f.DECallsigns = addPatternUnique(f.DECallsigns, pattern)
	f.BlockDECallsigns = removePattern(f.BlockDECallsigns, pattern)
}

// AddBlockDXCallsignPattern adds a DX callsign pattern to the blocklist.
// Key aspects: Stores uppercase patterns; blocklist wins over allowlist.
// Upstream: Telnet REJECT DXCALL list commands.
// Downstream: None.
func (f *Filter) AddBlockDXCallsignPattern(pattern string) {
	pattern = normalizeCallsignPattern(pattern)
	if pattern == "" {
		return
	}
	f.BlockDXCallsigns = addPatternUnique(f.BlockDXCallsigns, pattern)
	f.DXCallsigns = removePattern(f.DXCallsigns, pattern)
}

// AddBlockDECallsignPattern adds a DE callsign pattern to the blocklist.
// Key aspects: Stores uppercase patterns; blocklist wins over allowlist.
// Upstream: Telnet REJECT DECALL list commands.
// Downstream: None.
func (f *Filter) AddBlockDECallsignPattern(pattern string) {
	pattern = normalizeCallsignPattern(pattern)
	if pattern == "" {
		return
	}
	f.BlockDECallsigns = addPatternUnique(f.BlockDECallsigns, pattern)
	f.DECallsigns = removePattern(f.DECallsigns, pattern)
}

func normalizeCallsignPattern(pattern string) string {
	pattern = strutil.NormalizeUpper(pattern)
	return pattern
}

func addPatternUnique(list []string, pattern string) []string {
	for _, existing := range list {
		if existing == pattern {
			return list
		}
	}
	return append(list, pattern)
}

func removePattern(list []string, pattern string) []string {
	if len(list) == 0 {
		return list
	}
	out := list[:0]
	for _, existing := range list {
		if existing == pattern {
			continue
		}
		out = append(out, existing)
	}
	return out
}

// SetDXContinent allows or blocks a DX continent code.
// Key aspects: Normalizes to uppercase and updates allow/block flags.
// Upstream: Telnet PASS/REJECT DXCONT commands.
// Downstream: IsSupportedContinent.
func (f *Filter) SetDXContinent(cont string, enabled bool) {
	cont = strutil.NormalizeUpper(cont)
	if !IsSupportedContinent(cont) {
		return
	}
	applyAllowBlockToggle(&f.DXContinents, &f.BlockDXContinents, cont, enabled, &f.AllDXContinents, &f.BlockAllDXContinents)
}

// SetDEContinent allows or blocks a DE continent code.
// Key aspects: Normalizes to uppercase and updates allow/block flags.
// Upstream: Telnet PASS/REJECT DECONT commands.
// Downstream: IsSupportedContinent.
func (f *Filter) SetDEContinent(cont string, enabled bool) {
	cont = strutil.NormalizeUpper(cont)
	if !IsSupportedContinent(cont) {
		return
	}
	applyAllowBlockToggle(&f.DEContinents, &f.BlockDEContinents, cont, enabled, &f.AllDEContinents, &f.BlockAllDEContinents)
}

// SetDXZone allows or blocks a DX CQ zone.
// Key aspects: Validates zone range and updates allow/block flags.
// Upstream: Telnet PASS/REJECT DXZONE commands.
// Downstream: IsSupportedZone.
func (f *Filter) SetDXZone(zone int, enabled bool) {
	if !IsSupportedZone(zone) {
		return
	}
	applyAllowBlockToggle(&f.DXZones, &f.BlockDXZones, zone, enabled, &f.AllDXZones, &f.BlockAllDXZones)
}

// SetDEZone allows or blocks a DE CQ zone.
// Key aspects: Validates zone range and updates allow/block flags.
// Upstream: Telnet PASS/REJECT DEZONE commands.
// Downstream: IsSupportedZone.
func (f *Filter) SetDEZone(zone int, enabled bool) {
	if !IsSupportedZone(zone) {
		return
	}
	applyAllowBlockToggle(&f.DEZones, &f.BlockDEZones, zone, enabled, &f.AllDEZones, &f.BlockAllDEZones)
}

// SetDXDXCC allows or blocks a DX ADIF/DXCC code.
// Key aspects: Updates allow/block lists with deny precedence.
// Upstream: Telnet PASS/REJECT DXDXCC commands.
// Downstream: None.
func (f *Filter) SetDXDXCC(code int, enabled bool) {
	if code <= 0 {
		return
	}
	applyAllowBlockToggle(&f.DXDXCC, &f.BlockDXDXCC, code, enabled, &f.AllDXDXCC, &f.BlockAllDXDXCC)
}

// SetDEDXCC allows or blocks a DE ADIF/DXCC code.
// Key aspects: Updates allow/block lists with deny precedence.
// Upstream: Telnet PASS/REJECT DEDXCC commands.
// Downstream: None.
func (f *Filter) SetDEDXCC(code int, enabled bool) {
	if code <= 0 {
		return
	}
	applyAllowBlockToggle(&f.DEDXCC, &f.BlockDEDXCC, code, enabled, &f.AllDEDXCC, &f.BlockAllDEDXCC)
}

// ClearDXCallsignPatterns clears all DX callsign allowlist patterns.
// Key aspects: Resets the DX allowlist slice to empty.
// Upstream: Telnet REJECT DXCALL (no args) flows.
// Downstream: None.
func (f *Filter) ClearDXCallsignPatterns() {
	f.DXCallsigns = make([]string, 0)
}

// ClearDECallsignPatterns clears all DE callsign allowlist patterns.
// Key aspects: Resets the DE allowlist slice to empty.
// Upstream: Telnet REJECT DECALL (no args) flows.
// Downstream: None.
func (f *Filter) ClearDECallsignPatterns() {
	f.DECallsigns = make([]string, 0)
}

// ClearDXCallsignBlockPatterns clears all DX callsign blocklist patterns.
// Key aspects: Resets the DX blocklist slice to empty.
// Upstream: Telnet reset flows.
// Downstream: None.
func (f *Filter) ClearDXCallsignBlockPatterns() {
	f.BlockDXCallsigns = make([]string, 0)
}

// ClearDECallsignBlockPatterns clears all DE callsign blocklist patterns.
// Key aspects: Resets the DE blocklist slice to empty.
// Upstream: Telnet reset flows.
// Downstream: None.
func (f *Filter) ClearDECallsignBlockPatterns() {
	f.BlockDECallsigns = make([]string, 0)
}

// ClearCallsignPatterns clears both DX and DE callsign patterns (allow + block).
// Key aspects: Delegates to the per-direction clear helpers.
// Upstream: Telnet RESET CALLSIGN filters.
// Downstream: ClearDXCallsignPatterns, ClearDECallsignPatterns.
func (f *Filter) ClearCallsignPatterns() {
	f.ClearDXCallsignPatterns()
	f.ClearDECallsignPatterns()
	f.ClearDXCallsignBlockPatterns()
	f.ClearDECallsignBlockPatterns()
}

// SetConfidenceSymbol allows or blocks a confidence glyph.
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
	applyAllowBlockToggle(&f.Confidence, &f.BlockConfidence, canonical, enabled, &f.AllConfidence, &f.BlockAllConfidence)
}

// ResetConfidence clears confidence filters and allow all glyphs.
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

// SetPathClass allows or blocks a path reliability class.
// Key aspects: Normalizes tokens; updates allow/block lists and flags.
// Upstream: Telnet PASS/REJECT PATH commands.
// Downstream: normalizePathClass.
func (f *Filter) SetPathClass(class string, enabled bool) {
	if f == nil {
		return
	}
	canonical := normalizePathClass(class)
	if canonical == "" {
		return
	}
	applyAllowBlockToggle(&f.PathClasses, &f.BlockPathClasses, canonical, enabled, &f.AllPathClasses, &f.BlockAllPathClasses)
}

// ResetPathClasses clears path class filters and allow all classes.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet PASS/REJECT PATH ALL and RESET FILTER flows.
// Downstream: None.
func (f *Filter) ResetPathClasses() {
	f.PathClasses = make(map[string]bool)
	f.BlockPathClasses = make(map[string]bool)
	f.AllPathClasses = true
	f.BlockAllPathClasses = false
}

// SetBeaconEnabled sets whether beacon spots are delivered.
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

// BeaconsEnabled reports whether beacon spots are allowed.
// Key aspects: Defaults to true when unset.
// Upstream: Filter.Matches.
// Downstream: None.
func (f *Filter) BeaconsEnabled() bool {
	if f == nil || f.IncludeBeacons == nil {
		return true
	}
	return *f.IncludeBeacons
}

// SetWWVEnabled sets whether WWV bulletins are delivered.
// Key aspects: Stores a bool pointer to preserve tri-state.
// Upstream: Telnet PASS/REJECT WWV commands.
// Downstream: WWVEnabled.
func (f *Filter) SetWWVEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowWWV = boolPtr(enabled)
}

// SetWCYEnabled sets whether WCY bulletins are delivered.
// Key aspects: Stores a bool pointer to preserve tri-state.
// Upstream: Telnet PASS/REJECT WCY commands.
// Downstream: WCYEnabled.
func (f *Filter) SetWCYEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowWCY = boolPtr(enabled)
}

// SetAnnounceEnabled sets whether PC93 announcements are delivered.
// Key aspects: Stores a bool pointer to preserve tri-state.
// Upstream: Telnet PASS/REJECT ANNOUNCE commands.
// Downstream: AnnounceEnabled.
func (f *Filter) SetAnnounceEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowAnnounce = boolPtr(enabled)
}

// SetSelfEnabled sets whether self DX-call spots are delivered.
// Key aspects: Stores a bool pointer to preserve tri-state.
// Upstream: Telnet PASS/REJECT SELF commands.
// Downstream: SelfEnabled.
func (f *Filter) SetSelfEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowSelf = boolPtr(enabled)
}

// WWVEnabled reports whether WWV bulletins are allowed.
// Key aspects: Defaults to true when unset.
// Upstream: AllowsBulletin.
// Downstream: None.
func (f *Filter) WWVEnabled() bool {
	if f == nil || f.AllowWWV == nil {
		return true
	}
	return *f.AllowWWV
}

// WCYEnabled reports whether WCY bulletins are allowed.
// Key aspects: Defaults to true when unset.
// Upstream: AllowsBulletin.
// Downstream: None.
func (f *Filter) WCYEnabled() bool {
	if f == nil || f.AllowWCY == nil {
		return true
	}
	return *f.AllowWCY
}

// AnnounceEnabled reports whether PC93 announcements are allowed.
// Key aspects: Defaults to true when unset.
// Upstream: AllowsBulletin.
// Downstream: None.
func (f *Filter) AnnounceEnabled() bool {
	if f == nil || f.AllowAnnounce == nil {
		return true
	}
	return *f.AllowAnnounce
}

// SelfEnabled reports whether self DX-call spots are allowed.
// Key aspects: Defaults to true when unset.
// Upstream: Telnet self-spot delivery.
// Downstream: None.
func (f *Filter) SelfEnabled() bool {
	if f == nil || f.AllowSelf == nil {
		return true
	}
	return *f.AllowSelf
}

// AllowsBulletin reports whether a bulletin kind should be delivered.
// Key aspects: Maps legacy PC codes to WWV/WCY/ANNOUNCE; defaults to true.
// Upstream: Telnet bulletin broadcast.
// Downstream: WWVEnabled, WCYEnabled, AnnounceEnabled.
func (f *Filter) AllowsBulletin(kind string) bool {
	switch strutil.NormalizeUpper(kind) {
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

// ResetBands clears band filters and accept all bands.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET BAND or filter reset flows.
// Downstream: None.
func (f *Filter) ResetBands() {
	f.Bands = make(map[string]bool)
	f.AllBands = true
	f.BlockBands = make(map[string]bool)
	f.BlockAllBands = false
}

// ResetModes clears mode filters and accept all modes.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET MODE or filter reset flows.
// Downstream: None.
func (f *Filter) ResetModes() {
	f.Modes = make(map[string]bool)
	f.AllModes = true
	f.BlockModes = make(map[string]bool)
	f.BlockAllModes = false
}

// ResetSources clears source filters and allow all sources.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET SOURCE or filter reset flows.
// Downstream: None.
func (f *Filter) ResetSources() {
	f.Sources = make(map[string]bool)
	f.AllSources = true
	f.BlockSources = make(map[string]bool)
	f.BlockAllSources = false
}

// Reset resets all filter criteria back to permissive defaults.
// Key aspects: Invokes the specific reset helpers for each filter domain.
// Upstream: Telnet RESET ALL commands or new-session defaults.
// Downstream: ResetBands, ResetModes, ResetSources, ClearCallsignPatterns, ResetConfidence, Reset* helpers.
func (f *Filter) Reset() {
	f.ResetBands()
	f.ResetModes()
	f.ResetSources()
	f.ClearCallsignPatterns()
	f.ResetConfidence()
	f.ResetPathClasses()
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
	f.SetSelfEnabled(true)
	f.DisableNearby()
}

// ResetToDefaults resets all filter criteria back to configured defaults.
// Key aspects: Rebuilds defaults from the active default mode/source selections.
// Upstream: Telnet RESET FILTER command.
// Downstream: NewFilter.
func (f *Filter) ResetToDefaults() {
	if f == nil {
		return
	}
	// Replace contents in place so callers retain the same *Filter pointer.
	defaults := NewFilter()
	*f = *defaults
}

// EnableNearby enables nearby filtering with cached user H3 cells.
// Key aspects: Requires valid res-1 and res-2 cells; snapshots location filters.
// Upstream: PASS NEARBY ON.
// Downstream: MatchesWithPath (nearby evaluation).
func (f *Filter) EnableNearby(userFine, userCoarse pathreliability.CellID) error {
	if f == nil {
		return errors.New("nil filter")
	}
	if userFine == pathreliability.InvalidCell || userCoarse == pathreliability.InvalidCell {
		return errors.New("invalid user cell")
	}
	if f.NearbySnapshot == nil {
		f.NearbySnapshot = f.captureLocationSnapshot()
	}
	f.NearbyEnabled = true
	f.NearbyUserFine = userFine
	f.NearbyUserCoarse = userCoarse
	return nil
}

// DisableNearby disables nearby filtering and restore location filters.
// Key aspects: Restores snapshot when present; clears cached user cells.
// Upstream: PASS NEARBY OFF.
// Downstream: None.
func (f *Filter) DisableNearby() {
	if f == nil {
		return
	}
	if f.NearbySnapshot != nil {
		f.restoreLocationSnapshot(f.NearbySnapshot)
	}
	f.NearbyEnabled = false
	f.NearbySnapshot = nil
	f.NearbyUserFine = pathreliability.InvalidCell
	f.NearbyUserCoarse = pathreliability.InvalidCell
}

// UpdateNearbyUserCells updates cached user H3 cells for nearby filtering.
// Key aspects: No-op when filter is nil.
// Upstream: SET GRID updates.
// Downstream: MatchesWithPath.
func (f *Filter) UpdateNearbyUserCells(userFine, userCoarse pathreliability.CellID) {
	if f == nil {
		return
	}
	f.NearbyUserFine = userFine
	f.NearbyUserCoarse = userCoarse
}

// NearbyActive reports whether nearby filtering is active.
// Key aspects: Safe on nil filters.
// Upstream: Telnet command handling and filter summaries.
// Downstream: None.
func (f *Filter) NearbyActive() bool {
	if f == nil {
		return false
	}
	return f.NearbyEnabled
}

func (f *Filter) captureLocationSnapshot() *NearbyLocationSnapshot {
	if f == nil {
		return nil
	}
	return &NearbyLocationSnapshot{
		DXContinents:         copyStringBoolMap(f.DXContinents),
		BlockDXContinents:    copyStringBoolMap(f.BlockDXContinents),
		DEContinents:         copyStringBoolMap(f.DEContinents),
		BlockDEContinents:    copyStringBoolMap(f.BlockDEContinents),
		AllDXContinents:      f.AllDXContinents,
		BlockAllDXContinents: f.BlockAllDXContinents,
		AllDEContinents:      f.AllDEContinents,
		BlockAllDEContinents: f.BlockAllDEContinents,
		DXZones:              copyIntBoolMap(f.DXZones),
		BlockDXZones:         copyIntBoolMap(f.BlockDXZones),
		DEZones:              copyIntBoolMap(f.DEZones),
		BlockDEZones:         copyIntBoolMap(f.BlockDEZones),
		AllDXZones:           f.AllDXZones,
		BlockAllDXZones:      f.BlockAllDXZones,
		AllDEZones:           f.AllDEZones,
		BlockAllDEZones:      f.BlockAllDEZones,
		DXGrid2Prefixes:      copyStringBoolMap(f.DXGrid2Prefixes),
		BlockDXGrid2:         copyStringBoolMap(f.BlockDXGrid2),
		DEGrid2Prefixes:      copyStringBoolMap(f.DEGrid2Prefixes),
		BlockDEGrid2:         copyStringBoolMap(f.BlockDEGrid2),
		AllDXGrid2:           f.AllDXGrid2,
		BlockAllDXGrid2:      f.BlockAllDXGrid2,
		AllDEGrid2:           f.AllDEGrid2,
		BlockAllDEGrid2:      f.BlockAllDEGrid2,
		DXDXCC:               copyIntBoolMap(f.DXDXCC),
		BlockDXDXCC:          copyIntBoolMap(f.BlockDXDXCC),
		DEDXCC:               copyIntBoolMap(f.DEDXCC),
		BlockDEDXCC:          copyIntBoolMap(f.BlockDEDXCC),
		AllDXDXCC:            f.AllDXDXCC,
		BlockAllDXDXCC:       f.BlockAllDXDXCC,
		AllDEDXCC:            f.AllDEDXCC,
		BlockAllDEDXCC:       f.BlockAllDEDXCC,
	}
}

func (f *Filter) restoreLocationSnapshot(snapshot *NearbyLocationSnapshot) {
	if f == nil || snapshot == nil {
		return
	}
	f.DXContinents = copyStringBoolMap(snapshot.DXContinents)
	f.BlockDXContinents = copyStringBoolMap(snapshot.BlockDXContinents)
	f.DEContinents = copyStringBoolMap(snapshot.DEContinents)
	f.BlockDEContinents = copyStringBoolMap(snapshot.BlockDEContinents)
	f.AllDXContinents = snapshot.AllDXContinents
	f.BlockAllDXContinents = snapshot.BlockAllDXContinents
	f.AllDEContinents = snapshot.AllDEContinents
	f.BlockAllDEContinents = snapshot.BlockAllDEContinents

	f.DXZones = copyIntBoolMap(snapshot.DXZones)
	f.BlockDXZones = copyIntBoolMap(snapshot.BlockDXZones)
	f.DEZones = copyIntBoolMap(snapshot.DEZones)
	f.BlockDEZones = copyIntBoolMap(snapshot.BlockDEZones)
	f.AllDXZones = snapshot.AllDXZones
	f.BlockAllDXZones = snapshot.BlockAllDXZones
	f.AllDEZones = snapshot.AllDEZones
	f.BlockAllDEZones = snapshot.BlockAllDEZones

	f.DXGrid2Prefixes = copyStringBoolMap(snapshot.DXGrid2Prefixes)
	f.BlockDXGrid2 = copyStringBoolMap(snapshot.BlockDXGrid2)
	f.DEGrid2Prefixes = copyStringBoolMap(snapshot.DEGrid2Prefixes)
	f.BlockDEGrid2 = copyStringBoolMap(snapshot.BlockDEGrid2)
	f.AllDXGrid2 = snapshot.AllDXGrid2
	f.BlockAllDXGrid2 = snapshot.BlockAllDXGrid2
	f.AllDEGrid2 = snapshot.AllDEGrid2
	f.BlockAllDEGrid2 = snapshot.BlockAllDEGrid2

	f.DXDXCC = copyIntBoolMap(snapshot.DXDXCC)
	f.BlockDXDXCC = copyIntBoolMap(snapshot.BlockDXDXCC)
	f.DEDXCC = copyIntBoolMap(snapshot.DEDXCC)
	f.BlockDEDXCC = copyIntBoolMap(snapshot.BlockDEDXCC)
	f.AllDXDXCC = snapshot.AllDXDXCC
	f.BlockAllDXDXCC = snapshot.BlockAllDXDXCC
	f.AllDEDXCC = snapshot.AllDEDXCC
	f.BlockAllDEDXCC = snapshot.BlockAllDEDXCC
}

func copyStringBoolMap(src map[string]bool) map[string]bool {
	if len(src) == 0 {
		return make(map[string]bool)
	}
	dst := make(map[string]bool, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyIntBoolMap(src map[int]bool) map[int]bool {
	if len(src) == 0 {
		return make(map[int]bool)
	}
	dst := make(map[int]bool, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// ResetDXContinents clears DX continent filters and accept all.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DXCONT flows.
// Downstream: None.
func (f *Filter) ResetDXContinents() {
	f.DXContinents = make(map[string]bool)
	f.AllDXContinents = true
	f.BlockDXContinents = make(map[string]bool)
	f.BlockAllDXContinents = false
}

// ResetDEContinents clears DE continent filters and accept all.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DECONT flows.
// Downstream: None.
func (f *Filter) ResetDEContinents() {
	f.DEContinents = make(map[string]bool)
	f.AllDEContinents = true
	f.BlockDEContinents = make(map[string]bool)
	f.BlockAllDEContinents = false
}

// ResetDXZones clears DX CQ zone filters and accept all.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DXZONE flows.
// Downstream: None.
func (f *Filter) ResetDXZones() {
	f.DXZones = make(map[int]bool)
	f.AllDXZones = true
	f.BlockDXZones = make(map[int]bool)
	f.BlockAllDXZones = false
}

// ResetDEZones clears DE CQ zone filters and accept all.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DEZONE flows.
// Downstream: None.
func (f *Filter) ResetDEZones() {
	f.DEZones = make(map[int]bool)
	f.AllDEZones = true
	f.BlockDEZones = make(map[int]bool)
	f.BlockAllDEZones = false
}

// ResetDXDXCC clears DX ADIF/DXCC filters and accept all.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DXDXCC flows.
// Downstream: None.
func (f *Filter) ResetDXDXCC() {
	f.DXDXCC = make(map[int]bool)
	f.BlockDXDXCC = make(map[int]bool)
	f.AllDXDXCC = true
	f.BlockAllDXDXCC = false
}

// ResetDEDXCC clears DE ADIF/DXCC filters and accept all.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DEDXCC flows.
// Downstream: None.
func (f *Filter) ResetDEDXCC() {
	f.DEDXCC = make(map[int]bool)
	f.BlockDEDXCC = make(map[int]bool)
	f.AllDEDXCC = true
	f.BlockAllDEDXCC = false
}

// Matches evaluates whether a spot passes all active filters.
// Key aspects: Applies deny-first allow/block logic across bands, modes, sources,
// geography, grids, confidence, path classes, and callsign patterns.
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
	return f.matchesWithPath(s, PathClassInsufficient)
}

// MatchesWithPath evaluates filters that depend on a precomputed path class.
// Callers should pass INSUFFICIENT when prediction data is unavailable.
func (f *Filter) MatchesWithPath(s *spot.Spot, pathClass string) bool {
	return f.matchesWithPath(s, pathClass)
}

func (f *Filter) matchesWithPath(s *spot.Spot, pathClass string) bool {
	// Spot must be normalized upstream; this function treats the spot as immutable.
	if s != nil && s.IsBeacon && !f.BeaconsEnabled() {
		return false
	}

	modeUpper := s.ModeNorm
	if modeUpper == "" {
		modeUpper = strutil.NormalizeUpper(s.Mode)
	}

	bandNorm := spot.NormalizeBand(s.BandNorm)
	if bandNorm == "" {
		bandNorm = spot.NormalizeBand(s.Band)
	}

	dxCont := s.DXContinentNorm
	if dxCont == "" {
		dxCont = strutil.NormalizeUpper(s.DXMetadata.Continent)
	}
	deCont := s.DEContinentNorm
	if deCont == "" {
		deCont = strutil.NormalizeUpper(s.DEMetadata.Continent)
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

	if f.NearbyEnabled {
		if !f.matchesNearby(s, bandNorm) {
			return false
		}
	} else {
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
	}

	// Apply DX callsign blocklist first (deny wins).
	if len(f.BlockDXCallsigns) > 0 {
		for _, pattern := range f.BlockDXCallsigns {
			if matchesCallsignPattern(s.DXCall, pattern) {
				return false
			}
		}
	}

	// Check DX callsign allowlist (if any are set).
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
	deCall := s.DECall
	if s.DECallStripped != "" {
		deCall = s.DECallStripped
	}

	// Apply DE callsign blocklist first (deny wins).
	if len(f.BlockDECallsigns) > 0 {
		for _, pattern := range f.BlockDECallsigns {
			if matchesCallsignPattern(deCall, pattern) {
				return false
			}
		}
	}

	if len(f.DECallsigns) > 0 {
		matched := false
		for _, pattern := range f.DECallsigns {
			if matchesCallsignPattern(deCall, pattern) {
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

	pathClass = normalizePathClass(pathClass)
	if pathClass == "" {
		pathClass = PathClassInsufficient
	}
	if !passesStringFilter(pathClass, f.PathClasses, f.BlockPathClasses, f.AllPathClasses, f.BlockAllPathClasses) {
		return false
	}

	return true // Passed all filters
}

func (f *Filter) matchesNearby(s *spot.Spot, bandNorm string) bool {
	if f == nil || s == nil {
		return false
	}
	if bandNorm == "" || bandNorm == "???" {
		bandNorm = spot.NormalizeBand(spot.FreqToBand(s.Frequency))
	}
	if bandNorm == "" || bandNorm == "???" {
		return false
	}
	useCoarse := bandNorm == "160m" || bandNorm == "80m" || bandNorm == "60m"
	userCell := f.NearbyUserFine
	if useCoarse {
		userCell = f.NearbyUserCoarse
	}
	if userCell == pathreliability.InvalidCell {
		return false
	}
	dxCell, deCell := f.nearbySpotCells(s, useCoarse)
	if dxCell == pathreliability.InvalidCell || deCell == pathreliability.InvalidCell {
		return false
	}
	return userCell == dxCell || userCell == deCell
}

func (f *Filter) nearbySpotCells(s *spot.Spot, useCoarse bool) (pathreliability.CellID, pathreliability.CellID) {
	if s == nil {
		return pathreliability.InvalidCell, pathreliability.InvalidCell
	}
	dxGrid := strings.TrimSpace(s.DXMetadata.Grid)
	deGrid := strings.TrimSpace(s.DEMetadata.Grid)
	if useCoarse {
		if dxGrid == "" || deGrid == "" {
			return pathreliability.InvalidCell, pathreliability.InvalidCell
		}
		return pathreliability.EncodeCoarseCell(dxGrid),
			pathreliability.EncodeCoarseCell(deGrid)
	}
	if dxGrid == "" || deGrid == "" {
		return pathreliability.InvalidCell, pathreliability.InvalidCell
	}
	dxCell := pathreliability.InvalidCell
	if s.DXCellID != 0 {
		dxCell = pathreliability.CellID(s.DXCellID)
	}
	if dxCell == pathreliability.InvalidCell {
		dxCell = pathreliability.EncodeCell(dxGrid)
	}
	deCell := pathreliability.InvalidCell
	if s.DECellID != 0 {
		deCell = pathreliability.CellID(s.DECellID)
	}
	if deCell == pathreliability.InvalidCell {
		deCell = pathreliability.EncodeCell(deGrid)
	}
	return dxCell, deCell
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
	switch strutil.NormalizeUpper(mode) {
	case "FT8", "FT4", "PSK", "MSK144":
		return true
	default:
		return false
	}
}

// String render a human-readable summary of active filters.
// Key aspects: Describes allow/block state for each filter category.
// Upstream: Legacy summaries/diagnostics; telnet SHOW FILTER uses a dedicated snapshot formatter.
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
//   - Callsign filter: "DXCallsigns: allow=W1* block=K1*"
//   - Empty filter: "Bands: NONE (no spots will pass)"
//
// This is a legacy summary and does not include the full SHOW FILTER snapshot output.
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
	if len(f.DXCallsigns) > 0 || len(f.BlockDXCallsigns) > 0 {
		parts = append(parts, formatCallsignSummary("DXCallsigns", f.DXCallsigns, f.BlockDXCallsigns))
	}
	if len(f.DECallsigns) > 0 || len(f.BlockDECallsigns) > 0 {
		parts = append(parts, formatCallsignSummary("DECallsigns", f.DECallsigns, f.BlockDECallsigns))
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

	// Describe path reliability class filter.
	if f.AllPathClasses || len(f.PathClasses) == 0 {
		parts = append(parts, "Path: ALL")
	} else {
		parts = append(parts, "Path: "+strings.Join(enabledPathClasses(f.PathClasses), ", "))
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
	if f.SelfEnabled() {
		parts = append(parts, "SELF: ON")
	} else {
		parts = append(parts, "SELF: OFF")
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

func formatCallsignSummary(label string, allow, block []string) string {
	allowLabel := "ALL"
	if len(allow) > 0 {
		allowLabel = strings.Join(allow, ", ")
	}
	blockLabel := "NONE"
	if len(block) > 0 {
		blockLabel = strings.Join(block, ", ")
	}
	for _, pattern := range block {
		if strings.TrimSpace(pattern) == "*" {
			blockLabel = "ALL"
			break
		}
	}
	return fmt.Sprintf("%s: allow=%s block=%s", label, allowLabel, blockLabel)
}

// EnabledConfidenceSymbols returns whitelisted confidence glyphs in display order.
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

// ConfidenceSymbolEnabled reports whether a confidence glyph is allowed.
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

// Purpose: Return sorted path class labels.
// Key aspects: Returns "NONE" when the map is empty.
// Upstream: Filter.String.
// Downstream: sort.Strings.
func enabledPathClasses(m map[string]bool) []string {
	if len(m) == 0 {
		return []string{"NONE"}
	}
	out := make([]string, 0, len(m))
	for class := range m {
		out = append(out, class)
	}
	sort.Strings(out)
	return out
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

// IsSupportedConfidenceSymbol reports whether a confidence glyph is supported.
// Key aspects: Normalizes input before lookup.
// Upstream: Telnet filter validation.
// Downstream: supportedConfidenceSymbolSet.
func IsSupportedConfidenceSymbol(symbol string) bool {
	normalized := strutil.NormalizeUpper(symbol)
	if normalized == "" {
		return false
	}
	return supportedConfidenceSymbolSet[normalized]
}

// IsSupportedPathClass reports whether a path class label is supported.
// Key aspects: Normalizes the input before lookup.
// Upstream: Telnet PATH filter parsing and validation.
// Downstream: supportedPathClassSet.
func IsSupportedPathClass(class string) bool {
	normalized := normalizePathClass(class)
	if normalized == "" {
		return false
	}
	return supportedPathClassSet[normalized]
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

// normalizePathClass trims and uppercases a path class token, returning empty when unsupported.
func normalizePathClass(label string) string {
	normalized := strutil.NormalizeUpper(label)
	if normalized == "" {
		return ""
	}
	if supportedPathClassSet[normalized] {
		return normalized
	}
	return ""
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

// PathFilterActive reports whether PATH filtering requires a classification lookup.
// Key aspects: Returns true when allow/block lists are active or block-all is set.
// Upstream: Telnet broadcast filters and history queries.
// Downstream: None.
func (f *Filter) PathFilterActive() bool {
	if f == nil {
		return false
	}
	if f.BlockAllPathClasses {
		return true
	}
	if !f.AllPathClasses {
		return true
	}
	return len(f.PathClasses) > 0 || len(f.BlockPathClasses) > 0
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
	if len(f.PathClasses) == 0 {
		f.AllPathClasses = true
	}
}

// migratePSKModes collapses legacy PSK31/63/125 entries into the canonical PSK
// family so filters align with the new single-mode surface.
func (f *Filter) migratePSKModes() {
	if f == nil {
		return
	}
	if f.Modes == nil {
		f.Modes = make(map[string]bool)
	}
	if f.BlockModes == nil {
		f.BlockModes = make(map[string]bool)
	}

	for mode := range f.Modes {
		if canonical, _, ok := spot.CanonicalPSKMode(mode); ok && canonical != "" && canonical != mode {
			f.Modes[canonical] = true
			delete(f.Modes, mode)
		}
	}
	for mode := range f.BlockModes {
		if canonical, _, ok := spot.CanonicalPSKMode(mode); ok && canonical != "" && canonical != mode {
			f.BlockModes[canonical] = true
			delete(f.BlockModes, mode)
		}
	}

	f.AllModes = len(f.Modes) == 0
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
	if f.DXCallsigns == nil {
		f.DXCallsigns = make([]string, 0)
	}
	if f.BlockDXCallsigns == nil {
		f.BlockDXCallsigns = make([]string, 0)
	}
	if f.DECallsigns == nil {
		f.DECallsigns = make([]string, 0)
	}
	if f.BlockDECallsigns == nil {
		f.BlockDECallsigns = make([]string, 0)
	}
	if f.Confidence == nil {
		f.Confidence = make(map[string]bool)
	}
	if f.BlockConfidence == nil {
		f.BlockConfidence = make(map[string]bool)
	}
	if f.PathClasses == nil {
		f.PathClasses = make(map[string]bool)
	}
	if f.BlockPathClasses == nil {
		f.BlockPathClasses = make(map[string]bool)
	}
	f.migratePSKModes()
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
	if f.AllowSelf == nil {
		f.AllowSelf = boolPtr(true)
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
	if len(f.PathClasses) == 0 {
		f.AllPathClasses = true
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
	f.NearbySnapshot = nil
	f.NearbyUserFine = pathreliability.InvalidCell
	f.NearbyUserCoarse = pathreliability.InvalidCell
}

// SetDXGrid2Prefix allows or blocks a DX 2-character grid prefix.
// Key aspects: Normalizes to two characters; updates allow/block maps.
// Upstream: Telnet PASS/REJECT DXGRID2 commands.
// Downstream: normalizeGrid2Token.
func (f *Filter) SetDXGrid2Prefix(grid string, enabled bool) {
	token := normalizeGrid2Token(grid)
	if token == "" {
		return
	}
	applyAllowBlockToggle(&f.DXGrid2Prefixes, &f.BlockDXGrid2, token, enabled, &f.AllDXGrid2, &f.BlockAllDXGrid2)
}

// SetDEGrid2Prefix allows or blocks a DE 2-character grid prefix.
// Key aspects: Normalizes to two characters; updates allow/block maps.
// Upstream: Telnet PASS/REJECT DEGRID2 commands.
// Downstream: normalizeGrid2Token.
func (f *Filter) SetDEGrid2Prefix(grid string, enabled bool) {
	token := normalizeGrid2Token(grid)
	if token == "" {
		return
	}
	applyAllowBlockToggle(&f.DEGrid2Prefixes, &f.BlockDEGrid2, token, enabled, &f.AllDEGrid2, &f.BlockAllDEGrid2)
}

// ResetDXGrid2 clears DX grid filters and accept all grids.
// Key aspects: Resets allow/block maps and flags.
// Upstream: Telnet RESET DXGRID2 flows.
// Downstream: None.
func (f *Filter) ResetDXGrid2() {
	f.DXGrid2Prefixes = make(map[string]bool)
	f.AllDXGrid2 = true
	f.BlockDXGrid2 = make(map[string]bool)
	f.BlockAllDXGrid2 = false
}

// ResetDEGrid2 clears DE grid filters and accept all grids.
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
	grid = strutil.NormalizeUpper(grid)
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
