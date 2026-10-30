// Package filter implements bounded, prepared Boolean spot rules and v6
// preferences. A matching REJECT vetoes; otherwise any PASS can admit a spot.
// Omitted fields impose no restriction; absence of active PASS rules is
// unrestricted. Separate delivery controls remain mandatory gates; NEARBY is
// an ordinary predicate evaluated with the caller's captured user location.
package filter

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"dxcluster/pathreliability"
	"dxcluster/spot"
	"dxcluster/strutil"
)

// UnknownModeToken is the filter-visible token for spots with no decoded mode.
const UnknownModeToken = spot.UnknownModeToken

// SupportedModes returns an owned snapshot of taxonomy-defined filter modes.
func SupportedModes() []string {
	return spot.SupportedFilterModes()
}

func boolPtr(value bool) *bool {
	return &value
}

// canonicalModeToken keeps blank spot modes as UNKNOWN.
func canonicalModeToken(mode string) string {
	return spot.CanonicalModeForFilter(mode)
}

func builtInDefaultModeSelection() []string {
	return spot.DefaultFilterModes()
}

func modeFilterTokenForSpot(s *spot.Spot) string {
	if s == nil {
		return UnknownModeToken
	}
	modeUpper := s.ModeNorm
	if modeUpper == "" {
		modeUpper = canonicalModeToken(s.Mode)
	}
	if modeUpper == "" {
		return UnknownModeToken
	}
	return modeUpper
}

// SupportedSources enumerates the human/automated spot origins.
var SupportedSources = []string{"HUMAN", "SKIMMER"}

// SupportedEvents enumerates taxonomy-defined EVENT families.
func SupportedEvents() []string {
	return spot.SupportedEvents()
}

// SupportedContinents enumerates continent codes used in DX metadata.
var SupportedContinents = []string{"AF", "AN", "AS", "EU", "NA", "OC", "SA"}

const (
	minCQZone = 1
	maxCQZone = 40
)

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

// SupportedConfidenceSymbols enumerates the glyphs emitted in the telnet stream.
var SupportedConfidenceSymbols = []string{"?", "S", "C", "P", "V", "B"}

var supportedConfidenceSymbolSet = func() map[string]bool {
	m := make(map[string]bool, len(SupportedConfidenceSymbols))
	for _, symbol := range SupportedConfidenceSymbols {
		m[symbol] = true
	}
	return m
}()

const (
	PathClassHigh         = "HIGH"
	PathClassMedium       = "MEDIUM"
	PathClassLow          = "LOW"
	PathClassUnlikely     = "UNLIKELY"
	PathClassClosed       = "CLOSED"
	PathClassInsufficient = "INSUFFICIENT"
)

// SupportedPathClasses enumerates the exact path reliability labels.
var SupportedPathClasses = []string{
	PathClassHigh,
	PathClassMedium,
	PathClassLow,
	PathClassUnlikely,
	PathClassClosed,
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
func IsSupportedMode(mode string) bool {
	return spot.IsSupportedFilterMode(mode)
}

// IsSupportedSource reports whether a source label is supported for filtering.
func IsSupportedSource(source string) bool {
	source = strutil.NormalizeUpper(source)
	return supportedSourceSet[source]
}

// IsSupportedEvent reports whether an event label is supported for filtering.
func IsSupportedEvent(event string) bool {
	return normalizeEvent(event) != ""
}

func normalizeEvent(event string) string {
	return spot.NormalizeEvent(event)
}

// IsSupportedContinent reports whether a continent code is supported.
func IsSupportedContinent(cont string) bool {
	cont = strutil.NormalizeUpper(cont)
	return supportedContinentSet[cont]
}

// IsSupportedZone reports whether a CQ zone is within the supported range.
func IsSupportedZone(zone int) bool {
	return zone >= minCQZone && zone <= maxCQZone
}

// MinCQZone returns the minimum supported CQ zone.
func MinCQZone() int { return minCQZone }

// MaxCQZone returns the maximum supported CQ zone.
func MaxCQZone() int { return maxCQZone }

// UserDataDir is the base directory for persisted per-user data.
// Tests may redirect it without touching real data.
var UserDataDir = "data/users"

// SaveUserFilter persists a user's filter while preserving metadata.
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

// LoadUserFilter returns the saved filter, or os.ErrNotExist for a missing record.
func LoadUserFilter(callsign string) (*Filter, error) {
	record, err := LoadUserRecord(callsign)
	if err != nil {
		return nil, err
	}
	filter := record.Filter
	return &filter, nil
}

// EnsureUserDataDir creates the per-user data directory.
func EnsureUserDataDir() error {
	return os.MkdirAll(UserDataDir, 0o755)
}

// Filter owns one immutable rule-set generation and mutable delivery controls.
// Callers synchronize capture/publication; MatchSnapshot detaches delivery state.
// Programs are bounded by rule preparation and released with their last owner.
type Filter struct {
	preparedRules *PreparedRuleSet  `yaml:"-"`
	preparedCC    *PreparedCCFilter `yaml:"-"`

	IncludeBeacons *bool `yaml:"include_beacons,omitempty"`
	AllowWWV       *bool `yaml:"allow_wwv,omitempty"`
	AllowWCY       *bool `yaml:"allow_wcy,omitempty"`
	AllowAnnounce  *bool `yaml:"allow_announce,omitempty"`
	AllowSelf      *bool `yaml:"allow_self,omitempty"`
	AllowToxic     *bool `yaml:"allow_toxic,omitempty"`
}

// NewFilter installs the configured ordinary editable PASS rule in slot zero.
// Restored empty configurations do not use this new-user factory.
func NewFilter() *Filter {
	return &Filter{
		preparedRules:  newFilterDefaultRules(),
		IncludeBeacons: boolPtr(true), AllowWWV: boolPtr(true), AllowWCY: boolPtr(true),
		AllowAnnounce: boolPtr(true), AllowSelf: boolPtr(true), AllowToxic: boolPtr(true),
	}
}

// SetBeaconEnabled stores an explicit beacon delivery selection.
func (f *Filter) SetBeaconEnabled(enabled bool) {
	if f == nil {
		return
	}
	value := enabled
	f.IncludeBeacons = &value
}

// BeaconsEnabled defaults to true when unset.
func (f *Filter) BeaconsEnabled() bool {
	if f == nil || f.IncludeBeacons == nil {
		return true
	}
	return *f.IncludeBeacons
}

// SetWWVEnabled stores an explicit WWV delivery selection.
func (f *Filter) SetWWVEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowWWV = boolPtr(enabled)
}

// SetWCYEnabled stores an explicit WCY delivery selection.
func (f *Filter) SetWCYEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowWCY = boolPtr(enabled)
}

// SetAnnounceEnabled stores an explicit announcement delivery selection.
func (f *Filter) SetAnnounceEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowAnnounce = boolPtr(enabled)
}

// SetSelfEnabled stores an explicit self-spot delivery selection.
func (f *Filter) SetSelfEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowSelf = boolPtr(enabled)
}

// SetToxicEnabled stores an explicit toxic-spot delivery selection.
// UNKNOWN and UNAVAILABLE classifications fail open.
func (f *Filter) SetToxicEnabled(enabled bool) {
	if f == nil {
		return
	}
	f.AllowToxic = boolPtr(enabled)
}

// WWVEnabled defaults to true when unset.
func (f *Filter) WWVEnabled() bool {
	if f == nil || f.AllowWWV == nil {
		return true
	}
	return *f.AllowWWV
}

// WCYEnabled defaults to true when unset.
func (f *Filter) WCYEnabled() bool {
	if f == nil || f.AllowWCY == nil {
		return true
	}
	return *f.AllowWCY
}

// AnnounceEnabled defaults to true when unset.
func (f *Filter) AnnounceEnabled() bool {
	if f == nil || f.AllowAnnounce == nil {
		return true
	}
	return *f.AllowAnnounce
}

// SelfEnabled defaults to true when unset.
func (f *Filter) SelfEnabled() bool {
	if f == nil || f.AllowSelf == nil {
		return true
	}
	return *f.AllowSelf
}

// ToxicEnabled defaults to true when unset.
func (f *Filter) ToxicEnabled() bool {
	if f == nil || f.AllowToxic == nil {
		return true
	}
	return *f.AllowToxic
}

// AllowsToxicity reports whether a spot passes the toxicity toggle.
func (f *Filter) AllowsToxicity(s *spot.Spot) bool {
	if s == nil {
		return false
	}
	return spot.NormalizeToxicityStatus(string(s.ToxicityStatus)) != spot.ToxicityToxic || f.ToxicEnabled()
}

// AllowsBulletin maps wire bulletin kinds to the separate delivery controls.
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

// Reset empties personal rules while retaining separate controls.
func (f *Filter) Reset() {
	if f == nil {
		return
	}
	f.preparedRules = nil
}

// ResetToDefaults is the retained reset alias. Only NewFilter installs defaults.
func (f *Filter) ResetToDefaults() { f.Reset() }

// Matches evaluates rules with explicitly unavailable location and prediction.
func (f *Filter) Matches(s *spot.Spot) bool {
	return f.MatchSnapshot().MatchesWithPath(s, PathClassInsufficient)
}

// MatchesWithPath consumes a precomputed path class with unavailable location.
func (f *Filter) MatchesWithPath(s *spot.Spot, pathClass string) bool {
	return f.MatchSnapshot().MatchesWithPath(s, pathClass)
}

func (f *Filter) MatchesWithContext(s *spot.Spot, context EvaluationContext) bool {
	return f.MatchSnapshot().MatchesWithContext(s, context)
}

func matchesNearbyCells(s *spot.Spot, fine, coarse pathreliability.CellID, bandNorm string) bool {
	if s == nil {
		return false
	}
	if bandNorm == "" {
		bandNorm = spot.NormalizeBand(s.BandNorm)
	}
	if bandNorm == "" {
		bandNorm = spot.NormalizeBand(s.Band)
	}
	if bandNorm == "" || bandNorm == "???" {
		bandNorm = spot.NormalizeBand(spot.FreqToBand(s.Frequency))
	}
	if bandNorm == "" || bandNorm == "???" {
		return false
	}
	useCoarse := bandNorm == "160m" || bandNorm == "80m" || bandNorm == "60m"
	userCell := fine
	if useCoarse {
		userCell = coarse
	}
	if userCell == pathreliability.InvalidCell {
		return false
	}
	dxCell, deCell := nearbySpotCells(s, useCoarse)
	// Either endpoint may match. An unavailable opposite endpoint cannot veto
	// the available side; the valid user cell cannot equal InvalidCell.
	return userCell == dxCell || userCell == deCell
}

func nearbySpotCells(s *spot.Spot, useCoarse bool) (pathreliability.CellID, pathreliability.CellID) {
	if s == nil {
		return pathreliability.InvalidCell, pathreliability.InvalidCell
	}
	dxGrid := strings.TrimSpace(s.DXMetadata.Grid)
	deGrid := strings.TrimSpace(s.DEMetadata.Grid)
	if useCoarse {
		return pathreliability.EncodeCoarseCell(dxGrid),
			pathreliability.EncodeCoarseCell(deGrid)
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

func isConfidenceExemptMode(mode string) bool {
	return spot.IsConfidenceFilterExemptMode(mode)
}

// IsSupportedConfidenceSymbol reports whether a confidence glyph is supported.
func IsSupportedConfidenceSymbol(symbol string) bool {
	normalized := strutil.NormalizeUpper(symbol)
	if normalized == "" {
		return false
	}
	return supportedConfidenceSymbolSet[normalized]
}

// IsSupportedPathClass reports whether a path class label is supported.
func IsSupportedPathClass(class string) bool {
	normalized := normalizePathClass(class)
	if normalized == "" {
		return false
	}
	return supportedPathClassSet[normalized]
}

// normalizeConfidenceSymbol retains the spot-facing glyph/percentage mapping.
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

// PathFilterActive reports whether the active projection needs classification.
func (f *Filter) PathFilterActive() bool {
	return f.MatchSnapshot().PathFilterActive()
}

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
