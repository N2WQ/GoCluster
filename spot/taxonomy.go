// Package spot owns canonical spot data structures and bounded mode/EVENT
// taxonomy lookup tables used by ingest, filtering, correction, and fan-out.
package spot

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync/atomic"

	"dxcluster/internal/yamlconfig"

	"gopkg.in/yaml.v3"
)

// The taxonomy keeps mode and EVENT behavior in config-owned data instead of
// scattering special cases through ingest, filters, path reliability,
// and support commands. Runtime code reads immutable lookup tables built at
// startup; the hard caps below keep a bad YAML edit from creating oversized
// process-lifetime maps.
const (
	UnknownModeToken = "UNKNOWN"

	pskReporterRouteNormal   = "normal"
	pskReporterRoutePathOnly = "path_only"
	pskReporterRouteIgnore   = "ignore"

	eventReferenceSuffixAlnumHyphen = "alnum_hyphen"
)

const (
	ReportFormatDefault  = ""
	ReportFormatSignedDB = "signed_db"
	ReportFormatPlainDB  = "plain_db"
)

const (
	CallCorrectionProfileNone     = ""
	CallCorrectionProfileStandard = "standard"
	CallCorrectionProfileVoice    = "voice"
)

const (
	CustomSCPBucketNone  = ""
	CustomSCPBucketVoice = "voice"
	CustomSCPBucketCW    = "cw"
	CustomSCPBucketRTTY  = "rtty"
	CustomSCPBucketFT    = "ft"
	CustomSCPBucketFT2   = "ft2"
	CustomSCPBucketFT4   = "ft4"
	CustomSCPBucketFT8   = "ft8"
)

const (
	maxTaxonomyModes            = 128
	maxTaxonomyModeTokens       = 512
	maxTaxonomyModeVariants     = 512
	maxTaxonomyEvents           = 64
	maxTaxonomyEventTokens      = 256
	maxTaxonomyEventPrefixes    = 256
	maxTaxonomyAliasesPerMode   = 32
	maxTaxonomyVariantsPerMode  = 64
	maxTaxonomyTokensPerEvent   = 32
	maxTaxonomyPrefixesPerEvent = 32
)

// Taxonomy is the immutable runtime view of spot_taxonomy.yaml. The maps are
// process-lifetime after ConfigureTaxonomy, so buildTaxonomy validates
// uniqueness and cardinality before publishing the table atomically.
type Taxonomy struct {
	modesByName map[string]ModeDefinition
	modeOrder   []string

	modeCommentTokens map[string]string
	modeVariants      map[string]string
	filterModes       []string
	defaultModes      []string
	ccShortcutModes   []string

	eventsByName       map[string]EventDefinition
	eventMasksByName   map[string]EventMask
	eventOrder         []string
	filterEvents       []string
	eventExactTokens   map[string]EventMask
	eventReferenceList []eventReferenceMatcher

	keywordScanner *acScanner
}

// taxonomyFile mirrors the YAML shape so strict decoding can reject unknown
// fields before any runtime lookup table is built.
type taxonomyFile struct {
	Modes  []ModeDefinition  `yaml:"modes"`
	Events []EventDefinition `yaml:"events"`
}

// ModeDefinition is one mode contract shared by ingest, filtering, confidence,
// path reliability, source skew, and custom-SCP behavior.
// A field should live here only when support needs the mode behavior to be
// data-owned rather than hard-coded in one package.
type ModeDefinition struct {
	Name                      string                 `yaml:"name"`
	Display                   string                 `yaml:"display"`
	Synthetic                 bool                   `yaml:"synthetic"`
	FilterVisible             bool                   `yaml:"filter_visible"`
	DefaultFilterAllowed      bool                   `yaml:"default_filter_allowed"`
	CCShortcut                bool                   `yaml:"cc_shortcut"`
	BlankModeToken            bool                   `yaml:"blank_mode_token"`
	CommentTokens             []string               `yaml:"comment_tokens"`
	Variants                  []string               `yaml:"variants"`
	CanonicalFamily           string                 `yaml:"canonical_family"`
	PSKReporterRoute          string                 `yaml:"pskreporter_route"`
	PathReliabilityIngest     bool                   `yaml:"path_reliability_ingest"`
	ModeInferenceSeed         bool                   `yaml:"mode_inference_seed"`
	FTDialCanonicalization    bool                   `yaml:"ft_dial_canonicalization"`
	FTConfidence              FTConfidenceDefinition `yaml:"ft_confidence"`
	ReportFormat              string                 `yaml:"report_format"`
	BareNumericReport         bool                   `yaml:"bare_numeric_report"`
	ConfidenceFilterExempt    bool                   `yaml:"confidence_filter_exempt"`
	SourceSkewCorrection      bool                   `yaml:"source_skew_correction"`
	CallCorrectionProfile     string                 `yaml:"call_correction_profile"`
	FrequencyAveraging        bool                   `yaml:"frequency_averaging"`
	CustomSCPBucket           string                 `yaml:"custom_scp_bucket"`
	VoiceByFrequency          bool                   `yaml:"voice_by_frequency"`
	PathReliabilityBucket     string                 `yaml:"path_reliability_bucket"`
	PathReliabilityOffsetMode string                 `yaml:"path_reliability_offset_mode"`
}

// FTConfidenceDefinition links a mode to pipeline timing keys. The taxonomy
// stores key names, not durations, so timing remains owned by pipeline config.
type FTConfidenceDefinition struct {
	Enabled            bool   `yaml:"enabled"`
	QuietGapSecondsKey string `yaml:"quiet_gap_seconds_key"`
	HardCapSecondsKey  string `yaml:"hard_cap_seconds_key"`
}

// EventDefinition defines the operator-visible EVENT families and the comment
// tokens that turn raw spot text into filterable event masks.
type EventDefinition struct {
	Name              string   `yaml:"name"`
	Display           string   `yaml:"display"`
	FilterVisible     bool     `yaml:"filter_visible"`
	StandaloneTokens  []string `yaml:"standalone_tokens"`
	ReferencePrefixes []string `yaml:"reference_prefixes"`
	ReferenceSuffix   string   `yaml:"reference_suffix"`
}

type eventReferenceMatcher struct {
	prefix string
	mask   EventMask
	suffix string
}

// currentTaxonomy is the global read-mostly routing table. atomic.Value lets
// startup publish the checked YAML table once while tests can replace it without
// racing readers.
var currentTaxonomy atomic.Value

func init() {
	t, err := buildTaxonomy(defaultTaxonomyFile())
	if err != nil {
		panic(err)
	}
	currentTaxonomy.Store(t)
}

// LoadTaxonomyFile decodes the checked-in reference table. Unknown keys are
// warning-only startup diagnostics reported by LoadTaxonomyFileWithWarnings.
func LoadTaxonomyFile(path string) (*Taxonomy, error) {
	taxonomy, _, err := LoadTaxonomyFileWithWarnings(path)
	return taxonomy, err
}

func LoadTaxonomyFileWithWarnings(path string) (*Taxonomy, []string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	unknown, err := yamlconfig.UnknownFieldDiagnostics(path, data, taxonomyFile{})
	if err != nil {
		return nil, nil, err
	}
	warnings := make([]string, 0, len(unknown))
	for _, diag := range unknown {
		warnings = append(warnings, diag.Error())
	}
	var file taxonomyFile
	dec := yaml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&file); err != nil {
		return nil, warnings, fmt.Errorf("decode spot taxonomy: %w", err)
	}
	var extra struct{}
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, warnings, fmt.Errorf("decode spot taxonomy: %w", err)
		}
		return nil, warnings, fmt.Errorf("decode spot taxonomy: multiple YAML documents are not supported")
	}
	taxonomy, err := buildTaxonomy(file)
	return taxonomy, warnings, err
}

// ConfigureTaxonomy publishes a fully built taxonomy. It panics on nil because
// every reader depends on this process-lifetime table being present.
func ConfigureTaxonomy(t *Taxonomy) {
	if t == nil {
		panic("spot: nil taxonomy")
	}
	currentTaxonomy.Store(t)
}

// CurrentTaxonomy returns the active immutable taxonomy for package-level
// helpers. A missing table is a startup/programming error, not a recoverable
// per-spot condition.
func CurrentTaxonomy() *Taxonomy {
	loaded := currentTaxonomy.Load()
	t, ok := loaded.(*Taxonomy)
	if !ok || t == nil {
		panic("spot: taxonomy not configured")
	}
	return t
}

// buildTaxonomy turns YAML definitions into bounded lookup maps. The duplicate
// checks protect support semantics: one token or variant must not route to two
// different modes or EVENT families.
func buildTaxonomy(file taxonomyFile) (*Taxonomy, error) {
	if len(file.Modes) == 0 {
		return nil, fmt.Errorf("spot taxonomy: modes must not be empty")
	}
	if len(file.Modes) > maxTaxonomyModes {
		return nil, fmt.Errorf("spot taxonomy: modes count %d exceeds maximum %d", len(file.Modes), maxTaxonomyModes)
	}
	if len(file.Events) > maxTaxonomyEvents {
		return nil, fmt.Errorf("spot taxonomy: events count %d exceeds maximum %d", len(file.Events), maxTaxonomyEvents)
	}

	t := &Taxonomy{
		modesByName:       make(map[string]ModeDefinition, len(file.Modes)),
		modeCommentTokens: make(map[string]string),
		modeVariants:      make(map[string]string),
		eventsByName:      make(map[string]EventDefinition, len(file.Events)),
		eventMasksByName:  make(map[string]EventMask, len(file.Events)),
		eventExactTokens:  make(map[string]EventMask),
	}

	blankModeSeen := false
	tokenOwners := make(map[string]string)
	variantOwners := make(map[string]string)
	for i := range file.Modes {
		mode := file.Modes[i]
		normalized, err := normalizeTaxonomyName(mode.Name, "mode name")
		if err != nil {
			return nil, fmt.Errorf("spot taxonomy mode[%d]: %w", i, err)
		}
		display := normalizeTaxonomyDisplay(mode.Display, normalized)
		if _, ok := t.modesByName[normalized]; ok {
			return nil, fmt.Errorf("spot taxonomy: duplicate mode %q", normalized)
		}
		mode.Name = normalized
		mode.Display = display
		if mode.CanonicalFamily != "" {
			family, err := normalizeTaxonomyName(mode.CanonicalFamily, "mode canonical_family")
			if err != nil {
				return nil, fmt.Errorf("spot taxonomy mode %s: %w", normalized, err)
			}
			mode.CanonicalFamily = family
		}
		mode.PSKReporterRoute = normalizeOptionalEnum(mode.PSKReporterRoute, pskReporterRouteIgnore)
		if err := validateModeDefinition(mode); err != nil {
			return nil, fmt.Errorf("spot taxonomy mode %s: %w", normalized, err)
		}
		if mode.BlankModeToken {
			if blankModeSeen {
				return nil, fmt.Errorf("spot taxonomy: multiple blank_mode_token modes")
			}
			blankModeSeen = true
		}
		if len(mode.CommentTokens) > maxTaxonomyAliasesPerMode {
			return nil, fmt.Errorf("spot taxonomy mode %s: comment token count %d exceeds maximum %d", normalized, len(mode.CommentTokens), maxTaxonomyAliasesPerMode)
		}
		if len(mode.Variants) > maxTaxonomyVariantsPerMode {
			return nil, fmt.Errorf("spot taxonomy mode %s: variant count %d exceeds maximum %d", normalized, len(mode.Variants), maxTaxonomyVariantsPerMode)
		}
		t.modesByName[normalized] = mode
		t.modeOrder = append(t.modeOrder, normalized)
		if mode.FilterVisible {
			t.filterModes = append(t.filterModes, normalized)
		}
		if mode.DefaultFilterAllowed {
			t.defaultModes = append(t.defaultModes, normalized)
		}
		if mode.CCShortcut {
			t.ccShortcutModes = append(t.ccShortcutModes, normalized)
		}
		for _, token := range mode.CommentTokens {
			norm, err := normalizeTaxonomyName(token, "mode comment token")
			if err != nil {
				return nil, fmt.Errorf("spot taxonomy mode %s: %w", normalized, err)
			}
			if owner, ok := tokenOwners[norm]; ok && owner != normalized {
				return nil, fmt.Errorf("spot taxonomy: comment token %q maps to both %s and %s", norm, owner, normalized)
			}
			tokenOwners[norm] = normalized
			t.modeCommentTokens[norm] = normalized
		}
		for _, variant := range mode.Variants {
			norm, err := normalizeTaxonomyName(variant, "mode variant")
			if err != nil {
				return nil, fmt.Errorf("spot taxonomy mode %s: %w", normalized, err)
			}
			if owner, ok := variantOwners[norm]; ok && owner != normalized {
				return nil, fmt.Errorf("spot taxonomy: variant %q maps to both %s and %s", norm, owner, normalized)
			}
			variantOwners[norm] = normalized
			t.modeVariants[norm] = normalized
			if _, ok := tokenOwners[norm]; !ok {
				tokenOwners[norm] = normalized
				t.modeCommentTokens[norm] = norm
			}
		}
	}

	for _, modeName := range t.modeOrder {
		mode := t.modesByName[modeName]
		if mode.CanonicalFamily != "" {
			if _, ok := t.modesByName[mode.CanonicalFamily]; !ok {
				return nil, fmt.Errorf("spot taxonomy mode %s: canonical_family %s is not declared", modeName, mode.CanonicalFamily)
			}
		}
	}

	eventTokenOwners := make(map[string]string)
	eventPrefixOwners := make(map[string]string)
	for i, event := range file.Events {
		normalized, err := normalizeTaxonomyName(event.Name, "event name")
		if err != nil {
			return nil, fmt.Errorf("spot taxonomy event[%d]: %w", i, err)
		}
		display := normalizeTaxonomyDisplay(event.Display, normalized)
		if _, ok := t.eventsByName[normalized]; ok {
			return nil, fmt.Errorf("spot taxonomy: duplicate event %q", normalized)
		}
		if len(event.StandaloneTokens) > maxTaxonomyTokensPerEvent {
			return nil, fmt.Errorf("spot taxonomy event %s: standalone token count %d exceeds maximum %d", normalized, len(event.StandaloneTokens), maxTaxonomyTokensPerEvent)
		}
		if len(event.ReferencePrefixes) > maxTaxonomyPrefixesPerEvent {
			return nil, fmt.Errorf("spot taxonomy event %s: reference prefix count %d exceeds maximum %d", normalized, len(event.ReferencePrefixes), maxTaxonomyPrefixesPerEvent)
		}
		event.Name = normalized
		event.Display = display
		if event.ReferenceSuffix != "" && event.ReferenceSuffix != eventReferenceSuffixAlnumHyphen {
			return nil, fmt.Errorf("spot taxonomy event %s: unsupported reference_suffix %q", normalized, event.ReferenceSuffix)
		}
		mask := EventMask(1) << uint(i)
		t.eventsByName[normalized] = event
		t.eventMasksByName[normalized] = mask
		t.eventOrder = append(t.eventOrder, normalized)
		if event.FilterVisible {
			t.filterEvents = append(t.filterEvents, normalized)
		}
		for _, token := range event.StandaloneTokens {
			norm, err := normalizeTaxonomyName(token, "event standalone token")
			if err != nil {
				return nil, fmt.Errorf("spot taxonomy event %s: %w", normalized, err)
			}
			if owner, ok := eventTokenOwners[norm]; ok && owner != normalized {
				return nil, fmt.Errorf("spot taxonomy: event token %q maps to both %s and %s", norm, owner, normalized)
			}
			eventTokenOwners[norm] = normalized
			t.eventExactTokens[norm] = mask
		}
		for _, prefix := range event.ReferencePrefixes {
			norm, err := normalizeEventPrefix(prefix)
			if err != nil {
				return nil, fmt.Errorf("spot taxonomy event %s: %w", normalized, err)
			}
			if owner, ok := eventPrefixOwners[norm]; ok && owner != normalized {
				return nil, fmt.Errorf("spot taxonomy: event reference prefix %q maps to both %s and %s", norm, owner, normalized)
			}
			eventPrefixOwners[norm] = normalized
			t.eventReferenceList = append(t.eventReferenceList, eventReferenceMatcher{prefix: norm, mask: mask, suffix: event.ReferenceSuffix})
		}
	}
	if len(t.modeCommentTokens) > maxTaxonomyModeTokens {
		return nil, fmt.Errorf("spot taxonomy: mode comment token count %d exceeds maximum %d", len(t.modeCommentTokens), maxTaxonomyModeTokens)
	}
	if len(t.modeVariants) > maxTaxonomyModeVariants {
		return nil, fmt.Errorf("spot taxonomy: mode variant count %d exceeds maximum %d", len(t.modeVariants), maxTaxonomyModeVariants)
	}
	if len(t.eventExactTokens) > maxTaxonomyEventTokens {
		return nil, fmt.Errorf("spot taxonomy: event token count %d exceeds maximum %d", len(t.eventExactTokens), maxTaxonomyEventTokens)
	}
	if len(t.eventReferenceList) > maxTaxonomyEventPrefixes {
		return nil, fmt.Errorf("spot taxonomy: event reference prefix count %d exceeds maximum %d", len(t.eventReferenceList), maxTaxonomyEventPrefixes)
	}
	sortModesByPreferredOrder(t.filterModes, []string{"CW", "FT2", "FT4", "FT8", "JS8", "LSB", "USB", "RTTY", "MSK144", "PSK", "SSTV", UnknownModeToken})
	sortModesByPreferredOrder(t.defaultModes, []string{"CW", "LSB", "USB", "RTTY", UnknownModeToken})
	sortModesByPreferredOrder(t.ccShortcutModes, []string{"CW", "FT2", "FT4", "FT8", "RTTY"})

	sort.Slice(t.eventReferenceList, func(i, j int) bool {
		return len(t.eventReferenceList[i].prefix) > len(t.eventReferenceList[j].prefix)
	})
	t.keywordScanner = buildKeywordScanner(t)

	return t, nil
}

func sortModesByPreferredOrder(modes []string, preferred []string) {
	if len(modes) < 2 {
		return
	}
	order := make(map[string]int, len(preferred))
	for i, mode := range preferred {
		order[mode] = i
	}
	sort.SliceStable(modes, func(i, j int) bool {
		left, leftOK := order[modes[i]]
		right, rightOK := order[modes[j]]
		switch {
		case leftOK && rightOK:
			return left < right
		case leftOK:
			return true
		case rightOK:
			return false
		default:
			return false
		}
	})
}

// validateModeDefinition rejects behavior names the binary does not implement.
// This keeps spot_taxonomy.yaml extensible without pretending a config edit can
// invent new runtime behavior.
func validateModeDefinition(mode ModeDefinition) error {
	if mode.PSKReporterRoute != "" {
		switch mode.PSKReporterRoute {
		case pskReporterRouteNormal, pskReporterRoutePathOnly, pskReporterRouteIgnore:
		default:
			return fmt.Errorf("unsupported pskreporter_route %q", mode.PSKReporterRoute)
		}
	}
	switch mode.ReportFormat {
	case ReportFormatDefault, ReportFormatSignedDB, ReportFormatPlainDB:
	default:
		return fmt.Errorf("unsupported report_format %q", mode.ReportFormat)
	}
	switch mode.CallCorrectionProfile {
	case CallCorrectionProfileNone, CallCorrectionProfileStandard, CallCorrectionProfileVoice:
	default:
		return fmt.Errorf("unsupported call_correction_profile %q", mode.CallCorrectionProfile)
	}
	switch mode.CustomSCPBucket {
	case CustomSCPBucketNone, CustomSCPBucketVoice, CustomSCPBucketCW, CustomSCPBucketRTTY, CustomSCPBucketFT, CustomSCPBucketFT2, CustomSCPBucketFT4, CustomSCPBucketFT8:
	default:
		return fmt.Errorf("unsupported custom_scp_bucket %q", mode.CustomSCPBucket)
	}
	if mode.FTConfidence.Enabled {
		if mode.FTConfidence.QuietGapSecondsKey == "" || mode.FTConfidence.HardCapSecondsKey == "" {
			return fmt.Errorf("ft_confidence requires quiet_gap_seconds_key and hard_cap_seconds_key")
		}
	}
	return nil
}

// normalizeTaxonomyName keeps all taxonomy keys ASCII-ish and uppercase so the
// same tables can be used by telnet commands, parsers, and support tooling.
func normalizeTaxonomyName(value, field string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("%s must not be empty", field)
	}
	for _, r := range trimmed {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			continue
		}
		return "", fmt.Errorf("%s %q contains unsupported character %q", field, value, r)
	}
	return strings.ToUpper(trimmed), nil
}

func normalizeTaxonomyDisplay(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.ToUpper(strings.TrimSpace(value))
}

func normalizeOptionalEnum(value, defaultValue string) string {
	if strings.TrimSpace(value) == "" {
		return defaultValue
	}
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeEventPrefix(prefix string) (string, error) {
	trimmed := strings.TrimSpace(prefix)
	if trimmed == "" {
		return "", fmt.Errorf("event reference prefix must not be empty")
	}
	for _, r := range trimmed {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			continue
		}
		return "", fmt.Errorf("event reference prefix %q contains unsupported character %q", prefix, r)
	}
	return strings.ToUpper(trimmed), nil
}

func (t *Taxonomy) CanonicalMode(mode string) string {
	upper := strings.ToUpper(strings.TrimSpace(mode))
	if upper == "" {
		return ""
	}
	if canonical, ok := t.modeVariants[upper]; ok {
		return canonical
	}
	if def, ok := t.modesByName[upper]; ok {
		if def.CanonicalFamily != "" {
			return def.CanonicalFamily
		}
		return upper
	}
	return upper
}

// CanonicalFilterMode treats a blank filter token as UNKNOWN because telnet
// filters need an explicit bucket for spots whose mode could not be inferred.
func (t *Taxonomy) CanonicalFilterMode(mode string) string {
	upper := strings.ToUpper(strings.TrimSpace(mode))
	if upper == "" {
		upper = UnknownModeToken
	}
	return t.CanonicalMode(upper)
}

// CommentModeToken maps human/source comment tokens to canonical modes. It is
// intentionally separate from variants because comments are evidence, while
// variants are accepted alternate spellings of a mode.
func (t *Taxonomy) CommentModeToken(token string) (string, bool) {
	upper := strings.ToUpper(strings.TrimSpace(token))
	if upper == "" {
		return "", false
	}
	mode, ok := t.modeCommentTokens[upper]
	return mode, ok
}

func (t *Taxonomy) SupportedFilterModes() []string {
	return append([]string(nil), t.filterModes...)
}

func (t *Taxonomy) DefaultFilterModes() []string {
	return append([]string(nil), t.defaultModes...)
}

func (t *Taxonomy) CCShortcutModes() []string {
	return append([]string(nil), t.ccShortcutModes...)
}

func (t *Taxonomy) IsKnownMode(mode string) bool {
	upper := strings.ToUpper(strings.TrimSpace(mode))
	if upper == "" {
		return false
	}
	if _, ok := t.modesByName[upper]; ok {
		return true
	}
	_, ok := t.modeVariants[upper]
	return ok
}

func (t *Taxonomy) IsSupportedFilterMode(mode string) bool {
	canonical := t.CanonicalFilterMode(mode)
	for _, supported := range t.filterModes {
		if supported == canonical {
			return true
		}
	}
	return false
}

func (t *Taxonomy) IsCCShortcutMode(mode string) bool {
	canonical := t.CanonicalFilterMode(mode)
	for _, supported := range t.ccShortcutModes {
		if supported == canonical {
			return true
		}
	}
	return false
}

func (t *Taxonomy) PSKReporterRoute(mode string) string {
	canonical := t.CanonicalMode(mode)
	if def, ok := t.modesByName[canonical]; ok {
		return def.PSKReporterRoute
	}
	return pskReporterRouteIgnore
}

// HasPSKReporterPathOnlyModes lets startup/reporting explain whether any mode
// is intentionally routed into path reliability without normal spot broadcast.
func (t *Taxonomy) HasPSKReporterPathOnlyModes() bool {
	for _, modeName := range t.modeOrder {
		def := t.modesByName[modeName]
		if def.PSKReporterRoute == pskReporterRoutePathOnly {
			return true
		}
	}
	return false
}

func (t *Taxonomy) PathReliabilityIngestMode(mode string) bool {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	return ok && def.PathReliabilityIngest
}

func (t *Taxonomy) ModeInferenceSeedMode(mode string) bool {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	return ok && def.ModeInferenceSeed
}

func (t *Taxonomy) SupportsFTDialCanonicalization(mode string) bool {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	return ok && def.FTDialCanonicalization
}

func (t *Taxonomy) FTConfidenceTimingKeys(mode string) (quietGapKey, hardCapKey string, ok bool) {
	def, exists := t.modesByName[t.CanonicalMode(mode)]
	if !exists || !def.FTConfidence.Enabled {
		return "", "", false
	}
	return def.FTConfidence.QuietGapSecondsKey, def.FTConfidence.HardCapSecondsKey, true
}

func (t *Taxonomy) ReportFormat(mode string) string {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	if !ok {
		return ReportFormatDefault
	}
	return def.ReportFormat
}

func (t *Taxonomy) ModeWantsBareReport(mode string) bool {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	return ok && def.BareNumericReport
}

func (t *Taxonomy) ConfidenceFilterExemptMode(mode string) bool {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	return ok && def.ConfidenceFilterExempt
}

func (t *Taxonomy) SourceSkewCorrectedMode(mode string) bool {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	return ok && def.SourceSkewCorrection
}

func (t *Taxonomy) CallCorrectionProfile(mode string) string {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	if !ok {
		return CallCorrectionProfileNone
	}
	return def.CallCorrectionProfile
}

func (t *Taxonomy) FrequencyAveragingMode(mode string) bool {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	return ok && def.FrequencyAveraging
}

func (t *Taxonomy) CustomSCPBucket(mode string) string {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	if !ok {
		return CustomSCPBucketNone
	}
	return def.CustomSCPBucket
}

func (t *Taxonomy) VoiceByFrequencyMode(mode string) bool {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	return ok && def.VoiceByFrequency
}

func (t *Taxonomy) PathReliabilityBucket(mode string) string {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	if !ok {
		return ""
	}
	if def.PathReliabilityBucket != "" {
		return def.PathReliabilityBucket
	}
	return t.CanonicalMode(mode)
}

func (t *Taxonomy) PathReliabilityOffsetMode(mode string) string {
	def, ok := t.modesByName[t.CanonicalMode(mode)]
	if !ok {
		return ""
	}
	if def.PathReliabilityOffsetMode != "" {
		return strings.ToUpper(def.PathReliabilityOffsetMode)
	}
	return t.CanonicalMode(mode)
}

// PathReliabilityModePolicies exposes the ingest modes and offset families as a
// copy so path reliability can build its policy map without retaining taxonomy
// internals.
func (t *Taxonomy) PathReliabilityModePolicies() map[string]string {
	out := make(map[string]string)
	for _, modeName := range t.modeOrder {
		def := t.modesByName[modeName]
		if !def.PathReliabilityIngest {
			continue
		}
		offsetMode := def.PathReliabilityOffsetMode
		if offsetMode == "" {
			offsetMode = modeName
		}
		out[modeName] = strings.ToUpper(offsetMode)
	}
	return out
}

// EventMaskForName is the exact-name path used by SET/SHOW filters; comment
// parsing uses EventFromCommentToken below.
func (t *Taxonomy) EventMaskForName(event string) EventMask {
	upper := strings.ToUpper(strings.TrimSpace(event))
	if upper == "" {
		return 0
	}
	return t.eventMasksByName[upper]
}

func (t *Taxonomy) NormalizeEvent(event string) string {
	upper := strings.ToUpper(strings.TrimSpace(event))
	if _, ok := t.eventMasksByName[upper]; ok {
		return upper
	}
	return ""
}

func (t *Taxonomy) SupportedEvents() []string {
	return append([]string(nil), t.filterEvents...)
}

func (t *Taxonomy) EventNames(mask EventMask) []string {
	if mask == 0 {
		return nil
	}
	out := make([]string, 0, len(t.eventOrder))
	for _, name := range t.eventOrder {
		if mask&t.eventMasksByName[name] != 0 {
			out = append(out, name)
		}
	}
	return out
}

func (t *Taxonomy) EventString(mask EventMask) string {
	return strings.Join(t.EventNames(mask), ",")
}

func (t *Taxonomy) ParseEventString(value string) EventMask {
	var mask EventMask
	for _, part := range strings.Split(value, ",") {
		mask |= t.EventMaskForName(part)
	}
	return mask
}

func (t *Taxonomy) EventFromCommentToken(token string) EventMask {
	upper := strings.ToUpper(strings.TrimSpace(token))
	if upper == "" {
		return 0
	}
	if mask := t.eventExactTokens[upper]; mask != 0 {
		return mask
	}
	for _, matcher := range t.eventReferenceList {
		if !strings.HasPrefix(upper, matcher.prefix) {
			continue
		}
		suffix := upper[len(matcher.prefix):]
		if eventReferenceSuffixMatches(suffix, matcher.suffix) {
			return matcher.mask
		}
	}
	return 0
}

func eventReferenceSuffixMatches(suffix, matcher string) bool {
	if suffix == "" {
		return false
	}
	switch matcher {
	case eventReferenceSuffixAlnumHyphen:
		for _, r := range suffix {
			if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' {
				continue
			}
			return false
		}
		return true
	default:
		return false
	}
}

func CanonicalMode(mode string) string {
	return CurrentTaxonomy().CanonicalMode(mode)
}

func CanonicalModeForFilter(mode string) string {
	return CurrentTaxonomy().CanonicalFilterMode(mode)
}

func CommentModeToken(token string) (string, bool) {
	return CurrentTaxonomy().CommentModeToken(token)
}

func SupportedFilterModes() []string {
	return CurrentTaxonomy().SupportedFilterModes()
}

func DefaultFilterModes() []string {
	return CurrentTaxonomy().DefaultFilterModes()
}

func CCShortcutModes() []string {
	return CurrentTaxonomy().CCShortcutModes()
}

func IsKnownMode(mode string) bool {
	return CurrentTaxonomy().IsKnownMode(mode)
}

func IsSupportedFilterMode(mode string) bool {
	return CurrentTaxonomy().IsSupportedFilterMode(mode)
}

func IsCCShortcutMode(mode string) bool {
	return CurrentTaxonomy().IsCCShortcutMode(mode)
}

func PSKReporterRoute(mode string) string {
	return CurrentTaxonomy().PSKReporterRoute(mode)
}

func PSKReporterRouteIsNormal(mode string) bool {
	return PSKReporterRoute(mode) == pskReporterRouteNormal
}

func PSKReporterRouteIsPathOnly(mode string) bool {
	return PSKReporterRoute(mode) == pskReporterRoutePathOnly
}

func HasPSKReporterPathOnlyModes() bool {
	return CurrentTaxonomy().HasPSKReporterPathOnlyModes()
}

func PathReliabilityIngestMode(mode string) bool {
	return CurrentTaxonomy().PathReliabilityIngestMode(mode)
}

func IsModeInferenceSeedMode(mode string) bool {
	return CurrentTaxonomy().ModeInferenceSeedMode(mode)
}

func SupportsFTDialCanonicalization(mode string) bool {
	return CurrentTaxonomy().SupportsFTDialCanonicalization(mode)
}

func FTConfidenceTimingKeys(mode string) (quietGapKey, hardCapKey string, ok bool) {
	return CurrentTaxonomy().FTConfidenceTimingKeys(mode)
}

func ReportFormatForMode(mode string) string {
	return CurrentTaxonomy().ReportFormat(mode)
}

func ModeWantsBareReport(mode string) bool {
	return CurrentTaxonomy().ModeWantsBareReport(mode)
}

func IsConfidenceFilterExemptMode(mode string) bool {
	return CurrentTaxonomy().ConfidenceFilterExemptMode(mode)
}

func SourceSkewCorrectedMode(mode string) bool {
	return CurrentTaxonomy().SourceSkewCorrectedMode(mode)
}

func CallCorrectionProfileForMode(mode string) string {
	return CurrentTaxonomy().CallCorrectionProfile(mode)
}

func FrequencyAveragingMode(mode string) bool {
	return CurrentTaxonomy().FrequencyAveragingMode(mode)
}

func CustomSCPBucketForMode(mode string) string {
	return CurrentTaxonomy().CustomSCPBucket(mode)
}

func VoiceByFrequencyMode(mode string) bool {
	return CurrentTaxonomy().VoiceByFrequencyMode(mode)
}

func PathReliabilityBucketForMode(mode string) string {
	return CurrentTaxonomy().PathReliabilityBucket(mode)
}

func PathReliabilityOffsetMode(mode string) string {
	return CurrentTaxonomy().PathReliabilityOffsetMode(mode)
}

func PathReliabilityModePolicies() map[string]string {
	return CurrentTaxonomy().PathReliabilityModePolicies()
}

func defaultTaxonomyFile() taxonomyFile {
	return taxonomyFile{
		Modes: []ModeDefinition{
			{Name: "CW", Display: "CW", FilterVisible: true, DefaultFilterAllowed: true, CCShortcut: true, CommentTokens: []string{"CW", "CWT"}, PSKReporterRoute: pskReporterRouteNormal, PathReliabilityIngest: true, ReportFormat: ReportFormatPlainDB, BareNumericReport: true, SourceSkewCorrection: true, CallCorrectionProfile: CallCorrectionProfileStandard, FrequencyAveraging: true, CustomSCPBucket: CustomSCPBucketCW, PathReliabilityBucket: "CW", PathReliabilityOffsetMode: "CW"},
			{Name: "SSB", Display: "SSB", FilterVisible: false, CommentTokens: []string{"SSB"}, VoiceByFrequency: true, PathReliabilityBucket: "SSB", PathReliabilityOffsetMode: "SSB"},
			{Name: "USB", Display: "USB", FilterVisible: true, DefaultFilterAllowed: true, CommentTokens: []string{"USB"}, CallCorrectionProfile: CallCorrectionProfileVoice, CustomSCPBucket: CustomSCPBucketVoice, PathReliabilityBucket: "SSB", PathReliabilityOffsetMode: "SSB"},
			{Name: "LSB", Display: "LSB", FilterVisible: true, DefaultFilterAllowed: true, CommentTokens: []string{"LSB"}, CallCorrectionProfile: CallCorrectionProfileVoice, CustomSCPBucket: CustomSCPBucketVoice, PathReliabilityBucket: "SSB", PathReliabilityOffsetMode: "SSB"},
			{Name: "RTTY", Display: "RTTY", FilterVisible: true, DefaultFilterAllowed: true, CCShortcut: true, CommentTokens: []string{"RTTY"}, PSKReporterRoute: pskReporterRouteNormal, PathReliabilityIngest: true, ReportFormat: ReportFormatPlainDB, BareNumericReport: true, SourceSkewCorrection: true, CallCorrectionProfile: CallCorrectionProfileStandard, FrequencyAveraging: true, CustomSCPBucket: CustomSCPBucketRTTY, PathReliabilityBucket: "RTTY", PathReliabilityOffsetMode: "RTTY"},
			{Name: "FT8", Display: "FT8", FilterVisible: true, CCShortcut: true, CommentTokens: []string{"FT8", "FT-8"}, PSKReporterRoute: pskReporterRouteNormal, PathReliabilityIngest: true, ModeInferenceSeed: true, FTDialCanonicalization: true, FTConfidence: FTConfidenceDefinition{Enabled: true, QuietGapSecondsKey: "ft8_quiet_gap_seconds", HardCapSecondsKey: "ft8_hard_cap_seconds"}, ReportFormat: ReportFormatSignedDB, BareNumericReport: true, CustomSCPBucket: CustomSCPBucketFT8, PathReliabilityBucket: "FT8", PathReliabilityOffsetMode: "FT8"},
			{Name: "FT4", Display: "FT4", FilterVisible: true, CCShortcut: true, CommentTokens: []string{"FT4", "FT-4"}, PSKReporterRoute: pskReporterRouteNormal, PathReliabilityIngest: true, ModeInferenceSeed: true, FTDialCanonicalization: true, FTConfidence: FTConfidenceDefinition{Enabled: true, QuietGapSecondsKey: "ft4_quiet_gap_seconds", HardCapSecondsKey: "ft4_hard_cap_seconds"}, ReportFormat: ReportFormatSignedDB, BareNumericReport: true, CustomSCPBucket: CustomSCPBucketFT4, PathReliabilityBucket: "FT4", PathReliabilityOffsetMode: "FT4"},
			{Name: "FT2", Display: "FT2", FilterVisible: true, CCShortcut: true, CommentTokens: []string{"FT2", "FT-2"}, PSKReporterRoute: pskReporterRouteNormal, FTDialCanonicalization: true, FTConfidence: FTConfidenceDefinition{Enabled: true, QuietGapSecondsKey: "ft2_quiet_gap_seconds", HardCapSecondsKey: "ft2_hard_cap_seconds"}, ReportFormat: ReportFormatSignedDB, CustomSCPBucket: CustomSCPBucketFT2},
			{Name: "MSK144", Display: "MSK144", FilterVisible: true, CommentTokens: []string{"MSK", "MSK144", "MSK-144"}, PSKReporterRoute: pskReporterRouteNormal, ConfidenceFilterExempt: true, BareNumericReport: true},
			{Name: "PSK", Display: "PSK", FilterVisible: true, CommentTokens: []string{"PSK"}, Variants: []string{"PSK31", "PSK63", "PSK125"}, PSKReporterRoute: pskReporterRouteNormal, PathReliabilityIngest: true, ConfidenceFilterExempt: true, PathReliabilityBucket: "PSK", PathReliabilityOffsetMode: "PSK"},
			{Name: "JS8", Display: "JS8", FilterVisible: true, CommentTokens: []string{"JS8"}, ModeInferenceSeed: true},
			{Name: "SSTV", Display: "SSTV", FilterVisible: true, CommentTokens: []string{"SSTV"}},
			{Name: "WSPR", Display: "WSPR", FilterVisible: false, CommentTokens: []string{"WSPR"}, PSKReporterRoute: pskReporterRoutePathOnly, PathReliabilityIngest: true, PathReliabilityBucket: "WSPR", PathReliabilityOffsetMode: "WSPR"},
			{Name: UnknownModeToken, Display: UnknownModeToken, Synthetic: true, FilterVisible: true, DefaultFilterAllowed: true, BlankModeToken: true},
		},
		Events: []EventDefinition{
			{Name: "LLOTA", Display: "LLOTA", FilterVisible: true, StandaloneTokens: []string{"LLOTA"}, ReferencePrefixes: []string{"LLOTA-"}, ReferenceSuffix: eventReferenceSuffixAlnumHyphen},
			{Name: "IOTA", Display: "IOTA", FilterVisible: true, StandaloneTokens: []string{"IOTA"}, ReferencePrefixes: []string{"IOTA-"}, ReferenceSuffix: eventReferenceSuffixAlnumHyphen},
			{Name: "POTA", Display: "POTA", FilterVisible: true, StandaloneTokens: []string{"POTA"}, ReferencePrefixes: []string{"POTA-"}, ReferenceSuffix: eventReferenceSuffixAlnumHyphen},
			{Name: "SOTA", Display: "SOTA", FilterVisible: true, StandaloneTokens: []string{"SOTA"}, ReferencePrefixes: []string{"SOTA-"}, ReferenceSuffix: eventReferenceSuffixAlnumHyphen},
			{Name: "WWFF", Display: "WWFF", FilterVisible: true, StandaloneTokens: []string{"WWFF"}, ReferencePrefixes: []string{"WWFF-"}, ReferenceSuffix: eventReferenceSuffixAlnumHyphen},
		},
	}
}
