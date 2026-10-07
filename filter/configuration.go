// File role: Exact writable configuration shared by readbacks and client writes.
// Unlike legacy disk migration, these snapshots never normalize explicit values.
package filter

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"dxcluster/pathreliability"
	"gopkg.in/yaml.v3"
)

// CurrentConfigurationVersion identifies disk records that preserve exact values.
const CurrentConfigurationVersion = 2

// RuleSet preserves every configured selection, including explicit false entries.
type RuleSet[K string | int] struct {
	AllowAll bool       `yaml:"allow_all"`
	BlockAll bool       `yaml:"block_all"`
	Allow    map[K]bool `yaml:"allow"`
	Block    map[K]bool `yaml:"block"`
}

// StringRules stores a named category's exact string selections.
type StringRules = RuleSet[string]

// IntRules stores a named category's exact numeric selections.
type IntRules = RuleSet[int]

// DefaultBool keeps the inherited selection distinct from explicit true/false.
// YAML represents inheritance with DEFAULT rather than a null value.
type DefaultBool uint8

const (
	DefaultBoolDefault DefaultBool = iota
	DefaultBoolFalse
	DefaultBoolTrue
)

// DefaultBoolFromPointer captures a legacy pointer without borrowing it.
func DefaultBoolFromPointer(value *bool) DefaultBool {
	if value == nil {
		return DefaultBoolDefault
	}
	if *value {
		return DefaultBoolTrue
	}
	return DefaultBoolFalse
}

// Pointer returns a newly owned explicit selection, or nil for DEFAULT.
func (value DefaultBool) Pointer() *bool {
	if value == DefaultBoolDefault {
		return nil
	}
	result := value == DefaultBoolTrue
	return &result
}

// MarshalYAML emits the explicit bool-or-DEFAULT protocol representation.
func (value DefaultBool) MarshalYAML() (any, error) {
	switch value {
	case DefaultBoolDefault:
		return "DEFAULT", nil
	case DefaultBoolFalse:
		return false, nil
	case DefaultBoolTrue:
		return true, nil
	default:
		return nil, errors.New("invalid default boolean selection")
	}
}

// UnmarshalYAML accepts ordinary booleans or the literal DEFAULT selection.
func (value *DefaultBool) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return errors.New("selection must be true, false or DEFAULT")
	}
	if node.Tag == "!!str" && node.Value == "DEFAULT" {
		*value = DefaultBoolDefault
		return nil
	}
	if node.Tag == "!!bool" {
		var selected bool
		if err := node.Decode(&selected); err != nil {
			return err
		}
		*value = DefaultBoolFromPointer(&selected)
		return nil
	}
	return errors.New("selection must be true, false or DEFAULT")
}

// SettingsConfiguration contains writable preferences, excluding session state.
type SettingsConfiguration struct {
	Dialect                 string `yaml:"dialect"`
	Grid                    string `yaml:"grid"`
	NoiseClass              string `yaml:"noise_class"`
	DedupePolicy            string `yaml:"dedupe_policy"`
	PathMinObservationCount int    `yaml:"path_min_observation_count"`
	SolarSummaryMinutes     int    `yaml:"solar_summary_minutes"`
}

// FilterConfiguration contains all exact rules, excluding runtime NEARBY caches.
type FilterConfiguration struct { //nolint:revive // Names the filter portion alongside SettingsConfiguration in the public schema.
	Bands            StringRules `yaml:"bands"`
	Modes            StringRules `yaml:"modes"`
	Sources          StringRules `yaml:"sources"`
	Events           StringRules `yaml:"events"`
	Confidence       StringRules `yaml:"confidence"`
	PathClasses      StringRules `yaml:"path_classes"`
	DXStates         StringRules `yaml:"dx_states"`
	DEStates         StringRules `yaml:"de_states"`
	DXContinents     StringRules `yaml:"dx_continents"`
	DEContinents     StringRules `yaml:"de_continents"`
	DXZones          IntRules    `yaml:"dx_zones"`
	DEZones          IntRules    `yaml:"de_zones"`
	DXGrid2          StringRules `yaml:"dx_grid2"`
	DEGrid2          StringRules `yaml:"de_grid2"`
	DXDXCC           IntRules    `yaml:"dx_dxcc"`
	DEDXCC           IntRules    `yaml:"de_dxcc"`
	DXCallsigns      []string    `yaml:"dx_callsigns"`
	BlockDXCallsigns []string    `yaml:"block_dx_callsigns"`
	DECallsigns      []string    `yaml:"de_callsigns"`
	BlockDECallsigns []string    `yaml:"block_de_callsigns"`
	IncludeBeacons   DefaultBool `yaml:"include_beacons"`
	AllowWWV         DefaultBool `yaml:"allow_wwv"`
	AllowWCY         DefaultBool `yaml:"allow_wcy"`
	AllowAnnounce    DefaultBool `yaml:"allow_announce"`
	AllowSelf        DefaultBool `yaml:"allow_self"`
	AllowToxic       DefaultBool `yaml:"allow_toxic"`
	NearbyEnabled    bool        `yaml:"nearby_enabled"`
}

// Configuration captures settings and filter rules as one writable snapshot.
type Configuration struct {
	Filters  FilterConfiguration   `yaml:"filters"`
	Settings SettingsConfiguration `yaml:"settings"`
}

// ConfigurationFromFilter borrows maps and lists; callers must guard their
// source for the full access and preflight before an unrestricted detached clone.
func ConfigurationFromFilter(f *Filter, settings SettingsConfiguration) Configuration {
	if f == nil {
		return Configuration{Settings: settings}
	}
	return Configuration{Settings: settings, Filters: FilterConfiguration{
		Bands:        StringRules{f.AllBands, f.BlockAllBands, f.Bands, f.BlockBands},
		Modes:        StringRules{f.AllModes, f.BlockAllModes, f.Modes, f.BlockModes},
		Sources:      StringRules{f.AllSources, f.BlockAllSources, f.Sources, f.BlockSources},
		Events:       StringRules{f.AllEvents, f.BlockAllEvents, f.Events, f.BlockEvents},
		Confidence:   StringRules{f.AllConfidence, f.BlockAllConfidence, f.Confidence, f.BlockConfidence},
		PathClasses:  StringRules{f.AllPathClasses, f.BlockAllPathClasses, f.PathClasses, f.BlockPathClasses},
		DXStates:     StringRules{f.AllDXStates, f.BlockAllDXStates, f.DXStates, f.BlockDXStates},
		DEStates:     StringRules{f.AllDEStates, f.BlockAllDEStates, f.DEStates, f.BlockDEStates},
		DXContinents: StringRules{f.AllDXContinents, f.BlockAllDXContinents, f.DXContinents, f.BlockDXContinents},
		DEContinents: StringRules{f.AllDEContinents, f.BlockAllDEContinents, f.DEContinents, f.BlockDEContinents},
		DXZones:      IntRules{f.AllDXZones, f.BlockAllDXZones, f.DXZones, f.BlockDXZones},
		DEZones:      IntRules{f.AllDEZones, f.BlockAllDEZones, f.DEZones, f.BlockDEZones},
		DXGrid2:      StringRules{f.AllDXGrid2, f.BlockAllDXGrid2, f.DXGrid2Prefixes, f.BlockDXGrid2},
		DEGrid2:      StringRules{f.AllDEGrid2, f.BlockAllDEGrid2, f.DEGrid2Prefixes, f.BlockDEGrid2},
		DXDXCC:       IntRules{f.AllDXDXCC, f.BlockAllDXDXCC, f.DXDXCC, f.BlockDXDXCC},
		DEDXCC:       IntRules{f.AllDEDXCC, f.BlockAllDEDXCC, f.DEDXCC, f.BlockDEDXCC},
		DXCallsigns:  f.DXCallsigns, BlockDXCallsigns: f.BlockDXCallsigns,
		DECallsigns: f.DECallsigns, BlockDECallsigns: f.BlockDECallsigns,
		IncludeBeacons: DefaultBoolFromPointer(f.IncludeBeacons), AllowWWV: DefaultBoolFromPointer(f.AllowWWV),
		AllowWCY: DefaultBoolFromPointer(f.AllowWCY), AllowAnnounce: DefaultBoolFromPointer(f.AllowAnnounce),
		AllowSelf: DefaultBoolFromPointer(f.AllowSelf), AllowToxic: DefaultBoolFromPointer(f.AllowToxic),
		NearbyEnabled: f.NearbyEnabled,
	}}
}

// ConfigurationFromPreset borrows the saved snapshot's maps and pattern lists.
func ConfigurationFromPreset(set *SavedPreset) Configuration {
	if set == nil {
		return Configuration{}
	}
	return ConfigurationFromFilter(&set.Filter, SettingsConfiguration{
		Dialect: set.Dialect, Grid: set.Grid, NoiseClass: set.NoiseClass, DedupePolicy: set.DedupePolicy,
		PathMinObservationCount: set.PathMinObservationCount, SolarSummaryMinutes: set.SolarSummaryMinutes,
	})
}

// FilterValue borrows this snapshot's maps/lists and excludes runtime caches.
// Publish only a detached snapshot; the caller owns NEARBY runtime restoration.
func (c Configuration) FilterValue() Filter {
	f := c.Filters
	return Filter{
		Bands: f.Bands.Allow, BlockBands: f.Bands.Block, AllBands: f.Bands.AllowAll, BlockAllBands: f.Bands.BlockAll,
		Modes: f.Modes.Allow, BlockModes: f.Modes.Block, AllModes: f.Modes.AllowAll, BlockAllModes: f.Modes.BlockAll,
		Sources: f.Sources.Allow, BlockSources: f.Sources.Block, AllSources: f.Sources.AllowAll, BlockAllSources: f.Sources.BlockAll,
		Events: f.Events.Allow, BlockEvents: f.Events.Block, AllEvents: f.Events.AllowAll, BlockAllEvents: f.Events.BlockAll,
		Confidence: f.Confidence.Allow, BlockConfidence: f.Confidence.Block, AllConfidence: f.Confidence.AllowAll, BlockAllConfidence: f.Confidence.BlockAll,
		PathClasses: f.PathClasses.Allow, BlockPathClasses: f.PathClasses.Block, AllPathClasses: f.PathClasses.AllowAll, BlockAllPathClasses: f.PathClasses.BlockAll,
		DXStates: f.DXStates.Allow, BlockDXStates: f.DXStates.Block, AllDXStates: f.DXStates.AllowAll, BlockAllDXStates: f.DXStates.BlockAll,
		DEStates: f.DEStates.Allow, BlockDEStates: f.DEStates.Block, AllDEStates: f.DEStates.AllowAll, BlockAllDEStates: f.DEStates.BlockAll,
		DXContinents: f.DXContinents.Allow, BlockDXContinents: f.DXContinents.Block, AllDXContinents: f.DXContinents.AllowAll, BlockAllDXContinents: f.DXContinents.BlockAll,
		DEContinents: f.DEContinents.Allow, BlockDEContinents: f.DEContinents.Block, AllDEContinents: f.DEContinents.AllowAll, BlockAllDEContinents: f.DEContinents.BlockAll,
		DXZones: f.DXZones.Allow, BlockDXZones: f.DXZones.Block, AllDXZones: f.DXZones.AllowAll, BlockAllDXZones: f.DXZones.BlockAll,
		DEZones: f.DEZones.Allow, BlockDEZones: f.DEZones.Block, AllDEZones: f.DEZones.AllowAll, BlockAllDEZones: f.DEZones.BlockAll,
		DXGrid2Prefixes: f.DXGrid2.Allow, BlockDXGrid2: f.DXGrid2.Block, AllDXGrid2: f.DXGrid2.AllowAll, BlockAllDXGrid2: f.DXGrid2.BlockAll,
		DEGrid2Prefixes: f.DEGrid2.Allow, BlockDEGrid2: f.DEGrid2.Block, AllDEGrid2: f.DEGrid2.AllowAll, BlockAllDEGrid2: f.DEGrid2.BlockAll,
		DXDXCC: f.DXDXCC.Allow, BlockDXDXCC: f.DXDXCC.Block, AllDXDXCC: f.DXDXCC.AllowAll, BlockAllDXDXCC: f.DXDXCC.BlockAll,
		DEDXCC: f.DEDXCC.Allow, BlockDEDXCC: f.DEDXCC.Block, AllDEDXCC: f.DEDXCC.AllowAll, BlockAllDEDXCC: f.DEDXCC.BlockAll,
		DXCallsigns: f.DXCallsigns, BlockDXCallsigns: f.BlockDXCallsigns,
		DECallsigns: f.DECallsigns, BlockDECallsigns: f.BlockDECallsigns,
		IncludeBeacons: f.IncludeBeacons.Pointer(), AllowWWV: f.AllowWWV.Pointer(), AllowWCY: f.AllowWCY.Pointer(),
		AllowAnnounce: f.AllowAnnounce.Pointer(), AllowSelf: f.AllowSelf.Pointer(), AllowToxic: f.AllowToxic.Pointer(),
		NearbyEnabled: f.NearbyEnabled, NearbyUserFine: pathreliability.InvalidCell, NearbyUserCoarse: pathreliability.InvalidCell,
	}
}

func cloneRules[K string | int](rules RuleSet[K]) RuleSet[K] {
	rules.Allow = maps.Clone(rules.Allow)
	rules.Block = maps.Clone(rules.Block)
	return rules
}

// Clone detaches exact values after the caller has established preparation bounds.
func (c Configuration) Clone() Configuration {
	f := &c.Filters
	f.Bands, f.Modes, f.Sources = cloneRules(f.Bands), cloneRules(f.Modes), cloneRules(f.Sources)
	f.Events, f.Confidence, f.PathClasses = cloneRules(f.Events), cloneRules(f.Confidence), cloneRules(f.PathClasses)
	f.DXStates, f.DEStates = cloneRules(f.DXStates), cloneRules(f.DEStates)
	f.DXContinents, f.DEContinents = cloneRules(f.DXContinents), cloneRules(f.DEContinents)
	f.DXZones, f.DEZones = cloneRules(f.DXZones), cloneRules(f.DEZones)
	f.DXGrid2, f.DEGrid2 = cloneRules(f.DXGrid2), cloneRules(f.DEGrid2)
	f.DXDXCC, f.DEDXCC = cloneRules(f.DXDXCC), cloneRules(f.DEDXCC)
	f.DXCallsigns, f.BlockDXCallsigns = slices.Clone(f.DXCallsigns), slices.Clone(f.BlockDXCallsigns)
	f.DECallsigns, f.BlockDECallsigns = slices.Clone(f.DECallsigns), slices.Clone(f.BlockDECallsigns)
	return c
}

// Preset enforces the independent named-snapshot budget before cloning/encoding.
// Ordinary user records and LOAD admission do not inherit the readback budget.
func (c Configuration) Preset() (*SavedPreset, error) {
	if err := c.ValidateStateRules(); err != nil {
		return nil, err
	}
	if !c.MinimumSizeFits(MaxPresetBytes) {
		return nil, fmt.Errorf("preset exceeds %d KiB", MaxPresetBytes/1024)
	}
	for _, toggle := range c.Filters.toggles() {
		if toggle > DefaultBoolTrue {
			return nil, errors.New("invalid default boolean selection")
		}
	}
	c = c.Clone()
	set := &SavedPreset{
		ConfigurationVersion: CurrentConfigurationVersion, Filter: c.FilterValue(),
		Dialect: c.Settings.Dialect, Grid: c.Settings.Grid, NoiseClass: c.Settings.NoiseClass,
		DedupePolicy: c.Settings.DedupePolicy, PathMinObservationCount: c.Settings.PathMinObservationCount,
		SolarSummaryMinutes: c.Settings.SolarSummaryMinutes,
	}
	if err := checkPresetSize(set); err != nil {
		return nil, err
	}
	return set, nil
}

func (f FilterConfiguration) stringRules() [12]StringRules {
	return [12]StringRules{f.DXStates, f.DEStates, f.Bands, f.Modes, f.Sources, f.Events, f.Confidence, f.PathClasses, f.DXContinents, f.DEContinents, f.DXGrid2, f.DEGrid2}
}

func (f FilterConfiguration) intRules() [4]IntRules {
	return [4]IntRules{f.DXZones, f.DEZones, f.DXDXCC, f.DEDXCC}
}

func (f FilterConfiguration) patterns() [4][]string {
	return [4][]string{f.DXCallsigns, f.BlockDXCallsigns, f.DECallsigns, f.BlockDECallsigns}
}

func (f FilterConfiguration) toggles() [6]DefaultBool {
	return [6]DefaultBool{f.IncludeBeacons, f.AllowWWV, f.AllowWCY, f.AllowAnnounce, f.AllowSelf, f.AllowToxic}
}

// MinimumSizeFits is a linear lower bound with constant scratch space. It must
// precede cloning or encoding, and never replaces the final encoded-byte check.
func (c Configuration) MinimumSizeFits(limit int) bool {
	remaining := limit
	consume := func(size int) bool {
		if size < 0 || size > remaining {
			return false
		}
		remaining -= size
		return true
	}
	for _, value := range []string{c.Settings.Dialect, c.Settings.Grid, c.Settings.NoiseClass, c.Settings.DedupePolicy} {
		if !consume(len(value)) {
			return false
		}
	}
	for _, rules := range c.Filters.stringRules() {
		for _, entries := range []map[string]bool{rules.Allow, rules.Block} {
			if len(entries) > remaining/7 || !consume(len(entries)*7) {
				return false
			}
			for key, value := range entries {
				if !consume(len(key)) || (!value && !consume(1)) {
					return false
				}
			}
		}
	}
	for _, rules := range c.Filters.intRules() {
		for _, entries := range []map[int]bool{rules.Allow, rules.Block} {
			if len(entries) > remaining/8 || !consume(len(entries)*8) {
				return false
			}
		}
	}
	for _, patterns := range c.Filters.patterns() {
		if len(patterns) > remaining/3 || !consume(len(patterns)*3) {
			return false
		}
		for _, pattern := range patterns {
			if !consume(len(pattern)) {
				return false
			}
		}
	}
	return remaining >= 0
}
