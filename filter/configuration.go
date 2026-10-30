// File role: Compact native capture of immutable v6 rules and exact controls.
package filter

import (
	"errors"

	"gopkg.in/yaml.v3"
)

// CurrentConfigurationVersion identifies disk records that preserve exact values.
const CurrentConfigurationVersion = 7

// V6ConfigurationVersion freezes the historical GO protocol/codec independently
// of the generation selected by current profile storage.
const V6ConfigurationVersion = 6

// DefaultBool keeps the inherited selection distinct from explicit true/false.
// YAML represents inheritance with DEFAULT rather than a null value.
type DefaultBool uint8

const (
	DefaultBoolDefault DefaultBool = iota
	DefaultBoolFalse
	DefaultBoolTrue
)

// DefaultBoolFromPointer captures an optional selection without borrowing it.
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

// FilterConfiguration captures immutable rules and controls, not runtime cells.
type FilterConfiguration struct { //nolint:revive // Names the filter portion alongside SettingsConfiguration.
	preparedRules *PreparedRuleSet  `yaml:"-"`
	preparedCC    *PreparedCCFilter `yaml:"-"`

	IncludeBeacons DefaultBool `yaml:"include_beacons"`
	AllowWWV       DefaultBool `yaml:"allow_wwv"`
	AllowWCY       DefaultBool `yaml:"allow_wcy"`
	AllowAnnounce  DefaultBool `yaml:"allow_announce"`
	AllowSelf      DefaultBool `yaml:"allow_self"`
	AllowToxic     DefaultBool `yaml:"allow_toxic"`
}

// Configuration captures settings and filter rules as one writable snapshot.
type Configuration struct {
	Filters  FilterConfiguration   `yaml:"filters"`
	Settings SettingsConfiguration `yaml:"settings"`
}

// ConfigurationFromFilter captures controls by value and shares only immutable
// programs. The caller guards the source while taking this snapshot.
func ConfigurationFromFilter(f *Filter, settings SettingsConfiguration) Configuration {
	if f == nil {
		return Configuration{Settings: settings}
	}
	return Configuration{Settings: settings, Filters: FilterConfiguration{
		preparedRules:  f.preparedRules,
		preparedCC:     f.preparedCC,
		IncludeBeacons: DefaultBoolFromPointer(f.IncludeBeacons), AllowWWV: DefaultBoolFromPointer(f.AllowWWV),
		AllowWCY: DefaultBoolFromPointer(f.AllowWCY), AllowAnnounce: DefaultBoolFromPointer(f.AllowAnnounce),
		AllowSelf: DefaultBoolFromPointer(f.AllowSelf), AllowToxic: DefaultBoolFromPointer(f.AllowToxic),
	}}
}

// ConfigurationFromPreset captures exact preferences and immutable programs.
func ConfigurationFromPreset(set *SavedPreset) Configuration {
	if set == nil {
		return Configuration{}
	}
	return ConfigurationFromFilter(&set.Filter, SettingsConfiguration{
		Dialect: set.Dialect, Grid: set.Grid, NoiseClass: set.NoiseClass, DedupePolicy: set.DedupePolicy,
		PathMinObservationCount: set.PathMinObservationCount, SolarSummaryMinutes: set.SolarSummaryMinutes,
	})
}

// FilterValue detaches controls, shares immutable programs, and excludes runtime
// state. User location belongs to the publishing client, not the filter.
func (c Configuration) FilterValue() Filter {
	f := c.Filters
	return Filter{
		preparedRules:  f.preparedRules,
		preparedCC:     f.preparedCC,
		IncludeBeacons: f.IncludeBeacons.Pointer(), AllowWWV: f.AllowWWV.Pointer(), AllowWCY: f.AllowWCY.Pointer(),
		AllowAnnounce: f.AllowAnnounce.Pointer(), AllowSelf: f.AllowSelf.Pointer(), AllowToxic: f.AllowToxic.Pointer(),
	}
}

// Clone copies value selections while sharing only immutable prepared programs.
func (c Configuration) Clone() Configuration {
	return c
}

// V6 captures the private immutable programs and exact current controls/settings.
func (c Configuration) V6() V6Configuration {
	f := c.Filters
	return V6Configuration{Settings: c.Settings, Filters: V6FilterConfiguration{
		preparedRules: f.preparedRules, Rules: f.preparedRules.Definitions(), IncludeBeacons: f.IncludeBeacons,
		AllowWWV: f.AllowWWV, AllowWCY: f.AllowWCY, AllowAnnounce: f.AllowAnnounce,
		AllowSelf: f.AllowSelf, AllowToxic: f.AllowToxic,
	}}
}

// Preset uses the authoritative standalone v6 configuration budget before
// cloning/encoding. Transport/readback limits remain independent.
func (c Configuration) Preset() (*SavedPreset, error) {
	return RuntimeSavedPresetFromV6(&V6SavedPreset{
		ConfigurationVersion: V6ConfigurationVersion, Configuration: c.V6(),
	})
}

func (f FilterConfiguration) toggles() [6]DefaultBool {
	return [6]DefaultBool{f.IncludeBeacons, f.AllowWWV, f.AllowWCY, f.AllowAnnounce, f.AllowSelf, f.AllowToxic}
}

// MinimumSizeFits is a linear lower bound with constant scratch space. It must
// precede cloning or encoding, and never replaces the final encoded-byte check.
func (c Configuration) MinimumSizeFits(limit int) bool {
	_, fits := v6ConfigurationMinimumRemaining(c.V6(), limit)
	return fits
}
