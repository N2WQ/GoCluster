// File role: Authoritative v5 personal-rule snapshots and immutable preparation.
package filter

import (
	"fmt"
	"slices"

	"dxcluster/pathreliability"
)

// MVPConfigurationVersion identifies the explicit Boolean storage model.
const MVPConfigurationVersion = CurrentConfigurationVersion

// V5FilterConfiguration owns public definitions; only preparation attests that
// these still correspond to the private immutable programs after a caller edit.
type V5FilterConfiguration struct {
	Rules          []RuleDefinition `yaml:"rules"`
	NearbyEnabled  bool             `yaml:"nearby_enabled"`
	IncludeBeacons DefaultBool      `yaml:"include_beacons"`
	AllowWWV       DefaultBool      `yaml:"allow_wwv"`
	AllowWCY       DefaultBool      `yaml:"allow_wcy"`
	AllowAnnounce  DefaultBool      `yaml:"allow_announce"`
	AllowSelf      DefaultBool      `yaml:"allow_self"`
	AllowToxic     DefaultBool      `yaml:"allow_toxic"`
	preparedRules  *PreparedRuleSet `yaml:"-"`
}

// V5Configuration preserves exact settings and controls, without new-user defaults.
type V5Configuration struct {
	Filters  V5FilterConfiguration `yaml:"filters"`
	Settings SettingsConfiguration `yaml:"settings"`
}

// PreparedRules returns the immutable personal rule set. Nil is valid absence.
func (f *Filter) PreparedRules() *PreparedRuleSet {
	if f == nil {
		return nil
	}
	return f.preparedRules
}

// WithRuleSet returns detached canonical definitions while retaining prepared
// programs, including unchanged programs shared by a changed-slot edit.
func (c V5FilterConfiguration) WithRuleSet(s *PreparedRuleSet) V5FilterConfiguration {
	c.Rules = s.Definitions()
	c.preparedRules = s
	return c
}

// V5ConfigurationFromFilter detaches rule definitions, shares immutable programs,
// and captures controls by value. It never prepares or installs default rules.
func V5ConfigurationFromFilter(f *Filter, settings SettingsConfiguration) V5Configuration {
	c := V5Configuration{Settings: settings}
	if f == nil {
		return c
	}
	c.Filters = V5FilterConfiguration{
		NearbyEnabled:  f.NearbyEnabled,
		IncludeBeacons: DefaultBoolFromPointer(f.IncludeBeacons),
		AllowWWV:       DefaultBoolFromPointer(f.AllowWWV),
		AllowWCY:       DefaultBoolFromPointer(f.AllowWCY),
		AllowAnnounce:  DefaultBoolFromPointer(f.AllowAnnounce),
		AllowSelf:      DefaultBoolFromPointer(f.AllowSelf),
		AllowToxic:     DefaultBoolFromPointer(f.AllowToxic),
	}.WithRuleSet(f.preparedRules)
	return c
}

// PrepareV5Configuration validates before allocating a detached snapshot or
// converting controls to pointers. Public edits invalidate the cached attestation;
// no registry or unbounded cache is retained. Effective NEARBY cells remain the
// publishing caller's responsibility.
func PrepareV5Configuration(c V5Configuration) (V5Configuration, Filter, error) {
	for i, value := range c.Filters.toggles() {
		if value > DefaultBoolTrue {
			name := [6]string{"include_beacons", "allow_wwv", "allow_wcy", "allow_announce", "allow_self", "allow_toxic"}[i]
			return V5Configuration{}, Filter{}, fmt.Errorf("filters.%s must be DEFAULT, false or true", name)
		}
	}
	set := c.Filters.preparedRules
	if !c.Filters.hasPreparedDefinitions() {
		var err error
		// PrepareRuleSet checks slot count before copying or compiling input.
		set, err = PrepareRuleSet(c.Filters.Rules)
		if err != nil {
			return V5Configuration{}, Filter{}, err
		}
	}
	c.Filters = c.Filters.WithRuleSet(set)
	f := c.Filters
	return c, Filter{
		preparedRules:    set,
		NearbyEnabled:    f.NearbyEnabled,
		IncludeBeacons:   f.IncludeBeacons.Pointer(),
		AllowWWV:         f.AllowWWV.Pointer(),
		AllowWCY:         f.AllowWCY.Pointer(),
		AllowAnnounce:    f.AllowAnnounce.Pointer(),
		AllowSelf:        f.AllowSelf.Pointer(),
		AllowToxic:       f.AllowToxic.Pointer(),
		NearbyUserFine:   pathreliability.InvalidCell,
		NearbyUserCoarse: pathreliability.InvalidCell,
	}, nil
}

func (c V5FilterConfiguration) hasPreparedDefinitions() bool {
	if len(c.Rules) > MaxRuleSlots {
		return false
	}
	var prepared []preparedRule
	if c.preparedRules != nil {
		prepared = c.preparedRules.rules
	}
	if len(c.Rules) != len(prepared) {
		return false
	}
	for i := range c.Rules {
		if c.Rules[i] != prepared[i].definition {
			return false
		}
	}
	return true
}

func (c V5FilterConfiguration) toggles() [6]DefaultBool {
	return [6]DefaultBool{c.IncludeBeacons, c.AllowWWV, c.AllowWCY, c.AllowAnnounce, c.AllowSelf, c.AllowToxic}
}

// Clone detaches definitions and shares only immutable programs. As with
// Configuration.Clone, callers must establish preparation bounds before cloning
// untrusted input; this operation does not validate or recompile public edits.
func (c V5Configuration) Clone() V5Configuration {
	c.Filters.Rules = slices.Clone(c.Filters.Rules)
	return c
}
