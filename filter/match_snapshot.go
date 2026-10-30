package filter

import (
	"sync/atomic"

	"dxcluster/spot"
)

// One process-owned immutable default set is shared by new users and retired
// profile resets. Replacement retains no historical cache.
var configuredDefaultRules atomic.Pointer[PreparedRuleSet]

// ConfigureDefaultRules prepares effective defaults before publishing them.
// Startup calls this after loading the taxonomy and before accepting clients.
func ConfigureDefaultRules(modes, sources []string) error {
	set, err := PrepareDefaultRuleSet(modes, sources)
	if err != nil {
		return err
	}
	configuredDefaultRules.Store(set)
	return nil
}

func newFilterDefaultRules() *PreparedRuleSet {
	if set := configuredDefaultRules.Load(); set != nil {
		return set
	}
	set, err := PrepareDefaultRuleSet(nil, nil)
	if err != nil {
		// NewFilter cannot return an error. Invalid built-ins must never turn
		// into the nil (unrestricted) rule set.
		panic("invalid built-in filter defaults: " + err.Error())
	}
	configuredDefaultRules.CompareAndSwap(nil, set)
	return configuredDefaultRules.Load()
}

// MatchSnapshot is a detached delivery value. Programs are immutable; controls
// are captured by value under the caller's publication lock.
// Its zero value represents a nil Filter and fails closed.
type MatchSnapshot struct {
	rules         *PreparedRuleSet
	cc            *PreparedCCFilter
	valid         bool
	beacons, self bool
	toxic         bool
}

func (f *Filter) MatchSnapshot() MatchSnapshot {
	if f == nil {
		return MatchSnapshot{}
	}
	return MatchSnapshot{
		rules: f.preparedRules, valid: true,
		cc:      f.preparedCC,
		beacons: f.BeaconsEnabled(), self: f.SelfEnabled(), toxic: f.ToxicEnabled(),
	}
}

// MatchesWithPath is an explicitly location-unavailable convenience entry.
func (s MatchSnapshot) MatchesWithPath(value *spot.Spot, pathClass string) bool {
	return s.MatchesWithContext(value, EvaluationContext{PathClass: pathClass})
}

func (s MatchSnapshot) MatchesWithContext(value *spot.Spot, context EvaluationContext) bool {
	if !s.valid || value == nil || (value.IsBeacon && !s.beacons) || !s.AllowsToxicity(value) {
		return false
	}
	if s.cc != nil {
		return s.cc.Matches(value, context)
	}
	return s.rules.Matches(value, context)
}

func (s MatchSnapshot) PathFilterActive() bool { return s.valid && s.cc == nil && s.rules.NeedsPath() }
func (s MatchSnapshot) NearbyFilterActive() bool {
	return s.valid && s.cc == nil && s.rules.NeedsNearby()
}

func (s MatchSnapshot) SelfEnabled() bool { return s.valid && s.self }

func (s MatchSnapshot) AllowsToxicity(value *spot.Spot) bool {
	return s.valid && value != nil &&
		(s.toxic || spot.NormalizeToxicityStatus(string(value.ToxicityStatus)) != spot.ToxicityToxic)
}
