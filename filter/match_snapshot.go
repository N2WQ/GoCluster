package filter

import (
	"sync/atomic"

	"dxcluster/pathreliability"
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
// and NEARBY cells are captured by value under the caller's publication lock.
// Its zero value represents a nil Filter and fails closed.
type MatchSnapshot struct {
	rules         *PreparedRuleSet
	valid         bool
	beacons, self bool
	toxic         bool
	nearby        bool
	fine, coarse  pathreliability.CellID
}

func (f *Filter) MatchSnapshot() MatchSnapshot {
	if f == nil {
		return MatchSnapshot{}
	}
	return MatchSnapshot{
		rules: f.preparedRules, valid: true,
		beacons: f.BeaconsEnabled(), self: f.SelfEnabled(), toxic: f.ToxicEnabled(),
		nearby: f.NearbyEnabled, fine: f.NearbyUserFine, coarse: f.NearbyUserCoarse,
	}
}

func (s MatchSnapshot) MatchesWithPath(value *spot.Spot, pathClass string) bool {
	if !s.valid || value == nil || (value.IsBeacon && !s.beacons) || !s.AllowsToxicity(value) {
		return false
	}
	if !s.rules.Matches(value, pathClass, s.nearby) {
		return false
	}
	return !s.nearby || matchesNearbyCells(value, s.fine, s.coarse, "")
}

func (s MatchSnapshot) PathFilterActive() bool {
	return s.valid && s.rules.NeedsPath(s.nearby)
}

func (s MatchSnapshot) SelfEnabled() bool { return s.valid && s.self }

func (s MatchSnapshot) AllowsToxicity(value *spot.Spot) bool {
	return s.valid && value != nil &&
		(s.toxic || spot.NormalizeToxicityStatus(string(value.ToxicityStatus)) != spot.ToxicityToxic)
}
