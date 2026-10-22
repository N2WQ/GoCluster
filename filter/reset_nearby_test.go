package filter

import (
	"testing"

	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestResetEmptiesRulesAndNearbyRetainsControls(t *testing.T) {
	f := NewFilter()
	f.SetBeaconEnabled(false)
	f.SetWWVEnabled(false)
	f.SetWCYEnabled(false)
	f.SetAnnounceEnabled(false)
	f.SetSelfEnabled(false)
	f.SetToxicEnabled(false)
	if err := f.EnableNearby(10, 11); err != nil {
		t.Fatal(err)
	}
	f.Reset()
	if f.PreparedRules() != nil || f.NearbyEnabled ||
		f.NearbyUserFine != pathreliability.InvalidCell || f.NearbyUserCoarse != pathreliability.InvalidCell {
		t.Fatal("reset retained personal rules or NEARBY")
	}
	for _, value := range v5RuntimeControls(f) {
		if value == nil || *value {
			t.Fatal("reset rewrote a separate control")
		}
	}
	if !f.Matches(&spot.Spot{Mode: "FT8"}) {
		t.Fatal("reset installed new-user defaults")
	}
	f.ResetToDefaults()
	if f.PreparedRules() != nil || f.SelfEnabled() {
		t.Fatal("retained reset alias installed defaults or changed controls")
	}
}

func TestNearbyTogglePreservesPreparedGeography(t *testing.T) {
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "DXSTATE=CT AND DXZONE=5"})
	original := f.PreparedRules()
	if err := f.EnableNearby(10, 11); err != nil {
		t.Fatal(err)
	}
	if f.PreparedRules() != original {
		t.Fatal("NEARBY rewrote personal rules")
	}
	replacement := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, "DXSTATE=NY AND MODE=CW"})
	f.preparedRules = replacement
	f.DisableNearby()
	if f.PreparedRules() != replacement {
		t.Fatal("NEARBY restored an earlier generation")
	}
}
