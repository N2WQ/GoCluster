package filter

import (
	"testing"

	"dxcluster/spot"
)

func TestResetEmptiesRulesRetainsControls(t *testing.T) {
	f := NewFilter()
	f.SetBeaconEnabled(false)
	f.SetWWVEnabled(false)
	f.SetWCYEnabled(false)
	f.SetAnnounceEnabled(false)
	f.SetSelfEnabled(false)
	f.SetToxicEnabled(false)
	f.Reset()
	if f.PreparedRules() != nil {
		t.Fatal("reset retained personal rules")
	}

	for _, value := range v6RuntimeControls(f) {
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
