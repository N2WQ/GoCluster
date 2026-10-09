package filter

import (
	"testing"

	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func restrictiveNearbyResetFilter() *Filter {
	f := NewFilter()
	f.SetDXContinent("EU", true)
	f.SetDEContinent("EU", true)
	f.SetDXZone(14, true)
	f.SetDEZone(14, true)
	f.SetDXGrid2Prefix("JO", true)
	f.SetDEGrid2Prefix("JO", true)
	f.SetDXDXCC(230, true)
	f.SetDEDXCC(230, true)
	f.SetDXState("CA", true)
	f.SetDEState("CA", true)
	f.BlockDXContinents["NA"], f.BlockDEContinents["NA"] = true, true
	f.BlockDXContinents["AS"] = false // Exact OFF restoration retains false entries too.
	f.BlockDXZones[5], f.BlockDEZones[5] = true, true
	f.BlockDXGrid2["FN"], f.BlockDEGrid2["FN"] = true, true
	f.BlockDXDXCC[291], f.BlockDEDXCC[291] = true, true
	f.BlockDXStates["NY"], f.BlockDEStates["NY"] = true, true
	f.BlockAllDXContinents, f.BlockAllDEContinents = true, true
	f.BlockAllDXZones, f.BlockAllDEZones = true, true
	f.BlockAllDXGrid2, f.BlockAllDEGrid2 = true, true
	f.BlockAllDXDXCC, f.BlockAllDEDXCC = true, true
	f.BlockAllDXStates, f.BlockAllDEStates = true, true
	return f
}

func assertPermissiveResetGeography(t *testing.T, f *Filter) {
	t.Helper()
	g := ConfigurationFromFilter(f, SettingsConfiguration{}).Filters
	for name, rule := range map[string]StringRules{
		"DXCONT": g.DXContinents, "DECONT": g.DEContinents,
		"DXGRID2": g.DXGrid2, "DEGRID2": g.DEGrid2,
		"DXSTATE": g.DXStates, "DESTATE": g.DEStates,
	} {
		if !rule.AllowAll || rule.BlockAll || len(rule.Allow) != 0 || len(rule.Block) != 0 {
			t.Fatalf("%s retained restrictions: %#v", name, rule)
		}
	}
	for name, rule := range map[string]IntRules{
		"DXZONE": g.DXZones, "DEZONE": g.DEZones,
		"DXDXCC": g.DXDXCC, "DEDXCC": g.DEDXCC,
	} {
		if !rule.AllowAll || rule.BlockAll || len(rule.Allow) != 0 || len(rule.Block) != 0 {
			t.Fatalf("%s retained restrictions: %#v", name, rule)
		}
	}
	if f.NearbyEnabled || f.NearbySnapshot != nil || f.NearbyUserFine != pathreliability.InvalidCell || f.NearbyUserCoarse != pathreliability.InvalidCell {
		t.Fatal("reset retained NEARBY restoration state")
	}
}

func TestResetRetiresNearbyBeforeClearingEveryGeography(t *testing.T) {
	for _, active := range []bool{false, true} {
		f := restrictiveNearbyResetFilter()
		candidate := spot.NewSpot("K1ABC", "VE3XYZ", 14030, "CW")
		candidate.DXMetadata.Continent, candidate.DEMetadata.Continent = "NA", "NA"
		if f.Matches(candidate) {
			t.Fatal("restrictive control did not hide candidate")
		}
		if err := f.EnableNearby(1, 2); err != nil {
			t.Fatal(err)
		}
		f.NearbyEnabled = active // Also covers a stale snapshot with the toggle off.
		f.Reset()
		assertPermissiveResetGeography(t, f)
		if !f.Matches(candidate) {
			t.Fatal("NOFILTER retained effective geography")
		}
		f.DisableNearby()
		if err := f.EnableNearby(1, 2); err != nil {
			t.Fatal(err)
		}
		f.DisableNearby()
		assertPermissiveResetGeography(t, f)
	}
}

func TestNearbyOffStillRestoresExactGeography(t *testing.T) {
	f := restrictiveNearbyResetFilter()
	before := ConfigurationFromFilter(f, SettingsConfiguration{}).Clone()
	if err := f.EnableNearby(1, 2); err != nil {
		t.Fatal(err)
	}
	f.ResetDXContinents()
	f.ResetDEZones()
	f.ResetDXStates()
	f.DisableNearby()
	if !before.Equal(ConfigurationFromFilter(f, SettingsConfiguration{})) {
		t.Fatal("ordinary NEARBY OFF no longer restores exact prior geography")
	}
}

func TestNoFilterAndConfiguredResetRemainDistinct(t *testing.T) {
	SetDefaultModeSelection([]string{"FT8"})
	SetDefaultSourceSelection([]string{"HUMAN"})
	t.Cleanup(func() { SetDefaultModeSelection(nil); SetDefaultSourceSelection(nil) })
	f := NewFilter()
	f.Reset()
	if !f.AllModes || !f.AllSources {
		t.Fatal("NOFILTER used configured restrictive defaults")
	}
	f.ResetToDefaults()
	if f.AllModes || f.AllSources || !f.Modes["FT8"] || f.Modes["CW"] || !f.Sources["HUMAN"] || f.Sources["SKIMMER"] {
		t.Fatal("RESET FILTER lost configured new-user defaults")
	}
}
