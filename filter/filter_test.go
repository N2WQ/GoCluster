package filter

import (
	"testing"

	"dxcluster/spot"
)

func TestNewFilterDefaultsAllowAllContinentsAndZones(t *testing.T) {
	f := NewFilter()
	s := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Continent: "EU",
			CQZone:    14,
		},
		DEMetadata: spot.CallMetadata{
			Continent: "NA",
			CQZone:    5,
		},
	}
	if !f.Matches(s) {
		t.Fatalf("default filter should allow all continents/zones")
	}
}

func TestContinentFilters(t *testing.T) {
	f := NewFilter()
	f.SetDXContinent("EU", true)

	pass := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Continent: "EU",
			CQZone:    14,
		},
		DEMetadata: spot.CallMetadata{
			Continent: "NA",
			CQZone:    5,
		},
	}
	if !f.Matches(pass) {
		t.Fatalf("expected EU DX continent to pass")
	}

	fail := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Continent: "NA",
			CQZone:    14,
		},
		DEMetadata: spot.CallMetadata{
			Continent: "NA",
			CQZone:    5,
		},
	}
	if f.Matches(fail) {
		t.Fatalf("expected non-matching DX continent to be rejected")
	}

	missing := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Continent: "",
			CQZone:    14,
		},
		DEMetadata: spot.CallMetadata{
			Continent: "EU",
			CQZone:    5,
		},
	}
	if f.Matches(missing) {
		t.Fatalf("expected missing DX continent to be rejected when DX continent filter is active")
	}
}

func TestZoneFilters(t *testing.T) {
	f := NewFilter()
	f.SetDXZone(14, true)

	pass := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Continent: "EU",
			CQZone:    14,
		},
		DEMetadata: spot.CallMetadata{
			Continent: "NA",
			CQZone:    5,
		},
	}
	if !f.Matches(pass) {
		t.Fatalf("expected matching DX zone to pass")
	}

	otherZone := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Continent: "EU",
			CQZone:    15,
		},
		DEMetadata: spot.CallMetadata{
			Continent: "NA",
			CQZone:    5,
		},
	}
	if f.Matches(otherZone) {
		t.Fatalf("expected non-matching DX zone to be rejected")
	}

	missing := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Continent: "EU",
			CQZone:    0,
		},
		DEMetadata: spot.CallMetadata{
			Continent: "NA",
			CQZone:    5,
		},
	}
	if f.Matches(missing) {
		t.Fatalf("expected missing DX zone to be rejected when DX zone filter is active")
	}
}

func TestDXCCFilters(t *testing.T) {
	f := NewFilter()
	f.SetDXDXCC(291, true)
	f.SetDEDXCC(110, true)

	pass := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Continent: "NA",
			CQZone:    5,
			ADIF:      291,
		},
		DEMetadata: spot.CallMetadata{
			Continent: "EU",
			CQZone:    14,
			ADIF:      110,
		},
	}
	if !f.Matches(pass) {
		t.Fatalf("expected matching DX/DE ADIF codes to pass")
	}

	failDX := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Continent: "NA",
			CQZone:    5,
			ADIF:      1, // non-whitelisted
		},
		DEMetadata: spot.CallMetadata{
			Continent: "EU",
			CQZone:    14,
			ADIF:      110,
		},
	}
	if f.Matches(failDX) {
		t.Fatalf("expected non-whitelisted DX ADIF to be rejected")
	}

	failDE := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Continent: "NA",
			CQZone:    5,
			ADIF:      291,
		},
		DEMetadata: spot.CallMetadata{
			Continent: "EU",
			CQZone:    14,
			ADIF:      2, // non-whitelisted
		},
	}
	if f.Matches(failDE) {
		t.Fatalf("expected non-whitelisted DE ADIF to be rejected")
	}
}

func TestNormalizeDefaultsRestoresPermissiveFilters(t *testing.T) {
	var f Filter
	f.normalizeDefaults()
	if !f.AllSources || !f.AllDXContinents || !f.AllDEContinents || !f.AllDXZones || !f.AllDEZones {
		t.Fatalf("expected normalizeDefaults to restore permissive continent/zone flags")
	}
}

func TestSourceFilters(t *testing.T) {
	f := NewFilter()

	human := &spot.Spot{
		Mode:    "CW",
		Band:    "20m",
		IsHuman: true,
	}
	skimmer := &spot.Spot{
		Mode:    "CW",
		Band:    "20m",
		IsHuman: false,
	}

	if !f.Matches(human) {
		t.Fatalf("expected default filter to allow human spots")
	}
	if !f.Matches(skimmer) {
		t.Fatalf("expected default filter to allow skimmer/automated spots")
	}

	f.SetSource("HUMAN", true)
	if !f.Matches(human) {
		t.Fatalf("expected human spot to pass when HUMAN is allowed")
	}
	if f.Matches(skimmer) {
		t.Fatalf("expected skimmer spot to be rejected when only HUMAN is allowed")
	}

	f.ResetSources()
	if !f.Matches(skimmer) {
		t.Fatalf("expected ResetSources to allow skimmer spots")
	}

	f.SetSource("HUMAN", false)
	if f.Matches(human) {
		t.Fatalf("expected human spot to be rejected when HUMAN is blocked")
	}
	if !f.Matches(skimmer) {
		t.Fatalf("expected skimmer spot to pass when HUMAN is blocked")
	}
}

func TestSetDefaultSourceSelectionAffectsNewFilters(t *testing.T) {
	t.Cleanup(func() {
		SetDefaultSourceSelection(nil)
	})

	SetDefaultSourceSelection([]string{"HUMAN"})
	f := NewFilter()
	if f.AllSources {
		t.Fatalf("expected AllSources=false when default source is HUMAN")
	}
	if !f.Sources["HUMAN"] || f.Sources["SKIMMER"] {
		t.Fatalf("expected default sources to include only HUMAN, got: %#v", f.Sources)
	}

	SetDefaultSourceSelection([]string{"SKIMMER"})
	f = NewFilter()
	if f.AllSources {
		t.Fatalf("expected AllSources=false when default source is SKIMMER")
	}
	if !f.Sources["SKIMMER"] || f.Sources["HUMAN"] {
		t.Fatalf("expected default sources to include only SKIMMER, got: %#v", f.Sources)
	}

	// Both categories is equivalent to ALL (disable SOURCE filtering).
	SetDefaultSourceSelection([]string{"HUMAN", "SKIMMER"})
	f = NewFilter()
	if !f.AllSources {
		t.Fatalf("expected AllSources=true when default sources include both categories")
	}

	// Invalid config should fail open (allow all sources) rather than blocking everything.
	SetDefaultSourceSelection([]string{"BOGUS"})
	f = NewFilter()
	if !f.AllSources {
		t.Fatalf("expected invalid default sources to fail open (AllSources=true)")
	}
}

func TestGrid2DefaultsAllowAll(t *testing.T) {
	f := NewFilter()
	s := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Grid: "FN",
		},
		DEMetadata: spot.CallMetadata{
			Grid: "KN",
		},
	}
	if !f.Matches(s) {
		t.Fatalf("default filter should allow all 2-character grids")
	}
}

func TestDXGrid2WhitelistBlocksNonMatchingTwoCharGrids(t *testing.T) {
	f := NewFilter()
	f.SetDXGrid2Prefix("FN05", true) // truncated to FN

	pass := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Grid: "FN",
		},
		DEMetadata: spot.CallMetadata{
			Grid: "KN",
		},
	}
	if !f.Matches(pass) {
		t.Fatalf("expected FN grid to pass when whitelisted")
	}

	fail := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Grid: "KN44",
		},
		DEMetadata: spot.CallMetadata{
			Grid: "KN",
		},
	}
	if f.Matches(fail) {
		t.Fatalf("expected KN44 grid to be rejected when DX prefix is not whitelisted")
	}

	longGrid := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Grid: "FN15",
		},
		DEMetadata: spot.CallMetadata{
			Grid: "KN44",
		},
	}
	if !f.Matches(longGrid) {
		t.Fatalf("expected 4-character grids to be unaffected by DXGRID2 whitelist")
	}

	missing := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Grid: "",
		},
		DEMetadata: spot.CallMetadata{
			Grid: "FN",
		},
	}
	if f.Matches(missing) {
		t.Fatalf("expected missing DX grid to be rejected when DXGRID2 filter is active")
	}
}

func TestDEGrid2WhitelistBlocksNonMatchingTwoCharGrids(t *testing.T) {
	f := NewFilter()
	f.SetDEGrid2Prefix("FN", true)

	pass := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Grid: "KN",
		},
		DEMetadata: spot.CallMetadata{
			Grid: "FN",
		},
	}
	if !f.Matches(pass) {
		t.Fatalf("expected FN DE grid to pass when whitelisted")
	}

	fail := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Grid: "FN",
		},
		DEMetadata: spot.CallMetadata{
			Grid: "KN",
		},
	}
	if f.Matches(fail) {
		t.Fatalf("expected KN DE grid to be rejected when not whitelisted")
	}

	longGrid := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Grid: "FN",
		},
		DEMetadata: spot.CallMetadata{
			Grid: "FN44",
		},
	}
	if !f.Matches(longGrid) {
		t.Fatalf("expected DE grid prefix to allow longer grids when whitelisted")
	}

	missing := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Grid: "FN",
		},
		DEMetadata: spot.CallMetadata{
			Grid: "",
		},
	}
	if f.Matches(missing) {
		t.Fatalf("expected missing DE grid to be rejected when DEGRID2 filter is active")
	}
}

func TestGrid2UnsetClearsWhitelist(t *testing.T) {
	f := NewFilter()
	f.SetDXGrid2Prefix("FN", true)
	f.SetDXGrid2Prefix("KN", true)
	f.SetDXGrid2Prefix("KN", false)

	if f.AllDXGrid2 {
		t.Fatalf("expected DXGRID2 filter to remain active after removing one entry")
	}
	f.SetDXGrid2Prefix("FN", false)
	if !f.AllDXGrid2 {
		t.Fatalf("expected DXGRID2 filter to reset to ALL after removing last entry")
	}

	f.SetDEGrid2Prefix("FN", true)
	f.SetDEGrid2Prefix("FN", false)
	if !f.AllDEGrid2 {
		t.Fatalf("expected DEGRID2 filter to reset to ALL after removing last entry")
	}
}
