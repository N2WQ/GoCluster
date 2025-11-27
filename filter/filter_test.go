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

func TestNormalizeDefaultsRestoresPermissiveFilters(t *testing.T) {
	var f Filter
	f.normalizeDefaults()
	if !f.AllDXContinents || !f.AllDEContinents || !f.AllDXZones || !f.AllDEZones {
		t.Fatalf("expected normalizeDefaults to restore permissive continent/zone flags")
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
