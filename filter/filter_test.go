package filter

import (
	"testing"

	"dxcluster/pathreliability"
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

func TestBandAllowListNormalizesTokens(t *testing.T) {
	f := NewFilter()
	f.SetBand("20m", true)

	allowed := &spot.Spot{
		Mode:     "CW",
		Band:     "20m",
		BandNorm: "20M", // legacy uppercase token
	}
	if !f.Matches(allowed) {
		t.Fatalf("expected 20m spot to pass when band allowlist includes 20m (normalized from 20M)")
	}

	blocked := &spot.Spot{
		Mode:     "CW",
		Band:     "40m",
		BandNorm: "40M",
	}
	if f.Matches(blocked) {
		t.Fatalf("expected 40m spot to be rejected when only 20m is allowed")
	}
}

func TestDECallsignPatternUsesStrippedCall(t *testing.T) {
	f := NewFilter()
	f.DECallsigns = []string{"W1XYZ"}

	s := spot.NewSpot("K1ABC", "W1XYZ-1", 14074.0, "CW")
	s.DECallStripped = "W1XYZ"
	s.DECallNormStripped = "W1XYZ"

	if !f.Matches(s) {
		t.Fatalf("expected stripped DE call to satisfy pattern")
	}
}

func TestDXCallsignBlocklistRejects(t *testing.T) {
	f := NewFilter()
	f.BlockDXCallsigns = []string{"K1*"}

	s := spot.NewSpot("K1ABC", "DE1AA", 14074.0, "CW")
	if f.Matches(s) {
		t.Fatalf("expected DX blocklist to reject K1ABC")
	}
}

func TestCallsignBlocklistWinsOverAllowlist(t *testing.T) {
	f := NewFilter()
	f.DXCallsigns = []string{"K1*"}
	f.BlockDXCallsigns = []string{"K1ABC"}

	blocked := spot.NewSpot("K1ABC", "DE1AA", 14074.0, "CW")
	if f.Matches(blocked) {
		t.Fatalf("expected blocklist to win over allowlist")
	}

	allowed := spot.NewSpot("K1XYZ", "DE1AA", 14074.0, "CW")
	if !f.Matches(allowed) {
		t.Fatalf("expected allowlist to pass when not blocked")
	}
}

func TestDECallsignBlocklistUsesStrippedCall(t *testing.T) {
	f := NewFilter()
	f.BlockDECallsigns = []string{"W1XYZ"}

	s := spot.NewSpot("K1ABC", "W1XYZ-1", 14074.0, "CW")
	s.DECallStripped = "W1XYZ"
	s.DECallNormStripped = "W1XYZ"
	if f.Matches(s) {
		t.Fatalf("expected DE blocklist to reject stripped call")
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

func TestContinentFilterAfterMetadataRefresh(t *testing.T) {
	f := NewFilter()
	f.SetDXContinent("NA", false)

	s := &spot.Spot{
		Mode: "CW",
		Band: "20m",
		DXMetadata: spot.CallMetadata{
			Continent: "EU",
		},
		DEMetadata: spot.CallMetadata{
			Continent: "EU",
		},
	}
	s.EnsureNormalized()
	if !f.Matches(s) {
		t.Fatalf("expected EU DX continent to pass before metadata refresh")
	}

	// Simulate call correction updating CTY metadata to NA.
	s.DXMetadata.Continent = "NA"
	s.InvalidateMetadataCache()
	s.EnsureNormalized()
	if f.Matches(s) {
		t.Fatalf("expected NA DX continent to be rejected after metadata refresh")
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
	if !f.AllSources || !f.AllPathClasses || !f.AllDXContinents || !f.AllDEContinents || !f.AllDXZones || !f.AllDEZones {
		t.Fatalf("expected normalizeDefaults to restore permissive flags")
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

func TestPSKVariantsMatchCanonicalModeFilter(t *testing.T) {
	f := NewFilter()
	f.ResetModes()
	f.SetMode("PSK", true)

	psk31 := &spot.Spot{
		Mode: "psk31",
	}
	psk31.EnsureNormalized()

	if !f.Matches(psk31) {
		t.Fatalf("expected PSK31 variant to pass when PSK is allowed")
	}
}

func TestPathFilterWhitelist(t *testing.T) {
	f := NewFilter()
	f.SetPathClass(PathClassHigh, true)

	s := &spot.Spot{Mode: "CW", Band: "20m"}
	if !f.MatchesWithPath(s, PathClassHigh) {
		t.Fatalf("expected HIGH path class to pass when whitelisted")
	}
	if f.MatchesWithPath(s, PathClassLow) {
		t.Fatalf("expected LOW path class to be rejected when only HIGH is allowed")
	}
}

func TestPathFilterBlocklist(t *testing.T) {
	f := NewFilter()
	f.SetPathClass(PathClassHigh, false)

	s := &spot.Spot{Mode: "CW", Band: "20m"}
	if f.MatchesWithPath(s, PathClassHigh) {
		t.Fatalf("expected HIGH path class to be rejected when blocked")
	}
	if !f.MatchesWithPath(s, PathClassLow) {
		t.Fatalf("expected LOW path class to pass when only HIGH is blocked")
	}
}

func TestMatchesDefaultsToInsufficientPathClass(t *testing.T) {
	f := NewFilter()
	f.SetPathClass(PathClassHigh, true)

	s := &spot.Spot{Mode: "CW", Band: "20m"}
	if f.Matches(s) {
		t.Fatalf("expected default path class to be insufficient and fail HIGH-only filter")
	}
}

func requireH3Mappings(t *testing.T) {
	t.Helper()
	if err := pathreliability.InitH3MappingsFromDir("data/h3"); err != nil {
		t.Skipf("InitH3Mappings unavailable: %v", err)
	}
}

func TestNearbyFiltersMatchAndBypassLocationFilters(t *testing.T) {
	requireH3Mappings(t)
	userFine := pathreliability.EncodeCell("FN31")
	userCoarse := pathreliability.EncodeCoarseCell("FN31")
	if userFine == pathreliability.InvalidCell || userCoarse == pathreliability.InvalidCell {
		t.Fatalf("expected valid H3 cells for FN31")
	}

	f := NewFilter()
	if err := f.EnableNearby(userFine, userCoarse); err != nil {
		t.Fatalf("EnableNearby failed: %v", err)
	}
	f.SetMode("FT8", true)          // isolate nearby behavior from default mode profile
	f.SetDXContinent("EU", true) // should be ignored while nearby is on

	s := spot.NewSpot("K1ABC", "W1XYZ", 14074.0, "FT8")
	s.DXMetadata.Grid = "FN31"
	s.DEMetadata.Grid = "EM10"
	s.DXMetadata.Continent = "NA"
	s.DEMetadata.Continent = "NA"
	s.EnsureNormalized()

	if !f.Matches(s) {
		t.Fatalf("expected nearby match to pass and bypass location filters")
	}
}

func TestNearbyRejectsMissingGridOrBand(t *testing.T) {
	requireH3Mappings(t)
	userFine := pathreliability.EncodeCell("FN31")
	userCoarse := pathreliability.EncodeCoarseCell("FN31")
	f := NewFilter()
	if err := f.EnableNearby(userFine, userCoarse); err != nil {
		t.Fatalf("EnableNearby failed: %v", err)
	}

	missingGrid := &spot.Spot{Mode: "CW", Band: "20m"}
	if f.Matches(missingGrid) {
		t.Fatalf("expected missing grids to be rejected when nearby is enabled")
	}

	unknownBand := &spot.Spot{
		Mode: "CW",
		Band: "???",
		DXMetadata: spot.CallMetadata{
			Grid: "FN31",
		},
		DEMetadata: spot.CallMetadata{
			Grid: "FN31",
		},
	}
	if f.Matches(unknownBand) {
		t.Fatalf("expected unknown band to be rejected when nearby is enabled")
	}
}

func TestNearbyUsesCoarseFor60m(t *testing.T) {
	requireH3Mappings(t)
	userFine := pathreliability.EncodeCell("FN31")
	userCoarse := pathreliability.EncodeCoarseCell("FN31")
	if userFine == pathreliability.InvalidCell || userCoarse == pathreliability.InvalidCell {
		t.Fatalf("expected valid H3 cells for FN31")
	}

	f := NewFilter()
	if err := f.EnableNearby(userFine, userCoarse); err != nil {
		t.Fatalf("EnableNearby failed: %v", err)
	}

	spotSameFine := &spot.Spot{
		Mode: "CW",
		Band: "60m",
		DXMetadata: spot.CallMetadata{
			Grid: "FN31",
		},
		DEMetadata: spot.CallMetadata{
			Grid: "EM10",
		},
	}
	if !f.Matches(spotSameFine) {
		t.Fatalf("expected 60m nearby match to pass with coarse match")
	}
}

func TestNearbySnapshotRestore(t *testing.T) {
	requireH3Mappings(t)
	userFine := pathreliability.EncodeCell("FN31")
	userCoarse := pathreliability.EncodeCoarseCell("FN31")
	f := NewFilter()
	f.SetDXContinent("EU", true)
	f.SetDEZone(5, true)
	if err := f.EnableNearby(userFine, userCoarse); err != nil {
		t.Fatalf("EnableNearby failed: %v", err)
	}
	f.ResetDXContinents()
	f.ResetDEZones()
	f.DisableNearby()

	if f.AllDXContinents {
		t.Fatalf("expected DX continent whitelist to be restored after disabling nearby")
	}
	if !f.DEZones[5] {
		t.Fatalf("expected DE zone whitelist to be restored after disabling nearby")
	}
}
