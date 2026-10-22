package filter

import (
	"testing"

	"dxcluster/spot"
)

func TestNativeStateRulesAndMetadataRefresh(t *testing.T) {
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "DXSTATE=CT,ON AND DESTATE=TX"})
	for _, test := range []struct {
		dx, de string
		want   bool
	}{
		{"CT", "TX", true}, {"ON", "TX", true}, {"CT", "CA", false},
		{"", "TX", false}, {"CT", "", false}, {"ct", "TX", false},
	} {
		s := &spot.Spot{IsHuman: true, DXMetadata: spot.CallMetadata{State: test.dx}, DEMetadata: spot.CallMetadata{State: test.de}}
		if f.Matches(s) != test.want {
			t.Fatal("native state truth changed")
		}
	}
	s := spot.NewSpotNormalized("K1ABC", "N2ABC", 14025, "CW")
	s.DXMetadata.Continent = "EU"
	continent := nativeRuleFilter(t, RuleDefinition{0, RuleReject, "DXCONT=NA"})
	if !continent.Matches(s) {
		t.Fatal("initial metadata rejected")
	}
	s.DXMetadata.Continent = "NA"
	s.InvalidateMetadataCache()
	s.EnsureNormalized()
	if continent.Matches(s) {
		t.Fatal("metadata refresh retained stale geographic truth")
	}
}
