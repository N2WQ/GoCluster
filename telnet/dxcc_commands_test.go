package telnet

import (
	"testing"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

func canonicalTestCTY() *cty.CTYDatabase {
	return &cty.CTYDatabase{Data: map[string]cty.PrefixInfo{
		"K": {Prefix: "K", ADIF: 291}, "W6": {Prefix: "K", ADIF: 291},
		"VE": {Prefix: "VE", ADIF: 1},
		"I":  {Prefix: "I", ADIF: 248}, "IG9": {Prefix: "IG9", ADIF: 248}, "IT9": {Prefix: "IT9", ADIF: 248},
		"ROTUMA-CALL": {Prefix: "3D2/R", ADIF: 460}, "PETER-CALL": {Prefix: "3Y/P", ADIF: 199},
	}}
}

func TestDXCCBooleanCommandPersistsNumericEntity(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	s.filterEngine = newFilterCommandEngine()
	if response, handled := s.filterEngine.Handle(c, "REJECT RULE 0 DXDXCC=248"); !handled ||
		len(c.filter.PreparedRules().Definitions()) != 1 {
		t.Fatalf("Boolean DXCC command: %q", response)
	}
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil || record.PreparedRules().Definitions()[0].Expr != "DXDXCC=248" {
		t.Fatalf("numeric entity reload: %+v %v", record, err)
	}
	for _, call := range []string{"I1ABC", "IT9ABC", "IG9ABC", "K1ABC"} {
		candidate := spot.NewSpot(call, "W1ABC", 14030, "CW")
		candidate.DXMetadata.ADIF = 248
		if c.filter.Matches(candidate) {
			t.Fatalf("entity match depended on callsign spelling: %s", call)
		}
		candidate.DXMetadata.ADIF = 291
		if !c.filter.Matches(candidate) {
			t.Fatalf("unblocked entity rejected: %s", call)
		}
	}
}
