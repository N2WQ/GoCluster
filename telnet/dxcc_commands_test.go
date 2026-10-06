package telnet

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

func canonicalTestCTY() *cty.CTYDatabase {
	return &cty.CTYDatabase{Data: map[string]cty.PrefixInfo{
		"K": {Prefix: "K", ADIF: 291}, "W6": {Prefix: "K", ADIF: 291},
		"VE": {Prefix: "VE", ADIF: 1},
		"I":  {Prefix: "I", ADIF: 248}, "IG9": {Prefix: "IG9", ADIF: 248}, "IT9": {Prefix: "IT9", ADIF: 248},
		"ROTUMA-CALL": {Prefix: "3D2/R", ADIF: 460}, "PETER-CALL": {Prefix: "3Y/P", ADIF: 199},
	}}
}

func TestDXCCCanonicalPassReject(t *testing.T) {
	for _, domain := range []string{"DXDXCC", "DEDXCC"} {
		t.Run(domain, func(t *testing.T) {
			calls := 0
			engine := newFilterCommandEngineWithCTY(func() *cty.CTYDatabase { calls++; return canonicalTestCTY() })
			c := newTestClient()
			for _, action := range []string{"REJECT", "PASS"} {
				beforeCalls := calls
				response, handled := engine.Handle(c, action+" "+domain+" k,291,VE,1,IT9,I,IG9,3D2/R,3Y/P")
				if !handled || strings.Contains(response, "Invalid") || calls != beforeCalls+1 {
					t.Fatalf("command failed or captured CTY more than once: %q; calls=%d", response, calls)
				}
				allow, block := c.filter.DXDXCC, c.filter.BlockDXDXCC
				if domain == "DEDXCC" {
					allow, block = c.filter.DEDXCC, c.filter.BlockDEDXCC
				}
				selected, removed := allow, block
				if action == "REJECT" {
					selected, removed = block, allow
				}
				if len(selected) != 5 || len(removed) != 0 {
					t.Fatalf("entity deduplication or opposite-list removal failed: allow=%v block=%v", allow, block)
				}
				for _, code := range []int{1, 199, 248, 291, 460} {
					if !selected[code] {
						t.Fatalf("missing ADIF %d", code)
					}
				}
			}
			before := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone()
			for _, invalid := range []string{"K,ZZ", "W6", "K1ABC", "K,0", "K,ALL"} {
				response, _ := engine.Handle(c, "REJECT "+domain+" "+invalid)
				if !strings.Contains(response, "Invalid DXCC selection") || !reflect.DeepEqual(before, filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone()) {
					t.Fatalf("invalid list mutated rules: %q -> %q", invalid, response)
				}
			}
			beforeCalls := calls
			engine.Handle(c, "PASS "+domain+" ALL")
			engine.Handle(c, "REJECT "+domain+" ALL")
			if calls != beforeCalls {
				t.Fatal("ALL unnecessarily depends on CTY")
			}
		})
	}
}

func TestDXCCCanonicalUnavailableConflictAndNearby(t *testing.T) {
	engine := newFilterCommandEngine()
	c := newTestClient()
	for _, input := range []string{"291", "99999", "+291", "0291"} {
		response, _ := engine.Handle(c, "PASS DXDXCC "+input)
		if strings.Contains(response, "CTY") || strings.Contains(response, "Invalid") {
			t.Fatalf("numeric compatibility: %q", response)
		}
	}
	before := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone()
	response, _ := engine.Handle(c, "PASS DXDXCC 291,K")
	if response != "CTY database is not available.\n" || !reflect.DeepEqual(before, filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone()) {
		t.Fatal("unavailable CTY changed rules")
	}
	_, invalid, errText := parseDXCCList(strings.Repeat("9", 50), nil)
	if len(invalid) != 1 || errText != "" {
		t.Fatal("integer overflow acquired a CTY dependency")
	}
	engine = newFilterCommandEngineWithCTY(func() *cty.CTYDatabase { return nil })
	if response, _ := engine.Handle(c, "PASS DXDXCC K"); response != "CTY database is not loaded.\n" {
		t.Fatalf("nil snapshot: %q", response)
	}
	db := canonicalTestCTY()
	db.Data["CONFLICT"] = cty.PrefixInfo{Prefix: "K", ADIF: 999}
	engine = newFilterCommandEngineWithCTY(func() *cty.CTYDatabase { return db })
	if response, _ := engine.Handle(c, "REJECT DXDXCC 1,K"); !strings.Contains(response, "Invalid") || !reflect.DeepEqual(before, filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone()) {
		t.Fatal("conflict selected an entity")
	}
	c.filter.NearbyEnabled = true
	if response, _ := engine.Handle(c, "REJECT DXDXCC IT9"); response != nearbyLocationFilterWarning {
		t.Fatalf("NEARBY bypassed: %q", response)
	}
}

func TestDXCCCanonicalDialectAndEntityMatching(t *testing.T) {
	engine := newFilterCommandEngineWithCTY(func() *cty.CTYDatabase { return canonicalTestCTY() })
	c := newTestClient()
	c.dialect = DialectCC
	if response, handled := engine.Handle(c, "SET/FILTER DXDXCC IT9"); !handled || strings.Contains(response, "Invalid") || !c.filter.DXDXCC[248] {
		t.Fatalf("CC input failed: %q", response)
	}
	c.dialect = DialectGo
	engine.Handle(c, "PASS DXDXCC ALL")
	engine.Handle(c, "REJECT DXDXCC IT9")
	for _, call := range []string{"I1ABC", "IT9ABC", "IG9ABC", "K1ABC"} {
		s := spot.NewSpot(call, "W1ABC", 14030, "CW")
		s.DXMetadata.ADIF = 248
		if c.filter.Matches(s) {
			t.Fatalf("ADIF 248 not rejected for %s", call)
		}
		s.DXMetadata.ADIF = 291
		if !c.filter.Matches(s) {
			t.Fatalf("control entity rejected for %s", call)
		}
	}
	legacy := formatFilterSnapshot(c.filter, func() *cty.CTYDatabase { return canonicalTestCTY() })
	if !strings.Contains(legacy, "block=I, IG9, IT9") || strings.Contains(legacy, "block=248") {
		t.Fatalf("legacy display stayed numeric: %q", legacy)
	}
}

func TestDXCCCanonicalPersistenceRemainsNumeric(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "N2WQ-1")
	engine := newFilterCommandEngineWithCTY(func() *cty.CTYDatabase { return canonicalTestCTY() })
	if response, _ := engine.Handle(c, "REJECT DXDXCC IT9"); !strings.Contains(response, "248") {
		t.Fatalf("acknowledgement changed: %q", response)
	}
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil || !record.BlockDXDXCC[248] {
		t.Fatalf("numeric persisted rule missing: %v %v", record, err)
	}
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	engine.Handle(c, "PASS DXDXCC K,ZZ")
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("invalid list changed persisted bytes")
	}
	cfg := filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Clone()
	encoded, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var decoded filter.Configuration
	if err := yaml.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Filters.DXDXCC.Block[248] || bytes.Contains(encoded, []byte("IT9")) {
		t.Fatalf("client YAML changed representation: %s", encoded)
	}
	if err := s.validateMachineConfiguration(cfg); err != nil {
		t.Fatal(err)
	}
}

func FuzzDXCCList(f *testing.F) {
	for _, seed := range []string{"K,291,VE", "3D2/R 3Y/P", "IT9,I,248", "0", "999999999999999999999999", "K,ZZ", "+291", "", "\xff"} {
		f.Add(seed)
	}
	db := canonicalTestCTY()
	expected := map[string]int{"K": 291, "VE": 1, "I": 248, "IG9": 248, "IT9": 248, "3D2/R": 460, "3Y/P": 199}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 128 {
			return
		}
		got, invalid, errText := parseDXCCList(input, func() *cty.CTYDatabase { return db })
		want, bad := []int{}, []string{}
		seen := map[int]bool{}
		for _, token := range strings.Fields(strings.ReplaceAll(input, ",", " ")) {
			code, err := strconv.Atoi(token)
			if err != nil {
				code = expected[strings.ToUpper(strings.TrimSpace(token))]
			}
			if code <= 0 {
				bad = append(bad, token)
				continue
			}
			if !seen[code] {
				seen[code] = true
				want = append(want, code)
			}
		}
		if errText != "" || strings.Join(invalid, "|") != strings.Join(bad, "|") || !reflect.DeepEqual(append([]int{}, got...), want) {
			t.Fatalf("literal parser oracle: %q -> %v %v %q; want %v %v", input, got, invalid, errText, want, bad)
		}
	})
}
