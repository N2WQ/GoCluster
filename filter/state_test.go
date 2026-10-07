package filter

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"dxcluster/pathreliability"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

func stateTestSpot(dxState, deState string) *spot.Spot {
	s := spot.NewSpot("K1ABC", "W1XYZ", 14025, "CW")
	s.DXMetadata.State, s.DEMetadata.State = dxState, deState
	s.EnsureNormalized()
	return s
}

func TestStateFilterTruthMatrix(t *testing.T) {
	for _, domain := range []string{"DX", "DE"} {
		t.Run(domain, func(t *testing.T) {
			for _, rule := range []struct {
				name          string
				allow, block  map[string]bool
				all, blockAll bool
				want          [3]bool
			}{
				{"default", nil, nil, true, false, [3]bool{true, true, true}},
				{"pass TX", map[string]bool{"TX": true}, nil, false, false, [3]bool{true, false, false}},
				{"reject TX", nil, map[string]bool{"TX": true}, true, false, [3]bool{false, true, true}},
				{"reject all", nil, nil, false, true, [3]bool{false, false, false}},
				{"empty restrictive", nil, nil, false, false, [3]bool{false, false, false}},
				{"false-only allow", map[string]bool{"TX": false}, nil, true, false, [3]bool{false, false, false}},
				{"false block", nil, map[string]bool{"TX": false}, true, false, [3]bool{true, true, true}},
				{"deny wins", map[string]bool{"TX": true, "CA": true}, map[string]bool{"TX": true}, false, false, [3]bool{false, true, false}},
			} {
				t.Run(rule.name, func(t *testing.T) {
					f := NewFilter()
					if domain == "DX" {
						f.DXStates, f.BlockDXStates, f.AllDXStates, f.BlockAllDXStates = rule.allow, rule.block, rule.all, rule.blockAll
					} else {
						f.DEStates, f.BlockDEStates, f.AllDEStates, f.BlockAllDEStates = rule.allow, rule.block, rule.all, rule.blockAll
					}
					for i, state := range []string{"TX", "CA", ""} {
						s := stateTestSpot("TX", "TX")
						if domain == "DX" {
							s.DXMetadata.State = state
						} else {
							s.DEMetadata.State = state
						}
						if got := f.Matches(s); got != rule.want[i] {
							t.Errorf("state %q: got %v, want %v", state, got, rule.want[i])
						}
					}
				})
			}
		})
	}
}

func TestStateFilterMutationAndComposition(t *testing.T) {
	f := NewFilter()
	f.SetDXState(" tx ", true)
	f.SetDXState("CA", true)
	f.SetDEState("NY", true)
	if !f.Matches(stateTestSpot("TX", "NY")) || !f.Matches(stateTestSpot("CA", "NY")) || f.Matches(stateTestSpot("TX", "CA")) {
		t.Fatal("states within a domain must OR, and DX/DE domains must AND")
	}
	f.BlockDXStates["TX"] = true
	f.SetDXState("TX", true)
	if f.BlockDXStates["TX"] {
		t.Fatal("PASS failed to remove opposite block")
	}
	f.SetDXState("TX", false)
	if f.DXStates["TX"] || !f.BlockDXStates["TX"] || f.AllDXStates {
		t.Fatal("REJECT changed remaining allowlist")
	}
	f.SetDXState("CA", false)
	if !f.AllDXStates || !f.Matches(stateTestSpot("", "NY")) {
		t.Fatal("removing final allow must restore unrestricted passing with blocks retained")
	}
	before := ConfigurationFromFilter(f, SettingsConfiguration{}).Clone()
	f.SetDXState("ZZ", true)
	f.SetDEState("ZZ", false)
	if !before.Equal(ConfigurationFromFilter(f, SettingsConfiguration{})) {
		t.Fatal("unsupported setter changed rules")
	}
	f.BlockAllDXStates = true
	f.SetDXState("AA", true)
	if f.BlockAllDXStates || !f.DXStates["AA"] {
		t.Fatal("named PASS did not clear block-all")
	}
	f.ResetDXStates()
	f.ResetDEStates()
	if !f.Matches(stateTestSpot("", "")) || len(f.BlockDXStates) != 0 || len(f.BlockDEStates) != 0 {
		t.Fatal("state reset did not clear rules")
	}
	f.SetDXState("TX", true)
	f.SetDEState("NY", true)
	f.ResetToDefaults()
	if !f.Matches(stateTestSpot("", "")) {
		t.Fatal("RESET FILTER did not restore state defaults")
	}
}

func TestStateFilterMixedUSAndCanada(t *testing.T) {
	f := NewFilter()
	f.SetDXState("NY", true)
	f.SetDXState(" on ", true)
	f.SetDEState("CA", true)
	f.SetDEState("QC", true)
	f.BlockDXStates["ON"] = false
	f.DEStates["AB"] = false
	for _, test := range []struct {
		dx, de string
		want   bool
	}{
		{"NY", "CA", true}, {"NY", "QC", true}, {"ON", "CA", true}, {"ON", "QC", true},
		{"BC", "QC", false}, {"ON", "AB", false}, {"", "QC", false}, {"NY", "", false},
	} {
		if got := f.Matches(stateTestSpot(test.dx, test.de)); got != test.want {
			t.Errorf("DX=%q DE=%q: got %t want %t", test.dx, test.de, got, test.want)
		}
	}
	f.BlockDXStates["ON"] = true
	if f.Matches(stateTestSpot("ON", "QC")) || !f.Matches(stateTestSpot("NY", "QC")) {
		t.Fatal("province block did not take precedence within a mixed allowlist")
	}
}

func TestStateNearbySnapshotAndReset(t *testing.T) {
	requireH3Mappings(t)
	f := NewFilter()
	f.SetDXState("CA", true)
	f.SetDEState("NY", true)
	f.BlockDXStates["TX"] = false
	before := ConfigurationFromFilter(f, SettingsConfiguration{}).Clone()
	if err := f.EnableNearby(pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")); err != nil {
		t.Fatal(err)
	}
	s := stateTestSpot("TX", "")
	s.DXMetadata.Grid = "FN31"
	s.DEMetadata.Grid = "EM10"
	if !f.Matches(s) {
		t.Fatal("NEARBY did not override STATE")
	}
	f.DXStates["TX"] = true
	f.BlockAllDEStates = true
	f.DisableNearby()
	if !before.Equal(ConfigurationFromFilter(f, SettingsConfiguration{})) {
		t.Fatal("NEARBY did not restore exact detached state rules")
	}
	if f.Matches(s) {
		t.Fatal("restored restrictive states must reject")
	}
	if err := f.EnableNearby(pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")); err != nil {
		t.Fatal(err)
	}
	f.Reset()
	if f.NearbyEnabled || !f.AllDXStates || !f.AllDEStates || len(f.DXStates) != 0 || len(f.DEStates) != 0 {
		t.Fatal("NOFILTER reset retained states or NEARBY")
	}
}

func TestStateConfigurationDetachedFingerprintAndBound(t *testing.T) {
	f := NewFilter()
	f.SetDXState("TX", true)
	f.SetDEState("PR", false)
	cfg := ConfigurationFromFilter(f, SettingsConfiguration{})
	clone := cfg.Clone()
	f.DXStates["TX"] = false
	f.BlockDEStates["PR"] = false
	if !clone.Filters.DXStates.Allow["TX"] || !clone.Filters.DEStates.Block["PR"] {
		t.Fatal("state snapshot aliases source")
	}
	for _, mutate := range []func(*Configuration){
		func(c *Configuration) { c.Filters.DXStates.Allow["CA"] = false },
		func(c *Configuration) { c.Filters.DEStates.BlockAll = true },
		func(c *Configuration) { c.Filters.DXStates.AllowAll = true },
		func(c *Configuration) { c.Filters.DEStates.Block["GU"] = true },
	} {
		changed := clone.Clone()
		mutate(&changed)
		if clone.Equal(changed) || clone.Fingerprint() == changed.Fingerprint() {
			t.Fatal("state-only change was omitted from comparison or fingerprint")
		}
	}
	c := Configuration{Filters: FilterConfiguration{DXStates: StringRules{Allow: map[string]bool{"TX": true}}}}
	if c.MinimumSizeFits(8) || !c.MinimumSizeFits(9) {
		t.Fatal("state entries missing from preparation bound")
	}
	for _, code := range strings.Fields("AA AB AE AK AL AP AR AS AZ BC CA CO CT DC DE FL GA GU HI IA ID IL IN KS KY LA MA MB MD ME MI MN MO MP MS MT NB NC ND NE NH NJ NL NM NS NT NU NV NY OH OK ON OR PA PE PR QC RI SC SD SK TN TX UM UT VA VI VT WA WI WV WY YT") {
		c.Filters.DXStates.Allow[code] = false
	}
	if len(c.Filters.DXStates.Allow) != 73 || maxStateRuleEntries != 73 || c.ValidateStateRules() != nil {
		t.Fatal("complete US/Canadian vocabulary must be admissible")
	}
	delete(c.Filters.DXStates.Allow, "YT")
	if c.ValidateStateRules() != nil {
		t.Fatal("near-bound mixed vocabulary was rejected")
	}
	c.Filters.DXStates.Allow["YT"] = false
	c.Filters.DXStates.Allow["ZZ"] = false
	if err := c.ValidateStateRules(); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatal("map cardinality was not rejected before key validation")
	}
	for _, code := range []string{"tx", " TX", "on", "", "ZZ"} {
		bad := Configuration{Filters: FilterConfiguration{DEStates: StringRules{Block: map[string]bool{code: false}}}}
		if bad.ValidateStateRules() == nil {
			t.Fatalf("invalid inactive key %q admitted", code)
		}
		if _, err := bad.Preset(); err == nil {
			t.Fatalf("preset admitted invalid key %q", code)
		}
	}
}

func TestStateDiskMigrationAndPresetBaseline(t *testing.T) {
	usePresetTestDir(t)
	for _, version := range []int{0, 1} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			marker := ""
			if version != 0 {
				marker = "configuration_version: 1\n"
			}
			raw := marker + "bands: {20m: false}\nallbands: false\nallow_wwv: false\npreset:\n  name: CONTEST\n  baseline:\n"
			for _, line := range strings.Split(strings.TrimSuffix(marker+"bands: {20m: false}\nallbands: false\nallow_wwv: false\n", "\n"), "\n") {
				raw += "    " + line + "\n"
			}
			path := userRecordPath("N2WQ-1")
			if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
				t.Fatal(err)
			}
			record, err := LoadUserRecord("N2WQ-1")
			if err != nil {
				t.Fatal(err)
			}
			if record.ConfigurationVersion != 2 || !record.AllDXStates || !record.AllDEStates || record.Preset == nil || !record.Preset.Baseline.AllDXStates || !record.Preset.Baseline.AllDEStates {
				t.Fatal("old states were not migrated in record and nested baseline")
			}
			if version == 1 && (record.AllBands || record.Bands["20m"] || record.AllowWWV == nil || *record.AllowWWV) {
				t.Fatal("v1 migration normalized exact old fields")
			}
			if !ConfigurationFromFilter(&record.Filter, SettingsConfiguration{}).Equal(ConfigurationFromFilter(&record.Preset.Baseline.Filter, SettingsConfiguration{})) {
				t.Fatal("migration falsely marks matching preset modified")
			}
			unchanged, err := os.ReadFile(path)
			if err != nil || string(unchanged) != raw {
				t.Fatal("read bulk-rewrote legacy record")
			}
			if err := SaveUserRecord("N2WQ-1", record); err != nil {
				t.Fatal(err)
			}
			disk, err := os.ReadFile(path)
			if err != nil || !bytes.Contains(disk, []byte("configuration_version: 2")) {
				t.Fatal("successful save did not mark v2")
			}
		})
	}
}

func TestStateV2ExactRoundtripAndInvalidStorage(t *testing.T) {
	usePresetTestDir(t)
	f := NewFilter()
	f.DXStates, f.BlockDXStates = map[string]bool{"TX": false, "ON": true, "QC": false}, map[string]bool{"CA": false, "GU": true, "BC": false}
	f.AllDXStates, f.AllDEStates = false, false
	f.DEStates = map[string]bool{"AB": false}
	cfg := ConfigurationFromFilter(f, SettingsConfiguration{})
	baseline, err := cfg.Preset()
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveConfiguration("N2WQ-1", cfg, &PresetReference{Name: "CONTEST", Baseline: baseline}, nil); err != nil {
		t.Fatal(err)
	}
	record, err := LoadUserRecord("N2WQ-1")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Equal(ConfigurationFromFilter(&record.Filter, SettingsConfiguration{})) || !cfg.Equal(ConfigurationFromPreset(record.Preset.Baseline)) {
		t.Fatal("v2 roundtrip changed empty or inactive state values")
	}
	if err := SavePreset("N2WQ-1", "CONTEST", baseline); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadPreset("N2WQ-2", "CONTEST")
	if err != nil || !cfg.Equal(ConfigurationFromPreset(loaded)) {
		t.Fatalf("named preset state roundtrip failed: %v", err)
	}
	for _, raw := range []string{
		"configuration_version: 2\ndxstates: {tx: false}\n",
		"configuration_version: 2\nblockdestates: {ZZ: false}\n",
		"configuration_version: 2\ndxstates: null\n",
		"configuration_version: 2\nalldxstates: \"true\"\n",
		"configuration_version: 2\ndxstates: {TX: \"false\"}\n",
		"configuration_version: 2\ndxstates: {TX: true, TX: false}\n",
		"configuration_version: 1\ndxstates: {TX: true}\n",
		"configuration_version: 2\npreset: {name: CONTEST, baseline: {configuration_version: 2, destates: {zz: false}}}\n",
	} {
		path := userRecordPath("N2WQ-1")
		if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadUserRecord("N2WQ-1"); err == nil {
			t.Fatalf("invalid state storage admitted: %s", raw)
		}
		if err := SaveConfiguration("N2WQ-1", cfg, nil, nil); err == nil {
			t.Fatal("write replaced malformed state record")
		}
		disk, err := os.ReadFile(path)
		if err != nil || string(disk) != raw {
			t.Fatal("protected state record changed")
		}
		var preset SavedPreset
		if !strings.Contains(raw, "preset:") && yaml.Unmarshal([]byte(raw), &preset) == nil {
			t.Fatal("preset accepted same malformed state data")
		}
	}
}

func TestStateLegacySummary(t *testing.T) {
	f := NewFilter()
	f.SetDXState("TX", true)
	f.SetDXState("CA", true)
	f.SetDEState("GU", false)
	f.BlockDXStates["TX"] = true
	text := f.String()
	if !strings.Contains(text, "DXState: allow=CA block=TX") || !strings.Contains(text, "DEState: allow=ALL block=GU") {
		t.Fatalf("legacy summary disagrees with rules: %s", text)
	}
}

func BenchmarkStateFilterMatches(b *testing.B) {
	for _, selected := range []bool{false, true} {
		b.Run(fmt.Sprint(selected), func(b *testing.B) {
			f := NewFilter()
			if selected {
				f.SetDXState("TX", true)
				f.SetDEState("NY", true)
			}
			s := stateTestSpot("TX", "NY")
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if !f.Matches(s) {
					b.Fatal("expected match")
				}
			}
		})
	}
}
