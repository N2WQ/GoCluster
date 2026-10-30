package telnet

import (
	"fmt"
	"strings"
	"testing"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func v7PreparedFixture(t *testing.T) (filter.V7Configuration, *filter.PreparedRuleSet) {
	t.Helper()
	goConfig, _ := v6PreparedFixture(t)
	cc := filter.CCConfiguration{
		Modes: []string{"CW", "FT8"}, Sources: []string{"HUMAN", "SKIMMER"},
		Geography: make(map[string]filter.CCGeography),
		DXBM:      filter.CCDXBM{BaseAllow: true, Slots: map[string]bool{}},
	}
	for _, name := range []string{"DOC", "DOS", "DXCTY", "DXSTATE"} {
		cc.Geography[name] = filter.CCGeography{Global: filter.CCCategory{Action: filter.CCOff}, Bands: map[string]filter.CCCategory{}}
	}
	cc.Geography["DOC"] = filter.CCGeography{
		Global: filter.CCCategory{Action: filter.CCPass, Countries: []int{291}},
		Bands:  map[string]filter.CCCategory{"6": {Action: filter.CCOff}, "20": {Action: filter.CCReject, Countries: []int{1}}},
	}
	cfg, runtime, _, err := filter.PrepareV7Configuration(filter.V7Configuration{
		Filters: filter.V7FilterBanks{GO: goConfig.Filters, CC: cc}, Settings: goConfig.Settings,
	})
	if err != nil {
		t.Fatal(err)
	}
	return cfg, runtime.PreparedRules()
}

func v7MachineRequestFixture(configuration string, revision bool) []byte {
	body := "schema_version: 7\nrequest_id: MiXeD-01\n"
	if revision {
		body += "if_revision: opaque-revision-1\n"
	}
	return []byte(body + "configuration:\n" + indentMachineFixture(configuration))
}

func decodeV7PatchFixture(t *testing.T, resource, configuration string) v7MachineRequest {
	t.Helper()
	request, err := decodeV7MachineRequest(v7MachineRequestFixture(configuration, true), machineCommand{Verb: "PATCH", Resource: resource, SchemaVersion: 7}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func TestV7MachineSchemaCompleteRestoreAndValidation(t *testing.T) {
	cfg, _ := v7PreparedFixture(t)
	encoded, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, verb := range []string{"PUT", "VALIDATE"} {
		for _, active := range []string{"go", "cc"} {
			t.Run(verb+"/"+active, func(t *testing.T) {
				request, err := decodeV7MachineRequest(v7MachineRequestFixture(string(encoded), verb != "VALIDATE"), machineCommand{Verb: verb, Resource: "CONFIG", SchemaVersion: 7}, nil)
				if err != nil {
					t.Fatal(err)
				}
				before := cfg.Clone()
				before.Settings.Dialect = active
				before.Filters.CC.Modes = []string{}
				saved := before.Clone()
				got, err := request.apply(before, active)
				if err != nil {
					t.Fatal(err)
				}
				got, _, _, err = filter.PrepareV7Configuration(got)
				if err != nil || !got.Equal(cfg) || !before.Equal(saved) {
					t.Fatalf("complete restore lost a bank/settings or mutated input: err=%v", err)
				}
			})
		}
	}
}

func TestV7MachineSchemaCCPatchPresence(t *testing.T) {
	for _, tc := range []struct {
		name, patch string
		check       func(*testing.T, filter.CCConfiguration)
	}{
		{"empty-modes", "modes: []", func(t *testing.T, c filter.CCConfiguration) {
			if len(c.Modes) != 0 {
				t.Fatal("empty list was defaulted")
			}
		}},
		{"empty-sources", "sources: []", func(t *testing.T, c filter.CCConfiguration) {
			if len(c.Sources) != 0 {
				t.Fatal("empty list was defaulted")
			}
		}},
		{"switch-false", "allow_self: false", func(t *testing.T, c filter.CCConfiguration) {
			if c.AllowSelf != filter.DefaultBoolFalse {
				t.Fatal("false was omitted")
			}
		}},
		{"switch-default", "allow_self: DEFAULT", func(t *testing.T, c filter.CCConfiguration) {
			if c.AllowSelf != filter.DefaultBoolDefault {
				t.Fatal("DEFAULT was omitted")
			}
		}},
		{"same-global-clears-bands", "geography: {DOC: {global: {action: pass, countries: [291]}}}", func(t *testing.T, c filter.CCConfiguration) {
			if len(c.Geography["DOC"].Bands) != 0 {
				t.Fatal("identical global did not clear overrides")
			}
		}},
		{"empty-bands-noop", "geography: {DOC: {bands: {}}}", func(t *testing.T, c filter.CCConfiguration) {
			if len(c.Geography["DOC"].Bands) != 2 {
				t.Fatal("empty bands cleared existing rows")
			}
		}},
		{"inherit-one-band", "geography: {DOC: {bands: {'6': {action: INHERIT}}}}", func(t *testing.T, c filter.CCConfiguration) {
			if _, exists := c.Geography["DOC"].Bands["6"]; exists || len(c.Geography["DOC"].Bands) != 1 {
				t.Fatal("INHERIT did not remove exactly one override")
			}
		}},
		{"off-one-band", "geography: {DOC: {bands: {'20': {action: off}}}}", func(t *testing.T, c filter.CCConfiguration) {
			if c.Geography["DOC"].Bands["20"].Action != filter.CCOff || len(c.Geography["DOC"].Bands) != 2 {
				t.Fatal("OFF changed sibling rows")
			}
		}},
		{"whole-dxbm", "dxbm: {active: true, base_allow: false, slots: {20-CW: true}}", func(t *testing.T, c filter.CCConfiguration) {
			if !c.DXBM.Active || c.DXBM.BaseAllow || len(c.DXBM.Slots) != 1 || !c.DXBM.Slots["20-CW"] {
				t.Fatal("DXBM did not replace complete state")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, _ := v7PreparedFixture(t)
			before.Settings.Dialect = "" // Effective CC can come from the server default.
			before.Filters.CC.AllowSelf = filter.DefaultBoolTrue
			saved := before.Clone()
			request := decodeV7PatchFixture(t, "FILTER", "dialect: cc\nfilters:\n"+indentMachineFixture(tc.patch+"\n"))
			got, err := request.apply(before, "cc")
			if err != nil {
				t.Fatal(err)
			}
			got, _, _, err = filter.PrepareV7Configuration(got)
			if err != nil {
				t.Fatal(err)
			}
			if !before.Equal(saved) || !(filter.V6Configuration{Filters: got.Filters.GO}).Equal(filter.V6Configuration{Filters: before.Filters.GO}) || got.Settings != before.Settings {
				t.Fatal("CC edit changed input, inactive GO, or shared settings")
			}
			tc.check(t, got.Filters.CC)
		})
	}
}

func TestV7MachineSchemaOrdinaryOwnership(t *testing.T) {
	for _, tc := range []struct {
		resource, configuration string
		allowed                 bool
	}{
		{"FILTER", "dialect: go\nfilters: {}\n", false},
		{"CONFIG", "filters: {go: {}}\n", false},
		{"CONFIG", "filters: {cc: {}}\nsettings: {dialect: cc}\n", false},
		{"CONFIG", "filters: {cc: {modes: []}}\nsettings: {dialect: go}\n", false},
		{"CONFIG", "filters: {cc: {modes: []}}\nsettings: {grid: ''}\n", true},
		{"CONFIG", "settings: {dialect: go}\n", true},
		{"SETTINGS", "dialect: go\n", true},
	} {
		request := decodeV7PatchFixture(t, tc.resource, tc.configuration)
		before, _ := v7PreparedFixture(t)
		_, err := request.apply(before, "cc")
		if (err == nil) != tc.allowed {
			t.Fatalf("%s %q: allowed=%v err=%v", tc.resource, tc.configuration, tc.allowed, err)
		}
	}
}

func TestV7MachineSchemaRefusesContradictoryAndUntypedCC(t *testing.T) {
	for _, patch := range []string{
		"rules: []", "modes: null", "modes: [true]", "sources: [1]",
		"geography: {DOC: {global: {action: off}, bands: {}}}",
		"geography: {DOC: {global: {action: INHERIT}}}",
		"geography: {DOC: {bands: {'6': {action: INHERIT, countries: [291]}}}}",
		"geography: {DOC: {bands: {'6': {action: off, countries: [291]}}}}",
		"geography: {DOC: {global: {action: pass, countries: ['291']}}}",
		"geography: {DOC: {global: {action: pass, countries: [0]}}}",
		"geography: {DOC: {global: {action: pass, states: []}}}",
		"geography: {DOC: {}, doc: {}}", "geography: {UNKNOWN: {}}",
		"geography: {DOS: {bands: {'6': {action: pass, states: [true]}}}}",
		"dxbm: {active: true}", "dxbm: {active: false, base_allow: false, slots: {}}",
		"dxbm: {active: false, base_allow: true, slots: {20-CW: true}}",
		"dxbm: {active: true, base_allow: false, slots: {20: true}}",
		"dxbm: {active: true, base_allow: false, slots: {20-CW: 1}}",
		"modes: &selection [CW]", "modes: !custom [CW]",
	} {
		configuration := "dialect: cc\nfilters:\n" + indentMachineFixture(patch+"\n")
		if _, err := decodeV7MachineRequest(v7MachineRequestFixture(configuration, true), machineCommand{Verb: "PATCH", Resource: "FILTER", SchemaVersion: 7}, nil); err == nil {
			t.Fatalf("accepted invalid native PATCH: %s", patch)
		}
	}
}

func TestV7MachineSchemaEnvelopeAndCompleteness(t *testing.T) {
	for _, configuration := range []string{"{}", "filters: {}", "settings: {}", "filters: {go: {}}\nsettings: {}"} {
		if _, err := decodeV7MachineRequest(v7MachineRequestFixture(configuration, true), machineCommand{Verb: "PUT", Resource: "CONFIG", SchemaVersion: 7}, nil); err == nil {
			t.Fatalf("accepted incomplete complete resource: %q", configuration)
		}
	}
	valid := v7MachineRequestFixture("dialect: cc\nfilters: {}", true)
	for _, body := range [][]byte{
		[]byte(strings.Replace(string(valid), "schema_version: 7", "schema_version: 6", 1)),
		[]byte(string(valid) + "status: {}\n"), []byte(string(valid) + "preset_references: {}\n"),
		[]byte(string(valid) + "upgrade_notice: {}\n"), []byte(string(valid) + "configuration: {}\n"),
		[]byte(string(valid) + "---\n{}\n"),
	} {
		if _, err := decodeV7MachineRequest(body, machineCommand{Verb: "PATCH", Resource: "FILTER", SchemaVersion: 7}, nil); err == nil {
			t.Fatal("accepted mismatched version, read-only field, duplicate or extra document")
		}
	}
	for _, version := range []int{0, 6} {
		if _, err := decodeV7MachineRequest(valid, machineCommand{Verb: "PATCH", Resource: "FILTER", SchemaVersion: version}, nil); err == nil {
			t.Fatalf("body upgraded header version %d", version)
		}
	}
	var node yaml.Node
	if err := yaml.Unmarshal([]byte("{action: INHERIT}"), &node); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeV7Category(node.Content[0], "row", "DOC", false); err == nil {
		t.Fatal("complete row accepted PATCH-only INHERIT")
	}
	request := decodeV7PatchFixture(t, "FILTER", "dialect: cc\nfilters: {modes: [FT8]}\n")
	before, _ := v7PreparedFixture(t)
	got, err := request.apply(before, "cc")
	if err != nil || fmt.Sprint(got.Filters.CC.Modes) != "[FT8]" {
		t.Fatalf("detached mode list lost: %v", err)
	}
}
