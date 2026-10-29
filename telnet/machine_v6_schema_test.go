package telnet

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/filter/expr"
	"gopkg.in/yaml.v3"
)

const v6CompleteFilterFixture = `rules:
  - {slot: 9, action: reject, expr: "snr<-10"}
  - {slot: 0, action: pass, expr: "band=20m and dxcont=eu"}
include_beacons: false
allow_wwv: true
allow_wcy: DEFAULT
allow_announce: false
allow_self: DEFAULT
allow_toxic: true
`

func v6MachineRequestFixture(configuration string, revision bool) []byte {
	body := "schema_version: 6\nrequest_id: MiXeD-01\n"
	if revision {
		body += "if_revision: opaque-revision-1\n"
	}
	return []byte(body + "configuration:\n" + indentMachineFixture(configuration))
}

func v6PreparedFixture(t *testing.T) (filter.V6Configuration, *filter.PreparedRuleSet) {
	t.Helper()
	cfg := filter.V6Configuration{
		Settings: filter.SettingsConfiguration{Dialect: "cc", Grid: "FN42", NoiseClass: "URBAN", DedupePolicy: "SLOW", PathMinObservationCount: 9, SolarSummaryMinutes: 30},
		Filters:  filter.V6FilterConfiguration{IncludeBeacons: filter.DefaultBoolTrue, AllowSelf: filter.DefaultBoolFalse},
	}
	cfg.Filters.Rules = []filter.RuleDefinition{{Slot: 8, Action: filter.RuleReject, Expr: "MODE=CW"}}
	canonical, value, err := filter.PrepareV6Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, value.PreparedRules()
}

func TestV6MachineSchemaCompleteResources(t *testing.T) {
	for _, verb := range []string{"PUT", "VALIDATE"} {
		for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG"} {
			if verb == "VALIDATE" && resource != "CONFIG" {
				continue
			}
			t.Run(verb+"/"+resource, func(t *testing.T) {
				configuration := v6CompleteFilterFixture
				switch resource {
				case "SETTINGS":
					configuration = machineCompleteSettingsFixture
				case "CONFIG":
					configuration = "filters:\n" + indentMachineFixture(v6CompleteFilterFixture) +
						"settings:\n" + indentMachineFixture(machineCompleteSettingsFixture)
				}
				request, err := decodeV6MachineRequest(v6MachineRequestFixture(configuration, verb != "VALIDATE"), machineCommand{Verb: verb, Resource: resource}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if request.SchemaVersion != 6 || request.RequestID != "MiXeD-01" ||
					verb != "VALIDATE" && request.IfRevision != "opaque-revision-1" {
					t.Fatalf("wrong envelope: %+v", request)
				}
				before, oldSet := v6PreparedFixture(t)
				saved := before.Clone()
				got, err := request.apply(before)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before, saved) {
					t.Fatal("apply changed before")
				}
				_, value, err := filter.PrepareV6Configuration(got)
				if err != nil {
					t.Fatal(err)
				}
				if resource != "SETTINGS" {
					want := []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "BAND=20M AND DXCONT=EU"}, {Slot: 9, Action: filter.RuleReject, Expr: "SNR<-10"}}
					if !reflect.DeepEqual(got.Filters.Rules, want) || request.filterFields != 127 || value.PreparedRules() != request.preparedRules {
						t.Fatalf("canonical rules/cache changed: %+v", got.Filters)
					}
					f := got.Filters
					if f.IncludeBeacons != filter.DefaultBoolFalse || f.AllowWWV != filter.DefaultBoolTrue ||
						f.AllowWCY != filter.DefaultBoolDefault || f.AllowAnnounce != filter.DefaultBoolFalse ||
						f.AllowSelf != filter.DefaultBoolDefault || f.AllowToxic != filter.DefaultBoolTrue {
						t.Fatalf("false/DEFAULT controls changed: %+v", f)
					}
				} else if value.PreparedRules() != oldSet || !reflect.DeepEqual(got.Filters, before.Filters) {
					t.Fatal("SETTINGS replaced omitted rules")
				}
				if resource != "FILTER" {
					want := filter.SettingsConfiguration{Dialect: "GO", NoiseClass: "QUIET", DedupePolicy: "FAST"}
					if request.settingFields != 63 || got.Settings != want {
						t.Fatalf("empty/zero settings re-defaulted: %+v", got.Settings)
					}
				} else if got.Settings != before.Settings {
					t.Fatal("FILTER changed settings")
				}
			})
		}
	}
}

func TestV6MachineSchemaPatchPresenceAndPreparedReuse(t *testing.T) {
	for _, tt := range []struct {
		name, resource, configuration string
		clear                         bool
	}{
		{"filter-omitted-rules", "FILTER", "allow_self: DEFAULT\n", false},
		{"filter-clear-rules", "FILTER", "rules: []\n", true},
		{"settings-zero-empty", "SETTINGS", "grid: \"\"\npath_min_observation_count: 0\nsolar_summary_minutes: 0\n", false},
		{"config-mixed", "CONFIG", "filters:\n  include_beacons: false\nsettings:\n  grid: \"\"\n  solar_summary_minutes: 0\n", false},
		{"config-empty", "CONFIG", "{}\n", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before, oldSet := v6PreparedFixture(t)
			saved := before.Clone()
			request, err := decodeV6MachineRequest(v6MachineRequestFixture(tt.configuration, true), machineCommand{Verb: "PATCH", Resource: tt.resource}, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := request.apply(before)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, saved) {
				t.Fatal("PATCH mutated before")
			}
			_, value, err := filter.PrepareV6Configuration(got)
			if err != nil {
				t.Fatal(err)
			}
			wantSet := oldSet
			if tt.clear {
				wantSet = nil
			}
			if value.PreparedRules() != wantSet {
				t.Fatal("PATCH recompiled omitted rules or failed to clear them")
			}
			switch tt.name {
			case "filter-omitted-rules":
				if got.Filters.AllowSelf != filter.DefaultBoolDefault || got.Settings != before.Settings {
					t.Fatal("supplied false/DEFAULT or omitted settings changed")
				}
			case "filter-clear-rules":
				if len(got.Filters.Rules) != 0 || got.Settings != before.Settings {
					t.Fatal("empty list did not replace only rules")
				}
			case "settings-zero-empty":
				if got.Settings.Grid != "" || got.Settings.PathMinObservationCount != 0 || got.Settings.SolarSummaryMinutes != 0 ||
					got.Settings.Dialect != before.Settings.Dialect || !reflect.DeepEqual(got.Filters, before.Filters) {
					t.Fatal("PATCH lost empty/zero or omitted fields")
				}
			case "config-mixed":
				if got.Filters.IncludeBeacons != filter.DefaultBoolFalse || got.Settings.Grid != "" ||
					got.Settings.SolarSummaryMinutes != 0 || got.Settings.NoiseClass != before.Settings.NoiseClass {
					t.Fatal("CONFIG PATCH re-defaulted or replaced omitted fields")
				}
			case "config-empty":
				if !reflect.DeepEqual(got, before) {
					t.Fatal("empty PATCH changed configuration")
				}
			}
		})
	}
	// Public DTO edits cannot redirect apply to a stale definition/program pair.
	request, err := decodeV6MachineRequest(v6MachineRequestFixture("rules: [{slot: 0, action: pass, expr: 'mode=cw'}]\n", true), machineCommand{Verb: "PATCH", Resource: "FILTER"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Configuration.Filters.Rules[0].Expr = "invalid public edit"
	got, err := request.apply(filter.V6Configuration{})
	if err != nil || got.Filters.Rules[0].Expr != "MODE=CW" {
		t.Fatalf("apply trusted edited public definitions: %v %+v", err, got)
	}
	_, value, err := filter.PrepareV6Configuration(got)
	if err != nil || value.PreparedRules() != request.preparedRules {
		t.Fatal("apply lost prepared set")
	}
	got.Filters.Rules[0].Expr = "changed result"
	if request.preparedRules.Definitions()[0].Expr != "MODE=CW" {
		t.Fatal("result mutation changed private immutable rules")
	}
}

func TestV6MachineSchemaRejectsMalformedProposals(t *testing.T) {
	base := string(v6MachineRequestFixture("rules: []\n", true))
	for _, tt := range []struct {
		name, body, resource, path string
	}{
		{"old-version", strings.Replace(base, "schema_version: 6", "schema_version: 5", 1), "FILTER", "schema_version"},
		{"future-version", strings.Replace(base, "schema_version: 6", "schema_version: 7", 1), "FILTER", "schema_version"},
		{"quoted-version", strings.Replace(base, "schema_version: 6", `schema_version: "5"`, 1), "FILTER", "schema_version"},
		{"missing-version", strings.Replace(base, "schema_version: 6\n", "", 1), "FILTER", "schema_version"},
		{"missing-id", strings.Replace(base, "request_id: MiXeD-01\n", "", 1), "FILTER", "request_id"},
		{"unicode-id", strings.Replace(base, "MiXeD-01", "réquest", 1), "FILTER", "request_id"},
		{"bad-id", strings.Replace(base, "MiXeD-01", "a_b", 1), "FILTER", "request_id"},
		{"empty-revision", strings.Replace(base, "opaque-revision-1", `""`, 1), "FILTER", "if_revision"},
		{"unicode-revision", strings.Replace(base, "opaque-revision-1", "rév", 1), "FILTER", "if_revision"},
		{"missing-revision", strings.Replace(base, "if_revision: opaque-revision-1\n", "", 1), "FILTER", "if_revision"},
		{"missing-config", strings.Replace(base, "configuration:\n  rules: []\n", "", 1), "FILTER", "configuration"},
		{"unknown-envelope", base + "unexpected: false\n", "FILTER", "document"},
		{"unknown-filter", string(v6MachineRequestFixture("bands: {}\n", true)), "FILTER", "configuration"},
		{"unknown-settings", string(v6MachineRequestFixture("unknown: false\n", true)), "SETTINGS", "configuration"},
		{"quoted-boolean", string(v6MachineRequestFixture("include_beacons: 'false'\n", true)), "FILTER", "configuration.include_beacons"},
		{"numeric-toggle", string(v6MachineRequestFixture("allow_self: 0\n", true)), "FILTER", "configuration.allow_self"},
		{"lower-default", string(v6MachineRequestFixture("allow_self: default\n", true)), "FILTER", "configuration.allow_self"},
		{"string-integer", string(v6MachineRequestFixture("solar_summary_minutes: '0'\n", true)), "SETTINGS", "configuration.solar_summary_minutes"},
		{"float-integer", string(v6MachineRequestFixture("path_min_observation_count: 1.5\n", true)), "SETTINGS", "configuration.path_min_observation_count"},
		{"bool-setting", string(v6MachineRequestFixture("grid: false\n", true)), "SETTINGS", "configuration.grid"},
		{"null", string(v6MachineRequestFixture("rules: null\n", true)), "FILTER", "configuration.rules"},
		{"alias", string(v6MachineRequestFixture("rules: &r []\nallow_self: *r\n", true)), "FILTER", "configuration.rules"},
		{"merge", string(v6MachineRequestFixture("<<: {rules: []}\n", true)), "FILTER", "configuration"},
		{"tag", string(v6MachineRequestFixture("rules: !custom []\n", true)), "FILTER", "configuration.rules"},
		{"duplicate", string(v6MachineRequestFixture("rules: []\nrules: []\n", true)), "FILTER", "configuration"},
		{"second-document", base + "---\n{}\n", "FILTER", "document"},
		{"empty-second-document", base + "---\n", "FILTER", "document"},
		{"too-large", strings.Repeat(" ", maxYAMLBytes+1), "FILTER", "document"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeV6MachineRequest([]byte(tt.body), machineCommand{Verb: "PATCH", Resource: tt.resource}, nil)
			if err == nil || !strings.HasPrefix(err.Error(), tt.path+":") {
				t.Fatalf("expected path %q, got %v", tt.path, err)
			}
		})
	}
	for _, command := range []machineCommand{{Verb: "GET", Resource: "CONFIG"}, {Verb: "VALIDATE", Resource: "FILTER"}, {Verb: "PUT", Resource: "CAPABILITIES"}, {Verb: "PATCH", Resource: "UNKNOWN"}} {
		if _, err := decodeV6MachineRequest([]byte(base), command, nil); err == nil {
			t.Fatalf("accepted invalid body command: %+v", command)
		}
	}
	if _, err := (v6MachineRequest{resource: "CAPABILITIES"}).apply(filter.V6Configuration{}); err == nil {
		t.Fatal("apply accepted read-only resource")
	}
}

func TestV6MachineSchemaRefusesRetiredToggle(t *testing.T) {
	for _, verb := range []string{"PUT", "PATCH", "VALIDATE"} {
		for _, enabled := range []string{"false", "true"} {
			for _, resource := range []string{"FILTER", "CONFIG"} {
				if verb == "VALIDATE" && resource != "CONFIG" {
					continue
				}
				configuration := v6CompleteFilterFixture + "nearby_enabled: " + enabled + "\n"
				if resource == "CONFIG" {
					configuration = "filters:\n" + indentMachineFixture(configuration) + "settings:\n" + indentMachineFixture(machineCompleteSettingsFixture)
				}
				_, err := decodeV6MachineRequest(v6MachineRequestFixture(configuration, true), machineCommand{Verb: verb, Resource: resource}, nil)
				if err == nil || !strings.Contains(err.Error(), "nearby_enabled") {
					t.Fatalf("%s %s silently accepted retired toggle %s: %v", verb, resource, enabled, err)
				}
			}
		}
	}
}

func TestV6MachineSchemaCompletenessAndRulePaths(t *testing.T) {
	config := "filters:\n" + indentMachineFixture(v6CompleteFilterFixture) + "settings:\n" + indentMachineFixture(machineCompleteSettingsFixture)
	for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG"} {
		fixture, fields := v6CompleteFilterFixture, machineV6FilterFields[:]
		switch resource {
		case "SETTINGS":
			fixture, fields = machineCompleteSettingsFixture, machineSettingFields[:]
		case "CONFIG":
			fixture, fields = config, []string{"filters", "settings"}
		}
		for _, name := range fields {
			t.Run(resource+"/"+name, func(t *testing.T) {
				var mapping yaml.Node
				if err := yaml.Unmarshal([]byte(fixture), &mapping); err != nil {
					t.Fatal(err)
				}
				root := mapping.Content[0]
				for i := 0; i < len(root.Content); i += 2 {
					if root.Content[i].Value == name {
						root.Content = append(root.Content[:i], root.Content[i+2:]...)
						break
					}
				}
				data, err := yaml.Marshal(&mapping)
				if err != nil {
					t.Fatal(err)
				}
				_, err = decodeV6MachineRequest(v6MachineRequestFixture(string(data), true), machineCommand{Verb: "PUT", Resource: resource}, nil)
				if err == nil || !strings.Contains(err.Error(), "configuration."+name+":") {
					t.Fatalf("missing %s did not fail at its path: %v", name, err)
				}
			})
		}
	}
	for _, tt := range []struct{ rule, path string }{
		{`{action: pass, expr: "MODE=CW"}`, ".slot"},
		{`{slot: null, action: pass, expr: "MODE=CW"}`, ".slot"},
		{`{slot: "0", action: pass, expr: "MODE=CW"}`, ".slot"},
		{`{slot: -1, action: pass, expr: "MODE=CW"}`, ".slot"},
		{`{slot: 10, action: pass, expr: "MODE=CW"}`, ".slot"},
		{`{slot: 0, expr: "MODE=CW"}`, ".action"},
		{`{slot: 0, action: null, expr: "MODE=CW"}`, ".action"},
		{`{slot: 0, action: PASS, expr: "MODE=CW"}`, ".action"},
		{`{slot: 0, action: false, expr: "MODE=CW"}`, ".action"},
		{`{slot: 0, action: pass}`, ".expr"},
		{`{slot: 0, action: pass, expr: false}`, ".expr"},
		{`{slot: 0, action: pass, expr: "FREQ>abc"}`, ".expr: col 6"},
		{`{slot: 0, action: pass, expr: "MODE=é"}`, ".expr: col 6"},
		{`{slot: 0, action: pass, expr: "AR=ON"}`, ".expr: col 1"},
		{`{slot: 0, action: pass, expr: "NEARBY=TRUE"}`, ".expr"},
		{`{slot: 0, action: pass, expr: "NEARBY ON"}`, ".expr"},
	} {
		for _, resource := range []string{"FILTER", "CONFIG"} {
			configuration, path := "rules: ["+tt.rule+"]\n", "configuration.rules[0]"
			if resource == "CONFIG" {
				configuration, path = "filters:\n"+indentMachineFixture(configuration), "configuration.filters.rules[0]"
			}
			_, err := decodeV6MachineRequest(v6MachineRequestFixture(configuration, true), machineCommand{Verb: "PATCH", Resource: resource}, nil)
			if err == nil || !strings.Contains(err.Error(), path+tt.path) {
				t.Fatalf("%s %s: wrong path/column: %v", resource, tt.rule, err)
			}
		}
	}
	// Duplicate identity wins even when an earlier expression is malformed.
	configuration := "rules: [{slot: 0, action: pass, expr: bad}, {slot: 0, action: reject, expr: bad}]\n"
	_, err := decodeV6MachineRequest(v6MachineRequestFixture(configuration, true), machineCommand{Verb: "PATCH", Resource: "FILTER"}, nil)
	if err == nil || !strings.Contains(err.Error(), "configuration.rules[1].slot: duplicate") {
		t.Fatalf("duplicate was compiled as a replacement: %v", err)
	}
}

func v6RuleListFixture(sources []string) string {
	var value strings.Builder
	value.WriteString("rules:\n")
	for i, source := range sources {
		fmt.Fprintf(&value, "  - {slot: %d, action: pass, expr: %q}\n", i, source)
	}
	return value.String()
}

func TestV6MachineSchemaRuleBudgets(t *testing.T) {
	maxAtoms := strings.Repeat("SNR=1 OR ", expr.MaxAtoms-1) + "SNR=1"
	nearbyAtoms := strings.Repeat("NEARBY OR ", expr.MaxAtoms-1) + "NEARBY"
	maxValues := "SNR=" + strings.Repeat("1,", expr.MaxValuesPerAtom-1) + "1"
	maxNodes := strings.Repeat("NOT NOT SNR=1 OR ", 31) + "NOT NOT NOT SNR=1"
	list := strings.Repeat("1,", 31) + "1"
	manyValues := strings.Repeat("SNR="+list+" OR ", 7) + "SNR=" + list
	for _, tt := range []struct {
		name    string
		sources []string
		valid   bool
	}{
		{"bytes-at", []string{"MODE=CW" + strings.Repeat(" ", expr.MaxBytes-len("MODE=CW"))}, true},
		{"bytes-over", []string{"MODE=CW" + strings.Repeat(" ", expr.MaxBytes+1-len("MODE=CW"))}, false},
		{"atoms-at", []string{maxAtoms}, true},
		{"atoms-over", []string{maxAtoms + " OR SNR=1"}, false},
		{"list-at", []string{maxValues}, true},
		{"list-over", []string{maxValues + ",1"}, false},
		{"depth-at", []string{strings.Repeat("NOT ", expr.MaxDepth) + "SNR=1"}, true},
		{"depth-over", []string{strings.Repeat("NOT ", expr.MaxDepth+1) + "SNR=1"}, false},
		{"nodes-at", []string{maxNodes}, true},
		{"nodes-over", []string{"NOT " + maxNodes}, false},
		{"total-atoms-at", []string{maxAtoms, maxAtoms, maxAtoms, maxAtoms}, true},
		{"total-atoms-over", []string{maxAtoms, maxAtoms, maxAtoms, maxAtoms, "SNR=1"}, false},
		{"nearby-total-atoms-zero-values", []string{nearbyAtoms, nearbyAtoms, nearbyAtoms, nearbyAtoms}, true},
		{"nearby-total-atoms-over", []string{nearbyAtoms, nearbyAtoms, nearbyAtoms, nearbyAtoms, "NEARBY"}, false},
		{"total-values-at", []string{manyValues, manyValues}, true},
		{"total-values-over", []string{manyValues, manyValues, "SNR=1"}, false},
		{"slots-at", []string{"SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1"}, true},
		{"slots-over", []string{"SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1", "SNR=1"}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request, err := decodeV6MachineRequest(v6MachineRequestFixture(v6RuleListFixture(tt.sources), true), machineCommand{Verb: "PATCH", Resource: "FILTER"}, nil)
			if (err == nil) != tt.valid {
				t.Fatalf("valid=%v err=%v", tt.valid, err)
			}
			if err == nil && len(request.Configuration.Filters.Rules) != len(tt.sources) {
				t.Fatal("budget admission discarded rules")
			}
		})
	}
}

func TestV6MachineSchemaExactDocumentByteLimit(t *testing.T) {
	body := v6MachineRequestFixture("rules: []\n", true)
	body = append(body, bytes.Repeat([]byte(" "), maxYAMLBytes-len(body))...)
	if _, err := decodeV6MachineRequest(body, machineCommand{Verb: "PATCH", Resource: "FILTER"}, nil); err != nil {
		t.Fatalf("exact byte bound rejected: %v", err)
	}

	if _, err := decodeV6MachineRequest(append(body, ' '), machineCommand{Verb: "PATCH", Resource: "FILTER"}, nil); err == nil {
		t.Fatal("overlong document admitted")
	}
}

func TestV6MachineSchemaDefersSettingSemantics(t *testing.T) {
	// Only the later server coordinator knows runtime settings availability and
	// overrides. This boundary must not default or silently repair exact scalars.
	body := v6MachineRequestFixture("dialect: unrecognized\ngrid: \"\"\nnoise_class: other\ndedupe_policy: other\npath_min_observation_count: -1\nsolar_summary_minutes: -1\n", true)
	request, err := decodeV6MachineRequest(body, machineCommand{Verb: "PUT", Resource: "SETTINGS"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := request.apply(filter.V6Configuration{})
	if err != nil || got.Settings.Dialect != "unrecognized" || got.Settings.NoiseClass != "other" ||
		got.Settings.DedupePolicy != "other" || got.Settings.PathMinObservationCount != -1 || got.Settings.SolarSummaryMinutes != -1 {
		t.Fatalf("pure schema boundary reinterpreted setting semantics: %+v %v", got.Settings, err)
	}
}
