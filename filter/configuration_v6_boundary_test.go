package filter

import (
	"bytes"
	"reflect"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestV6ConfigurationLegacyCarrierBoundary(t *testing.T) {
	if CurrentConfigurationVersion != 6 {
		t.Fatal("v6 model is not the active storage version")
	}
	f := NewFilter()
	settings := SettingsConfiguration{Dialect: "cc", Grid: "FN31", SolarSummaryMinutes: 0}
	without := ConfigurationFromFilter(f, settings)
	filterYAML, err := yaml.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	configurationYAML, err := yaml.Marshal(without)
	if err != nil {
		t.Fatal(err)
	}
	set := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, "MODE=CW"})
	f.preparedRules = set
	with := ConfigurationFromFilter(f, settings)
	clone := with.Clone()
	restored := clone.FilterValue()
	if with.Filters.preparedRules != set || clone.Filters.preparedRules != set || restored.PreparedRules() != set {
		t.Fatal("legacy capture/clone/publication lost the exact immutable carrier")
	}
	if with.Equal(without) || with.Fingerprint() == without.Fingerprint() ||
		clone.Fingerprint() != with.Fingerprint() {
		t.Fatal("native identity omitted active rules")
	}
	for _, test := range []struct {
		name   string
		value  any
		before []byte
	}{
		{"filter", f, filterYAML},
		{"configuration", with, configurationYAML},
		{"configuration-clone", clone, configurationYAML},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := yaml.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(encoded, test.before) {
				t.Fatalf("active rules absent from native YAML:\n%s", encoded)
			}
			if !strings.Contains(string(encoded), "rules:") || !strings.Contains(string(encoded), "MODE=CW") {
				t.Fatal("native serialization omitted definitions")
			}
			for _, forbidden := range []string{"prepared", "program", "nodes:", "bands:", "allmodes:"} {
				if strings.Contains(string(encoded), forbidden) {
					t.Fatalf("legacy YAML leaked %q", forbidden)
				}
			}
		})
	}
	readback := V6ConfigurationFromFilter(&restored, settings)
	readback.Filters.Rules[0].Expr = "MODE=USB"
	if restored.PreparedRules() != set || set.Definitions()[0].Expr != "MODE=CW" {
		t.Fatal("legacy-restored readback aliases immutable definitions")
	}
}

func TestV6ConfigurationCopiedControlSetters(t *testing.T) {
	input := V6Configuration{}
	for _, control := range v6ControlFields(&input.Filters) {
		*control = DefaultBoolFalse
	}
	input.Filters = input.Filters.WithRuleSet(mustPrepareRuleSet(t, RuleDefinition{0, RulePass, "MODE=CW"}))
	_, original := mustPrepareV6Configuration(t, input)
	setters := [6]func(*Filter, bool){
		(*Filter).SetBeaconEnabled, (*Filter).SetWWVEnabled, (*Filter).SetWCYEnabled,
		(*Filter).SetAnnounceEnabled, (*Filter).SetSelfEnabled, (*Filter).SetToxicEnabled,
	}
	for i, set := range setters {
		copied := original
		set(&copied, true)
		if !*v6RuntimeControls(&copied)[i] || *v6RuntimeControls(&original)[i] ||
			copied.PreparedRules() != original.PreparedRules() {
			t.Fatalf("setter %d changed a previous filter or its carrier", i)
		}
	}
	captured := ConfigurationFromFilter(&original, SettingsConfiguration{}).Clone()
	restored := captured.FilterValue()
	for _, pointer := range v6RuntimeControls(&restored) {
		*pointer = true
	}
	for _, pointer := range v6RuntimeControls(&original) {
		if *pointer {
			t.Fatal("legacy conversion borrowed a runtime control pointer")
		}
	}
	_, fresh := mustPrepareV6Configuration(t, input)
	for i, pointer := range v6RuntimeControls(&fresh) {
		if pointer == v6RuntimeControls(&original)[i] || *pointer {
			t.Fatal("preparation borrowed another filter's controls")
		}
	}
}

func TestV6ConfigurationFingerprint(t *testing.T) {
	base, runtime := mustPrepareV6Configuration(t, V6Configuration{
		Filters: V6FilterConfiguration{Rules: []RuleDefinition{
			{0, RulePass, "MODE=CW"}, {9, RuleReject, "MODE=FT8"},
		}},
	})
	reordered, other := mustPrepareV6Configuration(t, V6Configuration{
		Filters: V6FilterConfiguration{Rules: []RuleDefinition{
			{9, RuleReject, "(mode=ft8)"}, {0, RulePass, "mode=cw"},
		}},
	})
	if runtime.PreparedRules() == other.PreparedRules() || !base.Equal(reordered) ||
		base.Fingerprint() != reordered.Fingerprint() {
		t.Fatal("canonical equality depends on input order, spelling or program identity")
	}
	for _, test := range []struct {
		name   string
		mutate func(*V6Configuration)
	}{
		{"slot", func(c *V6Configuration) { c.Filters.Rules[0].Slot = 1 }},
		{"action", func(c *V6Configuration) { c.Filters.Rules[0].Action = RuleReject }},
		{"expression", func(c *V6Configuration) { c.Filters.Rules[0].Expr = "MODE=USB" }},
		{"rule-count", func(c *V6Configuration) { c.Filters.Rules = c.Filters.Rules[:1] }},
		{"dialect", func(c *V6Configuration) { c.Settings.Dialect = "cc" }},
		{"grid", func(c *V6Configuration) { c.Settings.Grid = "FN31" }},
		{"noise-class", func(c *V6Configuration) { c.Settings.NoiseClass = "URBAN" }},
		{"dedupe-policy", func(c *V6Configuration) { c.Settings.DedupePolicy = "SLOW" }},
		{"path-minimum", func(c *V6Configuration) { c.Settings.PathMinObservationCount = 1 }},
		{"solar-minutes", func(c *V6Configuration) { c.Settings.SolarSummaryMinutes = 15 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			edited := base.Clone()
			test.mutate(&edited)
			changed, _ := mustPrepareV6Configuration(t, edited)
			if base.Equal(changed) || base.Fingerprint() == changed.Fingerprint() {
				t.Fatal("changed canonical value compares unchanged")
			}
		})
	}
	for i := range base.Filters.toggles() {
		off, on := base.Clone(), base.Clone()
		*v6ControlFields(&off.Filters)[i] = DefaultBoolFalse
		*v6ControlFields(&on.Filters)[i] = DefaultBoolTrue
		off, _ = mustPrepareV6Configuration(t, off)
		on, _ = mustPrepareV6Configuration(t, on)
		if base.Equal(off) || base.Equal(on) || off.Equal(on) ||
			base.Fingerprint() == off.Fingerprint() || base.Fingerprint() == on.Fingerprint() ||
			off.Fingerprint() == on.Fingerprint() {
			t.Fatalf("control %d lost a tri-state distinction", i)
		}
	}
	var nilRules V6Configuration
	emptyRules := V6Configuration{Filters: V6FilterConfiguration{Rules: []RuleDefinition{}}}
	if !nilRules.Equal(emptyRules) || nilRules.Fingerprint() != emptyRules.Fingerprint() {
		t.Fatal("nil/empty lists compare differently")
	}
	if nilRules.Fingerprint() != (Configuration{}).Fingerprint() {
		t.Fatal("native and v6 fingerprints disagree")
	}
}

func TestV6ConfigurationYAMLOnlyContainsDefinitions(t *testing.T) {
	c, runtime := mustPrepareV6Configuration(t, V6Configuration{
		Filters:  V6FilterConfiguration{Rules: []RuleDefinition{{0, RuleReject, "MODE=PSK31"}}},
		Settings: SettingsConfiguration{Dialect: "", SolarSummaryMinutes: 0},
	})
	encoded, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"prepared", "program", "nodes", "normal", "syntax", "nearby_enabled"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("v6 YAML leaked %q: %s", forbidden, encoded)
		}
	}
	var decoded V6Configuration
	if err := yaml.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Filters.preparedRules != nil || !slices.Equal(decoded.Filters.Rules, c.Filters.Rules) {
		t.Fatal("normal YAML decoding carried hidden programs or lost definitions")
	}
	prepared, fresh := mustPrepareV6Configuration(t, decoded)
	if !reflect.DeepEqual(prepared.Settings, c.Settings) || !prepared.Equal(c) ||
		prepared.Fingerprint() != c.Fingerprint() || fresh.PreparedRules() == runtime.PreparedRules() {
		t.Fatal("definition-only YAML changed exact values or reused an unattested program")
	}
	settings := SettingsConfiguration{Dialect: "not-validated-here", Grid: "", PathMinObservationCount: -1}
	readback := V6ConfigurationFromFilter(nil, settings)
	prepared, fresh = mustPrepareV6Configuration(t, readback)
	if prepared.Settings != settings || fresh.PreparedRules() != nil {
		t.Fatal("model applied server-owned settings validation or defaults")
	}
}
