package filter

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"dxcluster/pathreliability"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

func mustPrepareV5Configuration(t testing.TB, c V5Configuration) (V5Configuration, Filter) {
	t.Helper()
	prepared, runtime, err := PrepareV5Configuration(c)
	if err != nil {
		t.Fatal(err)
	}
	return prepared, runtime
}

func v5ControlFields(c *V5FilterConfiguration) [6]*DefaultBool {
	return [6]*DefaultBool{&c.IncludeBeacons, &c.AllowWWV, &c.AllowWCY, &c.AllowAnnounce, &c.AllowSelf, &c.AllowToxic}
}

func v5RuntimeControls(f *Filter) [6]*bool {
	return [6]*bool{f.IncludeBeacons, f.AllowWWV, f.AllowWCY, f.AllowAnnounce, f.AllowSelf, f.AllowToxic}
}

func TestV5ConfigurationPreparation(t *testing.T) {
	input := V5Configuration{
		Filters: V5FilterConfiguration{
			Rules: []RuleDefinition{
				{9, RuleReject, "(mode=psk31)"},
				{0, RulePass, "band=20M and mode=cw"},
			},
			NearbyEnabled: true, IncludeBeacons: DefaultBoolFalse,
			AllowWWV: DefaultBoolTrue, AllowAnnounce: DefaultBoolFalse,
		},
		Settings: SettingsConfiguration{
			Dialect: "cc", Grid: "", NoiseClass: "URBAN", DedupePolicy: "",
			PathMinObservationCount: 0, SolarSummaryMinutes: 0,
		},
	}
	before := input.Clone()
	c, f := mustPrepareV5Configuration(t, input)
	want := []RuleDefinition{{0, RulePass, "BAND=20M AND MODE=CW"}, {9, RuleReject, "MODE=PSK"}}
	if !slices.Equal(c.Filters.Rules, want) || !slices.Equal(f.PreparedRules().Definitions(), want) {
		t.Fatalf("noncanonical configuration: %#v", c.Filters.Rules)
	}
	if !reflect.DeepEqual(input, before) || c.Settings != input.Settings {
		t.Fatal("preparation mutated input or re-defaulted exact settings")
	}
	if !f.NearbyEnabled || f.NearbyUserFine != pathreliability.InvalidCell ||
		f.NearbyUserCoarse != pathreliability.InvalidCell {
		t.Fatal("preparation installed runtime NEARBY state")
	}
	wantRuntime := Filter{
		preparedRules: c.Filters.preparedRules, NearbyEnabled: true,
		IncludeBeacons: boolPtr(false), AllowWWV: boolPtr(true), AllowAnnounce: boolPtr(false),
		NearbyUserFine: pathreliability.InvalidCell, NearbyUserCoarse: pathreliability.InvalidCell,
	}
	if !reflect.DeepEqual(f, wantRuntime) {
		t.Fatal("preparation changed the compact runtime owner")
	}
	input.Filters.Rules[0].Expr = "MODE=FT8"
	c.Filters.Rules[0].Expr = "MODE=USB"
	if !slices.Equal(f.PreparedRules().Definitions(), want) ||
		!f.PreparedRules().Matches(&spot.Spot{Band: "20m", Mode: "CW", IsHuman: true}, "", false) ||
		f.PreparedRules().Matches(&spot.Spot{Band: "20m", Mode: "FT8", IsHuman: true}, "", false) {
		t.Fatal("public definition mutation changed runtime programs")
	}
}

func TestV5ConfigurationEmptyRulesAreNotDefaults(t *testing.T) {
	// Plain DTOs have no presence information. Disk and machine decoders must
	// reject null/missing required fields before invoking model preparation.
	for _, raw := range []string{
		"{}", "filters: {}", "filters: {rules: []}", "filters: {rules: null}",
	} {
		t.Run(raw, func(t *testing.T) {
			var input V5Configuration
			if err := yaml.Unmarshal([]byte(raw), &input); err != nil {
				t.Fatal(err)
			}
			c, f := mustPrepareV5Configuration(t, input)
			if len(c.Filters.Rules) != 0 || f.PreparedRules() != nil ||
				c.Settings != (SettingsConfiguration{}) || c.Filters.toggles() != ([6]DefaultBool{}) {
				t.Fatal("empty input acquired personal rules or settings defaults")
			}
			if !f.PreparedRules().Matches(&spot.Spot{Mode: "FT8"}, "", false) {
				t.Fatal("empty personal rules are not unrestricted")
			}
			for _, control := range v5RuntimeControls(&f) {
				if control != nil {
					t.Fatal("DEFAULT became explicit")
				}
			}
			encoded, err := yaml.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(encoded), "rules: []\n") || strings.Contains(string(encoded), "rules: null") {
				t.Fatalf("empty rules are not encoded as []: %s", encoded)
			}
		})
	}
	c, _ := mustPrepareV5Configuration(t, V5Configuration{
		Filters: V5FilterConfiguration{Rules: []RuleDefinition{{0, RulePass, "MODE=CW"}}},
	})
	for _, rules := range [][]RuleDefinition{nil, {}} {
		reset := c
		reset.Filters.Rules = rules
		prepared, runtime := mustPrepareV5Configuration(t, reset)
		if runtime.PreparedRules() != nil || len(prepared.Filters.Rules) != 0 || c.Filters.preparedRules == nil {
			t.Fatal("empty public edit did not reset rules independently")
		}
	}
	if (*Filter)(nil).PreparedRules() != nil {
		t.Fatal("nil filter does not represent absence")
	}
}

func TestV5ConfigurationControlsPreserveThreeStates(t *testing.T) {
	for value := DefaultBoolDefault; value <= DefaultBoolTrue; value++ {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			c := V5Configuration{}
			for _, control := range v5ControlFields(&c.Filters) {
				*control = value
			}
			prepared, runtime := mustPrepareV5Configuration(t, c)
			if prepared.Filters.toggles() != c.Filters.toggles() {
				t.Fatal("preparation changed controls")
			}
			for _, control := range v5RuntimeControls(&runtime) {
				if DefaultBoolFromPointer(control) != value {
					t.Fatalf("runtime lost control selection %d", value)
				}
			}
			encoded, err := yaml.Marshal(prepared)
			if err != nil {
				t.Fatal(err)
			}
			var decoded V5Configuration
			if err := yaml.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if !prepared.Equal(decoded) {
				t.Fatal("normal YAML roundtrip changed exact controls")
			}
		})
	}
}

func TestV5ConfigurationInvalidControlsRefused(t *testing.T) {
	base, runtime := mustPrepareV5Configuration(t, V5Configuration{
		Filters: V5FilterConfiguration{Rules: []RuleDefinition{{0, RulePass, "MODE=CW"}}},
	})
	names := [6]string{"include_beacons", "allow_wwv", "allow_wcy", "allow_announce", "allow_self", "allow_toxic"}
	for i, name := range names {
		for _, value := range []DefaultBool{3, 255} {
			t.Run(fmt.Sprintf("%s-%d", name, value), func(t *testing.T) {
				input := base.Clone()
				*v5ControlFields(&input.Filters)[i] = value
				before := input.Clone()
				got, f, err := PrepareV5Configuration(input)
				if err == nil || !strings.Contains(err.Error(), name) ||
					!reflect.DeepEqual(got, V5Configuration{}) || !reflect.DeepEqual(f, Filter{}) {
					t.Fatalf("invalid control yielded publishable state: %v", err)
				}
				if !reflect.DeepEqual(input, before) || runtime.PreparedRules() != base.Filters.preparedRules {
					t.Fatal("refused control mutated source")
				}
			})
		}
	}
	invalid := base.Clone()
	invalid.Filters.AllowToxic = 3
	invalid.Filters.Rules[0].Expr = "bad"
	if _, _, err := PrepareV5Configuration(invalid); err == nil || !strings.Contains(err.Error(), "allow_toxic") {
		t.Fatalf("rules compiled before control admission: %v", err)
	}
}

func TestV5ConfigurationRuleErrorsRefused(t *testing.T) {
	base, runtime := mustPrepareV5Configuration(t, V5Configuration{
		Filters: V5FilterConfiguration{Rules: []RuleDefinition{{0, RulePass, "MODE=CW"}}},
	})
	for _, test := range []struct {
		name, errorText string
		rules           []RuleDefinition
	}{
		{"negative-slot", "slot", []RuleDefinition{{-1, RulePass, "MODE=CW"}}},
		{"high-slot", "slot", []RuleDefinition{{10, RulePass, "MODE=CW"}}},
		{"missing-action", "action", []RuleDefinition{{0, "", "MODE=CW"}}},
		{"uppercase-action", "action", []RuleDefinition{{0, "PASS", "MODE=CW"}}},
		{"spaced-action", "action", []RuleDefinition{{0, "reject ", "MODE=CW"}}},
		{"missing-expression", "rule slot 0", []RuleDefinition{{0, RulePass, ""}}},
		{"invalid-expression", "rule slot 0", []RuleDefinition{{0, RulePass, "MODE=BAD"}}},
		{"oversized-expression", "rule slot 0", []RuleDefinition{{0, RulePass, strings.Repeat("x", 1025)}}},
		{"duplicate-slot", "duplicate", []RuleDefinition{{0, RulePass, "MODE=CW"}, {0, RuleReject, "MODE=FT8"}}},
		{"oversized-list", "slot limit", make([]RuleDefinition, MaxRuleSlots+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := base
			input.Filters.Rules = test.rules
			before := slices.Clone(input.Filters.Rules)
			got, f, err := PrepareV5Configuration(input)
			if err == nil || !strings.Contains(err.Error(), test.errorText) ||
				!reflect.DeepEqual(got, V5Configuration{}) || !reflect.DeepEqual(f, Filter{}) {
				t.Fatalf("invalid rules yielded publishable state: %v", err)
			}
			if !slices.Equal(input.Filters.Rules, before) ||
				!runtime.PreparedRules().Matches(&spot.Spot{Mode: "CW", IsHuman: true}, "", false) {
				t.Fatal("refused edit changed source rules")
			}
		})
	}
}
