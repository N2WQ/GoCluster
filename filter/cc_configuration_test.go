package filter

import (
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"testing"

	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

func ccTestBase() CCConfiguration {
	return CCConfiguration{Modes: []string{"CW", "FT8", "UNKNOWN"}, Sources: []string{"HUMAN", "SKIMMER"}}
}

func ccTestSpot() *spot.Spot {
	return &spot.Spot{Frequency: 14025, Mode: "CW", ModeNorm: "CW", IsHuman: true,
		DEMetadata: spot.CallMetadata{ADIF: 291, State: "CA"},
		DXMetadata: spot.CallMetadata{ADIF: 230, State: "ON"}}
}

func TestCCDefaultsFactoryEquivalenceAndLiteralOutcomes(t *testing.T) {
	previous := configuredDefaultRules.Load()
	t.Cleanup(func() { configuredDefaultRules.Store(previous) })
	if err := ConfigureDefaultRules([]string{"CW", "PSK31", "USB", "UNKNOWN"}, []string{"HUMAN"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := CurrentDefaultCCConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.Modes, []string{"CW", "PSK", "UNKNOWN", "USB"}) || !slices.Equal(cfg.Sources, []string{"HUMAN"}) {
		t.Fatalf("current operator defaults not canonicalized exactly: %+v", cfg)
	}
	for _, toggle := range cfg.toggles() {
		if toggle != DefaultBoolDefault {
			t.Fatal("native defaults must retain operator DEFAULT controls")
		}
	}
	_, native, err := PrepareCCConfiguration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	goDefault := newFilterDefaultRules()
	for _, probe := range []struct {
		mode  string
		human bool
		want  bool
	}{
		{"CW", true, true}, {"CW", false, false}, {"PSK31", true, true},
		{"USB", true, true}, {"", true, true}, {"FT8", true, false}, {"RTTY", true, false},
	} {
		sp := ccTestSpot()
		sp.Mode, sp.ModeNorm, sp.IsHuman = probe.mode, "", probe.human
		if got := native.Matches(sp, EvaluationContext{}); got != probe.want {
			t.Fatalf("literal native default outcome for %+v: %t", probe, got)
		}
		if native.Matches(sp, EvaluationContext{}) != goDefault.Matches(sp, EvaluationContext{}) {
			t.Fatalf("native/GO operator default disagreement for %+v", probe)
		}
	}
	if _, err := DefaultCCConfiguration([]string{"not-a-mode"}, nil); err == nil {
		t.Fatal("invalid configured defaults admitted")
	}
}

func TestCCExplicitEmptySelectorsAndNonShortcutModes(t *testing.T) {
	for _, cfg := range []CCConfiguration{
		{}, {Modes: []string{"CW"}}, {Sources: []string{"HUMAN"}},
	} {
		_, program, err := PrepareCCConfiguration(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if program.Matches(ccTestSpot(), EvaluationContext{}) {
			t.Fatal("empty native selection silently installed defaults or unrestricted matching")
		}
	}
	_, program, err := PrepareCCConfiguration(CCConfiguration{
		Modes: []string{"USB", "LSB", "MSK144", "PSK31", "SSTV", "UNKNOWN"}, Sources: []string{"HUMAN", "SKIMMER"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"USB", "LSB", "MSK144", "PSK", "SSTV", ""} {
		for _, human := range []bool{false, true} {
			sp := ccTestSpot()
			sp.Mode, sp.ModeNorm, sp.IsHuman = mode, mode, human
			if !program.Matches(sp, EvaluationContext{}) {
				t.Fatalf("filter-visible mode without CC shortcut unavailable: %q human=%t", mode, human)
			}
		}
	}
	var absent *PreparedCCFilter
	if absent.Matches(ccTestSpot(), EvaluationContext{}) || program.Matches(nil, EvaluationContext{}) {
		t.Fatal("nil prepared owner or nil spot must fail closed")
	}
}

func TestCCRawBoundsBeforeDeduplicationAndPreparation(t *testing.T) {
	for _, example := range []struct {
		name string
		edit func(*CCConfiguration)
	}{
		{"modes", func(c *CCConfiguration) { c.Modes = slices.Repeat([]string{"CW"}, MaxCCModes+1) }},
		{"sources", func(c *CCConfiguration) { c.Sources = []string{"HUMAN", "HUMAN", "HUMAN"} }},
		{"countries", func(c *CCConfiguration) {
			c.Geography = map[string]CCGeography{"DOC": {Global: CCCategory{Action: CCPass, Countries: slices.Repeat([]int{291}, MaxCCCountries+1)}}}
		}},
		{"states", func(c *CCConfiguration) {
			c.Geography = map[string]CCGeography{"DOS": {Global: CCCategory{Action: CCPass, States: slices.Repeat([]string{"CA"}, MaxCCStates+1)}}}
		}},
		{"category-count", func(c *CCConfiguration) {
			c.Geography = map[string]CCGeography{"DOC": {}, "DOS": {}, "DXCTY": {}, "DXSTATE": {}, "BAD": {}}
		}},
		{"band-count", func(c *CCConfiguration) {
			bands := map[string]CCCategory{}
			for i := 0; i < 16; i++ {
				bands[fmt.Sprint(i)] = CCCategory{Action: CCOff}
			}
			c.Geography = map[string]CCGeography{"DOC": {Global: CCCategory{Action: CCOff}, Bands: bands}}
		}},
		{"slot-count", func(c *CCConfiguration) {
			c.DXBM.Slots = map[string]bool{}
			for i := 0; i < 38; i++ {
				c.DXBM.Slots[fmt.Sprint(i)] = true
			}
		}},
		{"raw-scalar-bytes", func(c *CCConfiguration) { c.Modes = []string{strings.Repeat("X", MaxCCCanonicalBytes+1)} }},
	} {
		t.Run(example.name, func(t *testing.T) {
			cfg := ccTestBase()
			example.edit(&cfg)
			if _, _, err := PrepareCCConfiguration(cfg); !errors.Is(err, ErrStorageAdmission) {
				t.Fatalf("raw resource refusal must precede deduplication/domain preparation: %v", err)
			}
		})
	}
}

func TestCCMaximumTypedValuesAndInvalidDefinitions(t *testing.T) {
	countries := make([]int, MaxCCCountries)
	for i := range countries {
		countries[i] = math.MaxInt - i*2
	}
	cfg := ccTestBase()
	cfg.Geography = map[string]CCGeography{
		"DOC": {Global: CCCategory{Action: CCPass, Countries: countries}},
		"DOS": {Global: CCCategory{Action: CCPass, States: spot.StateCodes()}},
	}
	canonical, program, err := PrepareCCConfiguration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if program.Counts().Countries != 512 || program.Counts().States != 73 {
		t.Fatalf("typed sparse operand capacity: %+v", program.Counts())
	}
	sp := ccTestSpot()
	sp.DEMetadata.ADIF = math.MaxInt
	if !program.Matches(sp, EvaluationContext{}) {
		t.Fatal("sparse maximum positive ADIF did not match")
	}
	for _, bad := range []struct {
		name string
		edit func(*CCConfiguration)
	}{
		{"unknown-mode", func(c *CCConfiguration) { c.Modes = []string{"BAD"} }},
		{"non-filter-visible-mode", func(c *CCConfiguration) { c.Modes = []string{"WSPR"} }},
		{"blank-mode-operand", func(c *CCConfiguration) { c.Modes = []string{""} }},
		{"unknown-source", func(c *CCConfiguration) { c.Sources = []string{"RBN"} }},
		{"unknown-category", func(c *CCConfiguration) { c.Geography = map[string]CCGeography{"AOC": {}} }},
		{"duplicate-category", func(c *CCConfiguration) { c.Geography = map[string]CCGeography{"DOC": {}, "doc": {}} }},
		{"wrong-value-family", func(c *CCConfiguration) {
			c.Geography = map[string]CCGeography{"DOC": {Global: CCCategory{Action: CCPass, States: []string{"CA"}}}}
		}},
		{"off-has-values", func(c *CCConfiguration) {
			c.Geography = map[string]CCGeography{"DOC": {Global: CCCategory{Action: CCOff, Countries: []int{291}}}}
		}},
		{"country-zero", func(c *CCConfiguration) {
			c.Geography = map[string]CCGeography{"DOC": {Global: CCCategory{Action: CCPass, Countries: []int{0}}}}
		}},
		{"country-negative", func(c *CCConfiguration) {
			c.Geography = map[string]CCGeography{"DOC": {Global: CCCategory{Action: CCPass, Countries: []int{-1}}}}
		}},
		{"bad-state", func(c *CCConfiguration) {
			c.Geography = map[string]CCGeography{"DOS": {Global: CCCategory{Action: CCPass, States: []string{"ZZ"}}}}
		}},
		{"invalid-action", func(c *CCConfiguration) {
			c.Geography = map[string]CCGeography{"DOC": {Global: CCCategory{Action: "maybe"}}}
		}},
		{"unknown-band", func(c *CCConfiguration) {
			c.Geography = map[string]CCGeography{"DOC": {Global: CCCategory{Action: CCOff}, Bands: map[string]CCCategory{"70cm": {Action: CCOff}}}}
		}},
		{"unknown-slot-even-off", func(c *CCConfiguration) { c.DXBM = CCDXBM{Slots: map[string]bool{"20-FT8": true}} }},
		{"invalid-control", func(c *CCConfiguration) { c.AllowToxic = DefaultBool(42) }},
	} {
		t.Run(bad.name, func(t *testing.T) {
			candidate := canonical.Clone()
			bad.edit(&candidate)
			if _, _, err := PrepareCCConfiguration(candidate); err == nil {
				t.Fatal("invalid native definition accepted")
			}
			if !program.Matches(sp, EvaluationContext{}) {
				t.Fatal("failed preparation changed an earlier immutable program")
			}
		})
	}
}

func TestCCCanonicalByteAdmissionAndImmutableSnapshots(t *testing.T) {
	input := ccTestBase()
	input.Geography = map[string]CCGeography{"DOC": {
		Global: CCCategory{Action: CCPass, Countries: []int{291}},
		Bands:  map[string]CCCategory{"20": {Action: CCPass, Countries: []int{230}}},
	}}
	input.DXBM = CCDXBM{Active: true, BaseAllow: true, Slots: map[string]bool{"40-CW": false}}
	canonical, program, err := PrepareCCConfiguration(input)
	if err != nil {
		t.Fatal(err)
	}
	unchanged := program.CanonicalConfig()
	input.Modes[0], input.Sources[0] = "BAD", "BAD"
	input.Geography["DOC"].Global.Countries[0] = 999
	input.Geography["DOC"].Bands["20"].Countries[0] = 999
	input.DXBM.Slots["40-CW"] = true
	canonical.Modes[0] = "BAD"
	canonical.Geography["DOC"].Bands["20"].Countries[0] = -1
	if _, _, err := PrepareCCConfiguration(canonical); err == nil {
		t.Fatal("repreparation trusted an invalidated public DTO")
	}
	exported := program.CanonicalConfig()
	exported.Geography["DOC"].Global.Countries[0] = 999
	exported.DXBM.Slots["40-CW"] = true
	if !program.CanonicalConfig().Equal(unchanged) || program.CanonicalConfig().Fingerprint() != unchanged.Fingerprint() {
		t.Fatal("caller mutation reached the prepared generation")
	}
	sp := ccTestSpot()
	sp.DEMetadata.ADIF = 230
	if !program.Matches(sp, EvaluationContext{}) {
		t.Fatal("caller mutation changed matching")
	}
	huge := ccTestBase()
	huge.Geography = map[string]CCGeography{}
	values := make([]int, 512)
	for i := range values {
		values[i] = math.MaxInt - i
	}
	for _, name := range []string{"DOC", "DXCTY"} {
		geography := CCGeography{Global: CCCategory{Action: CCPass, Countries: values}, Bands: map[string]CCCategory{}}
		for _, band := range CCSupportedBands() {
			geography.Bands[band] = CCCategory{Action: CCPass, Countries: values}
		}
		huge.Geography[name] = geography
	}
	if _, _, err := PrepareCCConfiguration(huge); !errors.Is(err, ErrStorageAdmission) {
		t.Fatalf("overlarge complete canonical bank was admitted: %v", err)
	}
}

func TestCCCanonicalEqualityFlowEncodingAndChurnBounds(t *testing.T) {
	input := ccTestBase()
	input.Modes, input.Sources = []string{"psk31", "CW", "CW"}, []string{"skimmer", "human"}
	input.Geography = map[string]CCGeography{"doc": {Global: CCCategory{Action: "PASS", Countries: []int{291, 230, 291}}}}
	canonical, program, err := PrepareCCConfiguration(input)
	if err != nil {
		t.Fatal(err)
	}
	want, _, err := PrepareCCConfiguration(CCConfiguration{Modes: []string{"CW", "PSK"}, Sources: []string{"HUMAN", "SKIMMER"},
		Geography: map[string]CCGeography{"DOC": {Global: CCCategory{Action: CCPass, Countries: []int{230, 291}}}}})
	if err != nil || !canonical.Equal(want) || canonical.Fingerprint() != want.Fingerprint() {
		t.Fatalf("canonical equality/order: %v", err)
	}
	data, err := yaml.Marshal(canonical)
	if err != nil || !strings.Contains(string(data), "countries: [230, 291]") || !strings.Contains(string(data), "modes: [CW, PSK]") {
		t.Fatalf("native flow encoding missing: %s (%v)", data, err)
	}
	changed := canonical.Clone()
	changed.AllowSelf = DefaultBoolFalse
	if changed.Equal(canonical) || changed.Fingerprint() == canonical.Fingerprint() {
		t.Fatal("exact delivery control omitted from identity")
	}
	for i := 1; i <= 100; i++ {
		canonical, err = canonical.WithGeography("DOC", "20", CCCategory{Action: CCPass, Countries: []int{1000 + i}})
		if err != nil {
			t.Fatal(err)
		}
		canonical, err = canonical.WithGeography("DOC", "", CCCategory{Action: CCOff})
		if err != nil {
			t.Fatal(err)
		}
		_, current, err := PrepareCCConfiguration(canonical)
		if err != nil || current.Counts().Countries != 0 || current.Counts().GeographyRows != 4 {
			t.Fatalf("removed category retained historical selections: %+v (%v)", current.Counts(), err)
		}
	}
	if program.Counts().Countries != 2 || program.Counts().GeographyRows != 4 {
		t.Fatal("old snapshot ownership changed during later churn")
	}
}

func TestCCSupports128TaxonomyModesWithoutGORuleBudget(t *testing.T) {
	var raw strings.Builder
	raw.WriteString("modes:\n")
	modes := make([]string, 128)
	for i := range modes {
		modes[i] = fmt.Sprintf("CCMODE%03d", i)
		fmt.Fprintf(&raw, "  - name: %s\n    filter_visible: true\n", modes[i])
	}
	raw.WriteString("events: []\n")
	file, err := os.CreateTemp(".", "cc_taxonomy_*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	t.Cleanup(func() {
		if err := os.Remove(path); err != nil {
			t.Errorf("remove native taxonomy fixture: %v", err)
		}
	})
	_, writeErr := file.WriteString(raw.String())
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal(errors.Join(writeErr, closeErr))
	}
	taxonomy, err := spot.LoadTaxonomyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	previous := spot.CurrentTaxonomy()
	spot.ConfigureTaxonomy(taxonomy)
	t.Cleanup(func() { spot.ConfigureTaxonomy(previous) })
	cfg := CCConfiguration{Modes: modes, Sources: []string{"HUMAN", "SKIMMER"}}
	_, program, err := PrepareCCConfiguration(cfg)
	if err != nil || program.Counts().Modes != 128 {
		t.Fatalf("native capacity accidentally inherited GO's 32-value atom budget: %v", err)
	}
	sp := &spot.Spot{Mode: "CCMODE127", ModeNorm: "CCMODE127", IsHuman: true}
	if !program.Matches(sp, EvaluationContext{}) {
		t.Fatal("last native taxonomy mode missing")
	}
}
