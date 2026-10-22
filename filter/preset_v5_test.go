package filter

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestV5PresetCollectionRoundtrip(t *testing.T) {
	input := &V5PresetCollection{Presets: map[string]*V5SavedPreset{
		"SECOND": {Configuration: v5StorageTestConfiguration()}, "EMPTY": {},
	}}
	before, err := yaml.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	data, err := EncodeV5PresetCollection(input)
	if err != nil {
		t.Fatal(err)
	}
	after, err := yaml.Marshal(input)
	if err != nil || !bytes.Equal(before, after) || input.Presets["SECOND"].Configuration.Filters.preparedRules != nil {
		t.Fatal("encoding mutated collection")
	}
	got, err := DecodeV5PresetCollection(data)
	if err != nil {
		t.Fatal(err)
	}
	expected, _ := mustPrepareV5Configuration(t, v5StorageTestConfiguration())
	if len(got.Presets) != 2 || !got.Presets["SECOND"].Configuration.Equal(expected) ||
		got.Presets["SECOND"].ConfigurationVersion != 5 || got.Presets["EMPTY"].ConfigurationVersion != 5 ||
		len(got.Presets["EMPTY"].Configuration.Filters.Rules) != 0 {
		t.Fatal("collection values changed")
	}
	reencoded, err := EncodeV5PresetCollection(got)
	if err != nil || !bytes.Equal(data, reencoded) {
		t.Fatal("collection encoding is unstable", err)
	}
	input.Presets["SECOND"].Configuration.Filters.Rules[0].Expr = "MODE=FT8"
	delete(input.Presets, "EMPTY")
	if len(got.Presets) != 2 || !got.Presets["SECOND"].Configuration.Equal(expected) {
		t.Fatal("decoded collection borrowed caller data")
	}
	for _, collection := range []*V5PresetCollection{{}, {Presets: map[string]*V5SavedPreset{}}} {
		data, err := EncodeV5PresetCollection(collection)
		if err != nil || string(data) != "presets: {}\n" {
			t.Fatal("empty collection is not canonical", err)
		}
		if got, err := DecodeV5PresetCollection(data); err != nil || len(got.Presets) != 0 {
			t.Fatal("empty collection did not roundtrip", err)
		}
	}
}

func TestV5PresetCollectionStrict(t *testing.T) {
	for _, raw := range []string{
		"", "{}", "[]", "presets: null\n", "presets: []\n", "presets: {TEST: null}\n",
		"presets: {TEST: []}\n", "presets: {}\nextra: false\n", "presets: {}\npresets: {}\n",
		"presets: {1: {configuration_version: 5, configuration: " + v5StorageMinimalConfiguration + "}}\n",
		"presets: {TEST: {configuration_version: 5}}\n",
		"presets: {TEST: {configuration_version: 5, configuration: null}}\n",
		"presets: {TEST: {configuration_version: 5, configuration: " + v5StorageMinimalConfiguration + ", other: 1}}\n",
		"presets: {}\n---\npresets: {}\n",
		"presets: {TEST: {configuration_version: 5, configuration: " + v5StorageMinimalConfiguration +
			"}, TEST: {configuration_version: 5, configuration: " + v5StorageMinimalConfiguration + "}}\n",
	} {
		if got, err := DecodeV5PresetCollection([]byte(raw)); err == nil || got != nil {
			t.Fatalf("malformed collection admitted: %s", raw)
		}
	}
	for _, name := range []string{"test", "BAD_NAME", "-TEST", "", strings.Repeat("A", 33)} {
		collection := &V5PresetCollection{Presets: map[string]*V5SavedPreset{name: {}}}
		if _, err := EncodeV5PresetCollection(collection); err == nil {
			t.Fatalf("invalid name encoded: %q", name)
		}
		raw := fmt.Sprintf("presets: {%q: {configuration_version: 5, configuration: %s}}\n", name, v5StorageMinimalConfiguration)
		if _, err := DecodeV5PresetCollection([]byte(raw)); err == nil {
			t.Fatalf("invalid name decoded: %q", name)
		}
	}
	for _, collection := range []*V5PresetCollection{nil, {Presets: map[string]*V5SavedPreset{"TEST": nil}}} {
		if _, err := EncodeV5PresetCollection(collection); err == nil {
			t.Fatal("nil collection/preset admitted")
		}
	}
}

func TestV5PresetCollectionCountAndAliases(t *testing.T) {
	collection := &V5PresetCollection{Presets: make(map[string]*V5SavedPreset)}
	for i := range MaxPresets {
		collection.Presets[fmt.Sprintf("P%d", i)] = &V5SavedPreset{}
	}
	data, err := EncodeV5PresetCollection(collection)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeV5PresetCollection(data); err != nil || len(got.Presets) != MaxPresets {
		t.Fatal("max count refused", err)
	}
	collection.Presets["EXTRA"] = &V5SavedPreset{}
	if _, err := EncodeV5PresetCollection(collection); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("oversized count encoded", err)
	}
	raw := "presets:\n  P0: &set {configuration_version: 5, configuration: " + v5StorageMinimalConfiguration + "}\n"
	for i := 1; i <= MaxPresets; i++ {
		raw += fmt.Sprintf("  P%d: *set\n", i)
	}
	if _, err := DecodeV5PresetCollection([]byte(raw)); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("oversized alias collection decoded", err)
	}
	// Exercise the shared typed-map observer itself, not only v5's early strict
	// count check: no typed map is allocated by this accounting-only traversal.
	document := storageMetadataDocument(t, raw)
	state, err := newStorageDecodeStateWithLimits(document, storageDecodeLimits{work: maxPresetCollectionWork, presets: MaxPresets})
	if err != nil {
		t.Fatal(err)
	}
	guard := newStorageDecodeGuard(true)
	if _, _, err := state.decode(document, reflect.TypeFor[V5PresetCollection](), reflect.Value{}, guard); err != nil {
		t.Fatal(err)
	}
	if !state.presetCountExceeded || len(guard.typeErrors) != 0 {
		t.Fatal("v5 map did not use the shared pre-materialization count observer")
	}

	raw = strings.TrimSuffix(raw, fmt.Sprintf("  P%d: *set\n", MaxPresets))
	got, err := DecodeV5PresetCollection([]byte(raw))
	if err != nil || len(got.Presets) != MaxPresets {
		t.Fatal("bounded aliases refused", err)
	}
	got.Presets["P0"].Configuration.Filters.Rules = []RuleDefinition{{0, RulePass, "MODE=CW"}}
	if len(got.Presets["P1"].Configuration.Filters.Rules) != 0 {
		t.Fatal("alias presets share mutable definitions")
	}
}

func TestV5PresetBaselineRulesAndControls(t *testing.T) {
	base := "configuration_version: 5\nconfiguration: {filters: {rules: [{slot: 0, action: pass, expr: 'MODE=CW'}]}, settings: {}}\n"
	for _, raw := range []string{
		"presets:\n  TEST:\n" + indentStoredFixture(base),
		v5StorageMinimalRecord + "preset:\n  name: TEST\n  baseline:\n" + indentStoredFixture(indentStoredFixture(base)),
	} {
		var configuration V5Configuration
		if strings.HasPrefix(raw, "presets:") {
			collection, err := DecodeV5PresetCollection([]byte(raw))
			if err != nil {
				t.Fatal(err)
			}
			configuration = collection.Presets["TEST"].Configuration
		} else {
			record, err := DecodeV5UserRecord([]byte(raw))
			if err != nil {
				t.Fatal(err)
			}
			configuration = record.Preset.Baseline.Configuration
		}
		if !slices.Equal(configuration.Filters.Rules, []RuleDefinition{{0, RulePass, "MODE=CW"}}) {
			t.Fatal("nested slot zero disappeared")
		}
		invalid := strings.Replace(raw, "slot: 0, ", "", 1)
		if strings.HasPrefix(raw, "presets:") {
			if _, err := DecodeV5PresetCollection([]byte(invalid)); err == nil {
				t.Fatal("nested preset accepted missing slot")
			}
		} else if _, err := DecodeV5UserRecord([]byte(invalid)); err == nil {
			t.Fatal("baseline accepted missing slot")
		}
	}
}
