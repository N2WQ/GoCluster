package filter

import (
	"slices"
	"testing"
)

func TestV5StorageRuntimeAdaptersPreserveOnlyPreparedRules(t *testing.T) {
	cfg := v5StorageTestConfiguration()
	cfg, _, err := PrepareV5Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rules := slices.Clone(cfg.Filters.Rules)
	input := &V5PresetReference{Name: "EXACT", Baseline: &V5SavedPreset{Configuration: cfg}}
	runtime, err := RuntimePresetReferenceFromV5(input)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Baseline.ConfigurationVersion != CurrentConfigurationVersion ||
		!V5ConfigurationFromPreset(runtime.Baseline).Equal(cfg) {
		t.Fatal("runtime transport lost rules/settings/controls")
	}
	result, err := V5PresetReferenceFromRuntime(runtime)
	if err != nil || result.Baseline.ConfigurationVersion != 5 ||
		!result.Baseline.Configuration.Equal(cfg) {
		t.Fatal("runtime adapter changed prepared rules")
	}
	result.Baseline.Configuration.Filters.Rules[0].Expr = "MODE=FT8"
	input.Baseline.Configuration.Filters.Rules[0].Expr = "MODE=SSB"
	if !slices.Equal(runtime.Baseline.PreparedRules().Definitions(), rules) {
		t.Fatal("runtime immutable programs retained mutable definition aliases")
	}
	if runtime.Baseline.IncludeBeacons != nil {
		t.Fatal("DEFAULT pointer acquired a hidden default")
	}
	if ref, err := V5PresetReferenceFromRuntime(nil); err != nil || ref != nil {
		t.Fatal("nil runtime association not preserved")
	}
	if ref, err := RuntimePresetReferenceFromV5(nil); err != nil || ref != nil {
		t.Fatal("nil DTO association not preserved")
	}
	if set, err := RuntimeSavedPresetFromV5(nil); err == nil || set != nil {
		t.Fatal("nil saved preset accepted")
	}
	if set, err := V5SavedPresetFromRuntime(&SavedPreset{ConfigurationVersion: 4}); err == nil || set != nil {
		t.Fatal("runtime transport accepted a legacy marker")
	}
}
