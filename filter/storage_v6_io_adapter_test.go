package filter

import (
	"slices"
	"testing"
)

func TestV6StorageRuntimeAdaptersPreserveOnlyPreparedRules(t *testing.T) {
	cfg := v6StorageTestConfiguration()
	cfg, _, err := PrepareV6Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rules := slices.Clone(cfg.Filters.Rules)
	input := &V6PresetReference{Name: "EXACT", Baseline: &V6SavedPreset{Configuration: cfg}}
	runtime, err := RuntimePresetReferenceFromV6(input)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Baseline.ConfigurationVersion != V6ConfigurationVersion ||
		!V6ConfigurationFromPreset(runtime.Baseline).Equal(cfg) {
		t.Fatal("runtime transport lost rules/settings/controls")
	}
	result, err := V6PresetReferenceFromRuntime(runtime)
	if err != nil || result.Baseline.ConfigurationVersion != 6 ||
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
	if ref, err := V6PresetReferenceFromRuntime(nil); err != nil || ref != nil {
		t.Fatal("nil runtime association not preserved")
	}
	if ref, err := RuntimePresetReferenceFromV6(nil); err != nil || ref != nil {
		t.Fatal("nil DTO association not preserved")
	}
	if set, err := RuntimeSavedPresetFromV6(nil); err == nil || set != nil {
		t.Fatal("nil saved preset accepted")
	}
	if set, err := V6SavedPresetFromRuntime(&SavedPreset{ConfigurationVersion: 4}); err == nil || set != nil {
		t.Fatal("runtime transport accepted a legacy marker")
	}
}
