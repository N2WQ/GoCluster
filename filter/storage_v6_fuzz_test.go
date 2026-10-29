package filter

import (
	"bytes"
	"slices"
	"testing"
)

func FuzzV6StorageRoundtrip(f *testing.F) {
	const nearbySeed = "configuration_version: 6\nconfiguration: {filters: {rules: [{slot: 0, action: pass, expr: 'NEARBY OR MODE=CW'}]}, settings: {}}\n"
	// Keep an acceptance control outside the mutation callback: all-rejection
	// must fail even when every callback would otherwise return on decode error.
	nearby, err := DecodeV6UserRecord([]byte(nearbySeed))
	if err != nil || nearby == nil {
		f.Fatal("positive schema-6 NEARBY corpus was rejected", err)
	}
	if nearby.ConfigurationVersion != 6 || !slices.Equal(nearby.Configuration.Filters.Rules,
		[]RuleDefinition{{Slot: 0, Action: RulePass, Expr: "NEARBY OR MODE=CW"}}) {
		f.Fatal("positive schema-6 NEARBY corpus lost its canonical rule")
	}
	for _, seed := range []struct {
		raw        string
		collection bool
	}{
		{v6StorageMinimalRecord, false},
		{nearbySeed, false},
		{v6StorageMinimalRecord + "recent_ips: [one, two, three, four, five, six]\n", false},
		{v6StorageMinimalRecord + "preset: {name: TEST, baseline: {configuration_version: 6, configuration: " + v6StorageMinimalConfiguration + "}}\n", false},
		{"configuration_version: 6\nconfiguration: {filters: {rules: [{slot: 0, action: pass, expr: 'COMMENT=\" MiXeD \"'}]}, settings: {}}\n", false},
		{"configuration_version: 5\n", false},
		{"configuration_version: 7\n", false},
		{"configuration_version: 6\nconfiguration: {filters: {rules: [null]}, settings: {}}\n", false},
		{v6StorageMinimalRecord + "recent_ips: &cycle [*cycle]\n", false},
		{"presets: {}\n", true},
		{"presets: {TEST: {configuration_version: 6, configuration: " + v6StorageMinimalConfiguration + "}}\n", true},
		{"presets: {TEST: {configuration_version: 4}}\n", true},
		{"presets: {TEST: null}\n", true},
	} {
		f.Add([]byte(seed.raw), seed.collection)
	}
	f.Fuzz(func(t *testing.T, raw []byte, collection bool) {
		if len(raw) > 4096 {
			t.Skip()
		}
		original := bytes.Clone(raw)
		if collection {
			got, err := DecodeV6PresetCollection(raw)
			if err == nil {
				data, err := EncodeV6PresetCollection(got)
				if err != nil {
					t.Fatal("admitted collection could not encode", err)
				}
				reloaded, err := DecodeV6PresetCollection(data)
				if err != nil || len(reloaded.Presets) != len(got.Presets) {
					t.Fatal("canonical collection could not reload", err)
				}
				for name, set := range got.Presets {
					other := reloaded.Presets[name]
					if other == nil || set.ConfigurationVersion != 6 || other.ConfigurationVersion != 6 ||
						!set.Configuration.Equal(other.Configuration) {
						t.Fatal("collection roundtrip changed exact configuration")
					}
				}
			}
		} else if got, err := DecodeV6UserRecord(raw); err == nil {
			data, err := EncodeV6UserRecord(got)
			if err != nil {
				t.Fatal("admitted record could not encode", err)
			}
			reloaded, err := DecodeV6UserRecord(data)
			if err != nil || reloaded.ConfigurationVersion != 6 || !reloaded.Configuration.Equal(got.Configuration) ||
				!slices.Equal(got.RecentIPs, reloaded.RecentIPs) || !got.LastLoginUTC.Equal(reloaded.LastLoginUTC) {
				t.Fatal("record roundtrip changed exact configuration/metadata", err)
			}
			if (got.Preset == nil) != (reloaded.Preset == nil) {
				t.Fatal("record roundtrip changed baseline presence")
			}
			if got.Preset != nil && (got.Preset.Name != reloaded.Preset.Name ||
				reloaded.Preset.Baseline.ConfigurationVersion != 6 ||
				!got.Preset.Baseline.Configuration.Equal(reloaded.Preset.Baseline.Configuration)) {
				t.Fatal("record roundtrip changed applied baseline")
			}
		}
		if !bytes.Equal(raw, original) {
			t.Fatal("decoder mutated supplied bytes")
		}
	})
}
