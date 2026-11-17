package config

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"dxcluster/internal/yamlconfig"

	"gopkg.in/yaml.v3"
)

func TestCCV2BayesMaskYAMLPresenceAndValidation(t *testing.T) {
	for _, tc := range []struct {
		name, mask, wantError string
		want                  map[string][]int
	}{
		{name: "omitted"},
		{name: "empty", mask: "{}", want: map[string][]int{}},
		{name: "empty_mode", mask: "{CW: []}", want: map[string][]int{"CW": {}}},
		{name: "legacy_thresholds_distance_two", mask: "{CW: [2]}", want: map[string][]int{"CW": {2}}},
		{name: "normalized", mask: "{' cw ': [2, 1], USB: [2]}", want: map[string][]int{"CW": {1, 2}, "USB": {2}}},
		{name: "null", mask: "null", wantError: "must not be null"},
		{name: "null_mode", mask: "{CW: null}", wantError: "must not be null"},
		{name: "null_distance", mask: "{CW: [null]}", wantError: "integers 1 or 2"},
		{name: "unsupported_mode", mask: "{FT8: [1]}", wantError: "unsupported mode"},
		{name: "ssb_is_not_voice_alias", mask: "{SSB: [1]}", wantError: "unsupported mode"},
		{name: "normalized_duplicate", mask: "{CW: [1], ' cw ': [2]}", wantError: "duplicate normalized mode"},
		{name: "duplicate_distance", mask: "{CW: [1, 1]}", wantError: "duplicate distance"},
		{name: "zero_distance", mask: "{CW: [0]}", wantError: "unsupported distance"},
		{name: "negative_distance", mask: "{CW: [-1]}", wantError: "unsupported distance"},
		{name: "large_distance", mask: "{CW: [3]}", wantError: "unsupported distance"},
		{name: "noninteger_distance", mask: "{CW: [1.5]}", wantError: "integers 1 or 2"},
		{name: "not_mapping", mask: "[]", wantError: "mapping"},
		{name: "not_sequence", mask: "{CW: 1}", wantError: "distance list"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeRequiredFloodControlFile(t, dir)
			removeTestConfigKey(t, dir, "pipeline.yaml", "call_correction", "bayes_bonus", "enabled_distances_by_mode")
			if tc.mask != "" {
				writeTestConfigOverlay(t, dir, "pipeline.yaml", "call_correction:\n  bayes_bonus:\n    enabled_distances_by_mode: "+tc.mask+"\n")
			}
			cfg, diagnostics, err := LoadWithDiagnostics(dir)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) || !strings.Contains(err.Error(), bayesBonusMaskPath) {
					t.Fatalf("invalid mask accepted or misdiagnosed: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cfg.CallCorrection.BayesBonus.EnabledDistancesByMode, tc.want) {
				t.Fatalf("mask presence/value changed: got %#v want %#v", cfg.CallCorrection.BayesBonus.EnabledDistancesByMode, tc.want)
			}
			for _, warning := range diagnostics.Warnings {
				if strings.Contains(warning, "enabled_distances_by_mode") {
					t.Fatalf("known mask was treated as an unknown field: %s", warning)
				}
			}
		})
	}
}

func TestCCV2BayesMaskTypedRoundTrip(t *testing.T) {
	for _, mask := range []map[string][]int{nil, {}, {"CW": nil}, {"CW": {1}, "RTTY": {}}} {
		before := CallCorrectionBayesBonusConfig{Enabled: true, EnabledDistancesByMode: mask}
		data, err := yaml.Marshal(before)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "enabled_distances_by_mode:") != (mask != nil) {
			t.Fatalf("typed YAML collapsed omission/emptiness: %s", data)
		}
		var after CallCorrectionBayesBonusConfig
		if err := yamlconfig.DecodeBytes("roundtrip", data, &after, nil); err != nil {
			t.Fatal(err)
		}
		if (after.EnabledDistancesByMode == nil) != (mask == nil) || len(after.EnabledDistancesByMode) != len(mask) {
			t.Fatalf("typed round trip changed mask presence: before=%#v after=%#v", mask, after.EnabledDistancesByMode)
		}
		for _, mode := range []string{"CW", "RTTY", "USB", "LSB"} {
			for distance := 1; distance <= 2; distance++ {
				if BayesBonusCellEnabled(true, mask, mode, distance, 2) != BayesBonusCellEnabled(true, after.EnabledDistancesByMode, mode, distance, 2) {
					t.Fatalf("typed round trip changed %s/%d: %s", mode, distance, data)
				}
			}
		}
	}
	var unknown CallCorrectionBayesBonusConfig
	if err := yamlconfig.DecodeBytes("unknown", []byte("enabled: true\nenabled_distance_by_mode: {}\n"), &unknown, nil); err == nil {
		t.Fatal("presence-aware marshaling bypassed strict field diagnostics")
	}
}

func TestCCV2BayesMaskRuntimeFilesReplaceCompleteMask(t *testing.T) {
	for _, tc := range []struct {
		name, earlier, later string
		want                 map[string][]int
		wantError            bool
	}{
		{"empty_replaces", "{CW: [2]}", "{}", map[string][]int{}, false},
		{"nonempty_replaces", "{CW: [2], USB: [1]}", "{RTTY: [1]}", map[string][]int{"RTTY": {1}}, false},
		{"omitted_inherits", "{CW: [2]}", "", map[string][]int{"CW": {2}}, false},
		{"final_null", "{CW: [2]}", "null", nil, true},
		{"final_mode_null", "{CW: [2]}", "{RTTY: null}", nil, true},
		// Keep the existing merged-effective validation boundary. Superseded
		// definitions do not become a new per-file scalar/mask validity policy.
		{"superseded_invalid", "{FT8: [1]}", "{}", map[string][]int{}, false},
		{"superseded_null", "null", "{}", map[string][]int{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeRequiredFloodControlFile(t, dir)
			removeTestConfigKey(t, dir, "pipeline.yaml", "call_correction", "bayes_bonus", "enabled_distances_by_mode")
			removeTestConfigKey(t, dir, "pipeline.yaml", "call_correction", "bayes_bonus", "weight_distance2_milli")
			writeTestConfigOverlay(t, dir, "app.yaml", "server:\n  name: S6MaskAlpha\ncall_correction:\n  bayes_bonus:\n    weight_distance2_milli: 178\n    enabled_distances_by_mode: "+tc.earlier+"\n")
			later := "server:\n  node_id: S6MASK\ncall_correction:\n  bayes_bonus:\n    weight_distance1_milli: 321\n"
			if tc.later != "" {
				later += "    enabled_distances_by_mode: " + tc.later + "\n"
			}
			writeTestConfigOverlay(t, dir, "runtime.yaml", later)
			cfg, err := Load(dir)
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), bayesBonusMaskPath) {
					t.Fatalf("final null mask passed merged validation: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cfg.CallCorrection.BayesBonus.EnabledDistancesByMode, tc.want) {
				t.Fatalf("later complete mask inherited earlier cells: got %#v want %#v", cfg.CallCorrection.BayesBonus.EnabledDistancesByMode, tc.want)
			}
			if cfg.Server.Name != "S6MaskAlpha" || cfg.Server.NodeID != "S6MASK" ||
				cfg.CallCorrection.BayesBonus.WeightDistance1Milli != 321 || cfg.CallCorrection.BayesBonus.WeightDistance2Milli != 178 {
				t.Fatal("atomic mask replacement changed ordinary nested-map merge behavior")
			}
		})
	}
}

func TestCCV2BayesMaskEffectiveCells(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
		mask    map[string][]int
		max     int
		want    []string
	}{
		{"legacy", true, nil, 2, []string{"CW/1", "CW/2", "LSB/1", "LSB/2", "RTTY/1", "RTTY/2", "USB/1", "USB/2"}},
		{"legacy_distance_one", true, nil, 1, []string{"CW/1", "LSB/1", "RTTY/1", "USB/1"}},
		{"empty", true, map[string][]int{}, 2, nil},
		{"nil_mode", true, map[string][]int{"CW": nil}, 2, nil},
		{"empty_mode", true, map[string][]int{"CW": {}}, 2, nil},
		{"selected", true, map[string][]int{"CW": {1}, "RTTY": {2}, "USB": {2}}, 2, []string{"CW/1", "RTTY/2", "USB/2"}},
		{"direct_go_normalized", true, map[string][]int{" cw ": {2}}, -1, []string{"CW/2"}},
		{"master_disabled", false, map[string][]int{"CW": {1, 2}}, 2, nil},
		{"zero_maximum", true, nil, 0, nil},
		{"structurally_blocked", true, map[string][]int{"CW": {2}}, 1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := CallCorrectionConfig{MaxEditDistance: tc.max, BayesBonus: CallCorrectionBayesBonusConfig{Enabled: tc.enabled, EnabledDistancesByMode: tc.mask}}
			if err := cfg.BayesBonus.ValidateEnabledDistancesByMode(); err != nil {
				t.Fatal(err)
			}
			if got := cfg.bayesBonusEnabledCells(); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("effective cells: got %v want %v", got, tc.want)
			}
		})
	}
	for _, mode := range []string{"FT8", "SSB", ""} {
		if BayesBonusCellEnabled(true, nil, mode, 1, -1) {
			t.Fatalf("unsupported mode %q became active through the legacy mask", mode)
		}
	}
	if BayesBonusCellEnabled(true, nil, "CW", 0, -1) || BayesBonusCellEnabled(true, nil, "CW", 3, -1) {
		t.Fatal("Bayes admitted a distance outside 1/2")
	}
	mask := map[string][]int{"CW": {1, 2}, "RTTY": {2}}
	if allocations := testing.AllocsPerRun(100, func() {
		if !BayesBonusCellEnabled(true, mask, "CW", 1, 2) {
			panic("normalized selected cell disabled")
		}
	}); allocations != 0 {
		t.Fatalf("normalized hot-path cell selection allocated: %g", allocations)
	}
}

func TestCCV2BayesMaskNormalizationOwnsBoundedValues(t *testing.T) {
	backing := make([]int, 2, 64)
	backing[0], backing[1] = 2, 1
	cfg := Config{CallCorrection: CallCorrectionConfig{BayesBonus: CallCorrectionBayesBonusConfig{EnabledDistancesByMode: map[string][]int{" cw ": backing}}}}
	if err := normalizeCallCorrectionBayesMaskConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	backing[0] = 0
	selected := cfg.CallCorrection.BayesBonus.EnabledDistancesByMode["CW"]
	if !reflect.DeepEqual(selected, []int{1, 2}) || cap(selected) != 2 {
		t.Fatalf("load normalization retained input spellings/backing: %#v cap=%d", cfg.CallCorrection.BayesBonus.EnabledDistancesByMode, cap(selected))
	}
}

func TestCCV2BayesMaskCapacityConsumers(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*CallCorrectionConfig)
		wantError bool
	}{
		{"legacy", func(*CallCorrectionConfig) {}, true},
		{"empty", func(c *CallCorrectionConfig) { c.BayesBonus.EnabledDistancesByMode = map[string][]int{} }, false},
		{"nil_mode", func(c *CallCorrectionConfig) { c.BayesBonus.EnabledDistancesByMode = map[string][]int{"CW": nil} }, false},
		{"distance_two_only", func(c *CallCorrectionConfig) { c.BayesBonus.EnabledDistancesByMode = map[string][]int{"RTTY": {2}} }, true},
		{"voice_only", func(c *CallCorrectionConfig) { c.BayesBonus.EnabledDistancesByMode = map[string][]int{"USB": {1}} }, true},
		{"distance_two_blocked", func(c *CallCorrectionConfig) {
			c.BayesBonus.EnabledDistancesByMode, c.MaxEditDistance = map[string][]int{"CW": {2}}, 1
		}, false},
		{"distance_zero", func(c *CallCorrectionConfig) { c.MaxEditDistance = 0 }, false},
		{"master_disabled", func(c *CallCorrectionConfig) { c.BayesBonus.Enabled = false }, false},
		{"plus1_still_consumes_at_zero", func(c *CallCorrectionConfig) {
			c.BayesBonus.EnabledDistancesByMode, c.MaxEditDistance, c.ResolverRecentPlus1Enabled = map[string][]int{}, 0, true
		}, true},
		{"truncation_still_consumes", func(c *CallCorrectionConfig) {
			c.BayesBonus.EnabledDistancesByMode, c.RecentBandRecordMinUniqueSpotters = map[string][]int{}, 3
			c.FamilyPolicy.Truncation.Enabled, c.FamilyPolicy.Truncation.AllowPrefixMatch = true, true
			c.FamilyPolicy.Truncation.RelaxAdvantage.Enabled, c.FamilyPolicy.Truncation.RelaxAdvantage.RequireCandidateValidated = true, true
		}, true},
		{"stabilizer_still_consumes", func(c *CallCorrectionConfig) {
			c.BayesBonus.EnabledDistancesByMode, c.RecentBandRecordMinUniqueSpotters, c.StabilizerEnabled = map[string][]int{}, 3, true
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := b14RawCapacityConfig(2)
			cfg.MaxEditDistance, cfg.RecentBandRecordMinUniqueSpotters, cfg.ResolverRecentPlus1MinUniqueWinner = 2, 1, 3
			cfg.CustomSCP.ResolverMinUniqueSpotters, cfg.CustomSCP.ResolverMinUniqueH3Cells = 1, 1
			// Zero weights and unreachable scores still query history; do not use
			// the tuning search domain as a structural capacity predicate.
			cfg.BayesBonus = CallCorrectionBayesBonusConfig{Enabled: true, ReportThresholdDistance1Milli: 4000, ReportThresholdDistance2Milli: 4000}
			tc.configure(&cfg)
			for _, check := range []struct {
				name string
				err  error
			}{
				{"custom", cfg.ValidateCustomSCPCapacity()},
				{"legacy", cfg.ValidateLegacyHistoryCapacity("mask fixture", 2, LegacyHistoryConsumers{Resolver: true, Stabilizer: true})},
			} {
				if (check.err != nil) != tc.wantError {
					t.Fatalf("%s capacity uses different effective consumers: %v", check.name, check.err)
				}
			}
		})
	}
}

func TestCCV2BayesMaskDirectGoValidationBeforeOwners(t *testing.T) {
	for _, mask := range []map[string][]int{
		{"FT8": {}}, {"SSB": {1}}, {"CW": {0}}, {"CW": {3}},
		{"CW": {1, 1}}, {"CW": {1}, " cw ": {2}},
	} {
		cfg := CallCorrectionConfig{BayesBonus: CallCorrectionBayesBonusConfig{EnabledDistancesByMode: mask}}
		for _, err := range []error{cfg.ValidateCustomSCPCapacity(), cfg.ValidateLegacyHistoryCapacity("unused", 1, LegacyHistoryConsumers{})} {
			if err == nil || !strings.Contains(err.Error(), bayesBonusMaskPath) {
				t.Fatalf("invalid direct-Go mask passed an inactive startup boundary: %#v err=%v", mask, err)
			}
		}
	}
}

func TestCCV2BayesMaskLegacyValidationCells(t *testing.T) {
	for _, tc := range []struct {
		name               string
		mask               map[string][]int
		candidate, subject bool
		maximum            int
		wantError          bool
	}{
		{"distance_two_candidate", map[string][]int{"CW": {2}}, true, false, 2, true},
		{"distance_one_candidate", map[string][]int{"CW": {1}}, true, false, 2, true},
		{"distance_two_subject", map[string][]int{"CW": {2}}, false, true, 2, true},
		{"distance_one_subject_unused", map[string][]int{"CW": {1}}, false, true, 2, false},
		{"blocked_distance_two", map[string][]int{"CW": {2}}, true, true, 1, false},
		{"no_cells", map[string][]int{}, true, true, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := CallCorrectionConfig{Enabled: true, MaxEditDistance: tc.maximum, RecentBandRecordMinUniqueSpotters: 3, ResolverRecentPlus1MinUniqueWinner: 1,
				BayesBonus: CallCorrectionBayesBonusConfig{Enabled: true, EnabledDistancesByMode: tc.mask,
					RequireCandidateValidated: tc.candidate, RequireSubjectUnvalidatedDistance2: tc.subject}}
			err := cfg.ValidateLegacyHistoryCapacity("validation fixture", 2, LegacyHistoryConsumers{Resolver: true})
			if (err != nil) != tc.wantError {
				t.Fatalf("validation predicate disagrees with the selected cell: %v", err)
			}
		})
	}
}

func TestCCV2BayesMaskSummaryUsesEffectiveCells(t *testing.T) {
	for _, tc := range []struct {
		mask map[string][]int
		want string
	}{
		{nil, "enabled (mask=legacy cells=CW/1,LSB/1,RTTY/1,USB/1"},
		{map[string][]int{}, "inactive (mask=explicit cells=none"},
		{map[string][]int{"RTTY": {2}, "CW": {1}}, "enabled (mask=explicit cells=CW/1"},
	} {
		cfg := Config{CallCorrection: CallCorrectionConfig{MaxEditDistance: 1, BayesBonus: CallCorrectionBayesBonusConfig{Enabled: true, EnabledDistancesByMode: tc.mask}}}
		var summary bytes.Buffer
		cfg.PrintTo(&summary)
		if !strings.Contains(summary.String(), "Call correction bayes bonus: "+tc.want) {
			t.Fatalf("summary contradicts effective eligibility: %s", summary.String())
		}
	}
}
