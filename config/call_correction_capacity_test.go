// File role: Checks that enabled history consumers can satisfy their configured
// receiver and geographic requirements within the Custom SCP receiver bound.
package config

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

const b14CapacityFixture = `call_correction:
  enabled: true
  recent_band_bonus_enabled: true
  recent_band_record_min_unique_spotters: 2
  resolver_recent_plus1_enabled: true
  resolver_recent_plus1_min_unique_winner: 4
  stabilizer_enabled: true
  stabilizer_edit_neighbor_enabled: true
  stabilizer_edit_neighbor_min_spotters: 3
  bayes_bonus:
    enabled: false
    weight_distance1_milli: 0
    weight_distance2_milli: 0
    require_candidate_validated: false
    require_subject_unvalidated_distance2: false
  family_policy:
    truncation:
      enabled: false
      max_length_delta: 1
      allow_prefix_match: true
      allow_suffix_match: true
      relax_advantage:
        enabled: false
        require_candidate_validated: false
        require_subject_unvalidated: false
      length_bonus:
        enabled: false
        max: 1
        require_candidate_validated: false
        require_subject_unvalidated: false
      delta2_rails:
        enabled: false
        require_candidate_validated: false
        require_subject_unvalidated: false
  custom_scp:
    enabled: true
    resolver_min_score: 5
    resolver_min_unique_spotters: 4
    resolver_min_unique_h3_cells: 2
    stabilizer_min_score: 5
    stabilizer_min_unique_spotters: 3
    stabilizer_min_unique_h3_cells: 2
    s_floor_min_score: 3
    s_floor_min_unique_spotters_exact: 3
    s_floor_min_unique_h3_cells_exact: 2
    s_floor_min_unique_spotters_family: 5
    s_floor_min_unique_h3_cells_family: 3
    max_spotters_per_key: 8
`

func b14LoadCapacityConfig(t *testing.T, overlay string) (*Config, error) {
	t.Helper()
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	writeTestConfigOverlay(t, dir, "pipeline.yaml", b14CapacityFixture)
	writeTestConfigOverlay(t, dir, "pipeline.yaml", overlay)
	return Load(dir)
}

func b14RequireCapacityError(t *testing.T, err error, key string, requirement, capacity int) {
	t.Helper()
	if err == nil {
		t.Fatalf("enabled %s requirement %d exceeds receiver cap %d but configuration was accepted", key, requirement, capacity)
	}
	for _, fragment := range []string{key, fmt.Sprintf("requirement %d exceeds", requirement), fmt.Sprintf("max_spotters_per_key=%d", capacity)} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("capacity error must identify setting, effective requirement and cap; missing %q in %q", fragment, err)
		}
	}
}

func b14RawCapacityConfig(capacity int) CallCorrectionConfig {
	return CallCorrectionConfig{
		Enabled: true, MaxEditDistance: -1,
		CustomSCP: CallCorrectionCustomSCPConfig{
			Enabled: true, MaxSpottersPerKey: capacity,
			SFloorMinUniqueSpottersExact: 1, SFloorMinUniqueH3CellsExact: 1,
			SFloorMinUniqueSpottersFamily: 1, SFloorMinUniqueH3CellsFamily: 1,
		},
	}
}

func TestB14CustomSCPCapacityRawConsumerSemantics(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		capacity             int
		configure            func(*CallCorrectionConfig)
		wantKey              string
		wantRequirement, cap int
	}{
		{"default_cap_equality", 0, func(c *CallCorrectionConfig) { c.CustomSCP.SFloorMinUniqueSpottersExact = 64 }, "", 0, 64},
		{"default_cap_above", 0, func(c *CallCorrectionConfig) { c.CustomSCP.SFloorMinUniqueSpottersExact = 65 }, "s_floor_min_unique_spotters_exact", 65, 64},
		{"negative_cap_uses_constructor_default", -1, func(c *CallCorrectionConfig) { c.CustomSCP.SFloorMinUniqueSpottersFamily = 65 }, "s_floor_min_unique_spotters_family", 65, 64},
		{"exact_zero_receiver_minimum", 1, func(c *CallCorrectionConfig) { c.CustomSCP.SFloorMinUniqueSpottersExact = 0 }, "s_floor_min_unique_spotters_exact", 2, 1},
		{"family_negative_receiver_minimum", 1, func(c *CallCorrectionConfig) { c.CustomSCP.SFloorMinUniqueSpottersFamily = -1 }, "s_floor_min_unique_spotters_family", 2, 1},
		{"both_receiver_defaults_fit", 2, func(c *CallCorrectionConfig) {
			c.CustomSCP.SFloorMinUniqueSpottersExact = 0
			c.CustomSCP.SFloorMinUniqueSpottersFamily = 0
		}, "", 0, 2},
		{"exact_zero_cell_minimum", 1, func(c *CallCorrectionConfig) { c.CustomSCP.SFloorMinUniqueH3CellsExact = 0 }, "s_floor_min_unique_h3_cells_exact", 2, 1},
		{"family_zero_cell_minimum", 2, func(c *CallCorrectionConfig) { c.CustomSCP.SFloorMinUniqueH3CellsFamily = 0 }, "s_floor_min_unique_h3_cells_family", 3, 2},
		{"both_cell_defaults_fit", 3, func(c *CallCorrectionConfig) {
			c.CustomSCP.SFloorMinUniqueH3CellsExact = 0
			c.CustomSCP.SFloorMinUniqueH3CellsFamily = 0
		}, "", 0, 3},
		{"plus1_zero_threshold_is_not_boolean_default", 1, func(c *CallCorrectionConfig) { c.ResolverRecentPlus1Enabled = true }, "", 0, 1},
		{"bayes_zero_threshold_without_validation", 1, func(c *CallCorrectionConfig) { c.BayesBonus.Enabled = true }, "", 0, 1},
		{"bayes_candidate_validation_defaults_two", 1, func(c *CallCorrectionConfig) {
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireCandidateValidated = true
		}, "resolver_min_unique_spotters", 2, 1},
		{"bayes_subject_validation_defaults_two", 1, func(c *CallCorrectionConfig) {
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireSubjectUnvalidatedDistance2 = true
		}, "resolver_min_unique_spotters", 2, 1},
		{"bayes_zero_max_edit_unused", 8, func(c *CallCorrectionConfig) {
			c.MaxEditDistance = 0
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireCandidateValidated = true
			c.CustomSCP.ResolverMinUniqueSpotters = 9
			c.CustomSCP.ResolverMinUniqueH3Cells = 9
		}, "", 0, 8},
		{"truncation_zero_max_edit_unused", 8, func(c *CallCorrectionConfig) {
			c.MaxEditDistance = 0
			c.CustomSCP.ResolverMinUniqueSpotters = 9
			c.CustomSCP.ResolverMinUniqueH3Cells = 9
			c.FamilyPolicy.Truncation = CallCorrectionTruncationFamilyConfig{
				Enabled: true, AllowPrefixMatch: true,
				RelaxAdvantage: CallCorrectionTruncationAdvantageConfig{Enabled: true, RequireCandidateValidated: true},
			}
		}, "", 0, 8},
		{"plus1_zero_max_edit_still_queries_slash_identity", 8, func(c *CallCorrectionConfig) {
			c.MaxEditDistance = 0
			c.ResolverRecentPlus1Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = 9
		}, "resolver_recent_plus1_min_unique_winner", 9, 8},
		{"bayes_subject_validation_unreachable_at_distance_one", 1, func(c *CallCorrectionConfig) {
			c.MaxEditDistance = 1
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireSubjectUnvalidatedDistance2 = true
		}, "", 0, 1},
		{"bayes_candidate_validation_reachable_at_distance_one", 1, func(c *CallCorrectionConfig) {
			c.MaxEditDistance = 1
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireCandidateValidated = true
		}, "resolver_min_unique_spotters", 2, 1},
		{"delta2_truncation_needs_distance_two", 1, func(c *CallCorrectionConfig) {
			c.MaxEditDistance = 1
			c.FamilyPolicy.Truncation = CallCorrectionTruncationFamilyConfig{
				Enabled: true, AllowPrefixMatch: true, MaxLengthDelta: 2,
				Delta2Rails: CallCorrectionTruncationDelta2RailsConfig{Enabled: true, RequireCandidateValidated: true},
			}
		}, "", 0, 1},
		{"stabilizer_common_defaults_two", 1, func(c *CallCorrectionConfig) { c.StabilizerEnabled = true }, "recent_band_record_min_unique_spotters", 2, 1},
		{"stabilizer_raw_zero_cells_stays_explicit", 2, func(c *CallCorrectionConfig) { c.StabilizerEnabled = true }, "", 0, 2},
		{"disabled_edit_requirement_unused", 2, func(c *CallCorrectionConfig) {
			c.StabilizerEnabled = true
			c.StabilizerEditNeighborMinSpotters = 3
		}, "", 0, 2},
		{"enabled_edit_requirement_above", 2, func(c *CallCorrectionConfig) {
			c.StabilizerEnabled = true
			c.StabilizerEditNeighborEnabled = true
			c.StabilizerEditNeighborMinSpotters = 3
		}, "stabilizer_edit_neighbor_min_spotters", 3, 2},
		{"score_units_not_capped", 1, func(c *CallCorrectionConfig) {
			c.CustomSCP.ResolverMinScore = 100
			c.CustomSCP.StabilizerMinScore = 100
			c.CustomSCP.SFloorMinScore = 100
		}, "", 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := b14RawCapacityConfig(tc.capacity)
			tc.configure(&cfg)
			before := cfg
			err := cfg.ValidateCustomSCPCapacity()
			if tc.wantKey == "" {
				if err != nil {
					t.Fatalf("existing raw consumer semantics must be preserved: %v", err)
				}
			} else {
				b14RequireCapacityError(t, err, tc.wantKey, tc.wantRequirement, tc.cap)
			}
			if !reflect.DeepEqual(cfg, before) {
				t.Fatal("validation changed the supplied configuration")
			}
		})
	}
}

func TestB14CustomSCPCapacityRawTruncationBranches(t *testing.T) {
	for _, tc := range []struct {
		name       string
		policy     CallCorrectionTruncationFamilyConfig
		wantReject bool
	}{
		{"relax_candidate_default_delta", CallCorrectionTruncationFamilyConfig{
			Enabled: true, AllowPrefixMatch: true,
			RelaxAdvantage: CallCorrectionTruncationAdvantageConfig{Enabled: true, RequireCandidateValidated: true},
		}, true},
		{"relax_subject", CallCorrectionTruncationFamilyConfig{
			Enabled: true, AllowSuffixMatch: true,
			RelaxAdvantage: CallCorrectionTruncationAdvantageConfig{Enabled: true, RequireSubjectUnvalidated: true},
		}, true},
		{"disabled_nested_relax", CallCorrectionTruncationFamilyConfig{
			Enabled: true, AllowPrefixMatch: true,
			RelaxAdvantage: CallCorrectionTruncationAdvantageConfig{RequireCandidateValidated: true},
		}, false},
		{"length_zero_max_unused", CallCorrectionTruncationFamilyConfig{
			Enabled: true, AllowPrefixMatch: true,
			LengthBonus: CallCorrectionTruncationLengthBonusConfig{Enabled: true, RequireCandidateValidated: true},
		}, false},
		{"length_positive_max", CallCorrectionTruncationFamilyConfig{
			Enabled: true, AllowPrefixMatch: true,
			LengthBonus: CallCorrectionTruncationLengthBonusConfig{Enabled: true, Max: 1, RequireCandidateValidated: true},
		}, true},
		{"delta2_default_delta_unreachable", CallCorrectionTruncationFamilyConfig{
			Enabled: true, AllowPrefixMatch: true,
			Delta2Rails: CallCorrectionTruncationDelta2RailsConfig{Enabled: true, RequireCandidateValidated: true},
		}, false},
		{"delta2_zero_extra_still_validates", CallCorrectionTruncationFamilyConfig{
			Enabled: true, AllowPrefixMatch: true, MaxLengthDelta: 2,
			Delta2Rails: CallCorrectionTruncationDelta2RailsConfig{Enabled: true, RequireSubjectUnvalidated: true},
		}, true},
		{"no_matching_relation", CallCorrectionTruncationFamilyConfig{
			Enabled: true, MaxLengthDelta: 2,
			RelaxAdvantage: CallCorrectionTruncationAdvantageConfig{Enabled: true, RequireCandidateValidated: true},
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := b14RawCapacityConfig(1)
			cfg.FamilyPolicy.Truncation = tc.policy
			err := cfg.ValidateCustomSCPCapacity()
			if tc.wantReject {
				b14RequireCapacityError(t, err, "resolver_min_unique_spotters", 2, 1)
			} else if err != nil {
				t.Fatalf("unused validation gate must not impose a boolean minimum: %v", err)
			}
		})
	}
}

func TestB14CustomSCPCapacityLoadedBoundaries(t *testing.T) {
	for _, tc := range []struct {
		key, format string
	}{
		{"recent_band_record_min_unique_spotters", "call_correction:\n  recent_band_record_min_unique_spotters: %d\n"},
		{"resolver_recent_plus1_min_unique_winner", "call_correction:\n  resolver_recent_plus1_min_unique_winner: %d\n"},
		{"resolver_min_unique_spotters", "call_correction:\n  custom_scp:\n    resolver_min_unique_spotters: %d\n"},
		{"resolver_min_unique_h3_cells", "call_correction:\n  custom_scp:\n    resolver_min_unique_h3_cells: %d\n"},
		{"stabilizer_min_unique_spotters", "call_correction:\n  custom_scp:\n    stabilizer_min_unique_spotters: %d\n"},
		{"stabilizer_min_unique_h3_cells", "call_correction:\n  custom_scp:\n    stabilizer_min_unique_h3_cells: %d\n"},
		{"stabilizer_edit_neighbor_min_spotters", "call_correction:\n  stabilizer_edit_neighbor_min_spotters: %d\n"},
		{"s_floor_min_unique_spotters_exact", "call_correction:\n  custom_scp:\n    s_floor_min_unique_spotters_exact: %d\n"},
		{"s_floor_min_unique_h3_cells_exact", "call_correction:\n  custom_scp:\n    s_floor_min_unique_h3_cells_exact: %d\n"},
		{"s_floor_min_unique_spotters_family", "call_correction:\n  custom_scp:\n    s_floor_min_unique_spotters_family: %d\n"},
		{"s_floor_min_unique_h3_cells_family", "call_correction:\n  custom_scp:\n    s_floor_min_unique_h3_cells_family: %d\n"},
	} {
		for _, n := range []int{8, 9} {
			t.Run(fmt.Sprintf("%s/%d", tc.key, n), func(t *testing.T) {
				_, err := b14LoadCapacityConfig(t, fmt.Sprintf(tc.format, n))
				if n == 8 {
					if err != nil {
						t.Fatalf("equality at receiver cap must remain valid: %v", err)
					}
					return
				}
				b14RequireCapacityError(t, err, tc.key, 9, 8)
			})
		}
	}
}

func TestB14CustomSCPCapacityLoadedActivePolicies(t *testing.T) {
	for _, tc := range []struct {
		name, overlay, wantKey string
	}{
		{"global_disabled", "call_correction:\n  enabled: false\n  custom_scp:\n    s_floor_min_unique_spotters_exact: 9\n", ""},
		{"custom_disabled", "call_correction:\n  custom_scp:\n    enabled: false\n    s_floor_min_unique_spotters_family: 9\n", ""},
		{"stabilizer_disabled", "call_correction:\n  stabilizer_enabled: false\n  stabilizer_edit_neighbor_min_spotters: 9\n  custom_scp:\n    stabilizer_min_unique_spotters: 9\n    stabilizer_min_unique_h3_cells: 9\n", ""},
		{"edit_disabled", "call_correction:\n  stabilizer_edit_neighbor_enabled: false\n  stabilizer_edit_neighbor_min_spotters: 9\n", ""},
		{"resolver_unused", "call_correction:\n  resolver_recent_plus1_enabled: false\n  resolver_recent_plus1_min_unique_winner: 9\n  custom_scp:\n    resolver_min_unique_spotters: 9\n    resolver_min_unique_h3_cells: 9\n", ""},
		{"ft_common_independent", "call_correction:\n  resolver_recent_plus1_enabled: false\n  stabilizer_enabled: false\n  recent_band_record_min_unique_spotters: 9\n", ""},
		{"floor_without_optional_rails", "call_correction:\n  recent_band_bonus_enabled: false\n  resolver_recent_plus1_enabled: false\n  stabilizer_enabled: false\n  custom_scp:\n    s_floor_min_unique_spotters_family: 9\n", "s_floor_min_unique_spotters_family"},
		{"score_is_not_cardinality", "call_correction:\n  custom_scp:\n    resolver_min_score: 100\n    stabilizer_min_score: 100\n    s_floor_min_score: 100\n", ""},
		{"bayes_zero_weights_still_consumes_report_gate", "call_correction:\n  resolver_recent_plus1_enabled: false\n  resolver_recent_plus1_min_unique_winner: 9\n  bayes_bonus:\n    enabled: true\n", "resolver_recent_plus1_min_unique_winner"},
		{"bayes_zero_weights_still_counts_cells", "call_correction:\n  resolver_recent_plus1_enabled: false\n  bayes_bonus:\n    enabled: true\n  custom_scp:\n    resolver_min_unique_h3_cells: 9\n", "resolver_min_unique_h3_cells"},
		{"relax_candidate_validation", "call_correction:\n  resolver_recent_plus1_enabled: false\n  family_policy:\n    truncation:\n      enabled: true\n      relax_advantage:\n        enabled: true\n        require_candidate_validated: true\n  custom_scp:\n    resolver_min_unique_spotters: 9\n", "resolver_min_unique_spotters"},
		{"relax_subject_validation", "call_correction:\n  resolver_recent_plus1_enabled: false\n  family_policy:\n    truncation:\n      enabled: true\n      relax_advantage:\n        enabled: true\n        require_subject_unvalidated: true\n  custom_scp:\n    resolver_min_unique_h3_cells: 9\n", "resolver_min_unique_h3_cells"},
		{"nested_validation_unused", "call_correction:\n  resolver_recent_plus1_enabled: false\n  family_policy:\n    truncation:\n      enabled: true\n      relax_advantage:\n        enabled: true\n  custom_scp:\n    resolver_min_unique_spotters: 9\n", ""},
		{"length_bonus_validation", "call_correction:\n  resolver_recent_plus1_enabled: false\n  family_policy:\n    truncation:\n      enabled: true\n      length_bonus:\n        enabled: true\n        max: 1\n        require_subject_unvalidated: true\n  custom_scp:\n    resolver_min_unique_spotters: 9\n", "resolver_min_unique_spotters"},
		{"delta2_unreachable", "call_correction:\n  resolver_recent_plus1_enabled: false\n  family_policy:\n    truncation:\n      enabled: true\n      max_length_delta: 1\n      delta2_rails:\n        enabled: true\n        require_candidate_validated: true\n  custom_scp:\n    resolver_min_unique_spotters: 9\n", ""},
		{"delta2_reachable", "call_correction:\n  resolver_recent_plus1_enabled: false\n  family_policy:\n    truncation:\n      enabled: true\n      max_length_delta: 2\n      delta2_rails:\n        enabled: true\n        extra_confidence_percent: 1\n        require_candidate_validated: true\n  custom_scp:\n    resolver_min_unique_spotters: 9\n", "resolver_min_unique_spotters"},
		{"truncation_relation_disabled", "call_correction:\n  resolver_recent_plus1_enabled: false\n  family_policy:\n    truncation:\n      enabled: true\n      allow_prefix_match: false\n      allow_suffix_match: false\n      relax_advantage:\n        enabled: true\n        require_candidate_validated: true\n  custom_scp:\n    resolver_min_unique_spotters: 9\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := b14LoadCapacityConfig(t, tc.overlay)
			if tc.wantKey != "" {
				b14RequireCapacityError(t, err, tc.wantKey, 9, 8)
				return
			}
			if err != nil {
				t.Fatalf("unused requirement must not reject configuration: %v", err)
			}
			if cfg.CallCorrection.BayesBonus.WeightDistance1Milli != 0 || cfg.CallCorrection.BayesBonus.WeightDistance2Milli != 0 {
				t.Fatal("explicit zero Bayes scales were not preserved by fixture")
			}
		})
	}
}

func TestB14CustomSCPCapacityLoadedDelta2Reachability(t *testing.T) {
	for _, maximum := range []int{1, 2} {
		t.Run(fmt.Sprintf("maximum_distance_%d", maximum), func(t *testing.T) {
			overlay := fmt.Sprintf(`call_correction:
  max_edit_distance: %d
  resolver_recent_plus1_enabled: false
  family_policy:
    truncation:
      enabled: true
      max_length_delta: 2
      delta2_rails:
        enabled: true
        require_candidate_validated: true
  custom_scp:
    resolver_min_unique_h3_cells: 9
`, maximum)
			_, err := b14LoadCapacityConfig(t, overlay)
			if maximum == 1 {
				if err != nil {
					t.Fatalf("unreachable delta2 history gate must not reject loaded configuration: %v", err)
				}
			} else {
				b14RequireCapacityError(t, err, "resolver_min_unique_h3_cells", 9, 8)
			}
		})
	}
}
