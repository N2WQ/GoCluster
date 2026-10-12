// File role: Verifies legacy/FT startup capacity checks against actual consumer
// predicates, raw sentinels, normalized YAML and owner-specific use boundaries.
package config

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func requireLegacyCapacityError(t *testing.T, err error, key, store string, minimum, capacity int) {
	t.Helper()
	if err == nil {
		t.Fatalf("enabled %s minimum %d exceeds %s capacity %d but was accepted", key, minimum, store, capacity)
	}
	for _, fragment := range []string{key, fmt.Sprintf("effective requirement %d exceeds", minimum), store, fmt.Sprintf("receiver capacity=%d", capacity)} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("error must identify setting, effective requirement, store and cap; missing %q in %q", fragment, err)
		}
	}
}

func TestLegacyHistoryCapacityRawConsumers(t *testing.T) {
	floor := LegacyHistoryConsumers{SupportFloor: true}
	stabilizer := LegacyHistoryConsumers{Stabilizer: true}
	resolver := LegacyHistoryConsumers{Resolver: true}
	for _, tc := range []struct {
		name      string
		capacity  int
		consumers LegacyHistoryConsumers
		configure func(*CallCorrectionConfig)
		wantKey   string
		minimum   int
	}{
		{"floor_equality", 16, floor, func(c *CallCorrectionConfig) {
			c.RecentBandBonusEnabled = true
			c.RecentBandRecordMinUniqueSpotters = 16
		}, "", 0},
		{"floor_above", 16, floor, func(c *CallCorrectionConfig) {
			c.RecentBandBonusEnabled = true
			c.RecentBandRecordMinUniqueSpotters = 17
		}, "recent_band_record_min_unique_spotters", 17},
		{"FT_floor_global_correction_off", 16, floor, func(c *CallCorrectionConfig) {
			c.Enabled = false
			c.RecentBandBonusEnabled = true
			c.RecentBandRecordMinUniqueSpotters = 17
		}, "recent_band_record_min_unique_spotters", 17},
		{"disabled_bonus_unused", 1, floor, func(c *CallCorrectionConfig) { c.RecentBandRecordMinUniqueSpotters = 17 }, "", 0},
		{"floor_zero_defaults_two", 1, floor, func(c *CallCorrectionConfig) { c.RecentBandBonusEnabled = true }, "recent_band_record_min_unique_spotters", 2},
		{"floor_negative_defaults_two", 1, floor, func(c *CallCorrectionConfig) {
			c.RecentBandBonusEnabled = true
			c.RecentBandRecordMinUniqueSpotters = -1
		}, "recent_band_record_min_unique_spotters", 2},
		{"stabilizer_equality", 64, stabilizer, func(c *CallCorrectionConfig) { c.StabilizerEnabled = true; c.RecentBandRecordMinUniqueSpotters = 64 }, "", 0},
		{"stabilizer_above", 64, stabilizer, func(c *CallCorrectionConfig) { c.StabilizerEnabled = true; c.RecentBandRecordMinUniqueSpotters = 65 }, "recent_band_record_min_unique_spotters", 65},
		{"stabilizer_zero_defaults_two", 1, stabilizer, func(c *CallCorrectionConfig) { c.StabilizerEnabled = true }, "recent_band_record_min_unique_spotters", 2},
		{"stabilizer_negative_defaults_two", 1, stabilizer, func(c *CallCorrectionConfig) { c.StabilizerEnabled = true; c.RecentBandRecordMinUniqueSpotters = -1 }, "recent_band_record_min_unique_spotters", 2},
		{"disabled_stabilizer_unused", 1, stabilizer, func(c *CallCorrectionConfig) {
			c.RecentBandRecordMinUniqueSpotters = 65
			c.StabilizerEditNeighborEnabled = true
			c.StabilizerEditNeighborMinSpotters = 65
		}, "", 0},
		{"global_disabled_stabilizer_unused", 1, stabilizer, func(c *CallCorrectionConfig) {
			c.Enabled = false
			c.StabilizerEnabled = true
			c.RecentBandRecordMinUniqueSpotters = 65
		}, "", 0},
		{"edit_equality", 64, stabilizer, func(c *CallCorrectionConfig) {
			c.StabilizerEnabled = true
			c.StabilizerEditNeighborEnabled = true
			c.StabilizerEditNeighborMinSpotters = 64
		}, "", 0},
		{"edit_above", 64, stabilizer, func(c *CallCorrectionConfig) {
			c.StabilizerEnabled = true
			c.StabilizerEditNeighborEnabled = true
			c.StabilizerEditNeighborMinSpotters = 65
		}, "stabilizer_edit_neighbor_min_spotters", 65},
		{"disabled_edit_unused", 2, stabilizer, func(c *CallCorrectionConfig) { c.StabilizerEnabled = true; c.StabilizerEditNeighborMinSpotters = 65 }, "", 0},
		{"edit_zero_inherits_common", 2, stabilizer, func(c *CallCorrectionConfig) {
			c.StabilizerEnabled = true
			c.StabilizerEditNeighborEnabled = true
			c.RecentBandRecordMinUniqueSpotters = 2
		}, "", 0},
		{"plus1_equality", 64, resolver, func(c *CallCorrectionConfig) {
			c.ResolverRecentPlus1Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = 64
		}, "", 0},
		{"plus1_above", 64, resolver, func(c *CallCorrectionConfig) {
			c.ResolverRecentPlus1Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = 65
		}, "resolver_recent_plus1_min_unique_winner", 65},
		{"raw_plus1_zero_stays_zero", 1, resolver, func(c *CallCorrectionConfig) { c.ResolverRecentPlus1Enabled = true }, "", 0},
		{"raw_plus1_negative_stays_negative", 1, resolver, func(c *CallCorrectionConfig) {
			c.ResolverRecentPlus1Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = -1
		}, "", 0},
		{"unused_common_not_a_count_floor", 64, resolver, func(c *CallCorrectionConfig) {
			c.RecentBandRecordMinUniqueSpotters = 65
			c.ResolverRecentPlus1Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = 3
		}, "", 0},
		{"plus1_distance_zero_still_consumed", 64, resolver, func(c *CallCorrectionConfig) {
			c.MaxEditDistance = 0
			c.ResolverRecentPlus1Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = 65
		}, "resolver_recent_plus1_min_unique_winner", 65},
		{"bayes_reuses_plus1_gate", 64, resolver, func(c *CallCorrectionConfig) { c.BayesBonus.Enabled = true; c.ResolverRecentPlus1MinUniqueWinner = 65 }, "resolver_recent_plus1_min_unique_winner", 65},
		{"bayes_distance_zero_unused", 1, resolver, func(c *CallCorrectionConfig) {
			c.MaxEditDistance = 0
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireCandidateValidated = true
			c.RecentBandRecordMinUniqueSpotters = 65
			c.ResolverRecentPlus1MinUniqueWinner = 65
		}, "", 0},
		{"bayes_candidate_validation_defaults_two", 1, resolver, func(c *CallCorrectionConfig) {
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireCandidateValidated = true
		}, "recent_band_record_min_unique_spotters", 2},
		{"bayes_validation_equality", 64, resolver, func(c *CallCorrectionConfig) {
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireCandidateValidated = true
			c.RecentBandRecordMinUniqueSpotters = 64
		}, "", 0},
		{"bayes_validation_above", 64, resolver, func(c *CallCorrectionConfig) {
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireCandidateValidated = true
			c.RecentBandRecordMinUniqueSpotters = 65
		}, "recent_band_record_min_unique_spotters", 65},
		{"bayes_subject_distance_two_consumed", 1, resolver, func(c *CallCorrectionConfig) {
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireSubjectUnvalidatedDistance2 = true
		}, "recent_band_record_min_unique_spotters", 2},
		{"bayes_subject_distance_one_unused", 1, resolver, func(c *CallCorrectionConfig) {
			c.MaxEditDistance = 1
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireSubjectUnvalidatedDistance2 = true
			c.RecentBandRecordMinUniqueSpotters = 65
		}, "", 0},
		{"global_disabled_resolver_unused", 1, resolver, func(c *CallCorrectionConfig) {
			c.Enabled = false
			c.ResolverRecentPlus1Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = 65
		}, "", 0},
		{"reveng_does_not_run_stabilizer_or_floor", 64, resolver, func(c *CallCorrectionConfig) {
			c.RecentBandBonusEnabled = true
			c.StabilizerEnabled = true
			c.RecentBandRecordMinUniqueSpotters = 65
			c.StabilizerEditNeighborEnabled = true
			c.StabilizerEditNeighborMinSpotters = 65
		}, "", 0},
		{"replay_does_not_run_support_floor", 64, LegacyHistoryConsumers{Stabilizer: true, Resolver: true}, func(c *CallCorrectionConfig) {
			c.RecentBandBonusEnabled = true
			c.RecentBandRecordMinUniqueSpotters = 65
		}, "", 0},
		{"custom_receiver_validation_folded", 64, resolver, func(c *CallCorrectionConfig) {
			c.CustomSCP.Enabled = true
			c.CustomSCP.ResolverMinUniqueSpotters = 65
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireCandidateValidated = true
		}, "custom_scp.resolver_min_unique_spotters", 65},
		{"custom_receiver_count_folded", 64, resolver, func(c *CallCorrectionConfig) {
			c.CustomSCP.Enabled = true
			c.CustomSCP.ResolverMinUniqueSpotters = 65
			c.ResolverRecentPlus1Enabled = true
		}, "custom_scp.resolver_min_unique_spotters", 65},
		{"custom_common_count_folded", 64, resolver, func(c *CallCorrectionConfig) {
			c.CustomSCP.Enabled = true
			c.RecentBandRecordMinUniqueSpotters = 65
			c.ResolverRecentPlus1Enabled = true
		}, "recent_band_record_min_unique_spotters", 65},
		{"custom_stabilizer_receiver_folded", 64, stabilizer, func(c *CallCorrectionConfig) {
			c.CustomSCP.Enabled = true
			c.CustomSCP.StabilizerMinUniqueSpotters = 65
			c.StabilizerEnabled = true
		}, "custom_scp.stabilizer_min_unique_spotters", 65},
		{"custom_score_cells_not_legacy_requirements", 2, LegacyHistoryConsumers{SupportFloor: true, Stabilizer: true, Resolver: true}, func(c *CallCorrectionConfig) {
			c.CustomSCP = CallCorrectionCustomSCPConfig{Enabled: true, ResolverMinScore: 1000, ResolverMinUniqueH3Cells: 1000, StabilizerMinScore: 1000, StabilizerMinUniqueH3Cells: 1000, SFloorMinScore: 1000, SFloorMinUniqueSpottersExact: 1000, SFloorMinUniqueSpottersFamily: 1000, SFloorMinUniqueH3CellsExact: 1000, SFloorMinUniqueH3CellsFamily: 1000}
			c.RecentBandBonusEnabled = true
			c.StabilizerEnabled = true
			c.ResolverRecentPlus1Enabled = true
			c.BayesBonus.Enabled = true
			c.BayesBonus.RequireCandidateValidated = true
		}, "", 0},
		{"no_consumers", 1, LegacyHistoryConsumers{}, func(c *CallCorrectionConfig) {
			c.RecentBandBonusEnabled = true
			c.StabilizerEnabled = true
			c.RecentBandRecordMinUniqueSpotters = 65
			c.ResolverRecentPlus1Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = 65
		}, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := CallCorrectionConfig{Enabled: true, MaxEditDistance: -1}
			tc.configure(&cfg)
			before := cfg
			const store = "fixture legacy history"
			err := cfg.ValidateLegacyHistoryCapacity(store, tc.capacity, tc.consumers)
			if tc.wantKey == "" {
				if err != nil {
					t.Fatalf("unused gates or preserved raw semantics must not reject: %v", err)
				}
			} else {
				requireLegacyCapacityError(t, err, tc.wantKey, store, tc.minimum, tc.capacity)
			}
			if !reflect.DeepEqual(cfg, before) {
				t.Fatal("validation mutated supplied configuration")
			}
		})
	}
}

func TestLegacyHistoryCapacityTruncationReachability(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxEdit    int
		policy     CallCorrectionTruncationFamilyConfig
		wantReject bool
	}{
		{"relax_candidate", -1, CallCorrectionTruncationFamilyConfig{Enabled: true, AllowPrefixMatch: true, RelaxAdvantage: CallCorrectionTruncationAdvantageConfig{Enabled: true, RequireCandidateValidated: true}}, true},
		{"relax_subject", -1, CallCorrectionTruncationFamilyConfig{Enabled: true, AllowSuffixMatch: true, RelaxAdvantage: CallCorrectionTruncationAdvantageConfig{Enabled: true, RequireSubjectUnvalidated: true}}, true},
		{"relation_disabled", -1, CallCorrectionTruncationFamilyConfig{AllowPrefixMatch: true, RelaxAdvantage: CallCorrectionTruncationAdvantageConfig{Enabled: true, RequireCandidateValidated: true}}, false},
		{"no_relation_match", -1, CallCorrectionTruncationFamilyConfig{Enabled: true, RelaxAdvantage: CallCorrectionTruncationAdvantageConfig{Enabled: true, RequireCandidateValidated: true}}, false},
		{"nested_rail_disabled", -1, CallCorrectionTruncationFamilyConfig{Enabled: true, AllowPrefixMatch: true, RelaxAdvantage: CallCorrectionTruncationAdvantageConfig{RequireCandidateValidated: true}}, false},
		{"distance_zero", 0, CallCorrectionTruncationFamilyConfig{Enabled: true, AllowPrefixMatch: true, RelaxAdvantage: CallCorrectionTruncationAdvantageConfig{Enabled: true, RequireCandidateValidated: true}}, false},
		{"length_positive_max", -1, CallCorrectionTruncationFamilyConfig{Enabled: true, AllowPrefixMatch: true, LengthBonus: CallCorrectionTruncationLengthBonusConfig{Enabled: true, Max: 1, RequireCandidateValidated: true}}, true},
		{"length_zero_raw_unused", -1, CallCorrectionTruncationFamilyConfig{Enabled: true, AllowPrefixMatch: true, LengthBonus: CallCorrectionTruncationLengthBonusConfig{Enabled: true, RequireCandidateValidated: true}}, false},
		{"delta2_default_delta_one_unused", -1, CallCorrectionTruncationFamilyConfig{Enabled: true, AllowPrefixMatch: true, Delta2Rails: CallCorrectionTruncationDelta2RailsConfig{Enabled: true, RequireCandidateValidated: true}}, false},
		{"delta2_exact_distance_two", 2, CallCorrectionTruncationFamilyConfig{Enabled: true, AllowPrefixMatch: true, MaxLengthDelta: 2, Delta2Rails: CallCorrectionTruncationDelta2RailsConfig{Enabled: true, RequireCandidateValidated: true}}, true},
		{"delta2_subject_zero_extra_consumed", -1, CallCorrectionTruncationFamilyConfig{Enabled: true, AllowSuffixMatch: true, MaxLengthDelta: 2, Delta2Rails: CallCorrectionTruncationDelta2RailsConfig{Enabled: true, RequireSubjectUnvalidated: true}}, true},
		{"delta2_distance_one_unused", 1, CallCorrectionTruncationFamilyConfig{Enabled: true, AllowPrefixMatch: true, MaxLengthDelta: 2, Delta2Rails: CallCorrectionTruncationDelta2RailsConfig{Enabled: true, RequireCandidateValidated: true}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := CallCorrectionConfig{Enabled: true, MaxEditDistance: tc.maxEdit, FamilyPolicy: CallCorrectionFamilyPolicyConfig{Truncation: tc.policy}}
			err := cfg.ValidateLegacyHistoryCapacity("legacy history", 1, LegacyHistoryConsumers{Resolver: true})
			if tc.wantReject {
				requireLegacyCapacityError(t, err, "recent_band_record_min_unique_spotters", "legacy history", 2, 1)
			} else if err != nil {
				t.Fatalf("unreachable validation rail must not reject: %v", err)
			}
		})
	}
}

func TestLegacyHistoryCapacityNormalizedYAML(t *testing.T) {
	for _, tc := range []struct {
		name      string
		overlay   string
		capacity  int
		consumers LegacyHistoryConsumers
		wantKey   string
		minimum   int
	}{
		{"normalized_common_positive", "call_correction:\n  custom_scp:\n    enabled: false\n", 1, LegacyHistoryConsumers{SupportFloor: true}, "recent_band_record_min_unique_spotters", 2},
		{"normalized_plus1_positive", "call_correction:\n  custom_scp:\n    enabled: false\n", 3, LegacyHistoryConsumers{Resolver: true}, "resolver_recent_plus1_min_unique_winner", 4},
		{"normalized_length_positive_max", "call_correction:\n  custom_scp:\n    enabled: false\n  family_policy:\n    truncation:\n      enabled: true\n      length_bonus:\n        enabled: true\n        max: 1\n        require_candidate_validated: true\n", 1, LegacyHistoryConsumers{Resolver: true}, "recent_band_record_min_unique_spotters", 2},
		{"FT_impossible_does_not_change_generic_loader", "call_correction:\n  enabled: false\n  custom_scp:\n    enabled: false\n  recent_band_record_min_unique_spotters: 17\n", 16, LegacyHistoryConsumers{SupportFloor: true}, "recent_band_record_min_unique_spotters", 17},
		{"normalized_equality", "call_correction:\n  custom_scp:\n    enabled: false\n  recent_band_record_min_unique_spotters: 16\n", 16, LegacyHistoryConsumers{SupportFloor: true}, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeRequiredFloodControlFile(t, dir)
			writeTestConfigOverlay(t, dir, "pipeline.yaml", b14CapacityFixture)
			writeTestConfigOverlay(t, dir, "pipeline.yaml", tc.overlay)
			cfg, err := Load(dir)
			if err != nil {
				t.Fatalf("generic loader must retain its existing CustomSCP-only capacity contract: %v", err)
			}
			before := cfg.CallCorrection
			err = cfg.CallCorrection.ValidateLegacyHistoryCapacity("normalized legacy history", tc.capacity, tc.consumers)
			if tc.wantKey == "" {
				if err != nil {
					t.Fatalf("exact-capacity normalized requirement must be accepted: %v", err)
				}
			} else {
				requireLegacyCapacityError(t, err, tc.wantKey, "normalized legacy history", tc.minimum, tc.capacity)
			}
			if !reflect.DeepEqual(cfg.CallCorrection, before) {
				t.Fatal("owner capacity validation mutated normalized configuration")
			}
		})
	}
}

func TestLegacyHistoryCapacityExplicitZeroYAMLRejected(t *testing.T) {
	for _, tc := range []struct{ name, key, overlay string }{
		{"common", "recent_band_record_min_unique_spotters", "call_correction:\n  recent_band_record_min_unique_spotters: 0\n"},
		{"plus1", "resolver_recent_plus1_min_unique_winner", "call_correction:\n  resolver_recent_plus1_min_unique_winner: 0\n"},
		{"length", "length_bonus.max", "call_correction:\n  family_policy:\n    truncation:\n      length_bonus:\n        max: 0\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := b14LoadCapacityConfig(t, tc.overlay)
			if err == nil || !strings.Contains(err.Error(), tc.key) || !strings.Contains(err.Error(), "must be > 0") {
				t.Fatalf("existing explicit-zero YAML rejection must remain; key=%s error=%v", tc.key, err)
			}
		})
	}
}
