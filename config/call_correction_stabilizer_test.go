package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadCallCorrectionStabilizerDefaults(t *testing.T) {
	dir := t.TempDir()
	pipeline := `call_correction:
  enabled: true
  stabilizer_enabled: true
`
	if err := os.WriteFile(filepath.Join(dir, "pipeline.yaml"), []byte(pipeline), 0o644); err != nil {
		t.Fatalf("write pipeline.yaml: %v", err)
	}

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if !cfg.CallCorrection.StabilizerEnabled {
		t.Fatalf("expected stabilizer enabled")
	}
	if cfg.CallCorrection.StabilizerDelaySeconds != 5 {
		t.Fatalf("expected stabilizer delay default 5s, got %d", cfg.CallCorrection.StabilizerDelaySeconds)
	}
	if cfg.CallCorrection.StabilizerMaxChecks != 1 {
		t.Fatalf("expected stabilizer max checks default 1, got %d", cfg.CallCorrection.StabilizerMaxChecks)
	}
	if cfg.CallCorrection.StabilizerTimeoutAction != "release" {
		t.Fatalf("expected stabilizer timeout action default release, got %q", cfg.CallCorrection.StabilizerTimeoutAction)
	}
	if cfg.CallCorrection.StabilizerMaxPending != 20000 {
		t.Fatalf("expected stabilizer max pending default 20000, got %d", cfg.CallCorrection.StabilizerMaxPending)
	}
	if cfg.CallCorrection.StabilizerPDelayConfidencePercent != 0 {
		t.Fatalf("expected stabilizer P-delay confidence default 0, got %d", cfg.CallCorrection.StabilizerPDelayConfidencePercent)
	}
	if cfg.CallCorrection.StabilizerPDelayMaxChecks != 0 {
		t.Fatalf("expected stabilizer P-delay max checks default 0, got %d", cfg.CallCorrection.StabilizerPDelayMaxChecks)
	}
	if cfg.CallCorrection.StabilizerAmbiguousMaxChecks != 0 {
		t.Fatalf("expected stabilizer ambiguous max checks default 0, got %d", cfg.CallCorrection.StabilizerAmbiguousMaxChecks)
	}
	if cfg.CallCorrection.StabilizerEditNeighborEnabled {
		t.Fatalf("expected stabilizer edit-neighbor delay disabled by default")
	}
	if cfg.CallCorrection.StabilizerEditNeighborMaxChecks != 0 {
		t.Fatalf("expected stabilizer edit-neighbor max checks default 0, got %d", cfg.CallCorrection.StabilizerEditNeighborMaxChecks)
	}
	if cfg.CallCorrection.StabilizerEditNeighborMinSpotters != 0 {
		t.Fatalf("expected stabilizer edit-neighbor min spotters default 0, got %d", cfg.CallCorrection.StabilizerEditNeighborMinSpotters)
	}
	if cfg.CallCorrection.ResolverNeighborhoodEnabled {
		t.Fatalf("expected resolver neighborhood disabled by default")
	}
	if cfg.CallCorrection.ResolverNeighborhoodBucketRadius != 0 {
		t.Fatalf("expected resolver neighborhood radius default 0, got %d", cfg.CallCorrection.ResolverNeighborhoodBucketRadius)
	}
	if cfg.CallCorrection.ResolverNeighborhoodMaxDistance != 1 {
		t.Fatalf("expected resolver neighborhood max distance default 1, got %d", cfg.CallCorrection.ResolverNeighborhoodMaxDistance)
	}
	if !cfg.CallCorrection.ResolverNeighborhoodAllowTruncation {
		t.Fatalf("expected resolver neighborhood truncation-family allowance enabled by default")
	}
	if !cfg.CallCorrection.ResolverRecentPlus1Enabled {
		t.Fatalf("expected resolver recent plus1 enabled by default")
	}
	if cfg.CallCorrection.ResolverRecentPlus1MinUniqueWinner != 3 {
		t.Fatalf("expected resolver recent plus1 min unique winner default 3, got %d", cfg.CallCorrection.ResolverRecentPlus1MinUniqueWinner)
	}
	if !cfg.CallCorrection.ResolverRecentPlus1RequireSubjectWeaker {
		t.Fatalf("expected resolver recent plus1 subject-weaker requirement enabled by default")
	}
	if cfg.CallCorrection.ResolverRecentPlus1MaxDistance != 1 {
		t.Fatalf("expected resolver recent plus1 max distance default 1, got %d", cfg.CallCorrection.ResolverRecentPlus1MaxDistance)
	}
	if !cfg.CallCorrection.ResolverRecentPlus1AllowTruncation {
		t.Fatalf("expected resolver recent plus1 truncation-family allowance enabled by default")
	}
	if cfg.CallCorrection.BayesBonus.Enabled {
		t.Fatalf("expected bayes bonus disabled by default")
	}
	if cfg.CallCorrection.BayesBonus.WeightDistance1Milli != 350 || cfg.CallCorrection.BayesBonus.WeightDistance2Milli != 200 {
		t.Fatalf("expected bayes distance weights defaults 350/200, got %d/%d",
			cfg.CallCorrection.BayesBonus.WeightDistance1Milli,
			cfg.CallCorrection.BayesBonus.WeightDistance2Milli)
	}
	if cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli != 450 || cfg.CallCorrection.BayesBonus.ReportThresholdDistance2Milli != 650 {
		t.Fatalf("expected bayes report thresholds defaults 450/650, got %d/%d",
			cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli,
			cfg.CallCorrection.BayesBonus.ReportThresholdDistance2Milli)
	}
	if !cfg.CallCorrection.BayesBonus.RequireCandidateValidated {
		t.Fatalf("expected bayes require_candidate_validated enabled by default")
	}
	if !cfg.CallCorrection.BayesBonus.RequireSubjectUnvalidatedDistance2 {
		t.Fatalf("expected bayes require_subject_unvalidated_distance2 enabled by default")
	}
	if cfg.CallCorrection.TemporalDecoder.Enabled {
		t.Fatalf("expected temporal decoder disabled by default")
	}
	if cfg.CallCorrection.TemporalDecoder.Scope != "uncertain_only" {
		t.Fatalf("expected temporal decoder scope default uncertain_only, got %q", cfg.CallCorrection.TemporalDecoder.Scope)
	}
	if cfg.CallCorrection.TemporalDecoder.LagSeconds != 2 {
		t.Fatalf("expected temporal decoder lag default 2, got %d", cfg.CallCorrection.TemporalDecoder.LagSeconds)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxWaitSeconds != 6 {
		t.Fatalf("expected temporal decoder max wait default 6, got %d", cfg.CallCorrection.TemporalDecoder.MaxWaitSeconds)
	}
	if cfg.CallCorrection.TemporalDecoder.BeamSize != 8 {
		t.Fatalf("expected temporal decoder beam size default 8, got %d", cfg.CallCorrection.TemporalDecoder.BeamSize)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxObsCandidates != 8 {
		t.Fatalf("expected temporal decoder max obs default 8, got %d", cfg.CallCorrection.TemporalDecoder.MaxObsCandidates)
	}
	if cfg.CallCorrection.TemporalDecoder.StayBonus != 120 {
		t.Fatalf("expected temporal decoder stay bonus default 120, got %d", cfg.CallCorrection.TemporalDecoder.StayBonus)
	}
	if cfg.CallCorrection.TemporalDecoder.SwitchPenalty != 160 {
		t.Fatalf("expected temporal decoder switch penalty default 160, got %d", cfg.CallCorrection.TemporalDecoder.SwitchPenalty)
	}
	if cfg.CallCorrection.TemporalDecoder.FamilySwitchPenalty != 60 {
		t.Fatalf("expected temporal decoder family switch penalty default 60, got %d", cfg.CallCorrection.TemporalDecoder.FamilySwitchPenalty)
	}
	if cfg.CallCorrection.TemporalDecoder.Edit1SwitchPenalty != 90 {
		t.Fatalf("expected temporal decoder edit1 switch penalty default 90, got %d", cfg.CallCorrection.TemporalDecoder.Edit1SwitchPenalty)
	}
	if cfg.CallCorrection.TemporalDecoder.MinScore != 0 {
		t.Fatalf("expected temporal decoder min score default 0, got %d", cfg.CallCorrection.TemporalDecoder.MinScore)
	}
	if cfg.CallCorrection.TemporalDecoder.MinMarginScore != 80 {
		t.Fatalf("expected temporal decoder min margin default 80, got %d", cfg.CallCorrection.TemporalDecoder.MinMarginScore)
	}
	if cfg.CallCorrection.TemporalDecoder.OverflowAction != "fallback_resolver" {
		t.Fatalf("expected temporal decoder overflow action default fallback_resolver, got %q", cfg.CallCorrection.TemporalDecoder.OverflowAction)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxPending != 25000 {
		t.Fatalf("expected temporal decoder max pending default 25000, got %d", cfg.CallCorrection.TemporalDecoder.MaxPending)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxActiveKeys != 6000 {
		t.Fatalf("expected temporal decoder max active keys default 6000, got %d", cfg.CallCorrection.TemporalDecoder.MaxActiveKeys)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxEventsPerKey != 32 {
		t.Fatalf("expected temporal decoder max events default 32, got %d", cfg.CallCorrection.TemporalDecoder.MaxEventsPerKey)
	}
	if cfg.CallCorrection.FamilyPolicy.SlashPrecedenceMinReports != 2 {
		t.Fatalf("expected family slash precedence min reports default 2, got %d", cfg.CallCorrection.FamilyPolicy.SlashPrecedenceMinReports)
	}
	if !cfg.CallCorrection.FamilyPolicy.Truncation.Enabled {
		t.Fatalf("expected truncation family policy enabled by default")
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.MaxLengthDelta != 1 {
		t.Fatalf("expected truncation max length delta default 1, got %d", cfg.CallCorrection.FamilyPolicy.Truncation.MaxLengthDelta)
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.MinShorterLength != 3 {
		t.Fatalf("expected truncation min shorter length default 3, got %d", cfg.CallCorrection.FamilyPolicy.Truncation.MinShorterLength)
	}
	if !cfg.CallCorrection.FamilyPolicy.Truncation.AllowPrefixMatch || !cfg.CallCorrection.FamilyPolicy.Truncation.AllowSuffixMatch {
		t.Fatalf("expected truncation prefix/suffix matching enabled by default")
	}
	if !cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.Enabled {
		t.Fatalf("expected truncation relax advantage enabled by default")
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.MinAdvantage != 0 {
		t.Fatalf("expected truncation relax min advantage default 0, got %d", cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.MinAdvantage)
	}
	if !cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.RequireCandidateValidated {
		t.Fatalf("expected truncation relax to require candidate validation by default")
	}
	if !cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.RequireSubjectUnvalidated {
		t.Fatalf("expected truncation relax to require subject unvalidated by default")
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Enabled {
		t.Fatalf("expected truncation length bonus disabled by default")
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Max != 0 {
		t.Fatalf("expected truncation length bonus max default 0, got %d", cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Max)
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.Enabled {
		t.Fatalf("expected truncation delta2 rails disabled by default")
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.ExtraConfidencePercent != 0 {
		t.Fatalf("expected truncation delta2 extra confidence default 0, got %d", cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.ExtraConfidencePercent)
	}
	if !cfg.CallCorrection.FamilyPolicy.TelnetSuppression.Enabled {
		t.Fatalf("expected telnet family suppression enabled by default")
	}
	if cfg.CallCorrection.FamilyPolicy.TelnetSuppression.EditNeighborEnabled {
		t.Fatalf("expected telnet edit-neighbor suppression disabled by default")
	}
	if cfg.CallCorrection.FamilyPolicy.TelnetSuppression.WindowSeconds != cfg.CallCorrection.RecencySeconds {
		t.Fatalf("expected telnet family suppression window to follow recency default (%d), got %d", cfg.CallCorrection.RecencySeconds, cfg.CallCorrection.FamilyPolicy.TelnetSuppression.WindowSeconds)
	}
	if cfg.CallCorrection.FamilyPolicy.TelnetSuppression.MaxEntries != cfg.CallCorrection.StabilizerMaxPending {
		t.Fatalf("expected telnet family suppression max entries to follow stabilizer max pending (%d), got %d", cfg.CallCorrection.StabilizerMaxPending, cfg.CallCorrection.FamilyPolicy.TelnetSuppression.MaxEntries)
	}
	if cfg.CallCorrection.FamilyPolicy.TelnetSuppression.FrequencyToleranceFallbackHz != cfg.CallCorrection.FrequencyToleranceHz {
		t.Fatalf("expected telnet family suppression fallback tolerance %.1f, got %.1f", cfg.CallCorrection.FrequencyToleranceHz, cfg.CallCorrection.FamilyPolicy.TelnetSuppression.FrequencyToleranceFallbackHz)
	}
}

func TestLoadRejectsInvalidCallCorrectionStabilizerTimeoutAction(t *testing.T) {
	dir := t.TempDir()
	pipeline := `call_correction:
  enabled: true
  stabilizer_enabled: true
  stabilizer_timeout_action: "hold"
`
	if err := os.WriteFile(filepath.Join(dir, "pipeline.yaml"), []byte(pipeline), 0o644); err != nil {
		t.Fatalf("write pipeline.yaml: %v", err)
	}

	_, err := Load(dir)
	if err == nil {
		t.Fatalf("expected Load() error for invalid stabilizer_timeout_action")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "stabilizer_timeout_action") {
		t.Fatalf("expected stabilizer timeout action error, got %v", err)
	}
}

func TestLoadCallCorrectionFamilyPolicyOverrides(t *testing.T) {
	dir := t.TempDir()
	pipeline := `call_correction:
  enabled: true
  stabilizer_max_checks: 4
  stabilizer_p_delay_confidence_percent: 25
  stabilizer_p_delay_max_checks: 2
  stabilizer_ambiguous_max_checks: 3
  stabilizer_edit_neighbor_enabled: true
  stabilizer_edit_neighbor_max_checks: 6
  stabilizer_edit_neighbor_min_spotters: 4
  resolver_neighborhood_enabled: true
  resolver_neighborhood_bucket_radius: 2
  resolver_neighborhood_max_distance: 2
  resolver_neighborhood_allow_truncation_family: false
  resolver_recent_plus1_enabled: false
  resolver_recent_plus1_min_unique_winner: 5
  resolver_recent_plus1_require_subject_weaker: false
  resolver_recent_plus1_max_distance: 2
  resolver_recent_plus1_allow_truncation_family: false
  bayes_bonus:
    enabled: true
    weight_distance1_milli: 330
    weight_distance2_milli: 180
    weighted_smoothing_milli: 1200
    recent_smoothing: 3
    obs_log_cap_milli: 400
    prior_log_min_milli: -250
    prior_log_max_milli: 700
    report_threshold_distance1_milli: 500
    report_threshold_distance2_milli: 700
    advantage_threshold_distance1_milli: 750
    advantage_threshold_distance2_milli: 980
    advantage_min_weighted_delta_distance1_milli: 250
    advantage_min_weighted_delta_distance2_milli: 350
    advantage_extra_confidence_distance1: 4
    advantage_extra_confidence_distance2: 6
    require_candidate_validated: false
    require_subject_unvalidated_distance2: false
  temporal_decoder:
    enabled: true
    scope: "all_correction_candidates"
    lag_seconds: 3
    max_wait_seconds: 7
    beam_size: 6
    max_obs_candidates: 5
    stay_bonus: 200
    switch_penalty: 180
    family_switch_penalty: 70
    edit1_switch_penalty: 90
    min_score: 100
    min_margin_score: 25
    overflow_action: "abstain"
    max_pending: 9000
    max_active_keys: 7000
    max_events_per_key: 48
  slash_precedence_min_reports: 7
  family_policy:
    slash_precedence_min_reports: 3
    truncation:
      enabled: true
      max_length_delta: 2
      min_shorter_length: 4
      allow_prefix_match: false
      allow_suffix_match: true
      relax_advantage:
        enabled: true
        min_advantage: 1
        require_candidate_validated: false
        require_subject_unvalidated: false
      length_bonus:
        enabled: true
        max: 2
        require_candidate_validated: false
        require_subject_unvalidated: false
      delta2_rails:
        enabled: true
        extra_confidence_percent: 15
        require_candidate_validated: false
        require_subject_unvalidated: true
    telnet_suppression:
      enabled: false
      edit_neighbor_enabled: true
      window_seconds: 120
      max_entries: 1024
      frequency_tolerance_fallback_hz: 750
`
	if err := os.WriteFile(filepath.Join(dir, "pipeline.yaml"), []byte(pipeline), 0o644); err != nil {
		t.Fatalf("write pipeline.yaml: %v", err)
	}

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.CallCorrection.FamilyPolicy.SlashPrecedenceMinReports != 3 {
		t.Fatalf("expected family slash precedence min reports 3, got %d", cfg.CallCorrection.FamilyPolicy.SlashPrecedenceMinReports)
	}
	if cfg.CallCorrection.StabilizerMaxChecks != 4 {
		t.Fatalf("expected stabilizer max checks 4, got %d", cfg.CallCorrection.StabilizerMaxChecks)
	}
	if cfg.CallCorrection.StabilizerPDelayConfidencePercent != 25 {
		t.Fatalf("expected stabilizer P-delay confidence percent 25, got %d", cfg.CallCorrection.StabilizerPDelayConfidencePercent)
	}
	if cfg.CallCorrection.StabilizerPDelayMaxChecks != 2 {
		t.Fatalf("expected stabilizer P-delay max checks 2, got %d", cfg.CallCorrection.StabilizerPDelayMaxChecks)
	}
	if cfg.CallCorrection.StabilizerAmbiguousMaxChecks != 3 {
		t.Fatalf("expected stabilizer ambiguous max checks 3, got %d", cfg.CallCorrection.StabilizerAmbiguousMaxChecks)
	}
	if !cfg.CallCorrection.StabilizerEditNeighborEnabled {
		t.Fatalf("expected stabilizer edit-neighbor delay enabled")
	}
	if cfg.CallCorrection.StabilizerEditNeighborMaxChecks != 6 {
		t.Fatalf("expected stabilizer edit-neighbor max checks 6, got %d", cfg.CallCorrection.StabilizerEditNeighborMaxChecks)
	}
	if cfg.CallCorrection.StabilizerEditNeighborMinSpotters != 4 {
		t.Fatalf("expected stabilizer edit-neighbor min spotters 4, got %d", cfg.CallCorrection.StabilizerEditNeighborMinSpotters)
	}
	if !cfg.CallCorrection.ResolverNeighborhoodEnabled {
		t.Fatalf("expected resolver neighborhood enabled")
	}
	if cfg.CallCorrection.ResolverNeighborhoodBucketRadius != 2 {
		t.Fatalf("expected resolver neighborhood radius 2, got %d", cfg.CallCorrection.ResolverNeighborhoodBucketRadius)
	}
	if cfg.CallCorrection.ResolverNeighborhoodMaxDistance != 2 {
		t.Fatalf("expected resolver neighborhood max distance 2, got %d", cfg.CallCorrection.ResolverNeighborhoodMaxDistance)
	}
	if cfg.CallCorrection.ResolverNeighborhoodAllowTruncation {
		t.Fatalf("expected resolver neighborhood truncation-family allowance disabled by override")
	}
	if cfg.CallCorrection.ResolverRecentPlus1Enabled {
		t.Fatalf("expected resolver recent plus1 disabled by override")
	}
	if cfg.CallCorrection.ResolverRecentPlus1MinUniqueWinner != 5 {
		t.Fatalf("expected resolver recent plus1 min unique winner 5, got %d", cfg.CallCorrection.ResolverRecentPlus1MinUniqueWinner)
	}
	if cfg.CallCorrection.ResolverRecentPlus1RequireSubjectWeaker {
		t.Fatalf("expected resolver recent plus1 subject-weaker requirement disabled by override")
	}
	if cfg.CallCorrection.ResolverRecentPlus1MaxDistance != 2 {
		t.Fatalf("expected resolver recent plus1 max distance 2, got %d", cfg.CallCorrection.ResolverRecentPlus1MaxDistance)
	}
	if cfg.CallCorrection.ResolverRecentPlus1AllowTruncation {
		t.Fatalf("expected resolver recent plus1 truncation-family allowance disabled by override")
	}
	if !cfg.CallCorrection.BayesBonus.Enabled {
		t.Fatalf("expected bayes bonus enabled by override")
	}
	if cfg.CallCorrection.BayesBonus.WeightDistance1Milli != 330 || cfg.CallCorrection.BayesBonus.WeightDistance2Milli != 180 {
		t.Fatalf("expected bayes distance weights 330/180, got %d/%d",
			cfg.CallCorrection.BayesBonus.WeightDistance1Milli,
			cfg.CallCorrection.BayesBonus.WeightDistance2Milli)
	}
	if cfg.CallCorrection.BayesBonus.WeightedSmoothingMilli != 1200 || cfg.CallCorrection.BayesBonus.RecentSmoothing != 3 {
		t.Fatalf("expected bayes smoothing 1200/3, got %d/%d",
			cfg.CallCorrection.BayesBonus.WeightedSmoothingMilli,
			cfg.CallCorrection.BayesBonus.RecentSmoothing)
	}
	if cfg.CallCorrection.BayesBonus.ObsLogCapMilli != 400 ||
		cfg.CallCorrection.BayesBonus.PriorLogMinMilli != -250 ||
		cfg.CallCorrection.BayesBonus.PriorLogMaxMilli != 700 {
		t.Fatalf("expected bayes log caps 400/-250/700, got %d/%d/%d",
			cfg.CallCorrection.BayesBonus.ObsLogCapMilli,
			cfg.CallCorrection.BayesBonus.PriorLogMinMilli,
			cfg.CallCorrection.BayesBonus.PriorLogMaxMilli)
	}
	if cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli != 500 ||
		cfg.CallCorrection.BayesBonus.ReportThresholdDistance2Milli != 700 {
		t.Fatalf("expected bayes report thresholds 500/700, got %d/%d",
			cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli,
			cfg.CallCorrection.BayesBonus.ReportThresholdDistance2Milli)
	}
	if cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance1Milli != 750 ||
		cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance2Milli != 980 {
		t.Fatalf("expected bayes advantage thresholds 750/980, got %d/%d",
			cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance1Milli,
			cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance2Milli)
	}
	if cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli != 250 ||
		cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli != 350 {
		t.Fatalf("expected bayes weighted deltas 250/350, got %d/%d",
			cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli,
			cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli)
	}
	if cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1 != 4 ||
		cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2 != 6 {
		t.Fatalf("expected bayes extra confidence 4/6, got %d/%d",
			cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1,
			cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2)
	}
	if cfg.CallCorrection.BayesBonus.RequireCandidateValidated {
		t.Fatalf("expected bayes require_candidate_validated disabled by override")
	}
	if cfg.CallCorrection.BayesBonus.RequireSubjectUnvalidatedDistance2 {
		t.Fatalf("expected bayes require_subject_unvalidated_distance2 disabled by override")
	}
	if !cfg.CallCorrection.TemporalDecoder.Enabled {
		t.Fatalf("expected temporal decoder enabled by override")
	}
	if cfg.CallCorrection.TemporalDecoder.Scope != "all_correction_candidates" {
		t.Fatalf("expected temporal scope all_correction_candidates, got %q", cfg.CallCorrection.TemporalDecoder.Scope)
	}
	if cfg.CallCorrection.TemporalDecoder.LagSeconds != 3 {
		t.Fatalf("expected temporal lag 3, got %d", cfg.CallCorrection.TemporalDecoder.LagSeconds)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxWaitSeconds != 7 {
		t.Fatalf("expected temporal max wait 7, got %d", cfg.CallCorrection.TemporalDecoder.MaxWaitSeconds)
	}
	if cfg.CallCorrection.TemporalDecoder.BeamSize != 6 {
		t.Fatalf("expected temporal beam size 6, got %d", cfg.CallCorrection.TemporalDecoder.BeamSize)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxObsCandidates != 5 {
		t.Fatalf("expected temporal max obs 5, got %d", cfg.CallCorrection.TemporalDecoder.MaxObsCandidates)
	}
	if cfg.CallCorrection.TemporalDecoder.StayBonus != 200 {
		t.Fatalf("expected temporal stay bonus 200, got %d", cfg.CallCorrection.TemporalDecoder.StayBonus)
	}
	if cfg.CallCorrection.TemporalDecoder.SwitchPenalty != 180 {
		t.Fatalf("expected temporal switch penalty 180, got %d", cfg.CallCorrection.TemporalDecoder.SwitchPenalty)
	}
	if cfg.CallCorrection.TemporalDecoder.FamilySwitchPenalty != 70 {
		t.Fatalf("expected temporal family penalty 70, got %d", cfg.CallCorrection.TemporalDecoder.FamilySwitchPenalty)
	}
	if cfg.CallCorrection.TemporalDecoder.Edit1SwitchPenalty != 90 {
		t.Fatalf("expected temporal edit1 penalty 90, got %d", cfg.CallCorrection.TemporalDecoder.Edit1SwitchPenalty)
	}
	if cfg.CallCorrection.TemporalDecoder.MinScore != 100 {
		t.Fatalf("expected temporal min score 100, got %d", cfg.CallCorrection.TemporalDecoder.MinScore)
	}
	if cfg.CallCorrection.TemporalDecoder.MinMarginScore != 25 {
		t.Fatalf("expected temporal min margin 25, got %d", cfg.CallCorrection.TemporalDecoder.MinMarginScore)
	}
	if cfg.CallCorrection.TemporalDecoder.OverflowAction != "abstain" {
		t.Fatalf("expected temporal overflow action abstain, got %q", cfg.CallCorrection.TemporalDecoder.OverflowAction)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxPending != 9000 {
		t.Fatalf("expected temporal max pending 9000, got %d", cfg.CallCorrection.TemporalDecoder.MaxPending)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxActiveKeys != 7000 {
		t.Fatalf("expected temporal max active keys 7000, got %d", cfg.CallCorrection.TemporalDecoder.MaxActiveKeys)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxEventsPerKey != 48 {
		t.Fatalf("expected temporal max events per key 48, got %d", cfg.CallCorrection.TemporalDecoder.MaxEventsPerKey)
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.MaxLengthDelta != 2 {
		t.Fatalf("expected truncation max length delta 2, got %d", cfg.CallCorrection.FamilyPolicy.Truncation.MaxLengthDelta)
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.MinShorterLength != 4 {
		t.Fatalf("expected truncation min shorter length 4, got %d", cfg.CallCorrection.FamilyPolicy.Truncation.MinShorterLength)
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.AllowPrefixMatch {
		t.Fatalf("expected truncation prefix matching disabled")
	}
	if !cfg.CallCorrection.FamilyPolicy.Truncation.AllowSuffixMatch {
		t.Fatalf("expected truncation suffix matching enabled")
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.MinAdvantage != 1 {
		t.Fatalf("expected truncation relax min advantage 1, got %d", cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.MinAdvantage)
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.RequireCandidateValidated {
		t.Fatalf("expected truncation relax candidate validation requirement disabled")
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.RelaxAdvantage.RequireSubjectUnvalidated {
		t.Fatalf("expected truncation relax subject-unvalidated requirement disabled")
	}
	if !cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Enabled {
		t.Fatalf("expected truncation length bonus enabled")
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Max != 2 {
		t.Fatalf("expected truncation length bonus max 2, got %d", cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.Max)
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.RequireCandidateValidated {
		t.Fatalf("expected truncation length bonus candidate validation requirement disabled")
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.LengthBonus.RequireSubjectUnvalidated {
		t.Fatalf("expected truncation length bonus subject-unvalidated requirement disabled")
	}
	if !cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.Enabled {
		t.Fatalf("expected truncation delta2 rails enabled")
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.ExtraConfidencePercent != 15 {
		t.Fatalf("expected truncation delta2 extra confidence 15, got %d", cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.ExtraConfidencePercent)
	}
	if cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.RequireCandidateValidated {
		t.Fatalf("expected truncation delta2 candidate validation requirement disabled")
	}
	if !cfg.CallCorrection.FamilyPolicy.Truncation.Delta2Rails.RequireSubjectUnvalidated {
		t.Fatalf("expected truncation delta2 subject-unvalidated requirement enabled")
	}
	if cfg.CallCorrection.FamilyPolicy.TelnetSuppression.Enabled {
		t.Fatalf("expected telnet family suppression disabled")
	}
	if !cfg.CallCorrection.FamilyPolicy.TelnetSuppression.EditNeighborEnabled {
		t.Fatalf("expected telnet edit-neighbor suppression enabled")
	}
	if cfg.CallCorrection.FamilyPolicy.TelnetSuppression.WindowSeconds != 120 {
		t.Fatalf("expected telnet family suppression window 120, got %d", cfg.CallCorrection.FamilyPolicy.TelnetSuppression.WindowSeconds)
	}
	if cfg.CallCorrection.FamilyPolicy.TelnetSuppression.MaxEntries != 1024 {
		t.Fatalf("expected telnet family suppression max entries 1024, got %d", cfg.CallCorrection.FamilyPolicy.TelnetSuppression.MaxEntries)
	}
	if cfg.CallCorrection.FamilyPolicy.TelnetSuppression.FrequencyToleranceFallbackHz != 750 {
		t.Fatalf("expected telnet family suppression fallback tolerance 750, got %.1f", cfg.CallCorrection.FamilyPolicy.TelnetSuppression.FrequencyToleranceFallbackHz)
	}
}

func TestLoadCallCorrectionStabilizerDelayKnobSanitization(t *testing.T) {
	dir := t.TempDir()
	pipeline := `call_correction:
  enabled: true
  stabilizer_enabled: true
  stabilizer_p_delay_confidence_percent: 150
  stabilizer_p_delay_max_checks: -2
  stabilizer_ambiguous_max_checks: -4
  stabilizer_edit_neighbor_max_checks: -3
  stabilizer_edit_neighbor_min_spotters: -2
  resolver_neighborhood_bucket_radius: -1
  resolver_neighborhood_max_distance: -2
  resolver_recent_plus1_min_unique_winner: -3
  resolver_recent_plus1_max_distance: -2
  bayes_bonus:
    enabled: true
    weight_distance1_milli: -50
    weight_distance2_milli: 2500
    weighted_smoothing_milli: -10
    recent_smoothing: -2
    obs_log_cap_milli: -4
    prior_log_min_milli: 100
    prior_log_max_milli: -100
    report_threshold_distance1_milli: -5
    report_threshold_distance2_milli: -6
    advantage_threshold_distance1_milli: -7
    advantage_threshold_distance2_milli: -8
    advantage_min_weighted_delta_distance1_milli: -9
    advantage_min_weighted_delta_distance2_milli: -10
    advantage_extra_confidence_distance1: -11
    advantage_extra_confidence_distance2: -12
  temporal_decoder:
    scope: "uncertain_only"
    lag_seconds: -2
    max_wait_seconds: 0
    beam_size: -3
    max_obs_candidates: -4
    stay_bonus: -1
    switch_penalty: -6
    family_switch_penalty: 999
    edit1_switch_penalty: 999
    min_score: -7
    min_margin_score: -8
    max_pending: -9
    max_active_keys: -10
    max_events_per_key: 999
`
	if err := os.WriteFile(filepath.Join(dir, "pipeline.yaml"), []byte(pipeline), 0o644); err != nil {
		t.Fatalf("write pipeline.yaml: %v", err)
	}

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.CallCorrection.StabilizerPDelayConfidencePercent != 100 {
		t.Fatalf("expected stabilizer P-delay confidence percent clamped to 100, got %d", cfg.CallCorrection.StabilizerPDelayConfidencePercent)
	}
	if cfg.CallCorrection.StabilizerPDelayMaxChecks != 0 {
		t.Fatalf("expected stabilizer P-delay max checks clamped to 0, got %d", cfg.CallCorrection.StabilizerPDelayMaxChecks)
	}
	if cfg.CallCorrection.StabilizerAmbiguousMaxChecks != 0 {
		t.Fatalf("expected stabilizer ambiguous max checks clamped to 0, got %d", cfg.CallCorrection.StabilizerAmbiguousMaxChecks)
	}
	if cfg.CallCorrection.StabilizerEditNeighborMaxChecks != 0 {
		t.Fatalf("expected stabilizer edit-neighbor max checks clamped to 0, got %d", cfg.CallCorrection.StabilizerEditNeighborMaxChecks)
	}
	if cfg.CallCorrection.StabilizerEditNeighborMinSpotters != 0 {
		t.Fatalf("expected stabilizer edit-neighbor min spotters clamped to 0, got %d", cfg.CallCorrection.StabilizerEditNeighborMinSpotters)
	}
	if cfg.CallCorrection.ResolverNeighborhoodBucketRadius != 0 {
		t.Fatalf("expected resolver neighborhood radius clamped to 0, got %d", cfg.CallCorrection.ResolverNeighborhoodBucketRadius)
	}
	if cfg.CallCorrection.ResolverNeighborhoodMaxDistance != 1 {
		t.Fatalf("expected resolver neighborhood max distance defaulted to 1, got %d", cfg.CallCorrection.ResolverNeighborhoodMaxDistance)
	}
	if cfg.CallCorrection.ResolverRecentPlus1MinUniqueWinner != 3 {
		t.Fatalf("expected resolver recent plus1 min unique winner defaulted to 3, got %d", cfg.CallCorrection.ResolverRecentPlus1MinUniqueWinner)
	}
	if cfg.CallCorrection.ResolverRecentPlus1MaxDistance != 1 {
		t.Fatalf("expected resolver recent plus1 max distance defaulted to 1, got %d", cfg.CallCorrection.ResolverRecentPlus1MaxDistance)
	}
	if cfg.CallCorrection.BayesBonus.WeightDistance1Milli != 350 {
		t.Fatalf("expected bayes distance1 weight defaulted to 350, got %d", cfg.CallCorrection.BayesBonus.WeightDistance1Milli)
	}
	if cfg.CallCorrection.BayesBonus.WeightDistance2Milli != 1000 {
		t.Fatalf("expected bayes distance2 weight clamped to 1000, got %d", cfg.CallCorrection.BayesBonus.WeightDistance2Milli)
	}
	if cfg.CallCorrection.BayesBonus.WeightedSmoothingMilli != 1000 || cfg.CallCorrection.BayesBonus.RecentSmoothing != 2 {
		t.Fatalf("expected bayes smoothing defaults 1000/2, got %d/%d",
			cfg.CallCorrection.BayesBonus.WeightedSmoothingMilli,
			cfg.CallCorrection.BayesBonus.RecentSmoothing)
	}
	if cfg.CallCorrection.BayesBonus.ObsLogCapMilli != 350 {
		t.Fatalf("expected bayes obs log cap defaulted to 350, got %d", cfg.CallCorrection.BayesBonus.ObsLogCapMilli)
	}
	if cfg.CallCorrection.BayesBonus.PriorLogMinMilli != -10 || cfg.CallCorrection.BayesBonus.PriorLogMaxMilli != 10 {
		t.Fatalf("expected bayes prior bounds clamped to -10/10, got %d/%d",
			cfg.CallCorrection.BayesBonus.PriorLogMinMilli,
			cfg.CallCorrection.BayesBonus.PriorLogMaxMilli)
	}
	if cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli != 450 || cfg.CallCorrection.BayesBonus.ReportThresholdDistance2Milli != 650 {
		t.Fatalf("expected bayes report thresholds defaults 450/650, got %d/%d",
			cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli,
			cfg.CallCorrection.BayesBonus.ReportThresholdDistance2Milli)
	}
	if cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance1Milli != 700 || cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance2Milli != 950 {
		t.Fatalf("expected bayes advantage thresholds defaults 700/950, got %d/%d",
			cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance1Milli,
			cfg.CallCorrection.BayesBonus.AdvantageThresholdDistance2Milli)
	}
	if cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli != 200 || cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli != 300 {
		t.Fatalf("expected bayes weighted-delta defaults 200/300, got %d/%d",
			cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli,
			cfg.CallCorrection.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli)
	}
	if cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1 != 3 || cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2 != 5 {
		t.Fatalf("expected bayes extra-confidence defaults 3/5, got %d/%d",
			cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance1,
			cfg.CallCorrection.BayesBonus.AdvantageExtraConfidenceDistance2)
	}
	if !cfg.CallCorrection.BayesBonus.RequireCandidateValidated || !cfg.CallCorrection.BayesBonus.RequireSubjectUnvalidatedDistance2 {
		t.Fatalf("expected bayes conservative validation flags to default to true")
	}
	if cfg.CallCorrection.TemporalDecoder.LagSeconds != 2 {
		t.Fatalf("expected temporal lag defaulted to 2, got %d", cfg.CallCorrection.TemporalDecoder.LagSeconds)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxWaitSeconds != 6 {
		t.Fatalf("expected temporal max wait defaulted to 6, got %d", cfg.CallCorrection.TemporalDecoder.MaxWaitSeconds)
	}
	if cfg.CallCorrection.TemporalDecoder.BeamSize != 8 {
		t.Fatalf("expected temporal beam size defaulted to 8, got %d", cfg.CallCorrection.TemporalDecoder.BeamSize)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxObsCandidates != 8 {
		t.Fatalf("expected temporal max obs defaulted to 8, got %d", cfg.CallCorrection.TemporalDecoder.MaxObsCandidates)
	}
	if cfg.CallCorrection.TemporalDecoder.StayBonus != 0 {
		t.Fatalf("expected temporal stay bonus clamped to 0, got %d", cfg.CallCorrection.TemporalDecoder.StayBonus)
	}
	if cfg.CallCorrection.TemporalDecoder.SwitchPenalty != 160 {
		t.Fatalf("expected temporal switch penalty defaulted to 160, got %d", cfg.CallCorrection.TemporalDecoder.SwitchPenalty)
	}
	if cfg.CallCorrection.TemporalDecoder.FamilySwitchPenalty != 160 {
		t.Fatalf("expected temporal family penalty clamped to switch penalty 160, got %d", cfg.CallCorrection.TemporalDecoder.FamilySwitchPenalty)
	}
	if cfg.CallCorrection.TemporalDecoder.Edit1SwitchPenalty != 160 {
		t.Fatalf("expected temporal edit1 penalty clamped to switch penalty 160, got %d", cfg.CallCorrection.TemporalDecoder.Edit1SwitchPenalty)
	}
	if cfg.CallCorrection.TemporalDecoder.MinScore != 0 {
		t.Fatalf("expected temporal min score clamped to 0, got %d", cfg.CallCorrection.TemporalDecoder.MinScore)
	}
	if cfg.CallCorrection.TemporalDecoder.MinMarginScore != 80 {
		t.Fatalf("expected temporal min margin defaulted to 80, got %d", cfg.CallCorrection.TemporalDecoder.MinMarginScore)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxPending != 25000 {
		t.Fatalf("expected temporal max pending defaulted to 25000, got %d", cfg.CallCorrection.TemporalDecoder.MaxPending)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxActiveKeys != 6000 {
		t.Fatalf("expected temporal max active keys defaulted to 6000, got %d", cfg.CallCorrection.TemporalDecoder.MaxActiveKeys)
	}
	if cfg.CallCorrection.TemporalDecoder.MaxEventsPerKey != 256 {
		t.Fatalf("expected temporal max events clamped to 256, got %d", cfg.CallCorrection.TemporalDecoder.MaxEventsPerKey)
	}
}

func TestLoadRejectsInvalidTemporalDecoderScope(t *testing.T) {
	dir := t.TempDir()
	pipeline := `call_correction:
  enabled: true
  temporal_decoder:
    scope: "invalid"
`
	if err := os.WriteFile(filepath.Join(dir, "pipeline.yaml"), []byte(pipeline), 0o644); err != nil {
		t.Fatalf("write pipeline.yaml: %v", err)
	}

	_, err := Load(dir)
	if err == nil {
		t.Fatalf("expected Load() error for invalid temporal decoder scope")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "temporal_decoder.scope") {
		t.Fatalf("expected temporal scope error, got %v", err)
	}
}

func TestLoadRejectsInvalidTemporalDecoderOverflowAction(t *testing.T) {
	dir := t.TempDir()
	pipeline := `call_correction:
  enabled: true
  temporal_decoder:
    overflow_action: "hold"
`
	if err := os.WriteFile(filepath.Join(dir, "pipeline.yaml"), []byte(pipeline), 0o644); err != nil {
		t.Fatalf("write pipeline.yaml: %v", err)
	}

	_, err := Load(dir)
	if err == nil {
		t.Fatalf("expected Load() error for invalid temporal decoder overflow action")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "temporal_decoder.overflow_action") {
		t.Fatalf("expected temporal overflow action error, got %v", err)
	}
}
