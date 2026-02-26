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
	if cfg.CallCorrection.ResolverMode != CallCorrectionResolverModeShadow {
		t.Fatalf("expected resolver mode default %q, got %q", CallCorrectionResolverModeShadow, cfg.CallCorrection.ResolverMode)
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
	if cfg.CallCorrection.SlashPrecedenceMinReports != 2 {
		t.Fatalf("expected slash precedence min reports default 2, got %d", cfg.CallCorrection.SlashPrecedenceMinReports)
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

func TestLoadCallCorrectionResolverModePrimary(t *testing.T) {
	dir := t.TempDir()
	pipeline := `call_correction:
  enabled: true
  resolver_mode: "primary"
`
	if err := os.WriteFile(filepath.Join(dir, "pipeline.yaml"), []byte(pipeline), 0o644); err != nil {
		t.Fatalf("write pipeline.yaml: %v", err)
	}

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.CallCorrection.ResolverMode != CallCorrectionResolverModePrimary {
		t.Fatalf("expected resolver mode %q, got %q", CallCorrectionResolverModePrimary, cfg.CallCorrection.ResolverMode)
	}
}

func TestLoadRejectsInvalidCallCorrectionResolverMode(t *testing.T) {
	dir := t.TempDir()
	pipeline := `call_correction:
  enabled: true
  resolver_mode: "hybrid"
`
	if err := os.WriteFile(filepath.Join(dir, "pipeline.yaml"), []byte(pipeline), 0o644); err != nil {
		t.Fatalf("write pipeline.yaml: %v", err)
	}

	_, err := Load(dir)
	if err == nil {
		t.Fatalf("expected Load() error for invalid resolver_mode")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "resolver_mode") {
		t.Fatalf("expected resolver mode error, got %v", err)
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
	if cfg.CallCorrection.SlashPrecedenceMinReports != 3 {
		t.Fatalf("expected legacy slash precedence mirror 3, got %d", cfg.CallCorrection.SlashPrecedenceMinReports)
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
}
