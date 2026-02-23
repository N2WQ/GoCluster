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
	if cfg.CallCorrection.StabilizerTimeoutAction != "release" {
		t.Fatalf("expected stabilizer timeout action default release, got %q", cfg.CallCorrection.StabilizerTimeoutAction)
	}
	if cfg.CallCorrection.StabilizerMaxPending != 20000 {
		t.Fatalf("expected stabilizer max pending default 20000, got %d", cfg.CallCorrection.StabilizerMaxPending)
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
