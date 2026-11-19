// These fixtures prove declared study scalar values survive the ordinary loader
// and reach the replay consumer unchanged. The reached-gate examples intentionally
// control history and disable recent+1; they are not S6 opportunity qualification.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"

	"gopkg.in/yaml.v3"
)

func studyTestScalarCandidate(t *testing.T, value config.CallCorrectionBayesBonusConfig) studyCandidate {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	delete(fields, "EnabledDistancesByMode")
	data, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return studyCandidate{ID: "scalar", Parameters: data, ModeDistances: map[string][]int{"CW": {1, 2}, "RTTY": {1, 2}}}
}

func studyTestLoadBayes(t *testing.T, value config.CallCorrectionBayesBonusConfig) *config.Config {
	t.Helper()
	directory, _ := replayGeographyConfig(t, "custom_disabled")
	path := filepath.Join(directory, "pipeline.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	bayesData, err := yaml.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var bayes map[string]any
	if err := yaml.Unmarshal(bayesData, &bayes); err != nil {
		t.Fatal(err)
	}
	delete(bayes, "enabled_distances_by_mode") // Test scalars independently of mask schema presence.
	raw["call_correction"].(map[string]any)["bayes_bonus"] = bayes
	data, err = yaml.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(directory)
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func TestCCV2StudyScalarLoaderParity(t *testing.T) {
	legacy := studyTestBayesBaseline()
	minimum, maximum := legacy, legacy
	minimum.WeightDistance1Milli, minimum.WeightDistance2Milli = 0, 0
	minimum.WeightedSmoothingMilli, minimum.RecentSmoothing, minimum.ObsLogCapMilli = 1, 1, 1
	minimum.PriorLogMinMilli, minimum.PriorLogMaxMilli = 0, 10
	minimum.ReportThresholdDistance1Milli, minimum.ReportThresholdDistance2Milli = 1, 1
	minimum.AdvantageThresholdDistance1Milli, minimum.AdvantageThresholdDistance2Milli = 1, 1
	minimum.AdvantageMinWeightedDeltaDistance1Milli, minimum.AdvantageMinWeightedDeltaDistance2Milli = 0, 0
	minimum.AdvantageExtraConfidenceDistance1, minimum.AdvantageExtraConfidenceDistance2 = 0, 0
	minimum.RequireCandidateValidated, minimum.RequireSubjectUnvalidatedDistance2 = false, false
	maximum.WeightDistance1Milli, maximum.WeightDistance2Milli = 1000, 1000
	maximum.WeightedSmoothingMilli, maximum.RecentSmoothing, maximum.ObsLogCapMilli = 100000, 100, 2000
	maximum.PriorLogMinMilli, maximum.PriorLogMaxMilli = -2000, 3000
	maximum.ReportThresholdDistance1Milli, maximum.ReportThresholdDistance2Milli = 4000, 4000
	maximum.AdvantageThresholdDistance1Milli, maximum.AdvantageThresholdDistance2Milli = 4000, 4000
	maximum.AdvantageMinWeightedDeltaDistance1Milli, maximum.AdvantageMinWeightedDeltaDistance2Milli = 5000, 5000
	maximum.AdvantageExtraConfidenceDistance1, maximum.AdvantageExtraConfidenceDistance2 = 50, 50
	for _, tc := range []struct {
		name  string
		value config.CallCorrectionBayesBonusConfig
	}{{"legacy_450_650_700_950", legacy}, {"minimum_and_neutral_zeros", minimum}, {"maximum", maximum}} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := studyTestScalarCandidate(t, tc.value)
			if err := studyValidateCandidate(candidate); err != nil {
				t.Fatal(err)
			}
			loaded := studyTestLoadBayes(t, tc.value)
			actual := studyTestScalarCandidate(t, loaded.CallCorrection.BayesBonus)
			if !studyEqualJSON(actual.Parameters, candidate.Parameters) {
				t.Fatalf("ordinary loader rewrote accepted study scalars: declared=%s loaded=%s", candidate.Parameters, actual.Parameters)
			}
			if err := studyApplyCandidate(loaded, studyArm{Name: "repaired_on"}, candidate); err != nil {
				t.Fatal(err)
			}
			policy := correctionflow.BuildCorrectionSettings(correctionflow.BuildSettingsInput{Cfg: loaded.CallCorrection}).BayesBonusPolicy
			if !policy.Configured || policy.WeightDistance1Milli != tc.value.WeightDistance1Milli ||
				policy.WeightedSmoothingMilli != tc.value.WeightedSmoothingMilli || policy.PriorLogMinMilli != tc.value.PriorLogMinMilli ||
				policy.ReportThresholdDistance2Milli != tc.value.ReportThresholdDistance2Milli ||
				policy.AdvantageThresholdDistance1Milli != tc.value.AdvantageThresholdDistance1Milli ||
				policy.AdvantageThresholdDistance2Milli != tc.value.AdvantageThresholdDistance2Milli ||
				policy.AdvantageMinWeightedDeltaDistance1Milli != tc.value.AdvantageMinWeightedDeltaDistance1Milli ||
				policy.AdvantageExtraConfidenceDistance1 != tc.value.AdvantageExtraConfidenceDistance1 {
				t.Fatalf("runtime mapper changed selected scalar/neutral semantics: %+v", policy)
			}
		})
	}
}

func TestCCV2StudyScalarRefusals(t *testing.T) {
	tests := []struct {
		field   string
		invalid []int
	}{
		{"WeightDistance1Milli", []int{-1, 1001}}, {"WeightDistance2Milli", []int{-1, 1001}},
		{"WeightedSmoothingMilli", []int{0, -1, 100001}}, {"RecentSmoothing", []int{0, -1, 101}},
		{"ObsLogCapMilli", []int{0, -1, 2001}}, {"PriorLogMinMilli", []int{1, -2001}},
		{"PriorLogMaxMilli", []int{0, 9, 3001}},
		{"ReportThresholdDistance1Milli", []int{0, -1, 4001}}, {"ReportThresholdDistance2Milli", []int{0, -1, 4001, 449}},
		{"AdvantageThresholdDistance1Milli", []int{0, -1, 4001}}, {"AdvantageThresholdDistance2Milli", []int{0, -1, 4001, 699}},
		{"AdvantageMinWeightedDeltaDistance1Milli", []int{-1, 5001}}, {"AdvantageMinWeightedDeltaDistance2Milli", []int{-1, 5001, 199}},
		{"AdvantageExtraConfidenceDistance1", []int{-1, 51}}, {"AdvantageExtraConfidenceDistance2", []int{-1, 51, 2}},
	}
	for _, tc := range tests {
		for _, invalid := range tc.invalid {
			t.Run(fmt.Sprintf("%s_%d", tc.field, invalid), func(t *testing.T) {
				value := studyTestBayesBaseline()
				reflect.ValueOf(&value).Elem().FieldByName(tc.field).SetInt(int64(invalid))
				candidate := studyTestScalarCandidate(t, value)
				if err := studyValidateCandidate(candidate); err == nil {
					t.Fatalf("normalizing scalar %s=%d passed frozen plan validation", tc.field, invalid)
				}
				for _, arm := range []string{"repaired_off", "repaired_on"} {
					cfg := &config.Config{CallCorrection: config.CallCorrectionConfig{BayesBonus: studyTestBayesBaseline()}}
					before := hashAnyStable(cfg)
					if err := studyApplyCandidate(cfg, studyArm{Name: arm}, candidate); err == nil || before != hashAnyStable(cfg) {
						t.Fatalf("invalid scalar was applied or partially changed %s: err=%v", arm, err)
					}
				}
			})
		}
	}
}

func TestCCV2StudyFixedKnobRefusals(t *testing.T) {
	for _, field := range []string{"WeightDistance1Milli", "WeightDistance2Milli", "WeightedSmoothingMilli", "RecentSmoothing",
		"ObsLogCapMilli", "PriorLogMinMilli", "PriorLogMaxMilli", "AdvantageExtraConfidenceDistance1", "AdvantageExtraConfidenceDistance2",
		"RequireCandidateValidated", "RequireSubjectUnvalidatedDistance2"} {
		t.Run(field, func(t *testing.T) {
			value := studyTestBayesBaseline()
			setting := reflect.ValueOf(&value).Elem().FieldByName(field)
			if setting.Kind() == reflect.Bool {
				setting.SetBool(!setting.Bool())
			} else {
				setting.SetInt(setting.Int() + 1)
			}
			candidate := studyTestScalarCandidate(t, value)
			if err := studyValidateCandidate(candidate); err != nil {
				t.Fatalf("fixed-drift fixture must remain in loader-stable domain: %v", err)
			}
			cfg := studyTestLoadBayes(t, studyTestBayesBaseline())
			before := hashAnyStable(cfg)
			if err := studyApplyCandidate(cfg, studyArm{Name: "repaired_on"}, candidate); err == nil || before != hashAnyStable(cfg) {
				t.Fatalf("fixed knob %s changed or partially applied: %v", field, err)
			}
		})
	}
}

func TestCCV2StudyCandidateReachesReplayBayesGate(t *testing.T) {
	for _, neutral := range []bool{false, true} {
		// ln(3) observation is capped at350. History4:1 gives capped prior600;
		// fixed weight350 adds210 =>560. Explicit zero weight adds0 =>350.
		score := 560
		if neutral {
			score = 350
		}
		for _, threshold := range []int{score - 1, score + 1} {
			t.Run(fmt.Sprintf("neutral_%t_threshold_%d", neutral, threshold), func(t *testing.T) {
				now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
				cfg := &config.Config{CallCorrection: replayTemporalFreshConfig()}
				cfg.CallCorrection.BayesBonus = studyTestBayesBaseline()
				if neutral {
					cfg.CallCorrection.BayesBonus.WeightDistance1Milli, cfg.CallCorrection.BayesBonus.WeightDistance2Milli = 0, 0
				}
				cfg.CallCorrection.DistanceModelCW, cfg.CallCorrection.DistanceModelRTTY = "plain", "plain"
				cfg.CallCorrection.MinConsensusReports, cfg.CallCorrection.MinAdvantage, cfg.CallCorrection.MinConfidencePercent = 3, 1, 45
				cfg.CallCorrection.RecentBandRecordMinUniqueSpotters, cfg.CallCorrection.ResolverRecentPlus1MinUniqueWinner = 2, 2
				cfg.CallCorrection.ResolverRecentPlus1Enabled = false // Intentional scalar-parity control, not S6 opportunity evidence.
				selected := cfg.CallCorrection.BayesBonus
				selected.ReportThresholdDistance1Milli = threshold
				candidate := studyTestScalarCandidate(t, selected)
				if err := studyApplyCandidate(cfg, studyArm{Name: "repaired_on"}, candidate); err != nil {
					t.Fatal(err)
				}
				if _, err := studyValidateRuntimeConfig(cfg, studyArm{Name: "repaired_on"}, candidate, studyTestPlan(t)); err != nil {
					t.Fatal(err)
				}
				resolver := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 8, MaxActiveKeys: 4, MaxCandidatesPerKey: 4,
					MaxReportersPerCand: 8, InactiveTTL: time.Minute, EvalMinInterval: time.Nanosecond, HysteresisWindows: 1})
				t.Cleanup(resolver.Stop)
				driver, err := spot.NewSignalResolverDriver(resolver)
				if err != nil {
					t.Fatal(err)
				}
				key := spot.NewResolverSignalKey(14020, "20m", "CW", cfg.CallCorrection.FrequencyToleranceHz)
				for i, receiver := range []string{"N0BBB", "N0CCC"} {
					if !resolver.Enqueue(spot.ResolverEvidence{Key: key, DXCall: "K1ABC", Spotter: receiver, FrequencyKHz: 14020,
						ObservedAt: now.Add(time.Duration(i-2) * time.Second), RecencyWindow: time.Minute}) {
						t.Fatal("parity fixture receiver admission failed")
					}
				}
				driver.Step(now)
				recent := spot.NewRecentBandStore(time.Hour)
				for _, receiver := range []string{"N0BBB", "N0CCC", "N0DDD", "N0FFF"} {
					recent.RecordAt("K1ABC", "20m", "CW", receiver, now.Add(-time.Minute), now)
				}
				recent.RecordAt("K1ABD", "20m", "CW", "N0GGG", now.Add(-time.Minute), now)
				entry := spot.NewSpotNormalized("K1ABD", "N0HHH", 14020, "CW")
				outcome := maybeApplyResolverCorrectionReplay(entry, resolver, spot.ResolverEvidence{Key: key}, true,
					cfg.CallCorrection, nil, nil, nil, recent, now)
				if !outcome.GateEvaluated || !outcome.Gate.BayesReportBonusConsidered || outcome.Gate.BayesScoreMilli != score ||
					outcome.Gate.BayesWinnerRecent != 4 || outcome.Gate.BayesSubjectRecent != 1 ||
					outcome.Applied != (threshold < score) || outcome.Gate.BayesReportBonusApplied != (threshold < score) {
					t.Fatalf("declared threshold/neutral scalar changed before reached replay gate: threshold=%d score=%d outcome=%+v", threshold, score, outcome)
				}
			})
		}
	}
}
