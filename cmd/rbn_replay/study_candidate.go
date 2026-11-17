// File role: Apply selected candidates only at the offline study boundary.
// A complete vector and complete mask replace the study-owned repaired config;
// shared live/replay consumers then enforce their existing gates and capacity.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"dxcluster/config"
	"dxcluster/spot"
)

// Only study-owned repaired configs receive the selected complete vector. The
// frozen source files and shipped arm remain unchanged. The repaired executable
// applies the complete typed mask; old frozen shipped binaries keep their own
// pre-S6 instrumentation and report schema.
func studyApplyCandidate(cfg *config.Config, arm studyArm, candidate studyCandidate) error {
	if arm.Name == "shipped" {
		return nil
	}
	if cfg == nil || arm.Name != "repaired_off" && arm.Name != "repaired_on" {
		return errors.New("cannot apply candidate outside a repaired study arm")
	}
	if err := studyValidateCandidate(candidate); err != nil {
		return err
	}
	var selected config.CallCorrectionBayesBonusConfig
	if err := studyDecodeJSON(candidate.Parameters, &selected); err != nil {
		return err
	}
	if err := studyCheckFixedBayes(selected, cfg.CallCorrection.BayesBonus); err != nil {
		return err
	}
	selected.Enabled = arm.Name == "repaired_on"
	selected.EnabledDistancesByMode = make(map[string][]int, len(candidate.ModeDistances))
	for mode, distances := range candidate.ModeDistances {
		selected.EnabledDistancesByMode[mode] = append([]int{}, distances...)
	}
	cfg.CallCorrection.BayesBonus = selected
	return nil
}

// These are study refusal bounds, not new runtime startup restrictions. They
// describe the fixed points of config.normalizeCallCorrectionBayesDefaultsConfig
// and spot.normalizeCorrectionBayesBonusPolicy for explicitly present values.
// Refuse rather than let a declared frozen value be clamped or re-defaulted.
func studyValidateBayesScalarDomain(value config.CallCorrectionBayesBonusConfig) error {
	for _, field := range []struct {
		name                    string
		value, minimum, maximum int
	}{
		{"WeightDistance1Milli", value.WeightDistance1Milli, 0, 1000},
		{"WeightDistance2Milli", value.WeightDistance2Milli, 0, 1000},
		{"WeightedSmoothingMilli", value.WeightedSmoothingMilli, 1, 100000},
		{"RecentSmoothing", value.RecentSmoothing, 1, 100},
		{"ObsLogCapMilli", value.ObsLogCapMilli, 1, 2000},
		{"PriorLogMinMilli", value.PriorLogMinMilli, -2000, 0},
		{"PriorLogMaxMilli", value.PriorLogMaxMilli, 10, 3000},
		{"ReportThresholdDistance1Milli", value.ReportThresholdDistance1Milli, 1, 4000},
		{"ReportThresholdDistance2Milli", value.ReportThresholdDistance2Milli, 1, 4000},
		{"AdvantageThresholdDistance1Milli", value.AdvantageThresholdDistance1Milli, 1, 4000},
		{"AdvantageThresholdDistance2Milli", value.AdvantageThresholdDistance2Milli, 1, 4000},
		{"AdvantageMinWeightedDeltaDistance1Milli", value.AdvantageMinWeightedDeltaDistance1Milli, 0, 5000},
		{"AdvantageMinWeightedDeltaDistance2Milli", value.AdvantageMinWeightedDeltaDistance2Milli, 0, 5000},
		{"AdvantageExtraConfidenceDistance1", value.AdvantageExtraConfidenceDistance1, 0, 50},
		{"AdvantageExtraConfidenceDistance2", value.AdvantageExtraConfidenceDistance2, 0, 50},
	} {
		if field.value < field.minimum || field.value > field.maximum {
			return fmt.Errorf("candidate %s=%d is outside loader-stable domain [%d,%d]", field.name, field.value, field.minimum, field.maximum)
		}
	}
	for _, pair := range []struct {
		name                 string
		distance1, distance2 int
	}{
		{"report threshold", value.ReportThresholdDistance1Milli, value.ReportThresholdDistance2Milli},
		{"advantage threshold", value.AdvantageThresholdDistance1Milli, value.AdvantageThresholdDistance2Milli},
		{"advantage weighted delta", value.AdvantageMinWeightedDeltaDistance1Milli, value.AdvantageMinWeightedDeltaDistance2Milli},
		{"extra confidence", value.AdvantageExtraConfidenceDistance1, value.AdvantageExtraConfidenceDistance2},
	} {
		if pair.distance2 < pair.distance1 {
			return fmt.Errorf("candidate distance-2 %s is below distance-1 and would be normalized", pair.name)
		}
	}
	return nil
}

// V3 permits six scalar tuning knobs. Weights, smoothing, caps, validation
// qualifiers, extra confidence and any other fields remain exactly the loaded
// baseline values. Master enablement and the separately frozen mask are controls.
func studyCheckFixedBayes(selected, baseline config.CallCorrectionBayesBonusConfig) error {
	var objects [2]map[string]json.RawMessage
	for i, value := range []*config.CallCorrectionBayesBonusConfig{&selected, &baseline} {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if err := studyDecodeJSON(data, &objects[i]); err != nil {
			return err
		}
		for _, allowed := range []string{"Enabled", "EnabledDistancesByMode",
			"ReportThresholdDistance1Milli", "ReportThresholdDistance2Milli",
			"AdvantageThresholdDistance1Milli", "AdvantageThresholdDistance2Milli",
			"AdvantageMinWeightedDeltaDistance1Milli", "AdvantageMinWeightedDeltaDistance2Milli"} {
			delete(objects[i], allowed)
		}
	}
	if !studyEqualJSON(objects[0], objects[1]) {
		return errors.New("candidate changes a fixed Bayes parameter from the loaded baseline")
	}
	return nil
}

func studyValidateRuntimeConfig(cfg *config.Config, arm studyArm, candidate studyCandidate, p studyPlan) (bool, error) {
	if cfg == nil || !cfg.CallCorrection.Enabled {
		return false, errors.New("study requires enabled call correction")
	}
	cc := cfg.CallCorrection
	if p.WarmupSeconds < max(studyEpisodeSeconds, cc.RecencySeconds, cc.RecencySecondsCW, cc.RecencySecondsRTTY) {
		return false, errors.New("study warmup does not cover effective local resolver recency/reference context")
	}
	checks := max(cc.StabilizerMaxChecks, cc.StabilizerPDelayMaxChecks, cc.StabilizerAmbiguousMaxChecks, cc.StabilizerEditNeighborMaxChecks)
	budget := 0
	if cc.StabilizerEnabled {
		budget += max(1, checks) * cc.StabilizerDelaySeconds
	}
	if cc.TemporalDecoder.Enabled {
		budget += cc.TemporalDecoder.MaxWaitSeconds
	}
	if budget > p.DecisionCutoffSeconds {
		return false, errors.New("study fixed decision cutoff does not cover configured temporal/stabilizer budgets")
	}
	var actual, expected map[string]json.RawMessage
	if err := studyDecodeJSON(candidate.Parameters, &expected); err != nil {
		return false, err
	}
	data, err := json.Marshal(cc.BayesBonus)
	if err != nil {
		return false, err
	}
	if err := studyDecodeJSON(data, &actual); err != nil {
		return false, err
	}
	const maskSchema = true // This executable has the typed mask field.
	delete(actual, "EnabledDistancesByMode")
	if arm.Name != "shipped" {
		actual["Enabled"] = json.RawMessage("true")
		if !studyEqualJSON(actual, expected) {
			return maskSchema, errors.New("study arm differs from complete frozen Bayes parameter vector")
		}
		if cc.BayesBonus.Enabled != (arm.Name == "repaired_on") {
			return maskSchema, errors.New("study repaired arm Bayes enabled state is incorrect")
		}
	}
	if arm.Name == "repaired_on" {
		if cc.BayesBonus.EnabledDistancesByMode == nil {
			return maskSchema, errors.New("study repaired arm requires its explicit frozen mode/distance map")
		}
		effective := make(map[string][]int, len(cc.BayesBonus.EnabledDistancesByMode))
		for mode := range cc.BayesBonus.EnabledDistancesByMode {
			for distance := 1; distance <= 2; distance++ {
				if config.BayesBonusCellEnabled(cc.BayesBonus.Enabled, cc.BayesBonus.EnabledDistancesByMode, mode, distance, cc.MaxEditDistance) {
					effective[mode] = append(effective[mode], distance)
				}
			}
		}
		if !reflect.DeepEqual(studySortedCells(effective), studySortedCells(candidate.ModeDistances)) {
			return maskSchema, errors.New("study executable/config does not apply the exact frozen mode/distance map")
		}
	}
	if err := cc.ValidateCustomSCPCapacity(); err != nil {
		return maskSchema, err
	}
	if !cc.CustomSCP.Enabled && (cc.RecentBandBonusEnabled || cc.StabilizerEnabled) {
		if err := cc.ValidateLegacyHistoryCapacity("study legacy recent-band", spot.RecentBandDefaultMaxSpottersPerCall,
			config.LegacyHistoryConsumers{Stabilizer: true, Resolver: true}); err != nil {
			return maskSchema, err
		}
	}
	return maskSchema, nil
}

func studyEqualJSON(a, b any) bool {
	x, errA := json.Marshal(a)
	y, errB := json.Marshal(b)
	var left, right any
	if errA != nil || errB != nil || json.Unmarshal(x, &left) != nil || json.Unmarshal(y, &right) != nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}
