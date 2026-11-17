package main

import (
	"reflect"
	"testing"

	"dxcluster/config"
)

func TestCCV2StudyTypedMaskApplication(t *testing.T) {
	plan := studyTestPlan(t)
	for _, mask := range []map[string][]int{{}, {"CW": {}}, {"CW": {1}}, {"RTTY": {2}}, {"CW": {1}, "RTTY": {2}}} {
		candidate := plan.Candidates[0]
		candidate.ModeDistances = mask
		cfg := &config.Config{CallCorrection: replayTemporalFreshConfig()}
		cfg.CallCorrection.BayesBonus = studyTestBayesBaseline()
		if err := studyApplyCandidate(cfg, plan.Arms[2], candidate); err != nil {
			t.Fatal(err)
		}
		if cfg.CallCorrection.BayesBonus.EnabledDistancesByMode == nil || !reflect.DeepEqual(cfg.CallCorrection.BayesBonus.EnabledDistancesByMode, mask) {
			t.Fatalf("study lost explicit typed mask: source=%#v effective=%#v", mask, cfg.CallCorrection.BayesBonus.EnabledDistancesByMode)
		}
		if schema, err := studyValidateRuntimeConfig(cfg, plan.Arms[2], candidate, plan); err != nil || !schema {
			t.Fatalf("typed study mask was not enforced: schema=%v err=%v", schema, err)
		}
		cfg.CallCorrection.BayesBonus.EnabledDistancesByMode = nil
		if _, err := studyValidateRuntimeConfig(cfg, plan.Arms[2], candidate, plan); err == nil {
			t.Fatal("legacy nil mask silently replaced the frozen explicit study mask")
		}
	}
	for _, mask := range []map[string][]int{{"USB": {1}}, {"CW": nil}} {
		candidate := plan.Candidates[0]
		candidate.ModeDistances = mask
		if err := studyValidateCandidate(candidate); err == nil {
			t.Fatalf("study accepted cells outside its CW/RTTY non-null evidence contract: %#v", mask)
		}
	}
}

func TestCCV2StudyTypedMaskOwnsCandidateValues(t *testing.T) {
	plan := studyTestPlan(t)
	candidate := plan.Candidates[0]
	candidate.ModeDistances = map[string][]int{"CW": {1}}
	cfg := &config.Config{CallCorrection: replayTemporalFreshConfig()}
	cfg.CallCorrection.BayesBonus = studyTestBayesBaseline()
	if err := studyApplyCandidate(cfg, plan.Arms[2], candidate); err != nil {
		t.Fatal(err)
	}
	candidate.ModeDistances["CW"][0] = 2
	delete(candidate.ModeDistances, "CW")
	if !reflect.DeepEqual(cfg.CallCorrection.BayesBonus.EnabledDistancesByMode, map[string][]int{"CW": {1}}) {
		t.Fatal("candidate mutation changed the applied immutable runtime mask")
	}
}
