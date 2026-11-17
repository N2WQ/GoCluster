package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/config"
)

func TestCCV2StudyRepairedProvenanceDrift(t *testing.T) {
	for _, role := range []string{"fcc", "ised", "cty", "mode_asset_paths"} {
		t.Run(role, func(t *testing.T) {
			base := &config.Config{CallCorrection: replayTemporalFreshConfig()}
			before, err := studyPipelineFingerprint(base)
			if err != nil {
				t.Fatal(err)
			}
			switch role {
			case "fcc":
				base.FCCULS.Enabled = true
			case "ised":
				base.ISED.Enabled = true
			case "cty":
				base.CTY.Enabled = true
			case "mode_asset_paths":
				base.CallCorrection.SpotterReliabilityFileCW = "common/rtty.csv"
				base.CallCorrection.SpotterReliabilityFileRTTY = "common/cw.csv"
			}
			after, err := studyPipelineFingerprint(base)
			if err != nil || before == after {
				t.Fatalf("effective %s drift was omitted from fingerprint: %v", role, err)
			}
			p := studyTestPlan(t)
			for i := range p.Inputs {
				p.Inputs[i].MaxRows = 100
			}
			paths := studyTestComparisonReports(t, p, "drift-plan", false, false)
			data, err := os.ReadFile(paths[2])
			if err != nil {
				t.Fatal(err)
			}
			var report studyReport
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatal(err)
			}
			report.PipelineWithoutBayesSHA256 = after
			if err := writeJSONAtomic(paths[2], report); err != nil {
				t.Fatal(err)
			}
			if _, err := studyReadReports(p, "drift-plan", studyCLIRequest{Compare: strings.Join(paths, ","), Phase: "holdout"}, p.Candidates[0]); err == nil {
				t.Fatalf("effective %s OFF/ON mismatch was accepted", role)
			}
		})
	}
	for _, drift := range []string{"executable", "source_revision", "asset_role_binding"} {
		t.Run(drift, func(t *testing.T) {
			p := studyTestPlan(t)
			switch drift {
			case "executable":
				p.Arms[2].ExecutableSHA256 = strings.Repeat("c", 64)
			case "source_revision":
				p.Arms[2].SourceRevision = strings.Repeat("c", 40)
			case "asset_role_binding":
				cw := studyFile{Path: filepath.Join(t.TempDir(), "cw.csv"), SHA256: strings.Repeat("a", 64)}
				rtty := studyFile{Path: filepath.Join(t.TempDir(), "rtty.csv"), SHA256: strings.Repeat("b", 64)}
				p.Arms[1].Assets = []studyFile{cw, rtty}
				cw.SHA256, rtty.SHA256 = rtty.SHA256, cw.SHA256
				p.Arms[2].Assets = []studyFile{cw, rtty}
				if studyAssetDigest(p.Arms[1].Assets) == studyAssetDigest(p.Arms[2].Assets) {
					t.Fatal("swapping model-role hashes preserved the unordered asset digest")
				}
				if studyAssetDigest(p.Arms[1].Assets) != studyAssetDigest([]studyFile{p.Arms[1].Assets[1], p.Arms[1].Assets[0]}) {
					t.Fatal("manifest order changed a path-bound asset fingerprint")
				}
			}
			if err := validateStudyPlan(p); err == nil {
				t.Fatalf("plan accepted repaired %s mismatch", drift)
			}
			if _, err := studyReadReports(p, "drift-plan", studyCLIRequest{Phase: "holdout"}, p.Candidates[0]); err == nil {
				t.Fatalf("report reader accepted repaired %s mismatch", drift)
			}
		})
	}
	p := studyTestPlan(t)
	p.Arms[0].ExecutableSHA256, p.Arms[0].SourceRevision = strings.Repeat("f", 64), strings.Repeat("e", 40)
	if err := validateStudyPlan(p); err != nil {
		t.Fatalf("unchanged shipped baseline cannot differ from repaired executable: %v", err)
	}
}

func TestCCV2StudyDistinctCandidateExecution(t *testing.T) {
	p := studyTestPlan(t)
	p.Candidates = nil
	for _, spec := range []struct {
		id        string
		threshold int
	}{{"one", 100}, {"two", 200}} {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(studyTestParameters(t), &fields); err != nil {
			t.Fatal(err)
		}
		value, err := json.Marshal(spec.threshold)
		if err != nil {
			t.Fatal(err)
		}
		fields["ReportThresholdDistance1Milli"] = value
		parameters, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		p.Candidates = append(p.Candidates, studyCandidate{ID: spec.id, Parameters: parameters,
			ModeDistances: map[string][]int{"CW": {1, 2}, "RTTY": {1, 2}}})
	}
	archive := studyTestZIP(t, "N0BBB,14020,20m,K1ABD,-7,2026-11-14 00:10:00,CQ,CW\n")
	in := studyInput{DateUTC: "2026-11-14", Phase: "tuning", Archive: archive, MaxRows: 1, MaxCSVBytes: 4096}
	p.Inputs = append(p.Inputs, in)
	if err := validateStudyPlan(p); err != nil {
		t.Fatal(err)
	}
	day, err := loadStudyRawDay(in, nil, p.WarmupSeconds)
	if err != nil {
		t.Fatal(err)
	}
	var reports []studyReport
	for i, frozen := range p.Candidates {
		candidate, err := studySelectCandidate(p, studyCLIRequest{Phase: "tuning", Candidate: frozen.ID})
		if err != nil {
			t.Fatal(err)
		}
		cfg := &config.Config{CallCorrection: replayTemporalFreshConfig()}
		cfg.CallCorrection.BayesBonus = studyTestBayesBaseline()
		if err := studyApplyCandidate(cfg, p.Arms[2], candidate); err != nil {
			t.Fatal(err)
		}
		maskSchema, err := studyValidateRuntimeConfig(cfg, p.Arms[2], candidate, p)
		if err != nil || !cfg.CallCorrection.BayesBonus.Enabled || cfg.CallCorrection.BayesBonus.ReportThresholdDistance1Milli != (i+1)*100 {
			t.Fatalf("selected complete vector did not reach effective config: %+v %v", cfg.CallCorrection.BayesBonus, err)
		}
		out := filepath.Join(t.TempDir(), "run")
		if err := os.Mkdir(out, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := studyRunDay(p, "candidate-execution-plan", studyCLIRequest{Output: out, Phase: "tuning"}, p.Arms[2], candidate, in, cfg, day, maskSchema); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(out, "complete.json"))
		if err != nil {
			t.Fatal(err)
		}
		var report studyReport
		if err := json.Unmarshal(data, &report); err != nil {
			t.Fatal(err)
		}
		reports = append(reports, report)
	}
	if reports[0].CandidateSHA256 == reports[1].CandidateSHA256 || reports[0].CallCorrectionSHA256 == reports[1].CallCorrectionSHA256 ||
		reports[0].PipelineWithoutBayesSHA256 != reports[1].PipelineWithoutBayesSHA256 {
		t.Fatalf("distinct complete vectors were not represented in actual runtime reports: %+v", reports)
	}
	if _, err := studySelectCandidate(p, studyCLIRequest{Phase: "holdout", Candidate: "two"}); err == nil {
		t.Fatal("alternate tuning vector remained selectable on frozen holdout")
	}
	shipped := &config.Config{CallCorrection: replayTemporalFreshConfig()}
	before := hashAnyStable(shipped.CallCorrection.BayesBonus)
	if err := studyApplyCandidate(shipped, p.Arms[0], p.Candidates[1]); err != nil || before != hashAnyStable(shipped.CallCorrection.BayesBonus) {
		t.Fatalf("candidate application changed shipped baseline: %v", err)
	}
	probe := &config.Config{CallCorrection: replayTemporalFreshConfig()}
	probe.CallCorrection.BayesBonus = studyTestBayesBaseline()
	probe.CallCorrection.CustomSCP.Enabled, probe.CallCorrection.CustomSCP.MaxSpottersPerKey = true, 1
	if err := studyApplyCandidate(probe, p.Arms[2], p.Candidates[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := studyValidateRuntimeConfig(probe, p.Arms[2], p.Candidates[0], p); err == nil {
		t.Fatal("effective candidate bypassed the existing pre-owner CustomSCP capacity validator")
	}
	restricted := p.Candidates[0]
	restricted.ModeDistances = map[string][]int{"CW": {1}}
	typed := &config.Config{CallCorrection: config.CallCorrectionConfig{BayesBonus: studyTestBayesBaseline()}}
	err = studyApplyCandidate(typed, p.Arms[2], restricted)
	if err != nil || !config.BayesBonusCellEnabled(typed.CallCorrection.BayesBonus.Enabled, typed.CallCorrection.BayesBonus.EnabledDistancesByMode, "CW", 1, -1) ||
		config.BayesBonusCellEnabled(typed.CallCorrection.BayesBonus.Enabled, typed.CallCorrection.BayesBonus.EnabledDistancesByMode, "RTTY", 1, -1) {
		t.Fatalf("typed restricted mask was not applied exactly: %+v err=%v", typed.CallCorrection.BayesBonus, err)
	}
}
