package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func studyTestPlan(t *testing.T) studyPlan {
	t.Helper()
	sha := strings.Repeat("a", 64)
	plan := studyPlan{SchemaVersion: 2, InputScenario: studyInputScenario{ReceiverGeography: studyReceiverGeography,
		CTYSHA256: sha, H3Res1SHA256: strings.Repeat("c", 64), H3Res2SHA256: strings.Repeat("d", 64)}, Frozen: true, HoldoutContentUninspected: true,
		ReferenceVersion:        studyReferenceVersion,
		ReferenceContractSHA256: "879cc458ef9fa94f977051fb71ae0f07a72deede6728604fc7b18d556ed74626",
		FrozenAtUTC:             time.Now().UTC().Add(-time.Minute).Format(time.RFC3339),
		SelectedAtUTC:           time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339),
		AssetTrainingThroughUTC: "2026-11-14T23:59:59Z", TuningThroughUTC: "2026-11-14T23:59:59Z",
		ReceiverMetadataThroughUTC: "2026-11-14T23:59:59Z", ReceiverMetadata: studyFile{Path: "metadata.json", SHA256: sha},
		DecisionCutoffSeconds: 75, EmbargoSeconds: 195, WarmupSeconds: 300, MaxExamples: 3, SelectedCandidate: "one",
		Candidates: []studyCandidate{{ID: "one", Parameters: studyTestParameters(t), ModeDistances: map[string][]int{"CW": {1, 2}, "RTTY": {1, 2}}}},
		Inputs: []studyInput{{DateUTC: "2026-11-15", Phase: "holdout", Archive: studyFile{Path: "20261115.zip", SHA256: sha}, MaxRows: 10, MaxCSVBytes: 4096},
			{DateUTC: "2026-11-16", Phase: "holdout", Archive: studyFile{Path: "20261116.zip", SHA256: sha}, MaxRows: 10, MaxCSVBytes: 4096}}}
	for _, name := range []string{"shipped", "repaired_off", "repaired_on"} {
		plan.Arms = append(plan.Arms, studyArm{Name: name, ExecutableSHA256: sha, SourceRevision: strings.Repeat("b", 40),
			ConfigDir: "config", ConfigFiles: map[string]string{"pipeline.yaml": sha}, Assets: []studyFile{
				{Path: "fixture-cty.plist", SHA256: sha}, {Path: "fixture-res1.bin", SHA256: strings.Repeat("c", 64)},
				{Path: "fixture-res2.bin", SHA256: strings.Repeat("d", 64)}}})
	}
	return plan
}

func TestCCV2StudyFreezeAndProvenance(t *testing.T) {
	t.Run("embargo", func(t *testing.T) {
		p := studyTestPlan(t)
		if err := validateStudyPlan(p); err != nil {
			t.Fatal(err)
		}
		p.TuningThroughUTC = "2026-11-15T00:02:00Z"
		if err := validateStudyPlan(p); err == nil || !strings.Contains(err.Error(), "embargo") {
			t.Fatalf("training/label overlap was not refused: %v", err)
		}
		for _, exposed := range []string{"training", "tuning"} {
			p = studyTestPlan(t)
			if exposed == "training" {
				p.AssetTrainingThroughUTC = "2026-11-15T00:00:01Z"
			} else {
				p.TuningThroughUTC = "2026-11-15T00:00:01Z"
			}
			if err := validateStudyPlan(p); err == nil {
				t.Fatalf("%s exposure on the holdout date was absorbed by warmup", exposed)
			}
		}
		p = studyTestPlan(t)
		p.EmbargoSeconds = 194
		if err := validateStudyPlan(p); err == nil {
			t.Fatal("194s embargo passed fixed 195s horizon")
		}
		p = studyTestPlan(t)
		p.ReceiverMetadataThroughUTC = "2026-11-15T00:00:00Z"
		if err := validateStudyPlan(p); err == nil {
			t.Fatal("future receiver alias metadata passed")
		}
	})
	t.Run("frozen_candidate", func(t *testing.T) {
		p := studyTestPlan(t)
		for i := 2; i <= 16; i++ {
			candidate := p.Candidates[0]
			candidate.ID = fmt.Sprintf("candidate-%d", i)
			p.Candidates = append(p.Candidates, candidate)
		}
		if err := validateStudyPlan(p); err != nil {
			t.Fatal(err)
		}
		p.Candidates = append(p.Candidates, studyCandidate{ID: "seventeen"})
		if err := validateStudyPlan(p); err == nil {
			t.Fatal("seventeenth complete candidate passed")
		}
		p = studyTestPlan(t)
		p.Candidates[0].Parameters = json.RawMessage(`{"Enabled":true}`)
		if err := validateStudyPlan(p); err == nil {
			t.Fatal("partial parameter vector passed")
		}
		p = studyTestPlan(t)
		p.Candidates[0].ModeDistances = map[string][]int{"CW": {1, 1}}
		if err := validateStudyPlan(p); err == nil {
			t.Fatal("duplicate cell passed")
		}
		p = studyTestPlan(t)
		if _, err := studySelectCandidate(p, studyCLIRequest{Phase: "holdout", Candidate: "retuned"}); err == nil {
			t.Fatal("same-holdout retuning passed")
		}
		for _, raw := range []string{`{"x":1,"x":2}`, `{"unrecognized":1}`} {
			if err := studyDecodeJSON([]byte(raw), &studyPlan{}); err == nil {
				t.Fatal("unknown/duplicate study config field passed")
			}
		}
	})
}

func studyTestDecision(id, episode string, corrected bool, reference string) *studyDecision {
	at := time.Date(2026, 11, 15, 0, 10, 0, 0, time.UTC)
	d := &studyDecision{RawID: id, EpisodeID: episode, Raw: studyTestRaw(at, "N0BBB", "N0BBB", "K1ABD", true).Row,
		ReceiverLocation: studyReceiverLocation{LookupCall: "N0BBB", Reason: "cty_no_match"},
		SubjectIdentity:  "K1ABD", FinalIdentity: "K1ABD", FinalObserved: "K1ABD", Status: "output", DecisionAtUTC: at,
		ConfiguredDistance: 0, R1: studyReference{Strong: true, Identity: reference, Families: 3, Reason: "strong"},
		R2: studyReference{Strong: true, Identity: reference, Families: 3, Reason: "strong"}}
	if corrected {
		d.FinalIdentity, d.FinalObserved, d.IdentityCorrection, d.Applied, d.ConfiguredDistance = "K1ABC", "K1ABC", true, true, 1
	}
	d.R2Frozen = d.R2
	return d
}

func TestCCV2StudyPairedOutcomes(t *testing.T) {
	strong := studyTestDecision("one", "episode-one", true, "K1ABC")
	retained := studyTestDecision("one", "episode-one", false, "K1ABC")
	wrong := *strong
	wrong.R1.Identity = "K9BAD"
	if got := studyPairedClass(retained, strong); got != "helpful" {
		t.Fatalf("helpful=%s", got)
	}
	if got := studyPairedClass(retained, &wrong); got != "wrong_rename" {
		t.Fatalf("one strong contradiction was concealed by other reference: %s", got)
	}
	if !wrong.R1.Strong || !wrong.R2.Strong {
		t.Fatal("strong references with different labels were relabeled unknown")
	}
	if got := studyPairedClass(strong, retained); got != "retention" {
		t.Fatalf("retention=%s", got)
	}
	if got := studyPairedClass(nil, strong); got != "eligibility" {
		t.Fatalf("full-union missing arm=%s", got)
	}
	pairs := map[int64]*studyPair{
		1: {Arms: [3]*studyDecision{retained, retained, strong}},
		2: {Arms: [3]*studyDecision{retained, retained, strong}}, // Repeat same episode: once.
		3: {Arms: [3]*studyDecision{nil, nil, studyTestDecision("two", "episode-two", true, "K1ABC")}},
	}
	result := studyComparison{BayesOutcomes: make(map[string]int64), RepairOutcomes: make(map[string]int64)}
	var output bytes.Buffer
	counts, err := studyAccountDay(studyInput{DateUTC: "2026-11-15"}, pairs,
		studyCandidate{ModeDistances: map[string][]int{"CW": {1}}}, &result, json.NewEncoder(&output), 3)
	if err != nil {
		t.Fatal(err)
	}
	got := counts["CW/1"]
	if result.RawUnion != 3 || result.UnpairedRawIDs != 1 || got.Incremental != 2 || got.Joint != 2 || got.NetSupportedR1 != 2 || got.NetSupportedR2 != 2 {
		t.Fatalf("literal union/repeat accounting changed: result=%+v counts=%+v", result, got)
	}
	// Unknown repeated output must remain in the same episode's full coverage
	// denominator; an earlier strong receipt cannot hide it.
	ep := &studyEpisodeCell{CoveredR1: true, CoveredR2: true, Joint: true}
	studyAccountEpisode(ep, retained, strong, "CW/1", nil)
	unknown := *strong
	unknown.R2 = studyReference{Reason: "insufficient_families"}
	studyAccountEpisode(ep, retained, &unknown, "CW/1", nil)
	if !ep.Incremental || !ep.CoveredR1 || ep.CoveredR2 || ep.Joint {
		t.Fatalf("repeat/unknown coverage was inflated: %+v", ep)
	}
}

func TestCCV2StudyQualificationBoundaries(t *testing.T) {
	pass := []studyCellDate{
		{DateUTC: "2026-11-15", Incremental: 125, Joint: 100, CoveredR1: 100, CoveredR2: 100, NetSupportedR1: 100, NetSupportedR2: 100},
		{DateUTC: "2026-11-16", Incremental: 125, Joint: 100, CoveredR1: 100, CoveredR2: 100, NetSupportedR1: 100, NetSupportedR2: 100},
	}
	if v := studyQualifyCell("CW/1", pass); !v.Pass || v.CoverageR1 != 80 || v.Joint != 200 {
		t.Fatalf("exact 200 /80%% boundary failed: %+v", v)
	}
	for _, test := range []struct {
		name   string
		change func([]studyCellDate)
	}{
		{"199", func(ds []studyCellDate) { ds[0].Joint = 99 }},
		{"one_date_49", func(ds []studyCellDate) { ds[0].Joint, ds[1].Joint = 49, 151 }},
		{"r1_79_6_percent", func(ds []studyCellDate) { ds[0].CoveredR1 = 99 }},
		{"r2_79_6_percent", func(ds []studyCellDate) { ds[0].CoveredR2 = 99 }},
		{"one_r1_contradiction", func(ds []studyCellDate) { ds[0].NewWrongR1 = 1 }},
		{"one_r2_contradiction", func(ds []studyCellDate) { ds[0].NewWrongR2 = 1 }},
		{"no_gain_one_date", func(ds []studyCellDate) { ds[0].NetSupportedR2 = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			dates := append([]studyCellDate(nil), pass...)
			test.change(dates)
			if v := studyQualifyCell("CW/1", dates); v.Pass {
				t.Fatalf("boundary passed: %+v", v)
			}
		})
	}
}

// Existing replay tests already use the test executable as a subprocess. This
// invokes the actual main/flag path in that process instead of a second runner.
func TestCCV2StudyActualMainRefusals(t *testing.T) {
	const helper = "GOCLUSTER_CCV2_STUDY_MAIN"
	if encoded := os.Getenv(helper); encoded != "" {
		var args []string
		if err := json.Unmarshal([]byte(encoded), &args); err != nil {
			t.Fatal(err)
		}
		flag.CommandLine = flag.NewFlagSet("rbn_replay", flag.ExitOnError)
		os.Args = append([]string{os.Args[0]}, args...)
		main()
		os.Exit(0)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"unfrozen", "wrong_hash", "missing_input", "retuned_candidate", "partial_reports", "ordinary_override", "old_schema", "missing_scenario", "wrong_scenario", "scenario_asset_drift"} {
		t.Run(name, func(t *testing.T) {
			p := studyTestPlan(t)
			p.Inputs[0].Archive.Path = filepath.Join(t.TempDir(), "missing.zip")
			if name == "unfrozen" {
				p.Frozen = false
			}
			switch name {
			case "old_schema":
				p.SchemaVersion = 1
			case "missing_scenario":
				p.InputScenario = studyInputScenario{}
			case "wrong_scenario":
				p.InputScenario.ReceiverGeography = "historical_sites"
			case "scenario_asset_drift":
				p.InputScenario.CTYSHA256 = strings.Repeat("f", 64)
			}
			data, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "plan.json")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			sum, err := studyHashFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if name == "wrong_hash" {
				sum = strings.Repeat("f", 64)
			}
			args := []string{"-study-plan", path, "-study-freeze", sum, "-study-arm", "repaired_on", "-date", "2026-11-15",
				"-study-out", filepath.Join(t.TempDir(), "out")}
			want := map[string]string{"unfrozen": "frozen=true", "wrong_hash": "frozen digest", "missing_input": "missing.zip",
				"retuned_candidate": "single frozen", "partial_reports": "every frozen date", "ordinary_override": "ordinary replay overrides",
				"old_schema": "schema 2", "missing_scenario": "cty_representative_v1", "wrong_scenario": "cty_representative_v1", "scenario_asset_drift": "scenario asset"}[name]
			if name == "retuned_candidate" {
				args = append(args, "-study-candidate", "retuned")
			}
			if name == "partial_reports" {
				args = []string{"-study-plan", path, "-study-freeze", sum, "-study-compare", "partial.json", "-study-out", filepath.Join(t.TempDir(), "out")}
			}
			if name == "ordinary_override" {
				args = append(args, "-force-download")
			}
			encoded, err := json.Marshal(args)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCCV2StudyActualMainRefusals$", "-test.timeout=15s")
			cmd.Env = append(os.Environ(), helper+"="+string(encoded))
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), want) {
				t.Fatalf("actual main refusal=%v output=%s want=%s", err, output, want)
			}
		})
	}
}
