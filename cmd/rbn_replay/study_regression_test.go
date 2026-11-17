package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

// All report/decision bytes are private hand-counted software fixtures. No raw
// archive, fitted asset or external observation supplies the expected labels.
func studyTestWriteReports(t *testing.T, p studyPlan, digest string,
	decisions func(studyInput, studyArm) []studyDecision) []string {
	t.Helper()
	var paths []string
	pipeline, err := studyPipelineFingerprint(&config.Config{CallCorrection: replayTemporalFreshConfig()})
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range p.Inputs {
		for _, arm := range p.Arms {
			dir := t.TempDir()
			path := filepath.Join(dir, "decisions.jsonl")
			file, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			rows := decisions(in, arm)
			encoder := json.NewEncoder(file)
			for _, d := range rows {
				if err := encoder.Encode(d); err != nil {
					t.Fatal(err)
				}
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			sum, err := studyHashFile(path)
			if err != nil {
				t.Fatal(err)
			}
			candidate := p.Candidates[0]
			var coverage studyInputCoverage
			for _, d := range rows {
				coverage.observe(d.ReceiverLocation)
			}
			report := studyReport{SchemaVersion: 2, InputScenario: p.InputScenario, InputCoverage: coverage, Complete: true, PlanSHA256: digest, ReferenceVersion: p.ReferenceVersion,
				Arm: arm.Name, Candidate: candidate.ID, CandidateSHA256: hashAnyStable(candidate), DateUTC: in.DateUTC, Phase: in.Phase,
				SourceRevision: arm.SourceRevision, ExecutableSHA256: arm.ExecutableSHA256, AssetsSHA256: studyAssetDigest(arm.Assets),
				ConfigFilesSHA256: hashAnyStable(arm.ConfigFiles), PipelineWithoutBayesSHA256: pipeline,
				Decisions: studyFile{Path: path, SHA256: sum}, RawRows: len(rows), FocalRows: len(rows), TerminalRows: len(rows),
				MaxRows: in.MaxRows, MaxCSVBytes: in.MaxCSVBytes, WarmupSeconds: p.WarmupSeconds, MaskSchemaPresent: true}
			complete := filepath.Join(dir, "complete.json")
			if err := writeJSONAtomic(complete, report); err != nil {
				t.Fatal(err)
			}
			paths = append(paths, complete)
		}
	}
	return paths
}

func studyTestReadComparison(t *testing.T, output string) studyComparison {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(output, "comparison.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result studyComparison
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestCCV2StudyShiftedReceiptsCannotQualify(t *testing.T) {
	for _, conflictingLabel := range []bool{false, true} {
		t.Run(fmt.Sprintf("conflicting_identity_harm_%t", conflictingLabel), func(t *testing.T) {
			p := studyTestPlan(t)
			p.Candidates[0].ModeDistances = map[string][]int{"CW": {1}}
			for i := range p.Inputs {
				p.Inputs[i].MaxRows = 200
			}
			const digest = "literal-99-shifted-plus-one-additional-per-date"
			paths := studyTestWriteReports(t, p, digest, func(in studyInput, arm studyArm) []studyDecision {
				date, _, err := parseUTCDate(in.DateUTC)
				if err != nil {
					t.Fatal(err)
				}
				rows := make([]studyDecision, 0, 200)
				for episode := 0; episode < 100; episode++ {
					anchor := fmt.Sprintf("%s:%d", in.Archive.SHA256, 2*episode+1)
					for receipt := 0; receipt < 2; receipt++ {
						ordinal := 2*episode + receipt + 1
						corrected := arm.Name == "repaired_on" && receipt == 1 ||
							arm.Name == "repaired_off" && receipt == 0 && episode < 99
						d := studyTestDecision(fmt.Sprintf("%s:%d", in.Archive.SHA256, ordinal), anchor, corrected, "K1ABC")
						d.Ordinal, d.ReceiverFamily = int64(ordinal), "N0BBB"
						d.Raw.Time = date.Add(time.Duration(600+episode*196+receipt) * time.Second)
						d.DecisionAtUTC, d.RawSHA256 = d.Raw.Time, hashAnyStable(d.Raw)
						if conflictingLabel && episode == 0 && receipt == 1 {
							d.R1 = studyReference{Strong: true, Identity: "K9BAD", Families: 3, Reason: "strong"}
							d.R2, d.R2Frozen = d.R1, d.R1 // Same frozen row labels in all arms.
						}
						rows = append(rows, *d)
					}
				}
				return rows
			})
			output := filepath.Join(t.TempDir(), "comparison")
			if err := compareCCV2Study(p, digest, studyCLIRequest{Compare: strings.Join(paths, ","), Phase: "holdout", Output: output}); err != nil {
				t.Fatal(err)
			}
			got := studyTestReadComparison(t, output)
			if got.RawUnion != 400 || got.AllActiveCellsPass || got.Verdict != "whole_bayes_off" || len(got.Cells) != 1 || got.Cells[0].Joint != 2 {
				t.Fatalf("99 shifted +1 genuine/date incorrectly qualified: %+v", got)
			}
			for _, date := range got.Cells[0].Dates {
				if date.Incremental != 1 || date.Joint != 1 || date.CoveredR1 != 1 || date.CoveredR2 != 1 {
					t.Fatalf("receipt movement inflated the episode denominator: %+v", date)
				}
			}
			wantWrong := 0
			if conflictingLabel {
				wantWrong = 2
			}
			if got.NewStrongContradictionsR1 != wantWrong || got.NewStrongContradictionsR2 != wantWrong {
				t.Fatalf("episode dedup hid new raw-row contradiction: %+v", got)
			}
		})
	}
}

func TestCCV2StudyEpisodeIdentityAcrossCells(t *testing.T) {
	for _, outside := range []bool{false, true} {
		t.Run(fmt.Sprintf("outside_%t", outside), func(t *testing.T) {
			off := studyTestDecision("one", "episode", true, "K1ABC")
			off.ConfiguredDistance = 2
			on := studyTestDecision("two", "episode", true, "K9BAD")
			if outside {
				on.ConfiguredDistance = 3
			}
			retained := studyTestDecision("two", "episode", false, "K9BAD")
			pairs := map[int64]*studyPair{1: {Arms: [3]*studyDecision{off, off, nil}}, 2: {Arms: [3]*studyDecision{retained, retained, on}}}
			result := studyComparison{BayesOutcomes: make(map[string]int64), RepairOutcomes: make(map[string]int64)}
			var output bytes.Buffer
			counts, err := studyAccountDay(studyInput{DateUTC: "2026-11-15"}, pairs,
				studyCandidate{ModeDistances: map[string][]int{"CW": {1}}}, &result, json.NewEncoder(&output), 3)
			if err != nil {
				t.Fatal(err)
			}
			got := counts["CW/1"]
			if outside {
				if len(result.Spillover) != 1 {
					t.Fatalf("missing spillover contradiction: %+v", result)
				}
				got = result.Spillover[0].Counts
			}
			if got.Incremental != 0 || got.Joint != 0 || got.NewWrongR1 != 1 || got.NewWrongR2 != 1 {
				t.Fatalf("cross-cell OFF target was counted new or harm concealed: %+v", got)
			}
		})
	}
}
