package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These five literal rows give two focal IDs. The first winner has exactly
// three raw follow-ons, including a same-second beacon and an RTTY end-boundary
// beacon. The added second correction has none: 1/1 -> 1/2 is an exact decline.
func methodEvalTestFixture(t *testing.T) (studyMethodEvaluationPlan, studyFile) {
	return methodEvalTestFixtureVersion(t, 1)
}

func methodEvalTestFixtureVersion(t *testing.T, schema int) (studyMethodEvaluationPlan, studyFile) {
	t.Helper()
	method := methodTestPlan(t)
	if schema == 2 {
		method = methodTestPlanV2(t)
	}
	method.Enabled = true
	methodFile := studyAuditTestJSON(t, filepath.Join(t.TempDir(), "method.json"), method)
	control := method
	control.Enabled = false
	controlFile := studyAuditTestJSON(t, filepath.Join(t.TempDir(), "control.json"), control)
	capture := studyTestPlan(t)
	capture.Inputs = nil
	capture.TuningThroughUTC = "2026-11-16T23:59:59Z"
	capture.ReceiverMetadata = studyAuditTestJSON(t, filepath.Join(t.TempDir(), "receivers.json"), []studyReceiverGroup{})
	for i := range capture.Arms {
		capture.Arms[i].SourceRevision = method.SourceRevision
	}
	dates := []string{"2026-11-07", "2026-11-14", "2026-11-15", "2026-11-16"}
	for _, date := range dates {
		body := fmt.Sprintf("N0BBB,14020,20m,K1ABD,10,%s 00:10:00,CQ,CW\nN0BBB,14020,20m,K1ABC,10,%s 00:10:00,BEACON,CW\nN0BBB,14020,20m,K1ABC,10,%s 00:10:01,BEACON,CW\nN0BBB,14020,20m,K2ABD,10,%s 00:10:04,CQ,CW\nN0BBB,14020,20m,K1ABC,10,%s 00:25:00,BEACON,RTTY\n", date, date, date, date, date)
		capture.Inputs = append(capture.Inputs, studyInput{DateUTC: date, Phase: "tuning", Archive: studyTestZIPDate(t, date, body), MaxRows: 16, MaxCSVBytes: 1 << 16})
	}
	captureFile := studyAuditTestJSON(t, filepath.Join(t.TempDir(), "capture.json"), capture)
	plan := studyMethodEvaluationPlan{SchemaVersion: schema, Experimental: true, Phase: "tuning", CapturePlan: captureFile, MethodPlan: methodFile, ControlPlan: controlFile}
	plan.ParityCapturePlan, plan.ParityControlPlan = captureFile, controlFile
	for _, input := range capture.Inputs {
		day, err := loadStudyRawDay(input, nil, capture.WarmupSeconds)
		if err != nil {
			t.Fatal(err)
		}
		group := studyMethodEvalGroup{DateUTC: input.DateUTC}
		roles := 3
		if schema == 2 && input.DateUTC != "2026-11-07" && input.DateUTC != "2026-11-14" {
			roles = 1 // The other exposed dates provide disabled parity only.
		}
		for role := 0; role < roles; role++ {
			selected := method.Candidates[0]
			if role == 2 {
				index := 2
				if schema == 2 {
					index = 1
				}
				selected = method.Candidates[index]
			}
			rootPlan, rootFile := method, methodFile
			if role == 0 {
				rootPlan, rootFile = control, controlFile
			}
			model, err := studyLoadOrdinalModel(rootPlan.Model)
			if err != nil {
				t.Fatal(err)
			}
			run, err := newStudyMethodRun(t.TempDir(), &studyMethodPrepared{Plan: rootPlan, File: rootFile, Candidate: selected, Model: model})
			if err != nil {
				t.Fatal(err)
			}
			run.files.scratchBackend("no_support_history_backend", "", 0, 0)
			file, err := run.files.owner.openFile("decisions.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600, false)
			if err != nil {
				t.Fatal(err)
			}
			coverage := studyInputCoverage{}
			for _, ordinal := range []int{1, 4} {
				raw := day.Rows[ordinal-1]
				reference := "K1ABC"
				if ordinal == 4 {
					reference = "K2ABC"
				}
				d := studyTestDecision(fmt.Sprintf("%s:%d", input.Archive.SHA256, ordinal), raw.Episode, ordinal == 1 || role > 0, reference)
				d.Ordinal, d.Raw, d.RawSHA256, d.ReceiverFamily = int64(ordinal), raw.Row, hashAnyStable(raw.Row), raw.Family
				d.SubjectIdentity, d.DecisionAtUTC = raw.Row.DXCall, raw.Row.Time
				d.FinalIdentity, d.FinalObserved = raw.Row.DXCall, raw.Row.DXCall
				if d.Applied {
					d.FinalIdentity, d.FinalObserved = reference, reference
				}
				coverage.observe(d.ReceiverLocation)
				if err := run.files.owner.writeRow(file, d); err != nil {
					t.Fatal(err)
				}
			}
			if err := errors.Join(file.Sync(), file.Close()); err != nil {
				t.Fatal(err)
			}
			sum, err := studyHashFile(file.path)
			if err != nil {
				t.Fatal(err)
			}
			manifest := replayManifest{DateUTC: input.DateUTC, InputZipPath: input.Archive.Path}
			manifest.InputZipMeta.SHA256 = input.Archive.SHA256
			manifest.CSV.Header = []string{"fixture"}
			manifest.CSV.RecordsTotal, manifest.CSV.RecordsProcessed = 5, 5
			manifest.Outputs.ManifestJSON = filepath.Join(run.files.owner.root, "manifest.json")
			manifest.Results.Stability = replayStabilitySummary{WindowMinutes: 60, MinFollowOn: 2, FreqToleranceHz: 1000, TotalApplied: 1, StableApplied: 1}
			if role > 0 {
				manifest.Results.Stability.TotalApplied = 2
			}
			if err := run.files.owner.atomicJSON(manifest.Outputs.ManifestJSON, manifest); err != nil {
				t.Fatal(err)
			}
			marker, err := run.complete()
			if err != nil {
				t.Fatal(err)
			}
			arm := capture.Arms[1]
			report := studyReport{SchemaVersion: 2, InputScenario: capture.InputScenario, InputCoverage: coverage, Complete: true, PlanSHA256: captureFile.SHA256, ReferenceVersion: studyReferenceVersion, Arm: "repaired_off", Candidate: capture.SelectedCandidate, CandidateSHA256: hashAnyStable(capture.Candidates[0]), DateUTC: input.DateUTC, Phase: "tuning", SourceRevision: method.SourceRevision, ExecutableSHA256: arm.ExecutableSHA256, AssetsSHA256: studyAssetDigest(arm.Assets), ConfigFilesSHA256: hashAnyStable(arm.ConfigFiles), CallCorrectionSHA256: strings.Repeat("c", 64), PipelineWithoutBayesSHA256: strings.Repeat("d", 64), MaskSchemaPresent: true, Decisions: studyFile{Path: file.path, SHA256: sum}, RawRows: 5, FocalRows: 2, TerminalRows: 2, PendingBound: 1, MaxRows: input.MaxRows, MaxCSVBytes: input.MaxCSVBytes, WarmupSeconds: capture.WarmupSeconds, References: studyReferenceCoverage{Denominator: 2, R1Strong: 2, R2Strong: 2, R1Unknown: map[string]int{}, R2Unknown: map[string]int{}}, ClaimLimits: []string{"hand-counted synthetic fixture"}, Experimental: marker}
			path := filepath.Join(run.files.owner.root, "complete.json")
			if err := run.files.owner.atomicJSON(path, report); err != nil {
				t.Fatal(err)
			}
			completeSHA, err := studyHashFile(path)
			if err != nil {
				t.Fatal(err)
			}
			manifestSHA, err := studyHashFile(manifest.Outputs.ManifestJSON)
			if err != nil {
				t.Fatal(err)
			}
			binding := studyMethodEvalBinding{Complete: studyFile{Path: path, SHA256: completeSHA}, Manifest: studyFile{Path: manifest.Outputs.ManifestJSON, SHA256: manifestSHA}}
			if role == 0 {
				group.Control = binding
				plan.Parity = append(plan.Parity, studyMethodEvalParity{DateUTC: input.DateUTC, OriginalDecisions: report.Decisions, ControlDecisions: report.Decisions, Rows: 2, ControlComplete: binding.Complete, ControlManifest: binding.Manifest})
			} else {
				group.Variants = append(group.Variants, studyMethodEvalVariant{CandidateID: selected.ID, Complete: binding.Complete, Manifest: binding.Manifest})
			}
		}
		if roles == 3 {
			plan.Groups = append(plan.Groups, group)
		}
	}
	return plan, studyAuditTestJSON(t, filepath.Join(t.TempDir(), "evaluation.json"), plan)
}

// Freeze the synthetic original facts outside JSON before negative cases mutate
// plans or bindings. Production entry points always use the real fixed E1 table.
func methodEvalFixtureAuthority(plan studyMethodEvaluationPlan) [4]studyMethodEvalAuthority {
	var out [4]studyMethodEvalAuthority
	for i, parity := range plan.Parity {
		out[i] = studyMethodEvalAuthority{DateUTC: parity.DateUTC, Rows: parity.Rows, SHA256: parity.OriginalDecisions.SHA256}
	}
	return out
}

func methodEvalTestReadResult(t *testing.T, out string) studyMethodEvaluationResult {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(out, "aggregates.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result studyMethodEvaluationResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
