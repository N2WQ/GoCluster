package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestCCV2F3ExactObjective(t *testing.T) {
	for _, test := range []struct {
		oldStable, oldApplied, newStable, newApplied int64
		pass                                         bool
	}{{800, 1000, 959, 1200, false}, {800000, 1000000, 800159, 1000200, false}, {800, 1000, 960, 1200, true}, {0, 0, 1, 1, false}, {1, 1, 1, 1, false}} {
		counts := studyMethodEvalNewCounts()
		counts.Control = studyAuditFraction{Stable: test.oldStable, Applied: test.oldApplied}
		counts.Variant = studyAuditFraction{Stable: test.newStable, Applied: test.newApplied}
		if got := studyMethodEvalObjective(counts); got != test.pass {
			t.Fatalf("%d/%d -> %d/%d pass=%t", test.oldStable, test.oldApplied, test.newStable, test.newApplied, got)
		}
		if test.pass {
			counts.NewWrongR2 = 1
			if !studyMethodEvalObjective(counts) {
				t.Fatal("reference gate was mislabeled as part of the primary objective")
			}
			result := studyMethodEvaluationResult{Variants: []studyMethodEvalVariantResult{{Counts: counts, Candidate: studyMethodCandidate{CWDistances: []int{1}}}}}
			studyMethodEvalFinish(&result, "tuning")
			if !result.Variants[0].ObjectivePass || result.Variants[0].AllCellRenamePass || result.Variants[0].Eligible {
				t.Fatal("primary objective and rename-reference gate were not separately reported")
			}
		}
	}
}

func TestCCV2F3FullOutcomeAccounting(t *testing.T) {
	counts := studyMethodEvalNewCounts()
	active := map[string]bool{"CW/1": true}
	changes := make(map[string]uint8)
	for _, scenario := range []string{"retarget_retime_outside", "added", "lost", "license_and_cutoff"} {
		control := studyTestDecision("id", scenario, scenario == "retarget_retime_outside" || scenario == "lost", "K1ABC")
		variant := *control
		switch scenario {
		case "retarget_retime_outside":
			variant.FinalIdentity, variant.FinalObserved = "K9BAD", "K9BAD"
			control.Raw.Mode, variant.Raw.Mode = "RTTY", "RTTY"
			variant.DecisionAtUTC = variant.DecisionAtUTC.Add(time.Second)
		case "added":
			variant.Applied, variant.IdentityCorrection, variant.ConfiguredDistance = true, true, 1
			variant.FinalIdentity, variant.FinalObserved = "K1ABC", "K1ABC"
		case "lost":
			variant.Status = "suppressed" // An allowed/applied substage is not a delivered receipt.
		case "license_and_cutoff":
			control.Status, variant.Status = "license_rejected", "license_rejected"
			variant.DecisionAfterCutoff = true
			variant.DecisionAtUTC = variant.Raw.Time.Add(76 * time.Second)
			variant.R2 = studyReference{Reason: "decision_after_cutoff"}
		}
		archiveSHA := hashAnyStable(scenario)
		for _, d := range []*studyDecision{control, &variant} {
			d.RawID = archiveSHA + ":1"
			d.Ordinal = 1
			d.EpisodeID = archiveSHA + ":1"
			d.ReceiverFamily = "N0BBB"
			d.RawSHA256 = hashAnyStable(d.Raw)
			d.ConfiguredDistance = spot.CallDistance(d.SubjectIdentity, d.FinalIdentity, d.Raw.Mode, "plain", "plain")
			d.PlainDistance = d.ConfiguredDistance
			if !studyDecisionBinding(*d, archiveSHA) || d.DecisionAfterCutoff != d.DecisionAtUTC.After(d.Raw.Time.Add(75*time.Second)) {
				t.Fatal("metric fixture violates immutable-ID or cutoff binding")
			}
		}
		if control.Raw != variant.Raw || control.R1 != variant.R1 || control.R2Frozen != variant.R2Frozen {
			t.Fatal("metric fixture changes immutable input/reference facts")
		}
		a, b := studyAuditMakeReceipt(control, false), studyAuditMakeReceipt(&variant, false)
		a.CurrentStable, b.CurrentStable = true, true
		if err := studyMethodEvalAccount(&counts, control, &variant, a, b, active); err != nil {
			t.Fatal(err)
		}
		changes[scenario] = studyMethodEvalEpisodeMask(a, b)
	}
	if counts.RawIDs != 4 || counts.Control.Applied != 2 || counts.Variant.Applied != 2 || counts.Added != 1 || counts.Lost != 1 || counts.Retargeted != 1 || counts.Retimed != 1 || counts.RetargetedRetimed != 1 || counts.AffectedBaselineIDs != 2 || counts.NewWrongR1 != 1 || counts.NewWrongR2 != 1 || counts.OutsideCells != 1 || counts.VariantReferences.R2Unknown["decision_after_cutoff"] != 1 {
		t.Fatalf("full-ID effects were filtered or conflated: %+v", counts)
	}
	episodes := studyMethodEvalEpisodeTotals(changes)
	if episodes.Added != 1 || episodes.Lost != 1 || episodes.Retargeted != 1 || episodes.Retimed != 1 || episodes.RetargetedRetimed != 1 || episodes.Affected != 2 {
		t.Fatalf("episode overlap changed: %+v", episodes)
	}
}

func TestCCV2F3ConfirmationBothGates(t *testing.T) {
	for _, objective := range []bool{false, true} {
		candidate := studyMethodCandidate{ID: "f150_t60_geometry_d1", GapHz: 150, MaxWitnessSeconds: 60, Ranking: "geometry", CWDistances: []int{1}}
		counts := studyMethodEvalNewCounts()
		counts.Control = studyAuditFraction{Stable: 800, Applied: 1000}
		counts.Variant = studyAuditFraction{Stable: 959, Applied: 1200}
		if objective {
			counts.Variant.Stable = 960
		}
		variant := studyMethodEvalVariantResult{Candidate: candidate, Counts: counts}
		for _, date := range []string{"synthetic-day-one", "synthetic-day-two"} {
			variant.Days = append(variant.Days, studyMethodEvalDay{DateUTC: date, Cells: map[string]studyCellDate{"CW/1": {DateUTC: date, Incremental: 125, Joint: 100, CoveredR1: 100, CoveredR2: 100, NetSupportedR1: 1, NetSupportedR2: 1}}})
		}
		result := studyMethodEvaluationResult{Variants: []studyMethodEvalVariantResult{variant}}
		studyMethodEvalFinish(&result, "holdout")
		if !result.Variants[0].Cells[0].Pass || result.Variants[0].Eligible != objective {
			t.Fatal("reference floors substituted for the primary objective")
		}
		result.Variants[0].Days[1].Cells["CW/1"] = studyCellDate{Incremental: 125, Joint: 49, CoveredR1: 99, CoveredR2: 100, NetSupportedR1: 1, NetSupportedR2: 1}
		result.Variants[0].Cells = nil
		studyMethodEvalFinish(&result, "holdout")
		if result.Variants[0].Eligible {
			t.Fatal("objective substituted for two-date reference/coverage floors")
		}
	}
}

func TestCCV2F3NewlyAssessableRetime(t *testing.T) {
	control := studyTestDecision(strings.Repeat("a", 64)+":1", strings.Repeat("a", 64)+":1", true, "K1ABC")
	control.Ordinal = 1
	control.ReceiverFamily = "N0BBB"
	control.RawSHA256 = hashAnyStable(control.Raw)
	control.R2Frozen = studyReference{Strong: true, Identity: "K9BAD", Families: 3, Reason: "strong"}
	control.R2 = studyReference{Reason: "decision_after_cutoff"}
	control.DecisionAfterCutoff = true
	control.DecisionAtUTC = control.Raw.Time.Add(76 * time.Second)
	variant := *control
	variant.DecisionAtUTC = variant.Raw.Time.Add(75 * time.Second)
	variant.DecisionAfterCutoff = false
	variant.R2 = variant.R2Frozen
	for _, d := range []*studyDecision{control, &variant} {
		if err := studyMethodEvalR2Binding(d); err != nil {
			t.Fatal(err)
		}
		if !studyDecisionBinding(*d, strings.Repeat("a", 64)) || d.DecisionAfterCutoff != d.DecisionAtUTC.After(d.Raw.Time.Add(75*time.Second)) {
			t.Fatal("retime fixture violates immutable IDs or the inclusive cutoff")
		}
	}
	day := &studyRawDay{Start: control.Raw.Time, Rows: make([]studyRaw, 1)}
	candidate := studyMethodCandidate{CWDistances: []int{1}}
	a := newStudyMethodEvalDayAccumulator(day, candidate)
	x, y := studyAuditMakeReceipt(control, false), studyAuditMakeReceipt(&variant, false)
	identities := studyEpisodeCorrections{{control.EpisodeID, control.Raw.Mode, control.FinalIdentity}: struct{}{}}
	if err := a.account(control, &variant, x, y, identities); err != nil {
		t.Fatal(err)
	}
	result := studyMethodEvalVariantResult{Counts: studyMethodEvalNewCounts(), Modes: make(map[string]*studyMethodEvalCounts)}
	if err := studyMethodEvalAppendDay(&result, a.finish()); err != nil {
		t.Fatal(err)
	}
	if result.Counts.Retimed != 1 || result.Counts.NewWrongR2 != 0 || result.Counts.NewlyAssessableWrongR2 != 1 || result.Counts.NewlyAssessableWrongR1 != 0 || result.Modes["CW"].NewlyAssessableWrongR2 != 1 || result.Days[0].Counts.NewlyAssessableWrongR2 != 1 {
		t.Fatal("newly assessable wrong evidence was hidden or changed the inherited rename gate")
	}
}

func TestCCV2F3EvaluationFullPairedSlice(t *testing.T) {
	plan, file := methodEvalTestFixture(t)
	out := filepath.Join(t.TempDir(), "evaluation")
	if err := runStudyMethodEvaluationWithAuthority(file.Path, file.SHA256, out, studyDiagnosticArtifactFileOps(), methodEvalFixtureAuthority(plan)); err != nil {
		t.Fatal(err)
	}
	result := methodEvalTestReadResult(t, out)
	if !result.Experimental || !result.Complete || len(result.Variants) != 2 || len(result.RoleOrder) != 3 || result.OrdinalVsGeometry == nil {
		t.Fatal("named complete slice or paired attribution is missing")
	}
	for _, variant := range result.Variants {
		if variant.Counts.RawIDs != 8 || variant.Counts.Control.Applied != 4 || variant.Counts.Control.Stable != 4 || variant.Counts.Variant.Applied != 8 || variant.Counts.Variant.Stable != 4 || variant.Counts.Added != 4 || variant.Eligible || len(variant.Days) != 4 {
			t.Fatalf("literal four-date fraction should be 4/4 -> 4/8: %+v", variant.Counts)
		}
	}
	if result.OrdinalVsGeometry.Counts.Classes["common_same"] != 8 {
		t.Fatal("otherwise identical geometry/ordinal outcomes lost attribution")
	}
	for _, stream := range []studyFile{result.Paired, result.Receipts} {
		if err := studyCheckFile(stream); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(result.Paired.Path)
	if err != nil {
		t.Fatal(err)
	}
	if len(bytes.Split(bytes.TrimSpace(data), []byte{'\n'})) != 8 {
		t.Fatal("full paired union omitted uncorrected incoming IDs")
	}
	if _, err := os.Stat(filepath.Join(out, "COMPLETE.json")); err != nil {
		t.Fatal(err)
	}
}

func TestCCV2F3EvaluationPlanRefusals(t *testing.T) {
	plan, _ := methodEvalTestFixture(t)
	for _, scenario := range []string{"phase", "grid", "parity", "duplicate_date", "missing_date"} {
		t.Run(scenario, func(t *testing.T) {
			data, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			var changed studyMethodEvaluationPlan
			if err := json.Unmarshal(data, &changed); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "phase":
				changed.Phase = "holdout"
			case "grid":
				for i := range changed.Groups {
					changed.Groups[i].Variants[1].CandidateID = "f150_t60_ordinal_d2"
				}
			case "parity":
				changed.Parity[0].ControlDecisions.SHA256 = strings.Repeat("0", 64)
			case "duplicate_date":
				changed.Groups[1].DateUTC = changed.Groups[0].DateUTC
			case "missing_date":
				changed.Groups = changed.Groups[:3]
			}
			file := studyAuditTestJSON(t, filepath.Join(t.TempDir(), "evaluation.json"), changed)
			if prepared, err := prepareStudyMethodEvaluationWithAuthority(file.Path, file.SHA256, methodEvalFixtureAuthority(plan)); err == nil || prepared != nil {
				t.Fatal("invalid frozen evaluation plan was admitted")
			}
		})
	}
	valid, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"schema_version", "experimental", "phase", "capture_plan", "method_plan", "control_plan", "parity_capture_plan", "parity_control_plan", "parity", "groups"} {
		for _, missing := range []bool{false, true} {
			var root map[string]json.RawMessage
			if err := json.Unmarshal(valid, &root); err != nil {
				t.Fatal(err)
			}
			if missing {
				delete(root, key)
			} else {
				root[key] = json.RawMessage("null")
			}
			data, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			file := methodTestWrite(t, filepath.Join(t.TempDir(), "missing.json"), data)
			var decoded studyMethodEvaluationPlan
			if err := studyMethodEvalReadJSON(file, &decoded); err == nil {
				t.Fatalf("operative field %s missing=%t was admitted", key, missing)
			}
		}
	}
	for _, bad := range [][]byte{bytes.Replace(valid, []byte(`"experimental":true`), []byte(`"experimental":false`), 1), bytes.Replace(valid, []byte(`"experimental":true`), []byte(`"experimental":null`), 1), bytes.Replace(valid, []byte(`"experimental":true`), []byte(`"Experimental":true`), 1), bytes.Replace(valid, []byte(`"experimental":true`), []byte(`"experimental":true,"experimental":true`), 1), bytes.Replace(valid, []byte(`"groups":`), []byte(`"unknown":0,"groups":`), 1), bytes.Replace(valid, []byte(`"control":`), []byte(`"control":null,"ignored":`), 1)} {
		file := methodTestWrite(t, filepath.Join(t.TempDir(), "invalid.json"), bad)
		var decoded studyMethodEvaluationPlan
		if err := studyMethodEvalReadJSON(file, &decoded); err == nil {
			if err := validateStudyMethodEvaluationPlan(decoded); err == nil {
				t.Fatal("invalid operative metadata became an ordinary/defaulted evaluation")
			}
		}
	}
}

func TestCCV2F3ParityAuthorityRefusals(t *testing.T) {
	plan, file := methodEvalTestFixture(t)
	authority := methodEvalFixtureAuthority(plan)
	if _, err := prepareStudyMethodEvaluation(file.Path, file.SHA256); err == nil {
		t.Fatal("CLI accepted synthetic self-reference as an authoritative E1 original")
	}
	for _, scenario := range []string{"arbitrary_same_hash_file", "false_rows", "unrelated_disabled_report", "unrelated_source", "enabled_parity_control"} {
		t.Run(scenario, func(t *testing.T) {
			data, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			var changed studyMethodEvaluationPlan
			if err := json.Unmarshal(data, &changed); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "arbitrary_same_hash_file":
				arbitrary := methodTestWrite(t, filepath.Join(t.TempDir(), "arbitrary.jsonl"), []byte("{}\n"))
				changed.Parity[0].OriginalDecisions, changed.Parity[0].ControlDecisions = arbitrary, arbitrary
			case "false_rows":
				changed.Parity[0].Rows--
			case "unrelated_disabled_report":
				changed.Parity[0].ControlComplete, changed.Parity[0].ControlManifest = changed.Parity[1].ControlComplete, changed.Parity[1].ControlManifest
			case "unrelated_source", "enabled_parity_control":
				data, err := os.ReadFile(plan.ParityControlPlan.Path)
				if err != nil {
					t.Fatal(err)
				}
				var control studyMethodPlan
				if err := json.Unmarshal(data, &control); err != nil {
					t.Fatal(err)
				}
				if scenario == "unrelated_source" {
					control.SourceRevision = strings.Repeat("b", 40)
				} else {
					control.Enabled = true
				}
				changed.ParityControlPlan = studyAuditTestJSON(t, filepath.Join(t.TempDir(), "parity-control.json"), control)
			}
			file := studyAuditTestJSON(t, filepath.Join(t.TempDir(), "bad-evaluation.json"), changed)
			if _, err := prepareStudyMethodEvaluationWithAuthority(file.Path, file.SHA256, authority); err == nil {
				t.Fatal("unproven parity was accepted")
			}
		})
	}
}

func TestCCV2F3EvaluationInventoryRefusals(t *testing.T) {
	plan, file := methodEvalTestFixture(t)
	p, err := prepareStudyMethodEvaluationWithAuthority(file.Path, file.SHA256, methodEvalFixtureAuthority(plan))
	if err != nil {
		t.Fatal(err)
	}
	variant := plan.Groups[0].Variants[0]
	originalComplete, err := os.ReadFile(variant.Complete.Path)
	if err != nil {
		t.Fatal(err)
	}
	var original studyReport
	if err := json.Unmarshal(originalComplete, &original); err != nil {
		t.Fatal(err)
	}
	originalEvidence, err := os.ReadFile(original.Experimental.Report.Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"budget", "missing_copy", "self_inventory", "tampered_output"} {
		t.Run(scenario, func(t *testing.T) {
			var report studyReport
			var evidence studyMethodRunReport
			if err := json.Unmarshal(originalComplete, &report); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(originalEvidence, &evidence); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "budget":
				evidence.Accounting.ChargedBytes = studyAuditOutputBytes + 1
			case "missing_copy":
				evidence.Copies = evidence.Copies[1:]
			case "self_inventory":
				evidence.Outputs[0].File = report.Experimental.Report
			case "tampered_output":
				evidence.Outputs[0].File.SHA256 = strings.Repeat("0", 64)
			}
			report.Experimental.Report = studyAuditTestJSON(t, report.Experimental.Report.Path, evidence)
			binding := studyMethodEvalBinding{Complete: studyAuditTestJSON(t, variant.Complete.Path, report), Manifest: variant.Manifest}
			input, err := p.input(plan.Groups[0].DateUTC)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := p.readReport(binding, input, p.Method, true, variant.CandidateID); err == nil {
				t.Fatal("counterfeit method inventory passed admission")
			}
			methodTestWrite(t, original.Experimental.Report.Path, originalEvidence)
			methodTestWrite(t, variant.Complete.Path, originalComplete)
		})
	}
}

func TestCCV2F3EvaluationMarkerAndIntegrityRefusals(t *testing.T) {
	plan, file := methodEvalTestFixture(t)
	p, err := prepareStudyMethodEvaluationWithAuthority(file.Path, file.SHA256, methodEvalFixtureAuthority(plan))
	if err != nil {
		t.Fatal(err)
	}
	binding := plan.Groups[0].Variants[0]
	data, err := os.ReadFile(binding.Complete.Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"null", "false", `{"unfinished":`} {
		// Preserve all earlier required facts. Refusal must happen on this marker,
		// not by converting its value into an absent typed pointer/default.
		start := bytes.Index(data, []byte(`"experimental":`))
		if start < 0 {
			t.Fatal("fixture lacks marker")
		}
		bad := append(append([]byte{}, data[:start]...), []byte(`"experimental":`+value+`}`)...)
		file := methodTestWrite(t, filepath.Join(t.TempDir(), "bad.json"), bad)
		var report studyReport
		if err := studyMethodEvalReadJSON(file, &report); err == nil {
			t.Fatal("invalid experimental marker was admitted")
		}
		if err := studyDiagnosticRejectReportMarker(bad); !errors.Is(err, errStudyExperimentalReportMarker) {
			t.Fatal("legacy marker refusal was weakened:", err)
		}
	}
	methodTestWrite(t, p.Method.Model.Path, []byte("tampered"))
	if _, err := prepareStudyMethodEvaluationWithAuthority(file.Path, file.SHA256, methodEvalFixtureAuthority(plan)); err == nil {
		t.Fatal("tampered requested model was admitted")
	}
}

func TestCCV2F3EvaluationJoinAndR2Refusals(t *testing.T) {
	d := studyTestDecision(strings.Repeat("a", 64)+":1", "episode", false, "K1ABC")
	d.Ordinal = 1
	d.RawSHA256 = hashAnyStable(d.Raw)
	d.ReceiverFamily = "N0BBB"
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	line := append(data, '\n')
	input := studyInput{Archive: studyFile{SHA256: strings.Repeat("a", 64)}, MaxRows: 2}
	for _, test := range []struct {
		data     []byte
		expected int
	}{{append(append([]byte{}, line...), line...), 2}, {line, 2}} {
		if err := studyReadDecisionStream(bytes.NewReader(test.data), input, 0, test.expected, make(map[int64]*studyPair)); err == nil {
			t.Fatal("duplicate/missing immutable ID was admitted")
		}
	}
	d.Raw.ReportDB++
	bad, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if err := studyReadDecisionStream(bytes.NewReader(bad), input, 0, 1, make(map[int64]*studyPair)); err == nil {
		t.Fatal("tampered raw/SNR payload was admitted")
	}
	d.R2 = studyReference{Reason: "unknown"}
	if err := studyMethodEvalR2Binding(d); err == nil {
		t.Fatal("actual R2 was silently replaced by its frozen reference")
	}
	d.DecisionAfterCutoff = true
	d.R2 = studyReference{Reason: "decision_after_cutoff"}
	if err := studyMethodEvalR2Binding(d); err != nil {
		t.Fatal(err)
	}
}

func TestCCV2F3EvaluationWriterRefusals(t *testing.T) {
	for _, fault := range []string{"budget", "write", "sync", "close", "rename"} {
		t.Run(fault, func(t *testing.T) {
			cause := errors.New("evaluation " + fault)
			ops := studyDiagnosticArtifactFileOps()
			open, rename := ops.open, ops.rename
			ops.open = func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
				file, err := open(path, flags, mode)
				if err != nil {
					return file, err
				}
				sink := &diagnosticArtifactFaultSink{studyAuditSink: file}
				switch fault {
				case "write":
					sink.writeErr = cause
				case "sync":
					sink.syncErr = cause
				case "close":
					sink.closeErr = cause
				}
				return sink, nil
			}
			ops.rename = func(a, b string) error {
				if fault == "rename" {
					return cause
				}
				return rename(a, b)
			}
			owner := diagnosticArtifactOwner(t, 1024, 128, ops)
			writer, err := newStudyMethodEvalWriter(owner, "paired.jsonl")
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.record(struct{ ID int }{1}); err != nil {
				t.Fatal(err)
			}
			if fault == "budget" {
				owner.quota.used = owner.quota.limit - owner.quota.reserve
			}
			closeErr := writer.close()
			if fault == "rename" {
				if closeErr != nil {
					t.Fatal(closeErr)
				}
				if err := owner.atomicJSON("COMPLETE.json", true); !errors.Is(err, cause) {
					t.Fatal("late final rename failure disappeared:", err)
				}
			} else {
				if closeErr == nil || owner.Err() == nil {
					t.Fatal("late buffered writer failure disappeared")
				}
				if err := owner.atomicJSON("COMPLETE.json", true); err == nil {
					t.Fatal("writer failure published COMPLETE")
				}
			}
			if _, err := os.Stat(filepath.Join(owner.root, "COMPLETE.json")); !os.IsNotExist(err) {
				t.Fatal("failed evaluation published COMPLETE")
			}
		})
	}
}
