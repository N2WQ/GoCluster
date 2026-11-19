package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"dxcluster/config"
)

func diagnosticActualTraceFrames(t *testing.T, path string, states []studyDiagnosticSelectedState) map[string]studyDiagnosticCallCounts {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	selected := make(map[string]studyDiagnosticRawBinding, len(states))
	for _, state := range states {
		selected[state.Binding.RawID] = state.Binding
	}
	counts := make(map[string]studyDiagnosticCallCounts, len(states))
	sequences := make(map[string]int, len(states))
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 64<<10)
	var current *studyDiagnosticTraceRow
	bytesInFrame, rowsInFrame := 0, 0
	for scanner.Scan() {
		var row studyDiagnosticTraceRow
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		if selected[row.Binding.RawID] != row.Binding {
			t.Fatal("trace frame has nonselected/forged raw binding")
		}
		if row.Kind == "begin" {
			if current != nil || row.Sequence != sequences[row.Binding.RawID]+1 {
				t.Fatal("missing/reordered/interleaved capture begin")
			}
			copy := row
			current = &copy
			bytesInFrame, rowsInFrame = 0, 0
		}
		if current == nil || row.Sequence != current.Sequence || row.Stage != current.Stage || row.Binding != current.Binding {
			t.Fatal("capture rows escaped actual begin binding")
		}
		bytesInFrame += len(scanner.Bytes()) + 1
		rowsInFrame++
		if row.Kind == "end" {
			if row.Bytes != bytesInFrame || row.Rows != rowsInFrame {
				t.Fatal("actual end framing not complete")
			}
			value := counts[row.Binding.RawID]
			switch row.Stage {
			case "temporal_initial_selection":
				value.InitialSelector++
			case "pipeline_selection":
				value.PipelineSelector++
			case "temporal_evaluate":
				value.Evaluate++
			case "temporal_release_selection":
				value.Release++
			default:
				t.Fatal("unknown actual capture stage")
			}
			counts[row.Binding.RawID] = value
			sequences[row.Binding.RawID]++
			current = nil
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if current != nil {
		t.Fatal("unterminated actual capture")
	}
	return counts
}

func diagnosticSelectionForDay(t *testing.T, day *studyRawDay) *studyDiagnosticPrepared {
	t.Helper()
	selection := diagnosticSelectionFixture()
	for i := range selection.Days {
		selection.Days[i].SourceDecisions = studyAuditTestJSON(t, filepath.Join(t.TempDir(), "old.jsonl"), struct{ Frozen bool }{true})
	}
	selected := &selection.Days[2]
	selected.Selected = []studyDiagnosticSelectedRecord{}
	selected.AllFocalRows = 0
	for i := range day.Rows {
		raw := &day.Rows[i]
		if !studyFocalRow(*raw) {
			continue
		}
		selected.AllFocalRows++
		selected.Selected = append(selected.Selected, studyDiagnosticSelectedRecord{RawID: fmt.Sprintf("%s:%d", day.InputSHA256, raw.Ordinal), Ordinal: raw.Ordinal, RawSHA256: hashAnyStable(raw.Row), Categories: []string{"synthetic"}})
	}
	selected.SelectedCount = len(selected.Selected)
	file := studyAuditTestJSON(t, filepath.Join(t.TempDir(), "selection.json"), selection)
	return &studyDiagnosticPrepared{Selection: selection, File: file}
}

func diagnosticReadJSONFile(t *testing.T, path string, destination any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, destination); err != nil {
		t.Fatal(err)
	}
}

func TestDiagnosticRunActualReplayParityAndComplete(t *testing.T) {
	archive := studyTestZIP(t, "N0BBB,14020,20m,K1ABC,30,2026-11-15 00:10:00,CQ,CW\n"+
		"N0CCC,14020,20m,K1ABC,30,2026-11-15 00:10:01,CQ,CW\n"+
		"N0DDD,14020,20m,K1ABC,30,2026-11-15 00:10:02,CQ,CW\n"+
		"N0FFF,14020,20m,K1ABD,30,2026-11-15 00:10:03,CQ,CW\n"+
		"N0BBB,14020,20m,K1ABE,30,2026-11-15 00:10:04,CQ,CW\n"+
		"N0CCC,14020,20m,K1ABD,30,2026-11-15 00:10:05,CQ,CW\n"+
		"N0DDD,14020,20m,K1ABC,30,2026-11-15 00:10:06,CQ,CW\n")
	input := studyInput{DateUTC: "2026-11-15", Phase: "tuning", Archive: archive, MaxRows: 7, MaxCSVBytes: 8192}
	day, err := loadStudyRawDay(input, nil, 300)
	if err != nil {
		t.Fatal(err)
	}
	prepared := diagnosticSelectionForDay(t, day)
	var ordinaryDecisions []byte
	var ordinaryGates gatesSummary
	licenseSource := replayLicenseDB(t, "K1ABC", "K1ABD", "K1ABE")
	licenseSHA, err := studyHashFile(licenseSource)
	if err != nil {
		t.Fatal(err)
	}
	for _, diagnostic := range []bool{false, true} {
		cfg := &config.Config{CallCorrection: replayTemporalFreshConfig()}
		cfg.FCCULS.DBPath = licenseSource
		plan := studyTestPlan(t)
		studyTestBindGeography(t, &plan, cfg, studyTestCTYPoints())
		plan.Arms[0].Assets = append(plan.Arms[0].Assets, studyFile{Path: licenseSource, SHA256: licenseSHA})
		out := filepath.Join(t.TempDir(), "run")
		if err := os.Mkdir(out, 0o700); err != nil {
			t.Fatal(err)
		}
		req := studyCLIRequest{Output: out, Phase: "tuning", Date: input.DateUTC}
		if diagnostic {
			req.DiagnosticSelection, req.DiagnosticFreeze = prepared.File.Path, prepared.File.SHA256
			cwd, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}
			req.Output, err = filepath.Rel(cwd, out)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := studyRunDay(plan, "synthetic-plan", req, plan.Arms[0], plan.Candidates[0], input, cfg, day, false); err != nil {
			t.Fatal(err)
		}
		for _, asset := range plan.Arms[0].Assets {
			if err := studyCheckFile(asset); err != nil {
				t.Fatal("original frozen input changed:", err)
			}
		}
		decisions, err := os.ReadFile(filepath.Join(out, "decisions.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		var gates gatesSummary
		diagnosticReadJSONFile(t, filepath.Join(out, "gates.json"), &gates)
		if !diagnostic {
			ordinaryDecisions, ordinaryGates = decisions, gates
			continue
		}
		if !bytes.Equal(decisions, ordinaryDecisions) || !reflect.DeepEqual(gates, ordinaryGates) {
			t.Fatal("diagnostic instrumentation changed actual terminal receipts/gates/counters")
		}
		var complete studyReport
		diagnosticReadJSONFile(t, filepath.Join(out, "complete.json"), &complete)
		if complete.Diagnostics == nil || !complete.Diagnostics.NonQualification || !complete.Diagnostics.Exposed || complete.TerminalRows != 7 {
			t.Fatal("diagnostic run lacks complete non-qualification marker")
		}
		if err := studyCheckFile(complete.Diagnostics.Report); err != nil {
			t.Fatal(err)
		}
		var report studyDiagnosticRunReport
		diagnosticReadJSONFile(t, complete.Diagnostics.Report.Path, &report)
		if len(report.Selected) != 7 || report.AttemptEndNext-report.AttemptStartNext != report.AttemptCount || report.AttemptCount == 0 || report.Scratch.Applicable || report.Scratch.Backend != "no_support_history_backend" {
			t.Fatal("actual attempt/backend/selected provenance is incomplete")
		}
		frames := diagnosticActualTraceFrames(t, filepath.Join(out, "diagnostic-trace.jsonl"), report.Selected)
		evaluations := 0
		for _, state := range report.Selected {
			if state.Seen != 1 || state.Terminal != 1 || state.Calls.InitialSelector != 1 || state.Calls.PipelineSelector+state.Calls.Release != 1 || frames[state.Binding.RawID] != state.Calls || state.TraceCaptures != state.Calls.InitialSelector+state.Calls.PipelineSelector+state.Calls.Evaluate+state.Calls.Release || state.TraceCaptures+state.Terminal > 64 {
				t.Fatal("selected actual call/frame coverage incomplete:", state)
			}
			evaluations += state.Calls.Evaluate
		}
		if uint64(evaluations) != report.AttemptCount {
			t.Fatal("actual accepted-attempt Evaluate frames missing")
		}
		var bytesRetained int64
		for _, output := range report.Outputs {
			if err := studyCheckFile(output.File); err != nil {
				t.Fatal(err)
			}
			bytesRetained += output.Bytes
		}
		if bytesRetained != report.RetainedEvidence.RegularFileBytes || uint64(len(report.Outputs)) != report.RetainedEvidence.RegularFiles || report.Accounting.ChargedBytes != bytesRetained+report.DeletedCopyBytes || report.DeletedCopyBytes == 0 {
			t.Fatal("closed pre-report inventory/deleted license-copy accounting does not reconcile")
		}
		for _, copy := range report.Copies {
			if !copy.VerifiedBeforeCleanup {
				t.Fatal("unchecked source copy retained in successful report")
			}
		}
		if _, err := os.Stat(filepath.Join(out, "FAILED.json")); !os.IsNotExist(err) {
			t.Fatal("successful diagnostic invocation left FAILED")
		}
	}
}

func TestDiagnosticRunCorrectionDisabledStageZeros(t *testing.T) {
	archive := studyTestZIP(t, "N0BBB,14020,20m,K1ABC,30,2026-11-15 00:10:00,CQ,CW\n")
	input := studyInput{DateUTC: "2026-11-15", Phase: "tuning", Archive: archive, MaxRows: 1, MaxCSVBytes: 4096}
	day, err := loadStudyRawDay(input, nil, 300)
	if err != nil {
		t.Fatal(err)
	}
	prepared := diagnosticSelectionForDay(t, day)
	cfg := &config.Config{CallCorrection: replayTemporalFreshConfig()}
	cfg.CallCorrection.Enabled = false
	plan := studyTestPlan(t)
	studyTestBindGeography(t, &plan, cfg, studyTestCTYPoints())
	out := t.TempDir()
	req := studyCLIRequest{Output: out, Phase: "tuning", Date: input.DateUTC, DiagnosticSelection: prepared.File.Path, DiagnosticFreeze: prepared.File.SHA256}
	if err := studyRunDay(plan, "disabled", req, plan.Arms[0], plan.Candidates[0], input, cfg, day, false); err != nil {
		t.Fatal(err)
	}
	var complete studyReport
	diagnosticReadJSONFile(t, filepath.Join(out, "complete.json"), &complete)
	var report studyDiagnosticRunReport
	diagnosticReadJSONFile(t, complete.Diagnostics.Report.Path, &report)
	if report.Scratch.Applicable || report.Scratch.Backend != "correction_disabled" || report.AttemptCount != 0 || len(report.Selected) != 1 || report.Selected[0].Calls != (studyDiagnosticCallCounts{}) || report.Selected[0].Terminal != 1 {
		t.Fatal("disabled early guards invented stage calls/backend/attempts")
	}
	var execution studyDiagnosticExecutionRecord
	diagnosticReadJSONFile(t, filepath.Join(out, "diagnostic-execution.jsonl"), &execution)
	if execution.Calls != (studyDiagnosticCallCounts{}) || execution.Execution.ConfirmChecked || !execution.Execution.FinalLicenseChecked || !execution.Execution.OuterLearningHelperReached {
		t.Fatal("zero calls or outer-only learning reachability were inferred incorrectly")
	}
}

func TestDiagnosticRunActualScratchAfterCleanup(t *testing.T) {
	archive := studyTestZIP(t, "N0BBB,14020,20m,K1ABC,30,2026-11-15 00:10:00,CQ,CW\n")
	input := studyInput{DateUTC: "2026-11-15", Phase: "tuning", Archive: archive, MaxRows: 1, MaxCSVBytes: 4096}
	day, err := loadStudyRawDay(input, nil, 300)
	if err != nil {
		t.Fatal(err)
	}
	prepared := diagnosticSelectionForDay(t, day)
	cfg := &config.Config{CallCorrection: studyTestCustomSCPConfig()}
	plan := studyTestPlan(t)
	studyTestBindGeography(t, &plan, cfg, studyTestCTYPoints())
	out := t.TempDir()
	req := studyCLIRequest{Output: out, Date: input.DateUTC, Phase: "tuning", DiagnosticSelection: prepared.File.Path, DiagnosticFreeze: prepared.File.SHA256}
	if err := studyRunDay(plan, "scratch", req, plan.Arms[0], plan.Candidates[0], input, cfg, day, false); err != nil {
		t.Fatal(err)
	}
	var complete studyReport
	diagnosticReadJSONFile(t, filepath.Join(out, "complete.json"), &complete)
	var report studyDiagnosticRunReport
	diagnosticReadJSONFile(t, complete.Diagnostics.Report.Path, &report)
	if !report.Scratch.Applicable || report.Scratch.Backend != "custom_scp_runtime" || report.Scratch.Path != filepath.Join(out, studyDiagnosticScratchChild) || report.Scratch.MaxKeys != 8 || report.Scratch.MaxSpottersPerKey != 8 || report.Scratch.Measurement == nil || !report.Scratch.Measurement.Complete || report.Scratch.Measurement.RegularFileBytes == 0 || report.Scratch.Measurement.Stage != "post_existing_cleanup_with_unobserved_database_close_errors" {
		t.Fatal("actual constructed backend/post-cleanup scratch provenance missing")
	}
	for _, output := range report.Outputs {
		rel, err := filepath.Rel(out, output.File.Path)
		if err != nil {
			t.Fatal(err)
		}
		if studyDiagnosticReservedScratchChild(rel) {
			t.Fatal("scratch database was misrepresented as a quota-owned file")
		}
	}
}
