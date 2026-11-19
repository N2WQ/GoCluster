package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"dxcluster/config"
)

func TestCCV2F2ActualStudyRun(t *testing.T) {
	archive := studyTestZIP(t, "N0BBB,14020,20m,K1ABC,30,2026-11-15 00:10:00,CQ,CW\n")
	input := studyInput{DateUTC: "2026-11-15", Phase: "tuning", Archive: archive, MaxRows: 1, MaxCSVBytes: 4096}
	var baseline []byte
	for _, mode := range []string{"ordinary", "disabled", "enabled"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &config.Config{CallCorrection: replayTemporalFreshConfig()}
			cfg.CallCorrection.TemporalDecoder.Enabled = false
			plan := studyTestPlan(t)
			studyTestBindGeography(t, &plan, cfg, studyTestCTYPoints())
			day, err := loadStudyRawDay(input, nil, 300)
			if err != nil {
				t.Fatal(err)
			}
			arm := plan.Arms[1]
			out := t.TempDir()
			req := studyCLIRequest{Output: out, Date: input.DateUTC, Phase: "tuning", Arm: "repaired_off"}
			if mode != "ordinary" {
				methodPlan := methodTestPlan(t)
				methodPlan.SourceRevision = arm.SourceRevision
				methodPlan.Enabled = mode == "enabled"
				methodReq, _ := methodTestRequest(t, methodPlan)
				req.MethodPlan, req.MethodFreeze, req.MethodCandidate = methodReq.MethodPlan, methodReq.MethodFreeze, methodReq.MethodCandidate
			}
			if err := studyRunDayPrepared(plan, "method-run-fixture", req, arm, plan.Candidates[0], input, cfg, day, false, nil); err != nil {
				t.Fatal(err)
			}
			decisions, err := os.ReadFile(filepath.Join(out, "decisions.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			if len(bytes.TrimSpace(decisions)) == 0 {
				t.Fatal("actual study fixture produced no focal receipt")
			}
			if mode == "ordinary" {
				baseline = decisions
			} else if mode == "disabled" && !bytes.Equal(baseline, decisions) {
				t.Fatal("disabled method changed actual complete decision bytes")
			}
			data, err := os.ReadFile(filepath.Join(out, "complete.json"))
			if err != nil {
				t.Fatal(err)
			}
			var report studyReport
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatal(err)
			}
			if !report.Complete || report.FocalRows != 1 || report.TerminalRows != 1 {
				t.Fatal("actual study did not finish the complete focal population")
			}
			if mode == "ordinary" {
				if report.Experimental != nil {
					t.Fatal("ordinary run acquired method marker")
				}
			} else {
				if report.Experimental == nil || report.Experimental.Enabled != (mode == "enabled") {
					t.Fatal("explicit method marker/false identity lost")
				}
				if err := studyCheckFile(report.Experimental.Report); err != nil {
					t.Fatal(err)
				}
				if err := studyDiagnosticRejectReportMarker(data); err == nil {
					t.Fatal("complete method run admitted by legacy report reader")
				}
				methodReport := methodTestReadReport(t, report.Experimental)
				if methodReport.Accounting.LimitBytes != 4<<30 || methodReport.Scratch.Backend == "not_constructed" {
					t.Fatal("actual runner bypassed artifact/scratch hooks")
				}
			}
		})
	}
}
