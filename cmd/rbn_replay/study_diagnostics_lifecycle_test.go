package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/config"
)

func TestDiagnosticRunLateCompleteFailures(t *testing.T) {
	for _, failure := range []string{"write", "sync", "close", "rename"} {
		t.Run(failure, func(t *testing.T) {
			archive := studyTestZIP(t, "N0BBB,14020,20m,K1ABC,30,2026-11-15 00:10:00,CQ,CW\n")
			input := studyInput{DateUTC: "2026-11-15", Phase: "tuning", Archive: archive, MaxRows: 1, MaxCSVBytes: 4096}
			day, err := loadStudyRawDay(input, nil, 300)
			if err != nil {
				t.Fatal(err)
			}
			prepared := diagnosticSelectionForDay(t, day)
			ops := studyDiagnosticArtifactFileOps()
			open, rename := ops.open, ops.rename
			sentinel := errors.New("late complete " + failure)
			ops.open = func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
				file, err := open(path, flags, mode)
				if filepath.Base(path) == "complete.json.tmp" {
					sink := &diagnosticArtifactFaultSink{studyAuditSink: file}
					switch failure {
					case "write":
						sink.writeErr = sentinel
					case "sync":
						sink.syncErr = sentinel
					case "close":
						sink.closeErr = sentinel
					}
					return sink, err
				}
				return file, err
			}
			ops.rename = func(source, destination string) error {
				if failure == "rename" && filepath.Base(destination) == "complete.json" {
					return sentinel
				}
				return rename(source, destination)
			}
			prepared.artifactOps = &ops
			cfg := &config.Config{CallCorrection: replayTemporalFreshConfig()}
			cfg.CallCorrection.TemporalDecoder.Enabled = false
			plan := studyTestPlan(t)
			studyTestBindGeography(t, &plan, cfg, studyTestCTYPoints())
			out := t.TempDir()
			req := studyCLIRequest{Output: out, Date: input.DateUTC, Phase: "tuning", DiagnosticSelection: prepared.File.Path, DiagnosticFreeze: prepared.File.SHA256}
			err = studyRunDayPrepared(plan, "late-complete", req, plan.Arms[0], plan.Candidates[0], input, cfg, day, false, prepared)
			if !errors.Is(err, sentinel) {
				t.Fatal("actual run lost late COMPLETE failure:", err)
			}
			if _, err := os.Stat(filepath.Join(out, "complete.json")); !os.IsNotExist(err) {
				t.Fatal("late writer failure published COMPLETE")
			}
			if _, err := os.Stat(filepath.Join(out, "FAILED.json")); err != nil {
				t.Fatal("writable failure marker absent:", err)
			}
			if _, err := os.Stat(filepath.Join(out, "diagnostic-report.json")); err != nil {
				t.Fatal("late fault fixture failed before diagnostic finalization:", err)
			}
			entries, err := os.ReadDir(out)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), ".license-inputs-") {
					t.Fatal("COMPLETE attempted before ordinary license cleanup returned")
				}
			}
		})
	}
}

func TestDiagnosticRunIntegrityAndSelectedCompletenessRefusals(t *testing.T) {
	for _, failure := range []string{"asset_changed", "selection_changed", "missing_selected"} {
		t.Run(failure, func(t *testing.T) {
			archive := studyTestZIP(t, "N0BBB,14020,20m,K1ABC,30,2026-11-15 00:10:00,CQ,CW\n")
			input := studyInput{DateUTC: "2026-11-15", Phase: "tuning", Archive: archive, MaxRows: 1, MaxCSVBytes: 4096}
			day, err := loadStudyRawDay(input, nil, 300)
			if err != nil {
				t.Fatal(err)
			}
			prepared := diagnosticSelectionForDay(t, day)
			if failure == "missing_selected" {
				prepared.Selection.Days[2].Selected[0].Ordinal = 2
			}
			ops := studyDiagnosticArtifactFileOps()
			open := ops.open
			out := t.TempDir()
			ops.open = func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
				if filepath.Base(path) == "manifest.json.tmp" {
					if failure == "asset_changed" {
						entries, err := os.ReadDir(filepath.Join(out, ".study-assets"))
						if err != nil {
							return nil, err
						}
						for _, entry := range entries {
							if !entry.IsDir() {
								if err := os.WriteFile(filepath.Join(out, ".study-assets", entry.Name()), []byte("changed"), 0o600); err != nil {
									return nil, err
								}
								break
							}
						}
					}
					if failure == "selection_changed" {
						if err := os.WriteFile(prepared.File.Path, []byte("changed"), 0o600); err != nil {
							return nil, err
						}
					}
				}
				return open(path, flags, mode)
			}
			prepared.artifactOps = &ops
			cfg := &config.Config{CallCorrection: replayTemporalFreshConfig()}
			cfg.CallCorrection.TemporalDecoder.Enabled = false
			plan := studyTestPlan(t)
			studyTestBindGeography(t, &plan, cfg, studyTestCTYPoints())
			req := studyCLIRequest{Output: out, Date: input.DateUTC, Phase: "tuning", DiagnosticSelection: prepared.File.Path, DiagnosticFreeze: prepared.File.SHA256}
			if err := studyRunDayPrepared(plan, "integrity", req, plan.Arms[0], plan.Candidates[0], input, cfg, day, false, prepared); err == nil {
				t.Fatal("forged/mutated/incomplete diagnostic run accepted")
			}
			if _, err := os.Stat(filepath.Join(out, "complete.json")); !os.IsNotExist(err) {
				t.Fatal("failed integrity run published COMPLETE")
			}
		})
	}
	if _, err := prepareStudyDiagnostics(studyCLIRequest{DiagnosticFreeze: strings.Repeat("a", 64)}); err == nil {
		t.Fatal("private study preparation silently fell back to ordinary for an incomplete pair")
	}
}
