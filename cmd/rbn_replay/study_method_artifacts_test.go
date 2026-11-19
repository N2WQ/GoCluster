package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func methodTestRun(t *testing.T) *studyMethodRun {
	t.Helper()
	run, err := newStudyMethodRun(t.TempDir(), methodTestPrepared(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.closeStreams() })
	run.files.scratchBackend("no_support_history_backend", "", 0, 0)
	return run
}

func methodTestReadReport(t *testing.T, manifest *studyMethodManifest) studyMethodRunReport {
	t.Helper()
	if err := studyCheckFile(manifest.Report); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(manifest.Report.Path)
	if err != nil {
		t.Fatal(err)
	}
	var report studyMethodRunReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	return report
}

func TestCCV2F2ArtifactLifecycle(t *testing.T) {
	run := methodTestRun(t)
	if run.files.prepared != nil || run.files.selected != nil || run.files.trace != nil || run.files.journal != nil {
		t.Fatal("method artifact context invented a selected diagnostic capture")
	}
	if err := run.record(struct {
		ID int `json:"id"`
	}{1}); err != nil {
		t.Fatal(err)
	}
	manifest, err := run.complete()
	if err != nil {
		t.Fatal(err)
	}
	report := methodTestReadReport(t, manifest)
	if manifest.Enabled || !manifest.Experimental || manifest.CandidateID != run.prepared.Candidate.ID || !report.Experimental || report.Enabled || len(report.Outputs) != 3 || len(report.Copies) != 2 {
		t.Fatalf("explicit disabled run lost its method/artifact identity: %+v", manifest)
	}
	if report.Accounting.LimitBytes != 4<<30 || report.Accounting.ReservedCapacityBytes != 16<<20 || report.RetainedEvidence.RegularFiles != 3 || report.RetainedEvidence.RegularFileBytes != report.Accounting.ChargedBytes {
		t.Fatal("run did not share the inherited cumulative accounting and finite inventory")
	}
	for _, output := range report.Outputs {
		if filepath.Base(output.File.Path) == "method-report.json" || filepath.Base(output.File.Path) == "complete.json" {
			t.Fatal("report pretended to hash itself or a later completion")
		}
		if err := studyCheckFile(output.File); err != nil {
			t.Fatal(err)
		}
	}
	owner := run.files.owner
	if _, err := os.Stat(filepath.Join(owner.root, "complete.json")); !os.IsNotExist(err) {
		t.Fatal("artifact completion published the caller's final completion")
	}
	if err := owner.atomicJSON("complete.json", struct {
		Experimental *studyMethodManifest `json:"experimental"`
	}{manifest}); err != nil {
		t.Fatal(err)
	}
	if err := run.closeStreams(); err != nil {
		t.Fatal("post-completion close was not idempotent:", err)
	}
}

func TestCCV2F2ArtifactLateFailures(t *testing.T) {
	for _, failure := range []string{"receipt_write", "receipt_sync", "receipt_close", "report_write", "report_sync", "report_close", "report_rename", "complete_rename"} {
		t.Run(failure, func(t *testing.T) {
			ops := studyDiagnosticArtifactFileOps()
			open, rename := ops.open, ops.rename
			cause := errors.New(failure)
			ops.open = func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
				file, err := open(path, flags, mode)
				if err != nil {
					return file, err
				}
				sink := &diagnosticArtifactFaultSink{studyAuditSink: file}
				receipt := filepath.Base(path) == "method-receipts.jsonl"
				report := filepath.Base(path) == "method-report.json.tmp"
				switch {
				case receipt && failure == "receipt_write", report && failure == "report_write":
					sink.writeErr = cause
				case receipt && failure == "receipt_sync", report && failure == "report_sync":
					sink.syncErr = cause
				case receipt && failure == "receipt_close", report && failure == "report_close":
					sink.closeErr = cause
				}
				return sink, nil
			}
			ops.rename = func(source, destination string) error {
				if failure == "report_rename" && filepath.Base(destination) == "method-report.json" || failure == "complete_rename" && filepath.Base(destination) == "complete.json" {
					return cause
				}
				return rename(source, destination)
			}
			run, err := newStudyMethodRunWithOps(t.TempDir(), methodTestPrepared(t), ops)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = run.closeStreams() })
			run.files.scratchBackend("no_support_history_backend", "", 0, 0)
			recordErr := run.record(struct {
				ID int `json:"id"`
			}{1})
			manifest, completeErr := run.complete()
			err = errors.Join(recordErr, completeErr)
			if failure == "complete_rename" {
				if err != nil || manifest == nil {
					t.Fatal("late completion fixture failed before caller publication:", err)
				}
				err = run.files.owner.atomicJSON("complete.json", manifest)
			} else if manifest != nil {
				t.Fatal("failed artifact finalization returned a completion manifest")
			}
			if !errors.Is(err, cause) || !errors.Is(run.files.owner.Err(), cause) {
				t.Fatal("late fault lost its first-cause latch:", err)
			}
			if err := run.failure(err); err != nil {
				t.Fatal("failure reservation could not record a bounded failure:", err)
			}
			if _, err := os.Stat(filepath.Join(run.files.owner.root, "FAILED.json")); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(run.files.owner.root, "complete.json")); !os.IsNotExist(err) {
				t.Fatal("late fault published COMPLETE")
			}
		})
	}
}

func TestCCV2F2ArtifactIntegrityRefusals(t *testing.T) {
	for _, failure := range []string{"copied_model", "source_model", "open_writer", "unsynced_writer", "unowned_file", "missing_backend", "budget"} {
		t.Run(failure, func(t *testing.T) {
			run := methodTestRun(t)
			owner := run.files.owner
			switch failure {
			case "copied_model":
				methodTestWrite(t, run.model.Path, []byte("tampered"))
			case "source_model":
				methodTestWrite(t, run.prepared.Plan.Model.Path, []byte("tampered"))
			case "open_writer", "unsynced_writer":
				file, err := owner.openFile("other.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600, false)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = file.Close() })
				if _, err := file.Write([]byte("{}\n")); err != nil {
					t.Fatal(err)
				}
				if failure == "unsynced_writer" {
					if err := file.Close(); err != nil {
						t.Fatal(err)
					}
				}
			case "unowned_file":
				methodTestWrite(t, filepath.Join(owner.root, "bypass"), []byte("unowned"))
			case "missing_backend":
				run.files.scratch.Backend = "not_constructed"
			case "budget":
				owner.quota.used = owner.quota.limit - owner.quota.reserve
				if err := run.record(struct {
					ID int `json:"id"`
				}{1}); err == nil {
					t.Fatal("evidence consumed the reserved metadata allowance")
				}
			}
			if manifest, err := run.complete(); err == nil || manifest != nil || owner.Err() == nil {
				t.Fatal("failed integrity or boundedness produced a success manifest")
			}
			if err := owner.atomicJSON("complete.json", true); err == nil {
				t.Fatal("latched failure permitted later completion")
			}
		})
	}
}

func TestCCV2F2ArtifactScratchExclusion(t *testing.T) {
	run := methodTestRun(t)
	owner := run.files.owner
	scratch := filepath.Join(owner.root, studyDiagnosticScratchChild)
	if err := os.Mkdir(scratch, 0o700); err != nil {
		t.Fatal(err)
	}
	methodTestWrite(t, filepath.Join(scratch, "simulated-db-file"), []byte("separate scratch bytes"))
	run.files.scratchBackend("custom_scp_runtime", scratch, 100, 64)
	manifest, err := run.complete()
	if err != nil {
		t.Fatal(err)
	}
	report := methodTestReadReport(t, manifest)
	if !report.Scratch.Applicable || report.Scratch.MaxKeys != 100 || report.Scratch.MaxSpottersPerKey != 64 || report.Scratch.Measurement == nil || report.Scratch.Measurement.RegularFiles != 1 || report.Scratch.Measurement.RegularFileBytes != 22 {
		t.Fatal("actual scratch was not separately measured with its observed caps")
	}
	if report.RetainedEvidence.RegularFiles != 3 || report.RetainedEvidence.RegularFileBytes != report.Accounting.ChargedBytes {
		t.Fatal("scratch leaked into first-party evidence accounting")
	}
}

func TestCCV2F2ArtifactFreezeFailure(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		prepared := methodTestPrepared(t)
		data := []byte("changed before ownership")
		if oversized {
			data = make([]byte, (4<<20)+1)
		}
		methodTestWrite(t, prepared.Plan.Model.Path, data)
		root := t.TempDir()
		if run, err := newStudyMethodRun(root, prepared); err == nil || run != nil {
			t.Fatal("changed input was accepted while freezing")
		}
		if _, err := os.Stat(filepath.Join(root, "FAILED.json")); err != nil {
			t.Fatal("constructor lost owned failure evidence:", err)
		}
		if _, err := os.Stat(filepath.Join(root, "complete.json")); !os.IsNotExist(err) {
			t.Fatal("failed constructor published COMPLETE")
		}
		if oversized {
			if _, err := os.Stat(filepath.Join(root, "method-model.json")); !os.IsNotExist(err) {
				t.Fatal("oversized source was copied before its individual bound was checked")
			}
		}
	}
}
