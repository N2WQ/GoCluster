package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDiagnosticReplayNamedSinkPreservesFlagsAndAccounting(t *testing.T) {
	owner := diagnosticArtifactOwner(t, 4096, 0, studyDiagnosticArtifactFileOps())
	path := filepath.Join(owner.root, "samples.log")
	if err := os.WriteFile(path, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := openReplayArtifactWithDiagnostics(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644, owner)
	if err != nil {
		t.Fatal(err)
	}
	if file.Name() != path {
		t.Fatal("named runbook adapter lost its path")
	}
	fmt.Fprint(file, "add") // Existing runbook callers intentionally ignore n/error.
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "beforeadd" || owner.quota.used != 3 {
		t.Fatal("append flags/debits changed:", err)
	}
	file, err = openReplayArtifactWithDiagnostics(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644, owner)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(file, "z")
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(path)
	if err != nil || string(data) != "z" || owner.quota.used != 4 {
		t.Fatal("truncate refunded previous writes:", err)
	}
}

func TestDiagnosticReplayRelativeSinkNameIsActualOwnedPath(t *testing.T) {
	owner := diagnosticArtifactOwner(t, 4096, 0, studyDiagnosticArtifactFileOps())
	file, err := openReplayArtifactWithDiagnostics("runbook.log", os.O_CREATE|os.O_WRONLY, 0o600, owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("owned")); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(file.Name())
	if err != nil || string(data) != "owned" || file.Name() != filepath.Join(owner.root, "runbook.log") {
		t.Fatal("adapter Name did not identify actual owned bytes:", err)
	}
}

func TestDiagnosticReplayCopyAdapterKeepsIdentityReceiptAndSourceOwnership(t *testing.T) {
	for _, success := range []bool{true, false} {
		t.Run(fmt.Sprintf("success_%t", success), func(t *testing.T) {
			limit := int64(3)
			if !success {
				limit = 2
			}
			owner := diagnosticArtifactOwner(t, limit, 0, studyDiagnosticArtifactFileOps())
			r := newReplayRunner(replayRequest{})
			r.diagnostics = &studyDiagnosticRun{owner: owner}
			path := filepath.Join(t.TempDir(), "source.db")
			if err := os.WriteFile(path, []byte("abc"), 0o600); err != nil {
				t.Fatal(err)
			}
			source, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			input := replayLicenseInput{SourcePath: path, Availability: "test"}
			destination := filepath.Join(owner.root, "frozen.db")
			err = r.copyFrozenLicenseFile(source, destination, &input)
			if _, statErr := source.Stat(); statErr != nil {
				t.Fatal("adapter closed the caller-owned source:", statErr)
			}
			if !success {
				if err == nil || owner.Err() == nil || len(r.diagnostics.copies) != 0 || input.Bytes != 0 || input.SHA256 != "" {
					t.Fatal("failed adapter published a successful copy receipt or identity:", err)
				}
				return
			}
			if err != nil || owner.Err() != nil || len(r.diagnostics.copies) != 1 {
				t.Fatal("successful adapter did not register its receipt:", err)
			}
			const expectedSHA = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
			got, err := os.ReadFile(destination)
			receipt := r.diagnostics.copies[0].Receipt
			if err != nil || string(got) != "abc" || input.Bytes != 3 || input.SHA256 != expectedSHA || input.SourcePath != path || input.Availability != "test" || receipt.Path != destination || receipt.SHA256 != expectedSHA || receipt.BytesCopied != 3 || receipt.SourceSizeAtStat != 3 || owner.quota.used != 3 {
				t.Fatalf("copy adapter identity/provenance differs from actual bytes: input=%+v receipt=%+v err=%v", input, receipt, err)
			}
		})
	}
}

func TestDiagnosticReplayIgnoredRunbookWriteLatches(t *testing.T) {
	owner := diagnosticArtifactOwner(t, 3, 0, studyDiagnosticArtifactFileOps())
	file, err := openReplayArtifactWithDiagnostics("runbook.log", os.O_CREATE|os.O_WRONLY, 0o644, owner)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(file, "four")
	if owner.Err() == nil || owner.quota.used != 0 {
		t.Fatal("ignored Fprintf failure disappeared")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.atomicJSON("complete.json", true); err == nil {
		t.Fatal("ignored runbook failure permitted completion")
	}
}

func TestDiagnosticReplayCSVParityAndIgnoredClose(t *testing.T) {
	rows := []intervalRow{{T0: "2026/11/07 00:00:00", T1: "2026/11/07 00:01:00", Minutes: 1, DStateSplit: 3, DEPTH: 7}}
	ordinary := filepath.Join(t.TempDir(), "ordinary.csv")
	if err := writeIntervalsCSV(ordinary, rows); err != nil {
		t.Fatal(err)
	}
	expected, err := os.ReadFile(ordinary)
	if err != nil {
		t.Fatal(err)
	}
	owner := diagnosticArtifactOwner(t, 1<<20, 128, studyDiagnosticArtifactFileOps())
	path := filepath.Join(owner.root, "intervals.csv")
	if err := writeIntervalsCSVWithDiagnostics(path, rows, owner); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, expected) || owner.quota.used != int64(len(expected)) {
		t.Fatal("CSV changed content or bypassed quota:", err)
	}
	sentinel := errors.New("late CSV close")
	owner = diagnosticArtifactOwner(t, 1<<20, 128, studyDiagnosticArtifactFileOps())
	open := owner.ops.open
	owner.ops.open = func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
		f, err := open(path, flags, mode)
		if err != nil {
			return nil, err
		}
		return &diagnosticArtifactFaultSink{studyAuditSink: f, closeErr: sentinel}, nil
	}
	// The old helper ignores deferred Close. The diagnostic wrapper must latch
	// it without changing that helper's ordinary returned result.
	if err := writeIntervalsCSVWithDiagnostics(filepath.Join(owner.root, "intervals.csv"), rows, owner); err != nil || !errors.Is(owner.Err(), sentinel) {
		t.Fatal("late CSV close was lost or ordinary return changed:", err)
	}
	if err := owner.atomicJSON("complete.json", true); !errors.Is(err, sentinel) {
		t.Fatal("late CSV close permitted completion")
	}
}

func TestDiagnosticReplayHistoryBothWritersShareBudget(t *testing.T) {
	owner := diagnosticArtifactOwner(t, 1<<20, 128, studyDiagnosticArtifactFileOps())
	manifest := replayManifest{DateUTC: "2026-11-07", StartedAtUTC: "2026-11-18T12:00:00Z", FinishedAtUTC: "2026-11-18T12:00:01Z", ArchiveDir: owner.root, OutputDir: owner.root}
	if err := writeReplayRunHistoryWithDiagnostics(owner.root, "", nil, manifest, owner); err != nil {
		t.Fatal(err)
	}
	measurement, err := measureStudyDiagnosticEvidence(owner.root, "")
	if err != nil || measurement.RegularFiles != 2 || measurement.RegularFileBytes != owner.quota.used {
		t.Fatalf("history snapshot/index bypassed shared debit: %+v used=%d err=%v", measurement, owner.quota.used, err)
	}
	index := filepath.Join(owner.root, "rbn_replay_history", "runs.jsonl")
	before, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := appendJSONLWithDiagnostics(index, struct {
		More bool `json:"more"`
	}{true}, owner); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(index)
	if err != nil || !bytes.HasPrefix(after, before) || string(after[len(before):]) != "{\"more\":true}\n" {
		t.Fatal("history index lost APPEND contract:", err)
	}
	measurement, err = measureStudyDiagnosticEvidence(owner.root, "")
	if err != nil || measurement.RegularFileBytes != owner.quota.used {
		t.Fatal("appended index bytes escaped accounting:", err)
	}
}

func TestDiagnosticReplayHistoryIgnoredIndexCloseLatches(t *testing.T) {
	owner := diagnosticArtifactOwner(t, 1<<20, 128, studyDiagnosticArtifactFileOps())
	open := owner.ops.open
	sentinel := errors.New("late history index close")
	owner.ops.open = func(path string, flags int, mode os.FileMode) (studyAuditSink, error) {
		file, err := open(path, flags, mode)
		if err != nil {
			return nil, err
		}
		if filepath.Base(path) == "runs.jsonl" {
			return &diagnosticArtifactFaultSink{studyAuditSink: file, closeErr: sentinel}, nil
		}
		return file, nil
	}
	manifest := replayManifest{DateUTC: "2026-11-07", FinishedAtUTC: "2026-11-18T12:00:01Z"}
	if err := writeReplayRunHistoryWithDiagnostics(owner.root, "", nil, manifest, owner); err != nil || !errors.Is(owner.Err(), sentinel) {
		t.Fatal("ignored history index close did not latch:", err)
	}
	if err := owner.atomicJSON("complete.json", true); !errors.Is(err, sentinel) {
		t.Fatal("history index close permitted completion")
	}
}
