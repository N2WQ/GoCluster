package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDiagnosticReservedScratchWriterBoundary(t *testing.T) {
	for _, name := range []string{"custom_scp_runtime", "CUSTOM_SCP_RUNTIME", filepath.Join("custom_scp_runtime", "artifact.json")} {
		t.Run(name, func(t *testing.T) {
			a := diagnosticArtifactOwner(t, 1024, 0, studyDiagnosticArtifactFileOps())
			opened := false
			a.ops.open = func(string, int, os.FileMode) (studyAuditSink, error) {
				opened = true
				return nil, errors.New("must not open")
			}
			if _, err := a.openFile(name, os.O_CREATE|os.O_WRONLY, 0o600, false); err == nil || a.Err() == nil || opened || a.quota.used != 0 {
				t.Fatal("reserved lexical destination reached the opener or did not latch")
			}
		})
	}
	t.Run("resolved_alias", func(t *testing.T) {
		a := diagnosticArtifactOwner(t, 1024, 0, studyDiagnosticArtifactFileOps())
		a.ops.resolve = func(string) (string, error) { return filepath.Join(a.root, studyDiagnosticScratchChild), nil }
		if _, err := a.destination(filepath.Join(a.root, "alias", "artifact")); err == nil || a.Err() == nil {
			t.Fatal("resolved reserved child accepted")
		}
	})
	for _, name := range []string{filepath.Join("custom_scp_runtime_extra", "artifact"), filepath.Join("nested", "custom_scp_runtime", "artifact")} {
		t.Run(name, func(t *testing.T) {
			a := diagnosticArtifactOwner(t, 1024, 0, studyDiagnosticArtifactFileOps())
			if err := os.MkdirAll(filepath.Dir(filepath.Join(a.root, name)), 0o700); err != nil {
				t.Fatal(err)
			}
			f := diagnosticArtifactOpen(t, a, name)
			if _, err := f.Write([]byte("abc")); err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(f.Sync(), f.Close()); err != nil || a.quota.used != 3 {
				t.Fatal("ordinary sibling/nested artifact escaped accounting:", err)
			}
		})
	}
}

func TestDiagnosticEvidenceExactScratchExclusion(t *testing.T) {
	root := t.TempDir()
	scratch := filepath.Join(root, studyDiagnosticScratchChild)
	for _, name := range []string{studyDiagnosticScratchChild, "custom_scp_runtime_extra", filepath.Join("nested", studyDiagnosticScratchChild)} {
		if err := os.MkdirAll(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for name, data := range map[string]string{
		filepath.Join(studyDiagnosticScratchChild, "db"):                 "database",
		filepath.Join("custom_scp_runtime_extra", "evidence"):            "abc",
		filepath.Join("nested", studyDiagnosticScratchChild, "evidence"): "12345",
		"top": "z",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := measureStudyDiagnosticEvidence(root, scratch)
	if err != nil || !got.Complete || got.RegularFileBytes != 9 || got.RegularFiles != 3 || got.VisitedEntries != 8 || got.Stage != "retained_evidence_after_cleanup_before_diagnostic_report_and_complete" {
		t.Fatalf("exact exclusion totals: %+v %v", got, err)
	}
	whole, err := measureStudyDiagnosticEvidence(root, "")
	if err != nil || whole.RegularFileBytes != 17 || whole.RegularFiles != 4 || whole.VisitedEntries != 9 || whole.Stage != got.Stage {
		t.Fatalf("absent backend excluded data: %+v %v", whole, err)
	}
	db, err := measureStudyDiagnosticScratch(scratch)
	if err != nil || db.RegularFileBytes != 8 || db.RegularFiles != 1 || db.VisitedEntries != 2 {
		t.Fatalf("standalone scratch skipped itself: %+v %v", db, err)
	}
	for _, excluded := range []string{filepath.Join(root, "custom_scp_runtime_extra"), filepath.Join(root, "nested", studyDiagnosticScratchChild), filepath.Join(root, "missing")} {
		result, err := measureStudyDiagnosticEvidence(root, excluded)
		diagnosticScratchAssertFailure(t, result, err)
	}
}

func TestDiagnosticEvidenceExcludedChildMustBeDirectoryWithoutLinks(t *testing.T) {
	for _, mode := range []os.FileMode{0, os.ModeSymlink | os.ModeDir} {
		root := filepath.Join(t.TempDir(), "root")
		tree := diagnosticScratchTestTree(root)
		excluded := tree.add(root, studyDiagnosticScratchChild, mode, 10)
		result, err := measureStudyDiagnosticTree(root, excluded, diagnosticScratchTestLimits(), tree.ops())
		diagnosticScratchAssertFailure(t, result, err)
		if tree.active != 0 || tree.opens != tree.closes {
			t.Fatal("failure leaked handles")
		}
	}
	t.Run("skip_does_not_hide_bound", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "root")
		tree := diagnosticScratchTestTree(root)
		excluded := tree.add(root, studyDiagnosticScratchChild, os.ModeDir, 0)
		limits := diagnosticScratchTestLimits()
		limits.Entries = 1
		result, err := measureStudyDiagnosticTree(root, excluded, limits, tree.ops())
		diagnosticScratchAssertFailure(t, result, err)
		if tree.returned != 1 || tree.active != 0 {
			t.Fatal("excluded root child was not charged once as overflow probe")
		}
	})
}
