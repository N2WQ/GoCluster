package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func diagnosticFileOwner(t *testing.T) *studyDiagnosticRun {
	t.Helper()
	d := &studyDiagnosticRun{owner: diagnosticArtifactOwner(t, 1<<20, 128, studyDiagnosticArtifactFileOps()), files: make(map[string]*studyDiagnosticFileState)}
	d.trackFiles()
	return d
}

func TestDiagnosticRunRegistryActiveAndSyncedCloses(t *testing.T) {
	for _, scenario := range []string{"complete", "still_open", "unsynced"} {
		t.Run(scenario, func(t *testing.T) {
			d := diagnosticFileOwner(t)
			first, err := d.owner.openFile("append", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644, false)
			if err != nil {
				t.Fatal(err)
			}
			second, err := d.owner.openFile("append", os.O_WRONLY|os.O_APPEND, 0o644, false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := first.Write([]byte("abc")); err != nil {
				t.Fatal(err)
			}
			if _, err := second.Write([]byte("de")); err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(first.Sync(), first.Close()); err != nil {
				t.Fatal(err)
			}
			if len(d.files) != 1 {
				t.Fatal("repeated APPEND created duplicate physical inventory paths")
			}
			for _, state := range d.files {
				if state.Active != 1 {
					t.Fatal("first Close masked the second active APPEND handle")
				}
			}
			if scenario == "still_open" {
				if _, _, err := d.closedOutputs(); err == nil || d.owner.Err() == nil {
					t.Fatal("open output was accepted for final hashing")
				}
			}
			if scenario != "unsynced" {
				if err := second.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			if err := second.Close(); err != nil {
				t.Fatal(err)
			}
			if scenario == "still_open" {
				// Cleanup may make the files hashable. It must never clear the
				// earlier refusal or permit a successful completion marker.
				cause := d.owner.Err()
				if cause == nil {
					t.Fatal("closing the last handle cleared the original failure")
				}
				if err := d.owner.atomicJSON("complete.json", true); !errors.Is(err, cause) {
					t.Fatal("an earlier open-output failure permitted completion:", err)
				}
				return
			}
			outputs, deleted, err := d.closedOutputs()
			if scenario == "complete" {
				if err != nil || len(outputs) != 1 || outputs[0].Bytes != 5 || deleted != 0 {
					t.Fatal("closed APPEND inventory changed:", err)
				}
				if err := studyCheckFile(outputs[0].File); err != nil {
					t.Fatal(err)
				}
			} else if err == nil || d.owner.Err() == nil {
				t.Fatal("open/unsynced evidence lost its failure latch")
			}
		})
	}
}

func TestDiagnosticRunRegistry256BeforeOpen(t *testing.T) {
	d := diagnosticFileOwner(t)
	for i := range 256 {
		file, err := d.owner.openFile(fmt.Sprintf("file-%03d", i), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			t.Fatal(err)
		}
	}
	if len(d.files) != 256 {
		t.Fatal("literal registry boundary changed")
	}
	if _, err := d.owner.openFile("overflow", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600, false); err == nil || len(d.files) != 256 {
		t.Fatal("257th registry path grew/opened before rejection")
	}
	if _, err := os.Stat(filepath.Join(d.owner.root, "overflow")); !os.IsNotExist(err) {
		t.Fatal("path-budget rejection touched filesystem")
	}
}

func diagnosticTrackedCopy(t *testing.T, d *studyDiagnosticRun, destination string) studyDiagnosticCopyReceipt {
	t.Helper()
	sourcePath := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(sourcePath, []byte("copy-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	receipt, copyErr := d.owner.copyFile(destination, source)
	if err := errors.Join(copyErr, source.Close()); err != nil {
		t.Fatal(err)
	}
	if err := d.noteCopy(receipt); err != nil {
		t.Fatal(err)
	}
	return receipt
}

func TestDiagnosticRunCopyRenameAndExactDeletion(t *testing.T) {
	t.Run("retained_asset", func(t *testing.T) {
		d := diagnosticFileOwner(t)
		receipt := diagnosticTrackedCopy(t, d, filepath.Join(d.owner.root, "asset"))
		charged := d.owner.quota.used
		destination := filepath.Join(d.owner.root, "renamed")
		if err := d.renameAsset(receipt.Path, destination); err != nil {
			t.Fatal(err)
		}
		if d.owner.quota.used != charged || d.copies[0].Receipt.Path != destination {
			t.Fatal("rename double-debited or retained an obsolete copy path")
		}
		if err := d.verifyCopies(false); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(destination, []byte("changed"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := d.verifyCopies(true); err == nil {
			t.Fatal("retained frozen asset mutation excused as cleanup")
		}
	})
	t.Run("verified_license", func(t *testing.T) {
		d := diagnosticFileOwner(t)
		licenseRoot := filepath.Join(d.owner.root, ".license-inputs-fixture")
		if err := os.Mkdir(licenseRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		receipt := diagnosticTrackedCopy(t, d, filepath.Join(licenseRoot, "input"))
		if err := d.beforeCleanup(licenseRoot); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(receipt.Path); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(licenseRoot); err != nil {
			t.Fatal(err)
		}
		if err := d.verifyCopies(true); err != nil {
			t.Fatal(err)
		}
		outputs, deleted, err := d.closedOutputs()
		if err != nil || len(outputs) != 0 || deleted != 10 || d.owner.quota.used != 10 || !d.copies[0].DeletedAfterCleanup {
			t.Fatal("verified exact license-copy deletion did not reconcile without refund:", err)
		}
	})
	t.Run("other_missing_file", func(t *testing.T) {
		d := diagnosticFileOwner(t)
		licenseRoot := filepath.Join(d.owner.root, ".license-inputs-actual")
		if err := os.Mkdir(licenseRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		receipt := diagnosticTrackedCopy(t, d, filepath.Join(d.owner.root, ".license-inputs-prefix-sibling-file"))
		if err := d.beforeCleanup(licenseRoot); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(receipt.Path); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(licenseRoot); err != nil {
			t.Fatal(err)
		}
		if err := d.verifyCopies(true); err == nil {
			t.Fatal("a prefix-like unrelated deletion was excused")
		}
	})
}

func TestDiagnosticRunMissingSelectedCannotComplete(t *testing.T) {
	day, _ := diagnosticFoundationDay()
	prepared := diagnosticSelectionForDay(t, day)
	d, err := newStudyDiagnosticRun(t.TempDir(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.bindDay(studyInput{DateUTC: "2026-11-15", Archive: studyFile{SHA256: day.InputSHA256}}, day, 1); err != nil {
		t.Fatal(err)
	}
	d.scratchBackend("correction_disabled", "", 0, 0)
	if marker, err := d.complete(1); err == nil || marker != nil || !strings.Contains(err.Error(), "missing an input or terminal") {
		t.Fatal("incomplete selected input/terminal falsely completed:", err)
	}
	if err := d.closeStreams(); err != nil {
		t.Fatal(err)
	}
}
