package filter

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type v7ResetBoundary struct {
	name, owner, stage string
	existingBackup     bool
	fatal              bool
	profileReset       bool
	libraryReset       bool
}

func v7ResetBoundaryCases() []v7ResetBoundary {
	// Literal outcomes follow the two independent-file contract: library
	// failures permit a backed-up profile reset; profile failures are fatal;
	// only profile replacement failure follows a successful library reset.
	groups := []struct {
		owner                             string
		stages                            []string
		fatal, profileReset, libraryReset bool
	}{
		{"profile", []string{"read", "prepare", "admit"}, true, false, false},
		{"library", []string{"read", "prepare"}, false, true, false},
		{"profile-backup", []string{"write", "sync", "close"}, true, false, false},
		{"library-backup", []string{"write", "sync", "close"}, false, true, false},
		{"library-replacement", []string{"write", "sync", "close", "replace"}, false, true, false},
		{"profile-replacement", []string{"write", "sync", "close", "replace"}, true, false, true},
	}
	var result []v7ResetBoundary
	for _, group := range groups {
		for _, stage := range group.stages {
			result = append(result, v7ResetBoundary{name: group.owner + "/" + stage, owner: group.owner, stage: stage,
				fatal: group.fatal, profileReset: group.profileReset, libraryReset: group.libraryReset})
		}
	}
	for _, owner := range []string{"profile-backup", "library-backup"} {
		for _, stage := range []string{"sync", "close"} {
			result = append(result, v7ResetBoundary{name: owner + "/verified-existing-" + stage, owner: owner, stage: stage, existingBackup: true,
				fatal: owner == "profile-backup", profileReset: owner == "library-backup"})
		}
	}
	return result
}

func v7ResetCountedStages(stage string, failure error, hits *int) atomicUserFileStages {
	stages := upgradeFaultStages(stage, failure)
	switch stage {
	case "write":
		write := stages.write
		stages.write = func(file *os.File, data []byte) error { *hits++; return write(file, data) }
	case "sync":
		syncFile := stages.sync
		stages.sync = func(file *os.File) error { *hits++; return syncFile(file) }
	case "close":
		closeFile := stages.close
		stages.close = func(file *os.File) error { *hits++; return closeFile(file) }
	case "replace":
		replace := stages.replace
		stages.replace = func(from, to string) error { *hits++; return replace(from, to) }
	}
	return stages
}

func v7ResetBoundaryStore(t *testing.T, boundary v7ResetBoundary, profile, library string, failure error, hits *int) v7UpgradeStore {
	t.Helper()
	store := v7UpgradeStore{}
	store.read = func(path string, limit int) ([]byte, error) {
		if boundary.owner == "profile" && path == profile || boundary.owner == "library" && path == library {
			switch boundary.stage {
			case "read":
				*hits++
				return nil, failure
			case "prepare":
				// Exercise actual decoder preparation with injected returned
				// bytes. The authoritative on-disk original remains untouched.
				*hits++
				if path == profile {
					return []byte("configuration_version: 6\nconfiguration: null\n"), nil
				}
				return []byte("presets: null\n"), nil
			}
		}
		return readUpgradeFile(path, limit)
	}
	if boundary.stage == "admit" {
		store.admitProfile = func(record *V7UserRecord) error {
			*hits++
			if record.ConfigurationVersion != 7 || record.Notice.ProfileResetGeneration != 7 {
				t.Fatal("view admission did not receive a complete reset candidate")
			}
			return failure
		}
	}
	store.backup = func(path string, data []byte) error {
		if boundary.owner == "profile-backup" && path == profile+v7BackupSuffix || boundary.owner == "library-backup" && path == library+v7BackupSuffix {
			return writeLegacyOriginalBackupWithStages(path, data, v7ResetCountedStages(boundary.stage, failure, hits))
		}
		return writeLegacyOriginalBackup(path, data)
	}
	store.write = func(path string, data []byte) error {
		if boundary.owner == "profile-replacement" && path == profile || boundary.owner == "library-replacement" && path == library {
			return writeAtomicUserFileWithStages(path, data, v7ResetCountedStages(boundary.stage, failure, hits))
		}
		return writeAtomicUserFile(path, data)
	}
	return store
}

func v7ResetLibraryImage(t *testing.T, path string, original []byte, reset bool) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reset {
		if !bytes.Equal(data, original) {
			t.Fatal("failed replacement exposed a partial library or reset marker")
		}
		return
	}
	collection, err := DecodeV7PresetCollection(data)
	if err != nil || collection.ConfigurationVersion != 7 || collection.ResetGeneration != 7 || collection.Presets["OLD"] != nil || collection.Presets["NEW"] == nil {
		t.Fatalf("library marker and old-preset deletion were not one valid image: %v", err)
	}
}

func v7ResetNoTemporaryFiles(t *testing.T) {
	t.Helper()
	if err := filepath.WalkDir(UserDataDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".tmp") {
			t.Errorf("failed file operation retained temporary file %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func v7ResetPreserveCompletedProfile(t *testing.T, callsign string, fresh *V7SavedPreset) []byte {
	t.Helper()
	record, err := LoadV7UserRecord(callsign)
	if err != nil {
		t.Fatal(err)
	}
	// Literal post-reset edits differ from every starting default. Retrying
	// unfinished library/notice work must not reset these filters or controls.
	record.Configuration.Filters.GO.Rules = nil
	record.Configuration.Filters.GO.AllowSelf = DefaultBoolFalse
	record.Configuration.Filters.CC.Modes, record.Configuration.Filters.CC.Sources = nil, nil
	record.Configuration.Filters.CC.AllowToxic = DefaultBoolFalse
	if fresh != nil {
		record.Presets.GO = &V7PresetReference{Name: "FRESH", Baseline: fresh}
	}
	record.Notice.ProfileAcknowledgedGeneration = 7
	prepared, err := PrepareV7UserRecord(callsign, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	return bytes.Clone(prepared.data)
}

func v7ResetMetadataAcknowledgement(t *testing.T, callsign string) {
	t.Helper()
	record, err := LoadV7UserRecord(callsign)
	if err != nil {
		t.Fatal(err)
	}
	configuration, references := record.Configuration.Clone(), record.Presets
	// Simulates only the metadata write after a receipt. The telnet tests own
	// actual flush acknowledgement, session ownership and current-record reload.
	record.Notice.ProfileAcknowledgedGeneration, record.Notice.LibraryAcknowledgedGeneration = 7, 7
	prepared, err := PrepareV7UserRecord(callsign, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	after, err := LoadV7UserRecord(callsign)
	if err != nil || !after.Configuration.Equal(configuration) || !reflect.DeepEqual(after.Presets, references) {
		t.Fatal("metadata-only acknowledgement changed native banks or references")
	}
	_, outcome, err := UpgradeV7UserRecordForLoginWithOutcome(callsign, nil)
	if err != nil || outcome.ProfileReset || outcome.PresetLibraryReset {
		t.Fatalf("acknowledged retry performed another reset: %+v %v", outcome, err)
	}
	assertUpgradeBytes(t, prepared.path, prepared.data)
}

func v7ResetBoundaryRecover(t *testing.T, boundary v7ResetBoundary, profile, otherProfile, library string, original, libraryOriginal []byte) {
	t.Helper()
	var editedProfile []byte
	if boundary.profileReset {
		// Profile-event acknowledgement precedes completion of the failed
		// library reset. The later library event must remain independently due.
		editedProfile = v7ResetPreserveCompletedProfile(t, "LZ3NY-11", nil)
	}
	_, outcome, err := UpgradeV7UserRecordForLoginWithOutcome("LZ3NY-12", nil)
	if err != nil || !outcome.ProfileReset || outcome.PresetLibraryReset == boundary.libraryReset || outcome.LibraryError != nil || outcome.LibraryResetGeneration != 7 {
		t.Fatalf("other-SSID recovery: %+v %v", outcome, err)
	}
	v7ResetLibraryImage(t, library, libraryOriginal, true)
	if boundary.profileReset {
		assertUpgradeBytes(t, profile, editedProfile)
	}
	fresh, err := V7PresetFromConfiguration(v7TestConfiguration(t), "go")
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveV7Preset("LZ3NY-12", "FRESH", fresh); err != nil {
		t.Fatal(err)
	}
	freshLibrary, err := os.ReadFile(library)
	if err != nil {
		t.Fatal(err)
	}
	if boundary.profileReset {
		editedProfile = v7ResetPreserveCompletedProfile(t, "LZ3NY-11", fresh)
	}
	record, retried, err := UpgradeV7UserRecordForLoginWithOutcome("LZ3NY-11", nil)
	if err != nil || record == nil || retried.ProfileReset == boundary.profileReset || retried.PresetLibraryReset || retried.LibraryError != nil || retried.LibraryResetGeneration != 7 {
		t.Fatalf("original-SSID retry: %+v %v", retried, err)
	}
	if boundary.profileReset {
		assertUpgradeBytes(t, profile, editedProfile)
		if record.Notice.ProfileAcknowledgedGeneration != 7 || record.Notice.LibraryAcknowledgedGeneration != 0 {
			t.Fatal("late library completion erased or invented a notice acknowledgement")
		}
	}
	assertUpgradeBytes(t, library, freshLibrary)
	v7ResetMetadataAcknowledgement(t, "LZ3NY-11")
	assertUpgradeBytes(t, library, freshLibrary)
	assertUpgradeBytes(t, profile+v7BackupSuffix, original)
	assertUpgradeBytes(t, otherProfile+v7BackupSuffix, original)
	assertUpgradeBytes(t, library+v7BackupSuffix, libraryOriginal)
	collection, err := LoadV7PresetCollection("LZ3NY-12")
	if err != nil || len(collection.Presets) != 2 || collection.Presets["OLD"] != nil || collection.Presets["NEW"] == nil || collection.Presets["FRESH"] == nil || collection.ResetGeneration != 7 {
		t.Fatalf("retry resurrected old presets or removed a new preset: %v", err)
	}
	if _, err := LoadV7Preset("LZ3NY-11", "OLD"); !errors.Is(err, ErrPresetNotFound) {
		t.Fatalf("cleared OLD preset returned after retries: %v", err)
	}
	loaded, err := LoadV7Preset("LZ3NY-11", "FRESH")
	if err != nil {
		t.Fatal(err)
	}
	expected, err := EncodeV7SavedPreset(fresh)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := EncodeV7SavedPreset(loaded)
	if err != nil || !bytes.Equal(actual, expected) {
		t.Fatal("retry changed the newly saved preset's content")
	}
	assertUpgradeBytes(t, library, freshLibrary)
	v7ResetNoTemporaryFiles(t)
}

func TestV7ResetPersistenceBoundariesRetryAcrossSSIDs(t *testing.T) {
	for _, boundary := range v7ResetBoundaryCases() {
		t.Run(boundary.name, func(t *testing.T) {
			usePresetTestDir(t)
			original := []byte(v7OldProfileLiteral)
			profile := writeUpgradeFixture(t, "LZ3NY-11", original)
			other := writeUpgradeFixture(t, "LZ3NY-12", original)
			libraryOriginal := v7MixedLibrary(t)
			library := writeUpgradeLibrary(t, libraryOriginal)
			if boundary.existingBackup {
				path, data := profile, original
				if boundary.owner == "library-backup" {
					path, data = library, libraryOriginal
				}
				if err := os.WriteFile(path+v7BackupSuffix, data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			failure, hits := fmt.Errorf("injected %s: %w", boundary.name, os.ErrPermission), 0
			store := v7ResetBoundaryStore(t, boundary, profile, library, failure, &hits)
			record, outcome, err := store.upgrade("LZ3NY-11", nil)
			if hits != 1 || (err != nil) != boundary.fatal || outcome.ProfileReset != boundary.profileReset || outcome.PresetLibraryReset != boundary.libraryReset || errors.Is(err, ErrLoginUpgradeProfileMissing) {
				t.Fatalf("wrong first boundary outcome: hits=%d outcome=%+v error=%v", hits, outcome, err)
			}
			if boundary.fatal && boundary.stage != "prepare" && !errors.Is(err, failure) {
				t.Fatalf("fatal boundary lost its cause: %v", err)
			}
			if boundary.fatal && record != nil {
				t.Fatal("failed profile replacement returned usable reset preferences")
			}
			libraryFailure := strings.HasPrefix(boundary.owner, "library")
			if (outcome.LibraryError != nil) != libraryFailure || libraryFailure && boundary.stage != "prepare" && !errors.Is(outcome.LibraryError, failure) {
				t.Fatalf("independent library failure was lost or invented: %v", outcome.LibraryError)
			}
			if strings.HasSuffix(boundary.owner, "-backup") {
				path, data := profile, original
				if boundary.owner == "library-backup" {
					path, data = library, libraryOriginal
				}
				if boundary.existingBackup {
					assertUpgradeBytes(t, path+v7BackupSuffix, data)
				} else if _, err := os.Stat(path + v7BackupSuffix); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("failed original backup left its own incomplete file", err)
				}
			}
			if boundary.profileReset {
				if record == nil || record.Notice.ProfileResetGeneration != 7 || record.Notice.ProfileAcknowledgedGeneration != 0 || record.Configuration.Settings.Grid != "FN31" {
					t.Fatal("independent profile reset lost its event or preserved settings")
				}
			} else {
				assertUpgradeBytes(t, profile, original)
			}
			v7ResetLibraryImage(t, library, libraryOriginal, boundary.libraryReset)
			assertUpgradeBytes(t, other, original)
			v7ResetNoTemporaryFiles(t)
			v7ResetBoundaryRecover(t, boundary, profile, other, library, original, libraryOriginal)
		})
	}
}
