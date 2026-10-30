package filter

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Invalid expression semantics are intentional: reset recognizes frozen shape
// and discards old rules without invoking today's compiler.
const v7OldProfileLiteral = "configuration_version: 6\nconfiguration:\n  filters:\n    rules: [{slot: 0, action: pass, expr: 'OLD PROFILE NOT A CURRENT EXPRESSION'}]\n    include_beacons: false\n    allow_wwv: false\n    allow_wcy: false\n    allow_announce: false\n    allow_self: false\n    allow_toxic: false\n  settings:\n    dialect: cc\n    grid: FN31\n    noise_class: URBAN\n    dedupe_policy: SLOW\n    path_min_observation_count: 42\n    solar_summary_minutes: 30\nrecent_ips: [192.0.2.1]\nlast_login_utc: 2026-10-21T12:00:00Z\n"

const v7OldPresetLiteral = "configuration_version: 6\nconfiguration:\n  filters:\n    rules: [{slot: 2, action: reject, expr: 'OLD PRESET NOT A CURRENT EXPRESSION'}]\n  settings: {dialect: cc}\n"

func v7MixedLibrary(t *testing.T) []byte {
	t.Helper()
	set, err := V7PresetFromConfiguration(v7TestConfiguration(t), "cc")
	if err != nil {
		t.Fatal(err)
	}
	current, err := EncodeV7SavedPreset(set)
	if err != nil {
		t.Fatal(err)
	}
	return []byte("presets:\n  OLD:\n    " + strings.ReplaceAll(strings.TrimSpace(v7OldPresetLiteral), "\n", "\n    ") + "\n  NEW:\n    " + strings.ReplaceAll(strings.TrimSpace(string(current)), "\n", "\n    ") + "\n")
}

func TestV7ProfileResetLiteralWithoutOldCompilation(t *testing.T) {
	usePresetTestDir(t)
	raw := []byte(v7OldProfileLiteral)
	path := writeUpgradeFixture(t, "LZ3NY-11", raw)
	libraryRaw := v7MixedLibrary(t)
	library := writeUpgradeLibrary(t, libraryRaw)
	record, outcome, err := UpgradeV7UserRecordForLoginWithOutcome("LZ3NY-11", nil)
	if err != nil || !outcome.ProfileReset || !outcome.PresetLibraryReset || outcome.LibraryError != nil || outcome.LibraryResetGeneration != 7 {
		t.Fatalf("reset outcome=%+v, %v", outcome, err)
	}
	if record.Configuration.Settings.Dialect != "cc" || record.Configuration.Settings.Grid != "FN31" || record.Configuration.Settings.DedupePolicy != "SLOW" || record.Presets.GO != nil || record.Presets.CC != nil || record.Notice.ProfileResetGeneration != 7 {
		t.Fatal("reset changed shared values, retained refs, or lost notice event")
	}
	for _, toggle := range record.Configuration.Filters.GO.toggles() {
		if toggle != DefaultBoolDefault {
			t.Fatal("GO control was preserved instead of reset")
		}
	}
	for _, toggle := range record.Configuration.Filters.CC.toggles() {
		if toggle != DefaultBoolDefault {
			t.Fatal("CC control was preserved instead of reset")
		}
	}
	if len(record.Configuration.Filters.GO.Rules) != 1 || record.Configuration.Filters.GO.Rules[0].Expr == "OLD PROFILE NOT A CURRENT EXPRESSION" {
		t.Fatal("old profile rules survived")
	}
	assertUpgradeBytes(t, path+v7BackupSuffix, raw)
	assertUpgradeBytes(t, library+v7BackupSuffix, libraryRaw)
	if _, err := LoadV7Preset("LZ3NY-12", "OLD"); !errors.Is(err, ErrPresetNotFound) {
		t.Fatalf("old preset remained loadable: %v", err)
	}
	if _, err := LoadV7Preset("LZ3NY-12", "NEW"); err != nil {
		t.Fatal("valid current preset was removed", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := UpgradeV7UserRecordForLoginWithOutcome("LZ3NY-11", nil)
	if err != nil || second.ProfileReset || second.PresetLibraryReset {
		t.Fatalf("reset repeated: %+v, %v", second, err)
	}
	assertUpgradeBytes(t, path, before)
}

func TestV7LibraryPartialCommitRetryPreservesOtherSSIDPreset(t *testing.T) {
	usePresetTestDir(t)
	raw := []byte(v7OldProfileLiteral)
	profile := writeUpgradeFixture(t, "LZ3NY-11", raw)
	writeUpgradeFixture(t, "LZ3NY-12", raw)
	libraryRaw := v7MixedLibrary(t)
	library := writeUpgradeLibrary(t, libraryRaw)
	injected := errors.New("profile replacement refused")
	record, outcome, err := UpgradeV7UserRecordForLoginWithOutcome("LZ3NY-11", func(*PreparedUserRecord) error { return injected })
	if record != nil || !errors.Is(err, injected) || outcome.ProfileReset || !outcome.PresetLibraryReset {
		t.Fatalf("wrong partial result: %+v %v", outcome, err)
	}
	assertUpgradeBytes(t, profile, raw)
	set, err := V7PresetFromConfiguration(v7TestConfiguration(t), "go")
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveV7Preset("LZ3NY-12", "FRESH", set); err != nil {
		t.Fatal(err)
	}
	newLibrary, err := os.ReadFile(library)
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []string{"LZ3NY-11", "LZ3NY-12"} {
		_, result, err := UpgradeV7UserRecordForLoginWithOutcome(call, nil)
		if err != nil || !result.ProfileReset || result.PresetLibraryReset {
			t.Fatalf("retry %s: %+v %v", call, result, err)
		}
		assertUpgradeBytes(t, library, newLibrary)
	}
	assertUpgradeBytes(t, library+v7BackupSuffix, libraryRaw)
	if _, err := LoadV7Preset("LZ3NY", "FRESH"); err != nil {
		t.Fatal(err)
	}
}

func TestV7LibraryPendingDoesNotPreventBackedUpProfileReset(t *testing.T) {
	usePresetTestDir(t)
	raw := []byte(v7OldProfileLiteral)
	profile := writeUpgradeFixture(t, "LZ3NY-11", raw)
	libraryRaw := v7MixedLibrary(t)
	library := writeUpgradeLibrary(t, libraryRaw)
	fault := errors.New("library backup failed")
	store := v7UpgradeStore{backup: func(path string, data []byte) error {
		if path == library+v7BackupSuffix {
			return fault
		}
		return writeLegacyOriginalBackup(path, data)
	}}
	record, outcome, err := store.upgrade("LZ3NY-11", nil)
	if err != nil || record == nil || !outcome.ProfileReset || outcome.PresetLibraryReset || !errors.Is(outcome.LibraryError, fault) {
		t.Fatalf("independent reset: %+v %v", outcome, err)
	}
	assertUpgradeBytes(t, library, libraryRaw)
	current, err := os.ReadFile(profile)
	if err != nil {
		t.Fatal(err)
	}
	_, retry, err := UpgradeV7UserRecordForLoginWithOutcome("LZ3NY-11", nil)
	if err != nil || retry.ProfileReset || !retry.PresetLibraryReset {
		t.Fatalf("pending library retry: %+v %v", retry, err)
	}
	assertUpgradeBytes(t, profile, current)
}

func TestV7ResetFailureProtectsOriginalAndInitialMissingAuthority(t *testing.T) {
	for _, failure := range []string{"backup", "write"} {
		t.Run(failure, func(t *testing.T) {
			usePresetTestDir(t)
			raw := []byte(v7OldProfileLiteral)
			path := writeUpgradeFixture(t, "LZ3NY-11", raw)
			fault := fmt.Errorf("injected %s: %w", failure, os.ErrNotExist)
			store := v7UpgradeStore{}
			if failure == "backup" {
				store.backup = func(string, []byte) error { return fault }
			} else {
				store.write = func(string, []byte) error { return fault }
			}
			record, outcome, err := store.upgrade("LZ3NY-11", nil)
			if record != nil || outcome.ProfileReset || !errors.Is(err, fault) || errors.Is(err, ErrLoginUpgradeProfileMissing) {
				t.Fatalf("late failure granted creation: %+v %v", outcome, err)
			}
			assertUpgradeBytes(t, path, raw)
		})
	}
	usePresetTestDir(t)
	if _, _, err := UpgradeV7UserRecordForLoginWithOutcome("LZ3NY-99", nil); !errors.Is(err, ErrLoginUpgradeProfileMissing) {
		t.Fatalf("initial absence lost creation sentinel: %v", err)
	}
}

func TestV7PresetSharedStripeKeepsFreshConcurrentSave(t *testing.T) {
	usePresetTestDir(t)
	writeUpgradeFixture(t, "LZ3NY-11", []byte(v7OldProfileLiteral))
	writeUpgradeFixture(t, "LZ3NY-12", []byte(v7OldProfileLiteral))
	writeUpgradeLibrary(t, v7MixedLibrary(t))
	set, err := V7PresetFromConfiguration(v7TestConfiguration(t), "go")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan error, 2)
	wg.Go(func() {
		_, _, err := UpgradeV7UserRecordForLoginWithOutcome("LZ3NY-11", func(record *PreparedUserRecord) error { close(entered); <-release; return record.Commit() })
		results <- err
	})
	<-entered
	wg.Go(func() { results <- SaveV7Preset("LZ3NY-12", "FRESH", set) })
	close(release)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := UpgradeV7UserRecordForLoginWithOutcome("LZ3NY-12", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadV7Preset("LZ3NY", "FRESH"); err != nil {
		t.Fatal(err)
	}
}

func TestV7MalformedFutureLibraryStaysProtected(t *testing.T) {
	for _, raw := range []string{"presets: [broken]", "configuration_version: 8\nreset_generation: 8\npresets: {}\n"} {
		t.Run(fmt.Sprintf("case%d", len(raw)), func(t *testing.T) {
			usePresetTestDir(t)
			writeUpgradeFixture(t, "LZ3NY-11", []byte(v7OldProfileLiteral))
			path := writeUpgradeLibrary(t, []byte(raw))
			_, outcome, err := UpgradeV7UserRecordForLoginWithOutcome("LZ3NY-11", nil)
			if err != nil || !outcome.ProfileReset || outcome.LibraryError == nil {
				t.Fatalf("bad library affected profile: %+v %v", outcome, err)
			}
			assertUpgradeBytes(t, path, []byte(raw))
			if _, err := ListV7Presets("LZ3NY"); err == nil {
				t.Fatal("unsupported library exposed preset access")
			}
			if _, err := os.Stat(filepath.Clean(path + v7BackupSuffix)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unadmitted library created backup")
			}
		})
	}
}

func TestV7PresetSavePartialSuccessPreservesPreparedProfile(t *testing.T) {
	usePresetTestDir(t)
	c := v7TestConfiguration(t)
	set, err := V7PresetFromConfiguration(c, "cc")
	if err != nil {
		t.Fatal(err)
	}
	record := &V7UserRecord{Configuration: c}
	prepared, err := PrepareV7UserRecord("LZ3NY-11", record)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(prepared.data)
	fault := errors.New("profile association failed")
	saved, err := SaveV7PresetAndRecord("LZ3NY-11", "NAMED", set, func() (*PreparedUserRecord, error) { return prepared, nil }, func(*PreparedUserRecord) error { return fault })
	if !saved || !errors.Is(err, fault) {
		t.Fatalf("partial SAVE result: %v %v", saved, err)
	}
	assertUpgradeBytes(t, prepared.path, before)
	if _, err := LoadV7Preset("LZ3NY-12", "NAMED"); err != nil {
		t.Fatal(err)
	}
}

func TestV7ResetViewAdmissionPrecedesBackupsAndReplacements(t *testing.T) {
	usePresetTestDir(t)
	profileRaw := []byte(v7OldProfileLiteral)
	profile := writeUpgradeFixture(t, "LZ3NY-11", profileRaw)
	libraryRaw := v7MixedLibrary(t)
	library := writeUpgradeLibrary(t, libraryRaw)
	fault := errors.New("wrapped human view exceeds admission")
	called := false
	record, outcome, err := UpgradeV7UserRecordForLoginWithAdmission("LZ3NY-11", nil, func(candidate *V7UserRecord) error {
		called = true
		if candidate.Notice.ProfileResetGeneration != 7 {
			t.Fatal("callback did not receive reset candidate")
		}
		return fault
	})
	if !called || record != nil || !errors.Is(err, fault) || outcome.ProfileReset || outcome.PresetLibraryReset {
		t.Fatalf("admission refusal: %+v %v", outcome, err)
	}
	assertUpgradeBytes(t, profile, profileRaw)
	assertUpgradeBytes(t, library, libraryRaw)
	for _, path := range []string{profile + v7BackupSuffix, library + v7BackupSuffix} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("view refusal created original backup", err)
		}
	}
}
