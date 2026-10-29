package filter

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoginUpgradeProfileMissingAuthorityOnlyOnInitialRead(t *testing.T) {
	usePresetTestDir(t)
	if record, changed, err := UpgradeUserRecordForLogin("LZ3NY-11", nil); record != nil || changed ||
		!errors.Is(err, ErrLoginUpgradeProfileMissing) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing initial profile lost creation authority/compatibility: %v", err)
	}
	original := []byte("configuration_version: 4\nallow_self: false\n")
	path := writeUpgradeFixture(t, "LZ3NY-11", original)
	missing := fmt.Errorf("later stage: %w", os.ErrNotExist)
	t.Run("backup", func(t *testing.T) {
		store := loginUpgradeStore{backup: func(string, []byte) error { return missing }}
		record, changed, err := store.upgrade("LZ3NY-11", nil)
		if record != nil || changed || !errors.Is(err, os.ErrNotExist) || errors.Is(err, ErrLoginUpgradeProfileMissing) {
			t.Fatalf("backup failure granted new-user authority: %v", err)
		}
		assertUpgradeBytes(t, path, original)
	})
	t.Run("record callback first fails second would succeed", func(t *testing.T) {
		calls := 0
		record, changed, err := UpgradeUserRecordForLogin("LZ3NY-11", func(*PreparedUserRecord) error {
			calls++
			if calls == 1 {
				return missing
			}
			return nil
		})
		if record != nil || changed || calls != 1 || !errors.Is(err, os.ErrNotExist) || errors.Is(err, ErrLoginUpgradeProfileMissing) {
			t.Fatalf("commit failure granted creation authority/retried: calls=%d error=%v", calls, err)
		}
		assertUpgradeBytes(t, path, original)
	})
	t.Run("library replace", func(t *testing.T) {
		libraryOriginal := []byte("presets:\n  OLD: {configuration_version: 4, modes: {FT8: true}}\n")
		library := writeUpgradeLibrary(t, libraryOriginal)
		store := loginUpgradeStore{write: func(string, []byte) error { return missing }}
		record, changed, err := store.upgrade("LZ3NY-11", nil)
		if record != nil || changed || !errors.Is(err, os.ErrNotExist) || !errors.Is(err, ErrLoginUpgradeLibrary) ||
			errors.Is(err, ErrLoginUpgradeProfileMissing) {
			t.Fatalf("library replacement failure granted creation authority: %v", err)
		}
		assertUpgradeBytes(t, path, original)
		assertUpgradeBytes(t, library, libraryOriginal)
	})
}

func upgradeFaultStages(stage string, failure error) atomicUserFileStages {
	return atomicUserFileStages{
		write: func(file *os.File, data []byte) error {
			if stage == "write" {
				_, _ = file.Write([]byte("partial"))
				return failure
			}
			return writeUserFileBytes(file, data)
		},
		sync: func(file *os.File) error {
			if stage == "sync" {
				return failure
			}
			return file.Sync()
		},
		close: func(file *os.File) error {
			if stage == "close" {
				return failure
			}
			return file.Close()
		},
		replace: func(from, to string) error {
			if stage == "replace" {
				return failure
			}
			return replaceUserFile(from, to)
		},
	}
}

func TestLoginUpgradeBackupAndAtomicFailureStages(t *testing.T) {
	for _, target := range []string{"profile-backup", "library-backup", "library", "profile"} {
		for _, stage := range []string{"write", "sync", "close", "replace"} {
			if strings.HasSuffix(target, "-backup") && stage == "replace" {
				continue // Exclusive original backups never replace an existing file.
			}
			t.Run(target+"/"+stage, func(t *testing.T) {
				usePresetTestDir(t)
				original := []byte(retiredV5LiteralRecord)
				profile := writeUpgradeFixture(t, "LZ3NY-11", original)
				libraryOriginal := []byte("presets:\n  OLD: {configuration_version: 5, configuration: " + retiredV5LiteralConfiguration + "}\n")
				library := writeUpgradeLibrary(t, libraryOriginal)
				failure := errors.New("injected " + stage)
				store := loginUpgradeStore{
					backup: func(path string, data []byte) error {
						match := target == "profile-backup" && filepath.Base(path) == "LZ3NY-11.yaml"+legacyBackupSuffix ||
							target == "library-backup" && strings.Contains(path, string(filepath.Separator)+"presets"+string(filepath.Separator))
						if match {
							return writeLegacyOriginalBackupWithStages(path, data, upgradeFaultStages(stage, failure))
						}
						return writeLegacyOriginalBackup(path, data)
					},
					write: func(path string, data []byte) error {
						if target == "library" && path == library || target == "profile" && filepath.Base(path) == "LZ3NY-11.yaml" {
							return writeAtomicUserFileWithStages(path, data, upgradeFaultStages(stage, failure))
						}
						return writeAtomicUserFile(path, data)
					},
				}
				if _, changed, err := store.upgrade("LZ3NY-11", nil); !errors.Is(err, failure) || changed {
					t.Fatal("failure lost or reported success", changed, err)
				}
				assertUpgradeBytes(t, profile, original)
				if target != "profile" {
					assertUpgradeBytes(t, library, libraryOriginal)
				}
				if _, changed, err := UpgradeUserRecordForLogin("LZ3NY-11", nil); err != nil || !changed {
					t.Fatal("durable retry failed", err)
				}
				assertUpgradeBytes(t, profile+legacyBackupSuffix, original)
				assertUpgradeBytes(t, library+legacyBackupSuffix, libraryOriginal)
			})
		}
	}
}

func TestLoginUpgradeNeverOverwritesDifferentBackup(t *testing.T) {
	for _, libraryConflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "profile", true: "library"}[libraryConflict], func(t *testing.T) {
			usePresetTestDir(t)
			original := []byte("configuration_version: 4\nallmodes: true\n")
			profile := writeUpgradeFixture(t, "LZ3NY-11", original)
			libraryOriginal := []byte("presets:\n  OLD: {configuration_version: 4, allbands: true}\n")
			library := writeUpgradeLibrary(t, libraryOriginal)
			backup := profile + legacyBackupSuffix
			if libraryConflict {
				backup = library + legacyBackupSuffix
			}
			if err := os.WriteFile(backup, []byte("different original"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, changed, err := UpgradeUserRecordForLogin("LZ3NY-11", nil); err == nil || changed {
				t.Fatal("conflicting original backup accepted")
			}
			assertUpgradeBytes(t, backup, []byte("different original"))
			assertUpgradeBytes(t, profile, original)
			assertUpgradeBytes(t, library, libraryOriginal)
		})
	}
}

func TestLoginUpgradeBadLibraryPreservesValidV6Login(t *testing.T) {
	for _, raw := range []string{
		"presets: null\n", "presets:\n  OLD: {configuration_version: 7}\n",
		"presets:\n  OLD: {configuration_version: 4, dialect: null}\n",
		"presets: {BAD: null}\n", "presets: [\n", strings.Repeat("#", maxPresetCollectionBytes+1),
	} {
		t.Run(strings.TrimSpace(raw[:min(len(raw), 35)]), func(t *testing.T) {
			usePresetTestDir(t)
			record := &UserRecord{Filter: *nativeRuleFilter(t, RuleDefinition{2, RulePass, "MODE=FT8"}), Grid: "FN31", Dialect: "cc"}
			if err := SaveUserRecord("LZ3NY-11", record); err != nil {
				t.Fatal(err)
			}
			library := writeUpgradeLibrary(t, []byte(raw))
			loaded, changed, err := UpgradeUserRecordForLogin("LZ3NY-11", nil)
			if !errors.Is(err, ErrLoginUpgradeLibrary) || loaded == nil || changed ||
				loaded.Grid != "FN31" || loaded.Dialect != "cc" || loaded.PreparedRules().Definitions()[0].Slot != 2 {
				t.Fatal("bad library replaced valid v6 login preferences", err)
			}
			if err := SaveUserRecord("LZ3NY-11", loaded); err != nil {
				t.Fatal("valid v6 profile became protected", err)
			}
			assertUpgradeBytes(t, library, []byte(raw))
			if _, err := os.Stat(library + legacyBackupSuffix); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("bad library created original backup")
			}
		})
	}
}
