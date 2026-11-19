package filter

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func originalBackupTestStages() atomicUserFileStages {
	return atomicUserFileStages{write: writeUserFileBytes, sync: (*os.File).Sync, close: (*os.File).Close}
}

func TestOriginalBackupProcessInterruption(t *testing.T) {
	const childPath = "GOCLUSTER_TEST_BACKUP_PATH"
	const childPhase = "GOCLUSTER_TEST_BACKUP_PHASE"
	const interrupted = 73
	if path := os.Getenv(childPath); path != "" {
		stages := originalBackupTestStages()
		if os.Getenv(childPhase) == "staging" {
			stages.write = func(file *os.File, data []byte) error {
				if _, err := file.Write(data[:4]); err != nil {
					return err
				}
				os.Exit(interrupted)
				return nil
			}
		}
		publish := func(from, to string) error {
			if err := publishUserBackup(from, to); err != nil {
				return err
			}
			os.Exit(interrupted)
			return nil
		}
		t.Fatalf("expected abrupt exit, got %v", publishOriginalBackup(path, []byte(os.Getenv("GOCLUSTER_TEST_BACKUP_DATA")), stages, publish, syncUserBackupDirectory))
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"staging", "published"} {
		for _, owner := range []string{"profile", "library"} {
			t.Run(phase+"/"+owner, func(t *testing.T) {
				usePresetTestDir(t)
				profileBytes, libraryBytes := []byte(v7OldProfileLiteral), v7MixedLibrary(t)
				profile := writeUpgradeFixture(t, "LZ3NY-11", profileBytes)
				library := writeUpgradeLibrary(t, libraryBytes)
				final, original := profile+v7BackupSuffix, profileBytes
				if owner == "library" {
					final, original = library+v7BackupSuffix, libraryBytes
				}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				child := exec.CommandContext(ctx, exe, "-test.run=^TestOriginalBackupProcessInterruption$")
				child.Env = append(os.Environ(), childPath+"="+final, childPhase+"="+phase, "GOCLUSTER_TEST_BACKUP_DATA="+string(original))
				output, err := child.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != interrupted {
					t.Fatalf("child exit: %v %s", err, output)
				}
				if phase == "staging" {
					if _, err := os.Lstat(final); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("final backup visible during staging: %v", err)
					}
					assertUpgradeBytes(t, final+originalBackupStagingSuffix, original[:4])
				} else {
					assertUpgradeBytes(t, final, original)
				}
				assertUpgradeBytes(t, profile, profileBytes)
				assertUpgradeBytes(t, library, libraryBytes)
				_, outcome, err := UpgradeV7UserRecordForLoginWithOutcome("LZ3NY-11", nil)
				if err != nil || outcome.LibraryError != nil || !outcome.ProfileReset || !outcome.PresetLibraryReset {
					t.Fatalf("retry did not complete both resets: %+v, %v", outcome, err)
				}
				assertUpgradeBytes(t, profile+v7BackupSuffix, profileBytes)
				assertUpgradeBytes(t, library+v7BackupSuffix, libraryBytes)
				if _, err := os.Lstat(final + originalBackupStagingSuffix); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("staging file survived retry: %v", err)
				}
			})
		}
	}
}

func TestOriginalBackupPublicationBoundaries(t *testing.T) {
	original := []byte("complete original")
	for _, fault := range []string{"different collision", "equal collision", "publish error", "directory sync", "corrupt staging", "staging directory"} {
		t.Run(fault, func(t *testing.T) {
			final := filepath.Join(t.TempDir(), "test.pre-v7.bak")
			staging := final + originalBackupStagingSuffix
			stages := originalBackupTestStages()
			syncFailure := errors.New("directory sync refused")
			publishFailure := errors.New("publish refused")
			var publishCalls, syncCalls int
			if fault == "corrupt staging" {
				stages.write = func(file *os.File, _ []byte) error { return writeUserFileBytes(file, []byte("corrupt")) }
			}
			if fault == "staging directory" {
				if err := os.Mkdir(staging, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			publish := func(from, to string) error {
				publishCalls++
				switch fault {
				case "different collision", "equal collision":
					rival := []byte("other backup")
					if fault == "equal collision" {
						rival = original
					}
					if err := os.WriteFile(to, rival, 0o600); err != nil {
						return err
					}
				case "publish error":
					return publishFailure
				}
				return publishUserBackup(from, to)
			}
			syncDirectory := func(dir string) error {
				syncCalls++
				if fault == "directory sync" && syncCalls == 1 {
					return syncFailure
				}
				return syncUserBackupDirectory(dir)
			}
			err := publishOriginalBackup(final, original, stages, publish, syncDirectory)
			switch fault {
			case "different collision":
				if err == nil || !strings.Contains(err.Error(), "backup differs") {
					t.Fatalf("collision accepted: %v", err)
				}
				assertUpgradeBytes(t, final, []byte("other backup"))
			case "equal collision":
				if err != nil || publishCalls != 1 || syncCalls != 1 {
					t.Fatalf("equal backup did not pass durability: %v, publish=%d sync=%d", err, publishCalls, syncCalls)
				}
				assertUpgradeBytes(t, final, original)
			case "directory sync":
				if !errors.Is(err, syncFailure) {
					t.Fatalf("durability error was hidden: %v", err)
				}
				assertUpgradeBytes(t, final, original)
				err = publishOriginalBackup(final, original, stages, publish, syncDirectory)
				if err != nil || publishCalls != 1 || syncCalls != 2 {
					t.Fatalf("retry did not recheck durability: %v, publish=%d sync=%d", err, publishCalls, syncCalls)
				}
			case "publish error", "corrupt staging", "staging directory":
				if err == nil || (fault == "publish error" && !errors.Is(err, publishFailure)) || (fault != "publish error" && publishCalls != 0) {
					t.Fatalf("unexpected publication result: %v, calls=%d", err, publishCalls)
				}
				if _, err := os.Lstat(final); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unexpected final backup: %v", err)
				}
			}
			info, err := os.Lstat(staging)
			if fault == "staging directory" {
				if err != nil || !info.IsDir() {
					t.Fatalf("unexpected staging directory changed: %v", err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("owned staging survived: %v", err)
			}
		})
	}
}
