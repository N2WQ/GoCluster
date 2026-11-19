package filter

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

const originalBackupStagingSuffix = ".staging"

// Upgrade callers own the base-call stripe and run under the single-writer
// directory contract. A final backup is immutable and never used for staging.
func writeLegacyOriginalBackup(path string, original []byte) error {
	return writeLegacyOriginalBackupWithStages(path, original, atomicUserFileStages{
		write: writeUserFileBytes, sync: (*os.File).Sync, close: (*os.File).Close,
	})
}

func writeLegacyOriginalBackupWithStages(path string, original []byte, stages atomicUserFileStages) error {
	return publishOriginalBackup(path, original, stages, publishUserBackup, syncUserBackupDirectory)
}

func publishOriginalBackup(path string, original []byte, stages atomicUserFileStages, publish func(string, string) error, syncDirectory func(string) error) error {
	if len(original) > maxPresetCollectionBytes {
		return storageAdmissionf("original backup exceeds byte limit")
	}

	staging := path + originalBackupStagingSuffix
	if err := removeBackupStaging(staging); err != nil {
		return fmt.Errorf("clear original backup staging: %w", err)
	}
	dir := filepath.Dir(path)
	if _, err := os.Lstat(path); err == nil { // #nosec G703 -- Validated upgrade path with a fixed backup suffix.
		return verifyExistingBackup(path, dir, original, stages, syncDirectory)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	file, err := os.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G703 -- Fixed staging suffix beside the validated backup path.
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
		if err := removeBackupStaging(staging); err != nil {
			log.Printf("Warning: failed to remove original backup staging: %v", err)
		}
	}()
	if err := stages.write(file, original); err != nil {
		return err
	}
	if err := stages.sync(file); err != nil {
		return err
	}
	if err := stages.close(file); err != nil {
		return err
	}
	saved, err := readUpgradeFile(staging, maxPresetCollectionBytes)
	if err != nil {
		return err
	}
	if !bytes.Equal(saved, original) {
		return errors.New("new original backup verification failed")
	}

	if err := publish(staging, path); errors.Is(err, os.ErrExist) {
		return verifyExistingBackup(path, dir, original, stages, syncDirectory)
	} else if err != nil {
		return fmt.Errorf("publish original backup: %w", err)
	}
	return syncDirectory(dir)
}

func removeBackupStaging(path string) error {
	info, err := os.Lstat(path) // #nosec G703 -- Fixed backup or staging suffix under the operator's user directory.
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("original backup staging is not a regular file")
	}
	return os.Remove(path) // #nosec G703 -- Only the reserved regular staging entry can be removed.
}

func verifyExistingBackup(path, dir string, original []byte, stages atomicUserFileStages, syncDirectory func(string) error) error {
	info, err := os.Lstat(path) // #nosec G703 -- Fixed backup or staging suffix under the operator's user directory.
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("original backup is not a regular file")
	}
	saved, err := readUpgradeFile(path, maxPresetCollectionBytes)
	if err != nil {
		return err
	}
	if !bytes.Equal(saved, original) {
		return errors.New("existing original backup differs; refusing replacement")
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0) // #nosec G703 -- Verified regular backup under the operator's user directory; open does not truncate.
	if err != nil {
		return err
	}
	defer file.Close()
	if err := errors.Join(stages.sync(file), stages.close(file)); err != nil {
		return err
	}
	// A previous attempt may have published the backup and stopped before
	// confirming the directory entry. Verify durability again before reset.
	return syncDirectory(dir)
}
