//go:build !windows

package filter

import (
	"errors"
	"os"
)

func publishUserBackup(source, target string) error {
	return os.Link(source, target)
}

func syncUserBackupDirectory(path string) error {
	dir, err := os.Open(path) // #nosec G703 -- Directory of the validated user-record or preset backup path.
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
