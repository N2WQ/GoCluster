//go:build !windows

package bulletinhistory

import (
	"os"
	"path/filepath"
)

func replaceFile(source, target string) error { return os.Rename(source, target) }

// Linking publishes without replacing an existing artifact. Both links point
// to the independent copy, never the failed source.
func commitRecoveryFile(source, target string) error {
	if err := os.Link(source, target); err != nil {
		return err
	}
	if err := syncRecoveryDirectory(filepath.Dir(target)); err != nil {
		return err
	}
	return os.Remove(source)
}

func syncRecoveryDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	return err
}
