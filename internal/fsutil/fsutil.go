// Package fsutil provides shared filesystem helpers.
package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// EnsureParentDir creates path's parent directory when needed.
func EnsureParentDir(path string, errPrefix string) error {
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		if errPrefix == "" {
			return err
		}
		return fmt.Errorf("%s: %w", errPrefix, err)
	}
	return nil
}
