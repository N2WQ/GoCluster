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

// ResolvePathIdentity resolves existing ancestors of path even when the final
// file does not exist. A dangling link has no safe publication identity.
func ResolvePathIdentity(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	ancestor := abs
	var suffix []string
	for {
		resolved, err := filepath.EvalSymlinks(ancestor)
		if err == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return resolved, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		info, statErr := os.Lstat(ancestor)
		if statErr == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("dangling symlink %q", ancestor)
		}
		if statErr != nil && !os.IsNotExist(statErr) {
			return "", statErr
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", err
		}
		suffix = append(suffix, filepath.Base(ancestor))
		ancestor = parent
	}
}
