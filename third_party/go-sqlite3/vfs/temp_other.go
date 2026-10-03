//go:build !windows

package vfs

import "os"

func boundedTempDirectory() (string, error) {
	dir := os.Getenv("SQLITE_TMPDIR")
	if dir == "" {
		dir = os.TempDir()
	}
	if len(dir) > _MAX_PATHNAME-32 {
		return "", _IOERR_NOMEM
	}
	return dir, nil
}
