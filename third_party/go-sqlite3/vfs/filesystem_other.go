//go:build !windows

package vfs

import "os"

func osStat(path string) (os.FileInfo, error) { return os.Stat(path) }
func osOpenFile(path string, flags int, mode os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flags, mode)
}
func osRemove(path string) error                  { return os.Remove(path) }
func createTempFile(dir string) (*os.File, error) { return os.CreateTemp(dir, "*.db") }
