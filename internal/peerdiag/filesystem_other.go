//go:build !windows

package peerdiag

import "os"

//lint:ignore U1000 Shared sink layout names the Windows-only metadata owner.
type metadataOwner struct{}

func isFilesystemBudget(error) bool { return false }

func (s *helperSink) metadataFailed() bool                            { return false }
func (s *helperSink) closeMetadata() error                            { return nil }
func (s *helperSink) diagnosticStat(path string) (os.FileInfo, error) { return os.Stat(path) }
func (s *helperSink) diagnosticMkdirAll(path string, mode os.FileMode) error {
	return os.MkdirAll(path, mode)
}
func helperOpenFile(path string, flags int, mode os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flags, mode)
}
func diagnosticRemove(path string) error      { return os.Remove(path) }
func diagnosticRename(old, next string) error { return os.Rename(old, next) }
