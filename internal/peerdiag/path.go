package peerdiag

import "os"

func diagnosticOpen(path string) (*os.File, error) {
	return helperOpenFile(path, os.O_RDONLY, 0)
}

// This is the parent launcher's existing NUL route. Its separate reservation
// and validated behavior must not inherit helper-only filesystem changes.
func diagnosticOpenFile(path string, flags int, mode os.FileMode) (*os.File, error) {
	path, err := helperOSPath(path)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(path, flags, mode)
}
