//go:build !linux || sqlite3_flock

package vfs

import "os"

func osCreateTemp(flags OpenFlag) (*os.File, error) {
	dir, err := boundedTempDirectory()
	if err != nil {
		return nil, err
	}
	// Test before path composition: a hostile environment must not create
	// arbitrarily large copies or retained names outside the fixed host bound.
	if len(dir) > _MAX_PATHNAME-32 {
		return nil, _IOERR_NOMEM
	}
	dir, err = boundedOSPath(dir)
	if err != nil {
		return nil, err
	}
	if len(dir) > _MAX_PATHNAME-32 {
		return nil, _IOERR_NOMEM
	}
	f, err := createTempFile(dir)
	if err != nil {
		if err == _IOERR_NOMEM {
			return nil, err
		}
		return nil, sysError{err, _IOERR_GETTEMPPATH}
	}
	if isUnix && flags&OPEN_DELETEONCLOSE != 0 {
		os.Remove(f.Name())
	}
	return f, nil
}
