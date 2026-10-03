//go:build !windows

package vfs

import (
	"errors"
	"io/fs"
	"path/filepath"
)

// Preserve Unix's canonical path and symlink signal. Windows has its own
// lexical SQLite path contract and must not enter this resolver.
func (vfsOS) FullPathname(path string) (string, error) {
	if len(path) > _MAX_PATHNAME {
		return "", _IOERR_NOMEM
	}
	link, err := evalSymlinks(path)
	if err != nil {
		return "", err
	}
	full, err := boundedPathAbs(link)
	if err == nil && link != path {
		err = _OK_SYMLINK
	}
	return full, err
}

func evalSymlinks(path string) (string, error) {
	var file string
	_, err := boundedPathLstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		path, file = filepath.Split(path)
	}
	path, err = boundedEvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return boundedResolvedJoin(path, file)
}
