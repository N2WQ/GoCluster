// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by the BSD-style license in
// ../provenance/GO-LICENSE.txt.
//
// The bounded absolute-path helper preserves the PWD/getcwd selection in
// Go 1.26.4 os/getwd.go and syscall/syscall_linux.go for admitted workspaces.
package vfs

import (
	"os"
	"path/filepath"
	"syscall"
)

func boundedEvalSymlinks(path string) (string, error) {
	return boundedWalkSymlinks(path, os.Lstat, boundedReadlink)
}

func boundedPathLstat(path string) (os.FileInfo, error) { return os.Lstat(path) }
func boundedOSPath(path string) (string, error)         { return path, nil }

func boundedPathAbs(path string) (string, error) {
	if len(path) > _MAX_PATHNAME {
		return "", _IOERR_NOMEM
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	// Match Go's PWD preference when it names the current directory. Check
	// the borrowed environment value before any OS conversion or copy.
	wd := os.Getenv("PWD")
	if filepath.IsAbs(wd) {
		if len(wd) > _MAX_PATHNAME {
			return "", _IOERR_NOMEM
		}
		dot, err := os.Stat(".")
		if err != nil {
			return "", err
		}
		if info, err := os.Stat(wd); err == nil && os.SameFile(dot, info) {
			return boundedResolvedJoin(wd, path)
		}
	}
	var buf [_MAX_PATHNAME + 1]byte
	for {
		n, err := syscall.Getcwd(buf[:])
		if err == syscall.EINTR {
			continue
		}
		if err == syscall.ERANGE || err == syscall.ENAMETOOLONG {
			return "", _IOERR_NOMEM
		}
		if err != nil {
			return "", os.NewSyscallError("getwd", err)
		}
		if n < 1 || n > len(buf) || buf[n-1] != 0 {
			return "", os.NewSyscallError("getwd", syscall.EINVAL)
		}
		if buf[0] != '/' {
			return "", os.NewSyscallError("getwd", syscall.ENOENT)
		}
		return boundedResolvedJoin(string(buf[:n-1]), path)
	}
}

func boundedReadlink(path string) (string, error) {
	// One extra byte distinguishes an admitted 1,024-byte target from a
	// possibly truncated target. Never retry with a growing allocation.
	var buf [_MAX_PATHNAME + 1]byte
	for {
		n, err := syscall.Readlink(path, buf[:])
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return "", &os.PathError{Op: "readlink", Path: path, Err: err}
		}
		if n >= len(buf) {
			return "", _IOERR_NOMEM
		}
		return string(buf[:n]), nil
	}
}
