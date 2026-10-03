// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by the BSD-style license in
// ../provenance/GO-LICENSE.txt.
//
// Adapted from Go 1.26.4 path/filepath/symlink.go. The resolution algorithm is
// unchanged for admitted Linux workspaces. Every constructed path is
// admitted before allocation so link targets cannot accumulate an unbounded suffix.

//go:build linux

package vfs

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

// This is the existing VFS filename budget, applied to each composed resolver
// workspace. Long intermediates are refused even if later '..' could shrink
// them; the resource-refusal contract never truncates or resolves another file.
func boundedPathConcat(parts ...string) (string, error) {
	remaining := _MAX_PATHNAME
	for _, p := range parts {
		if len(p) > remaining {
			return "", _IOERR_NOMEM
		}
		remaining -= len(p)
	}
	return strings.Join(parts, ""), nil
}

func boundedResolvedJoin(path, file string) (string, error) {
	remaining := _MAX_PATHNAME - len(path)
	if len(file) > remaining || (path != "" && file != "" && len(file) == remaining) {
		return "", _IOERR_NOMEM
	}
	return filepath.Join(path, file), nil
}

func boundedWalkSymlinks(path string, lstat func(string) (fs.FileInfo, error), readlink func(string) (string, error)) (string, error) {
	if len(path) > _MAX_PATHNAME {
		return "", _IOERR_NOMEM
	}
	volLen := len(filepath.VolumeName(path))
	pathSeparator := string(os.PathSeparator)
	if volLen < len(path) && os.IsPathSeparator(path[volLen]) {
		volLen++
	}
	vol := path[:volLen]
	dest := vol
	linksWalked := 0
	for start, end := volLen, volLen; start < len(path); start = end {
		for start < len(path) && os.IsPathSeparator(path[start]) {
			start++
		}
		end = start
		for end < len(path) && !os.IsPathSeparator(path[end]) {
			end++
		}
		isWindowsDot := runtime.GOOS == "windows" && path[len(filepath.VolumeName(path)):] == "."
		if end == start {
			break
		} else if path[start:end] == "." && !isWindowsDot {
			continue
		} else if path[start:end] == ".." {
			var r int
			for r = len(dest) - 1; r >= volLen; r-- {
				if os.IsPathSeparator(dest[r]) {
					break
				}
			}
			if r < volLen || dest[r+1:] == ".." {
				sep := ""
				if len(dest) > volLen {
					sep = pathSeparator
				}
				var err error
				dest, err = boundedPathConcat(dest, sep, "..")
				if err != nil {
					return "", err
				}
			} else {
				dest = dest[:r]
			}
			continue
		}
		sep := ""
		if len(dest) > len(filepath.VolumeName(dest)) && !os.IsPathSeparator(dest[len(dest)-1]) {
			sep = pathSeparator
		}
		var err error
		dest, err = boundedPathConcat(dest, sep, path[start:end])
		if err != nil {
			return "", err
		}
		fi, err := lstat(dest)
		if err != nil {
			return "", err
		}
		if fi.Mode()&fs.ModeSymlink == 0 {
			if !fi.IsDir() && end < len(path) {
				return "", syscall.ENOTDIR
			}
			continue
		}
		linksWalked++
		if linksWalked > 255 {
			return "", errors.New("EvalSymlinks: too many links")
		}
		link, err := readlink(dest)
		if err != nil {
			return "", err
		}
		if isWindowsDot && !filepath.IsAbs(link) {
			break
		}
		path, err = boundedPathConcat(link, path[end:])
		if err != nil {
			return "", err
		}
		v := len(filepath.VolumeName(link))
		if v > 0 {
			if v < len(link) && os.IsPathSeparator(link[v]) {
				v++
			}
			vol = link[:v]
			dest = vol
			end = len(vol)
		} else if len(link) > 0 && os.IsPathSeparator(link[0]) {
			dest = link[:1]
			end = 1
			vol = link[:1]
			volLen = 1
		} else {
			var r int
			for r = len(dest) - 1; r >= volLen; r-- {
				if os.IsPathSeparator(dest[r]) {
					break
				}
			}
			if r < volLen {
				dest = vol
			} else {
				dest = dest[:r]
			}
			end = 0
		}
	}
	return filepath.Clean(dest), nil
}
