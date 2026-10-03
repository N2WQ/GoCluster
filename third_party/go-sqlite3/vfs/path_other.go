//go:build !windows && !linux

package vfs

import (
	"os"
	"path/filepath"
)

// Preserve the upstream baseline outside the two qualified platforms.
func boundedEvalSymlinks(path string) (string, error) { return filepath.EvalSymlinks(path) }

func boundedResolvedJoin(path, file string) (string, error) { return filepath.Join(path, file), nil }
func boundedPathLstat(path string) (os.FileInfo, error)     { return os.Lstat(path) }
func boundedOSPath(path string) (string, error)             { return path, nil }
func boundedPathAbs(path string) (string, error)            { return filepath.Abs(path) }
