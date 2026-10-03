//go:build windows

// Portions copyright 2009 The Go Authors. All rights reserved.
// Adapted only for the helper's consumed operations from Go 1.26.4 os/path.go,
// os/path_windows.go and os/file_windows.go. The BSD license is retained in
// ../../third_party/go-sqlite3/provenance/GO-LICENSE.txt.

package peerdiag

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/windows"
)

var diagnosticCreateDirectory = syscall.CreateDirectory

func isFilesystemBudget(err error) bool { return errors.Is(err, errNativePathBudget) }

// Native names are operation-local. Errors and returned os.File owners retain
// only the logical name; recursive MkdirAll errors therefore borrow one input
// backing instead of retaining a different expanded path at every level.
// Do not pass the result back to os pathname APIs: legacy device names would
// enter their unbounded fixLongPath retry again.
func helperOperationPath(path string) (string, error) {
	if helperLongPaths || path == "" || len(path) >= 4 && (path[:4] == `\??\` || isPathSeparator(path[0]) && isPathSeparator(path[1]) && path[2] == '?' && isPathSeparator(path[3])) {
		return path, nil
	}
	length := uint64(len(path))
	if !filepath.IsAbs(path) {
		cwd, err := helperCurrentDirectoryBytes(0, 0)
		if errors.Is(err, errNativePathBudget) {
			return "", err
		}
		// Pinned Go ignores Getwd failure when estimating the prefix threshold.
		if err == nil {
			if cwd > ^uint64(0)-length-1 {
				return "", errNativePathBudget
			}
			length += cwd + 1
		} else {
			length++
		}
	}
	if length < 248 {
		return path, nil
	}
	var prefix string
	if len(path) >= 2 && isPathSeparator(path[0]) && isPathSeparator(path[1]) {
		if len(path) < 4 || path[2] != '.' || !isPathSeparator(path[3]) {
			prefix = `\\?\UNC\`
		}
	} else {
		prefix = `\\?\`
	}
	if length > ^uint64(0)-8 {
		return "", errNativePathBudget
	}
	name, err := helperBoundedFullPathWithin(path, prefix, length+8)
	if err != nil && !errors.Is(err, errNativePathBudget) {
		// Match addExtendedPrefix's ordinary native-error fallback. Refusing an
		// unadmitted allocation must never turn into an unbounded stdlib retry.
		return path, nil
	}
	return name, err
}

func helperOpenFile(path string, flags int, mode os.FileMode) (*os.File, error) {
	// These are the sink's entire open surface. O_TRUNC/O_DIRECTORY would add
	// syscall.Open partial-handle cleanup branches and need their own proof.
	const supported = os.O_WRONLY | os.O_RDWR | os.O_CREATE | os.O_APPEND
	if flags & ^supported != 0 {
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.EINVAL}
	}
	name, err := helperOperationPath(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	if path == "" {
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.ENOENT}
	}
	handle, err := syscall.Open(name, flags|syscall.O_CLOEXEC, uint32(mode.Perm()))
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	// The freshly opened synchronous handle has no outstanding I/O. NewFile's
	// fixed NtQueryInformationFile probe adds no worker or payload buffer. The
	// sink uses Write/ReadAt, never append-mode WriteAt (whose guard is private
	// to os.OpenFile); native FILE_APPEND_DATA semantics are supplied by Open.
	return os.NewFile(uintptr(handle), path), nil
}

func diagnosticRemove(path string) error {
	name, err := helperOperationPath(path)
	if err != nil {
		return &os.PathError{Op: "remove", Path: path, Err: err}
	}
	p, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return &os.PathError{Op: "remove", Path: path, Err: err}
	}
	err = syscall.DeleteFile(p)
	if err == nil {
		return nil
	}
	dirErr := syscall.RemoveDirectory(p)
	if dirErr == nil {
		return nil
	}
	if !errors.Is(dirErr, err) {
		attributes, attrErr := syscall.GetFileAttributes(p)
		if attrErr != nil {
			err = attrErr
		} else if attributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0 {
			err = dirErr
		} else if attributes&syscall.FILE_ATTRIBUTE_READONLY != 0 {
			if syscall.SetFileAttributes(p, attributes&^syscall.FILE_ATTRIBUTE_READONLY) == nil {
				err = syscall.DeleteFile(p)
				if err == nil {
					return nil
				}
			}
		}
	}
	return &os.PathError{Op: "remove", Path: path, Err: err}
}

func diagnosticRename(old, next string) error {
	oldName, err := helperOperationPath(old)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: old, New: next, Err: err}
	}
	newName, err := helperOperationPath(next)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: old, New: next, Err: err}
	}
	oldNative, err := syscall.UTF16PtrFromString(oldName)
	if err == nil {
		var nextNative *uint16
		nextNative, err = syscall.UTF16PtrFromString(newName)
		if err == nil {
			err = windows.MoveFileEx(oldNative, nextNative, windows.MOVEFILE_REPLACE_EXISTING)
		}
	}
	if err != nil {
		return &os.LinkError{Op: "rename", Old: old, New: next, Err: err}
	}
	return nil
}

func diagnosticMkdir(path string, _ os.FileMode) error {
	name, err := helperOperationPath(path)
	if err == nil {
		var native *uint16
		native, err = syscall.UTF16PtrFromString(name)
		if err == nil {
			err = diagnosticCreateDirectory(native, nil)
		}
	}
	if err != nil {
		return &os.PathError{Op: "mkdir", Path: path, Err: err}
	}
	return nil
}

// This is pinned MkdirAll's algorithm, with owned metadata and native calls.
// Recursion MUST use the original logical slices (L <= P+1), never prepared
// absolute names. Charge 64 bytes/error and at most ceil(L/2)+1 levels, plus
// leaf errors, inside 64P+64C. Failed native release/resource admission takes
// priority over the ordinary errors MkdirAll intentionally probes through.
func (s *helperSink) diagnosticMkdirAll(path string, mode os.FileMode) error {
	info, err := s.diagnosticStat(path)
	if s.metadataFailed() || errors.Is(err, errNativePathBudget) {
		return err
	}
	if err == nil {
		if info.IsDir() {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
	}
	i := len(path) - 1
	for i >= 0 && isPathSeparator(path[i]) {
		i--
	}
	for i >= 0 && !isPathSeparator(path[i]) {
		i--
	}
	if i < 0 {
		i = 0
	}
	if parent := path[:i]; len(parent) > len(filepath.VolumeName(path)) {
		if err = s.diagnosticMkdirAll(parent, mode); err != nil {
			return err
		}
	}
	err = diagnosticMkdir(path, mode)
	if err != nil {
		if errors.Is(err, errNativePathBudget) {
			return err
		}
		info, statErr := s.diagnosticLstat(path)
		if s.metadataFailed() || errors.Is(statErr, errNativePathBudget) {
			return statErr
		}
		if statErr == nil && info.IsDir() {
			return nil
		}
	}
	return err
}
