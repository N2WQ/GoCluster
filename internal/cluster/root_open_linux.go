//go:build linux

package cluster

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// openRootNoFollow opens a single entry relative to the retained directory.
// It never resolves a final symlink, so substitution cannot create an unbounded
// readlink buffer. The caller owns the returned file and runs on a log worker.
func openRootNoFollow(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	return openRootNoFollowOwned(root, name, flags, mode, nil)
}

func openRootNoFollowOwned(root *os.Root, name string, flags int, mode os.FileMode, owner *logCloseOwner) (*os.File, error) {
	if root == nil || name == "" || name == "." || name == ".." || filepath.Base(name) != name || !filepath.IsLocal(name) {
		return nil, &os.PathError{Op: "openat", Path: name, Err: os.ErrInvalid}
	}
	dir, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	perm := uint32(mode.Perm())
	if mode&os.ModeSetuid != 0 {
		perm |= unix.S_ISUID
	}
	if mode&os.ModeSetgid != 0 {
		perm |= unix.S_ISGID
	}
	if mode&os.ModeSticky != 0 {
		perm |= unix.S_ISVTX
	}
	fd, openErr := unix.Openat(int(dir.Fd()), name, flags|unix.O_NOFOLLOW|unix.O_CLOEXEC, perm)
	var closeErr error
	if owner != nil {
		closeErr = owner.close(dir)
	} else {
		closeErr = dir.Close()
	}
	if openErr != nil {
		pathErr := &os.PathError{Op: "openat", Path: name, Err: openErr}
		if closeErr != nil {
			return nil, errors.Join(pathErr, closeErr)
		}
		return nil, pathErr
	}
	if closeErr != nil {
		file := os.NewFile(uintptr(fd), name)
		if file != nil && owner != nil {
			return nil, errors.Join(closeErr, owner.close(file))
		}
		return nil, errors.Join(closeErr, unix.Close(fd))
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		return nil, errors.Join(syscall.EBADF, unix.Close(fd))
	}
	return file, nil
}
