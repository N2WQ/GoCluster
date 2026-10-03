// Portions copyright 2009 The Go Authors. All rights reserved.
// Native operation/prefix/remove/temp branch order adapts Go 1.26.4 os sources;
// BSD license in ../provenance/GO-LICENSE.txt. No stdlib pathname retry follows.

package vfs

import (
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"golang.org/x/sys/windows"
)

const nativeDerivedPathBytes = _MAX_PATHNAME + 4 // already-owned -shm suffix
const nativePathUnits = nativeDerivedPathBytes + 8 + 1

var nativeLongPaths = func() bool {
	v := windows.RtlGetVersion()
	return v.MajorVersion > 10 || v.MajorVersion == 10 && (v.MinorVersion > 0 || v.BuildNumber >= 15063)
}()
var nativeFullPath = syscall.GetFullPathName
var nativeCurrentDirectory = windows.GetCurrentDirectory
var tempRandom = rand.Uint64
var tempOpenFile = osOpenFile

func pathSeparator(c byte) bool { return c == '\\' || c == '/' }

func nativeWTF8Size(units []uint16) int {
	n := 0
	for i := 0; i < len(units); i++ {
		u := units[i]
		if u == 0 {
			return -1
		}
		switch {
		case u < 0x80:
			n++
		case u < 0x800:
			n += 2
		case u >= 0xd800 && u <= 0xdbff && i+1 < len(units) && units[i+1] >= 0xdc00 && units[i+1] <= 0xdfff:
			n += 4
			i++
		default:
			n += 3
		}
	}
	return n
}

// Preserve Go's OS-facing branch without handing the result back to fixLongPath.
// SQL's separate FullPathname retains the prior native CP_UTF8 contract.
func nativeOperationPath(path string, limit int) (string, error) {
	if limit > nativeDerivedPathBytes || len(path) > limit {
		return "", _IOERR_NOMEM
	}
	if nativeLongPaths || path == "" || len(path) >= 4 && (path[:4] == `\??\` || pathSeparator(path[0]) && pathSeparator(path[1]) && path[2] == '?' && pathSeparator(path[3])) {
		return path, nil
	}
	length := len(path)
	if !filepath.IsAbs(path) {
		var cwd [nativePathUnits]uint16
		n, err := nativeCurrentDirectory(uint32(len(cwd)), &cwd[0])
		if err == nil {
			if n == 0 || n >= uint32(len(cwd)) || cwd[n] != 0 {
				return "", _IOERR_NOMEM
			}
			count := nativeWTF8Size(cwd[:n])
			if count < 0 || count > nativeDerivedPathBytes {
				return "", _IOERR_NOMEM
			}
			length += count + 1
		} else {
			length++
		} // pinned Go ignores Getwd failure for this estimate
	}
	if length < 248 {
		return path, nil
	}
	prefix := `\\?\`
	if len(path) >= 2 && pathSeparator(path[0]) && pathSeparator(path[1]) {
		if len(path) >= 4 && path[2] == '.' && pathSeparator(path[3]) {
			prefix = ""
		} else {
			prefix = `\\?\UNC\`
		}
	}
	input, err := syscall.UTF16FromString(path)
	if err != nil {
		return path, nil
	}
	var output [nativePathUnits]uint16
	n, err := nativeFullPath(&input[0], uint32(len(output)), &output[0], nil)
	if err != nil {
		return path, nil
	} // ordinary native error keeps pinned fallback
	if n == 0 || n >= uint32(len(output)) || output[n] != 0 {
		return "", _IOERR_NOMEM
	}
	count := nativeWTF8Size(output[:n])
	if count < 0 || count+len(prefix) > nativeDerivedPathBytes+8 {
		return "", _IOERR_NOMEM
	}
	full := syscall.UTF16ToString(output[:n])
	if !filepath.IsAbs(full) {
		return "", _IOERR_NOMEM
	}
	if prefix == `\\?\UNC\` {
		if len(full) < 2 || !pathSeparator(full[0]) || !pathSeparator(full[1]) {
			return "", _IOERR_NOMEM
		}
		full = full[2:]
	}
	return prefix + full, nil
}

func osOpenFile(path string, flags int, mode os.FileMode) (*os.File, error) {
	// This is the VFS's complete flag set; no O_TRUNC/O_DIRECTORY branch can
	// acquire a handle and then hide failed partial cleanup in syscall.Open.
	const supported = os.O_WRONLY | os.O_RDWR | os.O_CREATE | os.O_EXCL
	if flags & ^supported != 0 {
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.EINVAL}
	}
	name, err := nativeOperationPath(path, nativeDerivedPathBytes)
	if err != nil {
		return nil, err
	}
	if path == "" {
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.ENOENT}
	}
	h, err := syscall.Open(name, flags|syscall.O_CLOEXEC, uint32(mode.Perm()))
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}

func osRemove(path string) error {
	name, err := nativeOperationPath(path, nativeDerivedPathBytes)
	if err != nil {
		return err
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
	if dirErr != err {
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

func createWindowsTemp(dir string) (*os.File, error) {
	if len(dir) > _MAX_PATHNAME-32 {
		return nil, _IOERR_NOMEM
	}
	prefix := dir
	if len(prefix) == 0 || !pathSeparator(prefix[len(prefix)-1]) {
		prefix += `\`
	}
	for attempts := 0; attempts < 10000; attempts++ {
		name := prefix + strconv.FormatUint(uint64(uint32(tempRandom())), 10) + ".db"
		file, err := tempOpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if os.IsExist(err) {
			continue
		}
		return file, err
	}
	return nil, &os.PathError{Op: "createtemp", Path: prefix + "*.db", Err: os.ErrExist}
}

func createTempFile(dir string) (*os.File, error) { return createWindowsTemp(dir) }
