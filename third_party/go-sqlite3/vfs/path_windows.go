// Portions copyright 2011 The Go Authors. All rights reserved.
// The bounded native conversion adapts Go 1.26.4 syscall/exec_windows.go
// under the BSD-style license in ../provenance/GO-LICENSE.txt.

package vfs

import (
	"bytes"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var pathFullName = syscall.GetFullPathName
var pathUTF8ToWide = windows.MultiByteToWideChar
var pathWideToUTF8 = nativePathWideToUTF8
var pathWideToUTF8Proc = windows.NewLazySystemDLL("kernel32.dll").NewProc("WideCharToMultiByte")

const pathCodePageUTF8 = 65001

func nativePathWideToUTF8(input *uint16, output *byte, capacity int32) (int32, error) {
	if err := pathWideToUTF8Proc.Find(); err != nil {
		return 0, err
	}
	// The native call is synchronous and receives pointers to live Go arrays.
	// Flags0 and NUL-terminated input match modernc's winUnicodeToUtf8.
	n, _, err := pathWideToUTF8Proc.Call(pathCodePageUTF8, 0,
		uintptr(unsafe.Pointer(input)), ^uintptr(0), uintptr(unsafe.Pointer(output)), uintptr(capacity), 0, 0)
	if n == 0 {
		if err == syscall.Errno(0) {
			err = syscall.EINVAL
		}
		return 0, err
	}
	if n > uintptr(capacity) {
		return 0, _IOERR_NOMEM
	}
	return int32(n), nil
}

// Match the inherited SQLite Windows VFS: lexical normalization precedes the
// OS open, which follows reparse points itself. Resolving them here would
// change saved-file selection and the names used for WAL/shared-memory files.
// The prior VFS never reported OK_SYMLINK on Windows.
func (vfsOS) FullPathname(path string) (string, error) {
	if len(path) > _MAX_PATHNAME {
		return "", _IOERR_NOMEM
	}
	// sqlite3 winFullPathnameNoMutex accepts URI-style /X: and /\\?\ forms.
	// Strip only this one slash; ordinary volume-relative /name is unchanged.
	if len(path) >= 3 && path[0] == '/' && ((path[1] >= 'A' && path[1] <= 'Z' || path[1] >= 'a' && path[1] <= 'z') && path[2] == ':' || len(path) >= 5 && path[1:5] == `\\?\`) {
		path = path[1:]
	}
	return boundedWindowsFullPath(path)
}

// No OS-sized retry, Clean, case lookup or reparse lookup. Both conversions
// use the prior driver's CP_UTF8/flags0 APIs: Go's WTF-8 conversion selects
// different files for some malformed URI-decoded byte sequences.
func boundedWindowsFullPath(path string) (string, error) {
	if len(path) > _MAX_PATHNAME {
		return "", _IOERR_NOMEM
	}
	if strings.IndexByte(path, 0) >= 0 {
		return "", syscall.EINVAL
	}
	var raw [_MAX_PATHNAME + 1]byte
	copy(raw[:], path)
	var input [_MAX_PATHNAME + 1]uint16
	units, err := pathUTF8ToWide(pathCodePageUTF8, 0, &raw[0], -1, &input[0], int32(len(input)))
	if err != nil {
		if err == windows.ERROR_INSUFFICIENT_BUFFER {
			return "", _IOERR_NOMEM
		}
		return "", err
	}
	if units < 1 || units > int32(len(input)) || input[units-1] != 0 {
		return "", _IOERR_NOMEM
	}
	for _, u := range input[:units-1] {
		if u == 0 {
			return "", _IOERR_NOMEM
		}
	}
	var output [_MAX_PATHNAME + 1]uint16
	n, err := pathFullName(&input[0], uint32(len(output)), &output[0], nil)
	if err != nil {
		return "", err
	}
	if n == 0 || n >= uint32(len(output)) || output[n] != 0 {
		return "", _IOERR_NOMEM
	}
	for _, u := range output[:n] {
		if u == 0 {
			return "", _IOERR_NOMEM
		}
	}
	// Three bytes per UTF-16 unit plus NUL bounds CP_UTF8 output, including
	// replacement characters. Native size never chooses another allocation.
	var converted [3*_MAX_PATHNAME + 1]byte
	count, err := pathWideToUTF8(&output[0], &converted[0], int32(len(converted)))
	if err != nil {
		if err == windows.ERROR_INSUFFICIENT_BUFFER {
			return "", _IOERR_NOMEM
		}
		return "", err
	}
	if count < 1 || count > _MAX_PATHNAME+1 || converted[count-1] != 0 || bytes.IndexByte(converted[:count-1], 0) >= 0 {
		return "", _IOERR_NOMEM
	}
	full := string(converted[:count-1])
	if !filepath.IsAbs(full) {
		return "", _IOERR_NOMEM
	}
	return full, nil
}

func boundedOSPath(path string) (string, error) {
	if len(path) > _MAX_PATHNAME {
		return "", _IOERR_NOMEM
	}
	// OS-facing absolute/device paths keep their literal spelling. Relative
	// Stat/temporary/modeof paths need bounded acquisition before stdlib I/O.
	if path == "" || filepath.IsAbs(path) {
		return path, nil
	}
	return boundedWindowsFullPath(path)
}
