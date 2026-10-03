package vfs

import (
	"golang.org/x/sys/windows"
	"unsafe"
)

var boundedGetTempPath2 = windows.NewLazySystemDLL("kernel32.dll").NewProc("GetTempPath2W")

// Both Windows APIs write into the same fixed buffer. Do not call os.Getenv or
// os.TempDir here: their retry loops copy the environment-reported full length
// before a caller can apply the private VFS's filename bound.
func boundedTempDirectory() (string, error) {
	var buf [_MAX_PATHNAME]uint16
	key, _ := windows.UTF16PtrFromString("SQLITE_TMPDIR")
	n, err := windows.GetEnvironmentVariable(key, &buf[0], uint32(len(buf)))
	if n == 0 {
		// Empty and missing values both select the OS directory, matching
		// os.Getenv. The syscall wrapper reports EINVAL for an empty value.
		err = nil
	}
	if err != nil {
		return "", sysError{err, _IOERR_GETTEMPPATH}
	}
	if n == 0 {
		if boundedGetTempPath2.Find() == nil {
			r, _, callErr := boundedGetTempPath2.Call(uintptr(len(buf)), uintptr(unsafe.Pointer(&buf[0])))
			n = uint32(r)
			if n == 0 {
				return "", sysError{callErr, _IOERR_GETTEMPPATH}
			}
		} else {
			n, err = windows.GetTempPath(uint32(len(buf)), &buf[0])
			if err != nil {
				return "", sysError{err, _IOERR_GETTEMPPATH}
			}
		}
		if n < uint32(len(buf)) && n > 0 && buf[n-1] == '\\' && !(n == 3 && buf[1] == ':') {
			n--
		}
	}
	if n >= uint32(len(buf)) {
		return "", _IOERR_NOMEM
	}
	dir := windows.UTF16ToString(buf[:n])
	if len(dir) > _MAX_PATHNAME-32 {
		return "", _IOERR_NOMEM
	}
	return dir, nil
}
