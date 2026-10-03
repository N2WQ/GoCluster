//go:build sqlite3_qualification && windows

package sqlite3_wrap

import "golang.org/x/sys/windows"

// FailFallbackHandleCloseForQualification injects failure at the real native
// release site after view unmapping. Tests using this hook must be serialized.
func FailFallbackHandleCloseForQualification() func() {
	previous := closeFallbackHandle
	closeFallbackHandle = func(windows.Handle) error { return windows.ERROR_ACCESS_DENIED }
	return func() { closeFallbackHandle = previous }
}
