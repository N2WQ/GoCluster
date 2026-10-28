//go:build windows

package bulletinhistory

import "golang.org/x/sys/windows"

func replaceFile(source, target string) error {
	src, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	dst, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(src, dst, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}

// No REPLACE_EXISTING: an earlier recovery artifact is never overwritten.
func commitRecoveryFile(source, target string) error {
	src, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	dst, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(src, dst, windows.MOVEFILE_WRITE_THROUGH)
}

// Windows has no portable directory sync here. Publication requests
// WRITE_THROUGH; this is not a guarantee against every power-loss scenario.
func syncRecoveryDirectory(string) error { return nil }
