package main

import (
	"os"
	"syscall"
)

// Some Windows reparse tags are reported as ordinary directories by Lstat.
func reparsePoint(info os.FileInfo) bool {
	raw, ok := info.Sys().(*syscall.Win32FileAttributeData)
	return !ok || raw.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0
}
