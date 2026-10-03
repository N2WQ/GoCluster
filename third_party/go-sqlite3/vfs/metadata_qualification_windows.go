//go:build sqlite3_qualification

package vfs

import (
	"golang.org/x/sys/windows"
	"os"
	"syscall"
	"unsafe"
)

// FailMetadataCloseForQualification forces the existing named metadata handle
// branch to encounter a real consumed os.File.Close error. It is process-local,
// qualification-only, and must be installed without concurrent connections.
func FailMetadataCloseForQualification(path string) (restore func()) {
	attributes, closeFile := metadataGetAttributes, metadataFileClose
	metadataGetAttributes = func(name *uint16, kind uint32, out *byte) error {
		err := attributes(name, kind, out)
		if err == nil && windows.UTF16PtrToString(name) == path {
			(*syscall.Win32FileAttributeData)(unsafe.Pointer(out)).FileAttributes |= syscall.FILE_ATTRIBUTE_REPARSE_POINT
		}
		return err
	}
	metadataFileClose = func(file *os.File) error {
		if file.Name() == path {
			if err := syscall.CloseHandle(syscall.Handle(file.Fd())); err != nil {
				return err
			}
		}
		return closeFile(file)
	}
	return func() { metadataGetAttributes, metadataFileClose = attributes, closeFile }
}
