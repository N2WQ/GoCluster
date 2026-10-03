// Portions copyright 2009 The Go Authors. All rights reserved.
// Branch order adapts Go 1.26.4 os/stat_windows.go under the BSD license in
// ../provenance/GO-LICENSE.txt. Failed native release remains explicitly owned.

package vfs

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	metadataGetAttributes = syscall.GetFileAttributesEx
	metadataFindFirst     = syscall.FindFirstFile
	metadataFindClose     = syscall.FindClose
	metadataCreateFile    = syscall.CreateFile
	metadataFileClose     = (*os.File).Close
	errMetadataRelease    = errors.New("SQLite metadata native release unconfirmed; process restart required")
)

type nativeMetadataOwner struct {
	handle   syscall.Handle
	file     *os.File
	find     bool
	terminal bool
}

func (o *nativeMetadataOwner) Close() error {
	if o.terminal {
		return errMetadataRelease
	}
	var err error
	if o.find {
		err = metadataFindClose(o.handle)
	} else if o.file != nil {
		err = metadataFileClose(o.file)
	}
	if err != nil {
		// File.Close consumes its FD state even on failure. Do not infer that a
		// second close is safe from its handle value or from a cleared test fault.
		o.terminal = true
		return errMetadataRelease
	}
	*o = nativeMetadataOwner{}
	return nil
}

func closeMetadata(o *nativeMetadataOwner) error {
	if err := o.Close(); err != nil {
		return retainedCloseError{owner: o, error: err}
	}
	return nil
}

type plainMetadata struct {
	name string
	data syscall.Win32FileAttributeData
}

func (m *plainMetadata) Name() string { return m.name }
func (m *plainMetadata) Size() int64 {
	return int64(m.data.FileSizeHigh)<<32 + int64(m.data.FileSizeLow)
}
func (m *plainMetadata) ModTime() time.Time { return time.Unix(0, m.data.LastWriteTime.Nanoseconds()) }
func (m *plainMetadata) IsDir() bool {
	return m.data.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0
}
func (m *plainMetadata) Mode() os.FileMode {
	mode := os.FileMode(0666)
	if m.data.FileAttributes&syscall.FILE_ATTRIBUTE_READONLY != 0 {
		mode = 0444
	}
	if m.IsDir() {
		mode |= os.ModeDir | 0111
	}
	return mode
}
func (m *plainMetadata) Sys() any { data := m.data; return &data }

func osStat(path string) (os.FileInfo, error) { return metadataStat("Stat", path, true) }

func metadataStat(operation, path string, follow bool) (os.FileInfo, error) {
	if path == "" {
		return nil, &os.PathError{Op: operation, Path: path, Err: syscall.ERROR_PATH_NOT_FOUND}
	}
	name, err := nativeOperationPath(path, _MAX_PATHNAME)
	if err != nil {
		return nil, err
	}
	native, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return nil, &os.PathError{Op: operation, Path: path, Err: err}
	}
	var attributes syscall.Win32FileAttributeData
	err = metadataGetAttributes(native, syscall.GetFileExInfoStandard, (*byte)(unsafe.Pointer(&attributes)))
	if errors.Is(err, os.ErrNotExist) {
		return nil, &os.PathError{Op: "GetFileAttributesEx", Path: path, Err: err}
	}
	if err == nil && attributes.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
		return &plainMetadata{name: filepath.Base(path), data: attributes}, nil
	}
	if err == windows.ERROR_SHARING_VIOLATION {
		var data syscall.Win32finddata
		h, findErr := metadataFindFirst(native, &data)
		if findErr != nil {
			return nil, &os.PathError{Op: "FindFirstFile", Path: path, Err: findErr}
		}
		owner := &nativeMetadataOwner{handle: h, find: true}
		if err := closeMetadata(owner); err != nil {
			return nil, err
		}
		if data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
			return &plainMetadata{name: filepath.Base(path), data: syscall.Win32FileAttributeData{FileAttributes: data.FileAttributes, CreationTime: data.CreationTime, LastAccessTime: data.LastAccessTime, LastWriteTime: data.LastWriteTime, FileSizeHigh: data.FileSizeHigh, FileSizeLow: data.FileSizeLow}}, nil
		}
	}
	flags := uint32(syscall.FILE_FLAG_BACKUP_SEMANTICS | syscall.FILE_FLAG_OPEN_REPARSE_POINT)
	h, err := metadataCreateFile(native, 0, 0, nil, syscall.OPEN_EXISTING, flags, 0)
	if err == windows.ERROR_INVALID_PARAMETER {
		h, err = metadataCreateFile(native, syscall.GENERIC_READ, 0, nil, syscall.OPEN_EXISTING, flags, 0)
	}
	if err != nil {
		return nil, &os.PathError{Op: "CreateFile", Path: path, Err: err}
	}
	owner := &nativeMetadataOwner{handle: h}
	owner.file = os.NewFile(uintptr(h), path)
	info, statErr := owner.file.Stat()
	surrogate := false
	if statErr == nil && follow {
		if data, ok := info.Sys().(*syscall.Win32FileAttributeData); ok && data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
			var tag struct{ attributes, tag uint32 }
			statErr = windows.GetFileInformationByHandleEx(windows.Handle(h), windows.FileAttributeTagInfo, (*byte)(unsafe.Pointer(&tag)), uint32(unsafe.Sizeof(tag)))
			if statErr != nil {
				statErr = &os.PathError{Op: "GetFileInformationByHandleEx", Path: path, Err: statErr}
			}
			surrogate = tag.tag&0x20000000 != 0
		}
	}
	if err := closeMetadata(owner); err != nil {
		return nil, err
	}
	if statErr != nil || !surrogate {
		return info, statErr
	}
	h, err = metadataCreateFile(native, 0, 0, nil, syscall.OPEN_EXISTING, syscall.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return nil, &os.PathError{Op: "CreateFile", Path: path, Err: err}
	}
	owner = &nativeMetadataOwner{handle: h}
	owner.file = os.NewFile(uintptr(h), path)
	info, statErr = owner.file.Stat()
	if err := closeMetadata(owner); err != nil {
		return nil, err
	}
	return info, statErr
}
