// Portions copyright 2009 The Go Authors. All rights reserved.
// Branch order adapts Go 1.26.4 os/stat_windows.go under the BSD license in
// ../third_party/go-sqlite3/provenance/GO-LICENSE.txt. Failed native release remains explicitly owned.

package peer

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
	topologyMetadataGetAttributes = syscall.GetFileAttributesEx
	topologyMetadataFindFirst     = syscall.FindFirstFile
	topologyMetadataFindClose     = syscall.FindClose
	topologyMetadataCreateFile    = syscall.CreateFile
	topologyMetadataFileClose     = (*os.File).Close
	errTopologyMetadataRelease    = errors.New("SQLite metadata native release unconfirmed; process restart required")
)

type topologyDirectoryOwner struct {
	handle   syscall.Handle
	file     *os.File
	find     bool
	terminal bool
}

func (o *topologyDirectoryOwner) Close() error {
	if o.terminal {
		return errTopologyMetadataRelease
	}
	var err error
	if o.find {
		err = topologyMetadataFindClose(o.handle)
	} else if o.file != nil {
		err = topologyMetadataFileClose(o.file)
	}
	if err != nil {
		// File.Close consumes its FD state even on failure. Do not infer that a
		// second close is safe from its handle value or from a cleared test fault.
		o.terminal = true
		return errTopologyMetadataRelease
	}
	*o = topologyDirectoryOwner{}
	return nil
}

func closeTopologyMetadata(o *topologyDirectoryOwner) error {
	if err := o.Close(); err != nil {
		return err
	}
	return nil
}

type topologyPlainMetadata struct {
	name string
	data syscall.Win32FileAttributeData
}

func (m *topologyPlainMetadata) Name() string { return m.name }
func (m *topologyPlainMetadata) Size() int64 {
	return int64(m.data.FileSizeHigh)<<32 + int64(m.data.FileSizeLow)
}
func (m *topologyPlainMetadata) ModTime() time.Time {
	return time.Unix(0, m.data.LastWriteTime.Nanoseconds())
}
func (m *topologyPlainMetadata) IsDir() bool {
	return m.data.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0
}
func (m *topologyPlainMetadata) Mode() os.FileMode {
	mode := os.FileMode(0666)
	if m.data.FileAttributes&syscall.FILE_ATTRIBUTE_READONLY != 0 {
		mode = 0444
	}
	if m.IsDir() {
		mode |= os.ModeDir | 0111
	}
	return mode
}
func (m *topologyPlainMetadata) Sys() any { data := m.data; return &data }

func (db *topologyDatabase) directoryMetadata(operation, path string, follow bool) (os.FileInfo, error) {
	if db.directoryOwner.terminal {
		return nil, errTopologyMetadataRelease
	}
	if path == "" {
		return nil, &os.PathError{Op: operation, Path: path, Err: syscall.ERROR_PATH_NOT_FOUND}
	}
	name, err := topologyDirectoryPath(path)
	if err != nil {
		return nil, err
	}
	native, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return nil, &os.PathError{Op: operation, Path: path, Err: err}
	}
	var attributes syscall.Win32FileAttributeData
	err = topologyMetadataGetAttributes(native, syscall.GetFileExInfoStandard, (*byte)(unsafe.Pointer(&attributes)))
	if errors.Is(err, os.ErrNotExist) {
		return nil, &os.PathError{Op: "GetFileAttributesEx", Path: path, Err: err}
	}
	if err == nil && attributes.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
		return &topologyPlainMetadata{name: filepath.Base(path), data: attributes}, nil
	}
	if err == windows.ERROR_SHARING_VIOLATION {
		var data syscall.Win32finddata
		h, findErr := topologyMetadataFindFirst(native, &data)
		if findErr != nil {
			return nil, &os.PathError{Op: "FindFirstFile", Path: path, Err: findErr}
		}
		owner := &db.directoryOwner
		*owner = topologyDirectoryOwner{handle: h, find: true}
		if err := closeTopologyMetadata(owner); err != nil {
			return nil, err
		}
		if data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
			return &topologyPlainMetadata{name: filepath.Base(path), data: syscall.Win32FileAttributeData{FileAttributes: data.FileAttributes, CreationTime: data.CreationTime, LastAccessTime: data.LastAccessTime, LastWriteTime: data.LastWriteTime, FileSizeHigh: data.FileSizeHigh, FileSizeLow: data.FileSizeLow}}, nil
		}
	}
	flags := uint32(syscall.FILE_FLAG_BACKUP_SEMANTICS | syscall.FILE_FLAG_OPEN_REPARSE_POINT)
	h, err := topologyMetadataCreateFile(native, 0, 0, nil, syscall.OPEN_EXISTING, flags, 0)
	if err == windows.ERROR_INVALID_PARAMETER {
		h, err = topologyMetadataCreateFile(native, syscall.GENERIC_READ, 0, nil, syscall.OPEN_EXISTING, flags, 0)
	}
	if err != nil {
		return nil, &os.PathError{Op: "CreateFile", Path: path, Err: err}
	}
	owner := &db.directoryOwner
	*owner = topologyDirectoryOwner{handle: h}
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
	if err := closeTopologyMetadata(owner); err != nil {
		return nil, err
	}
	if statErr != nil || !surrogate {
		return info, statErr
	}
	h, err = topologyMetadataCreateFile(native, 0, 0, nil, syscall.OPEN_EXISTING, syscall.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return nil, &os.PathError{Op: "CreateFile", Path: path, Err: err}
	}
	*owner = topologyDirectoryOwner{handle: h}
	owner.file = os.NewFile(uintptr(h), path)
	info, statErr = owner.file.Stat()
	if err := closeTopologyMetadata(owner); err != nil {
		return nil, err
	}
	return info, statErr
}

// Pinned MkdirAll branching with an owner-local metadata boundary. Recursive
// errors borrow the admitted logical string; no generic filesystem API exists.
func (db *topologyDatabase) makeTopologyDirectory(path string) error {
	info, err := db.directoryMetadata("Stat", path, true)
	if db.directoryOwner.terminal || errors.Is(err, errTopologyBudget) {
		return err
	}
	if err == nil {
		if info.IsDir() {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
	}
	separator := func(c byte) bool { return c == '\\' || c == '/' }
	i := len(path) - 1
	for i >= 0 && separator(path[i]) {
		i--
	}
	for i >= 0 && !separator(path[i]) {
		i--
	}
	if i < 0 {
		i = 0
	}
	if parent := path[:i]; len(parent) > len(filepath.VolumeName(path)) {
		if err = db.makeTopologyDirectory(parent); err != nil {
			return err
		}
	}
	name, pathErr := topologyDirectoryPath(path)
	if pathErr != nil {
		return pathErr
	}
	native, pathErr := syscall.UTF16PtrFromString(name)
	if pathErr != nil {
		return &os.PathError{Op: "mkdir", Path: path, Err: pathErr}
	}
	err = syscall.CreateDirectory(native, nil)
	if err == nil {
		return nil
	}
	mkdirErr := &os.PathError{Op: "mkdir", Path: path, Err: err}
	info, err = db.directoryMetadata("Lstat", path, len(path) != 0 && separator(path[len(path)-1]))
	if db.directoryOwner.terminal || errors.Is(err, errTopologyBudget) {
		return err
	}
	if err == nil && info.IsDir() {
		return nil
	}
	return mkdirErr
}
