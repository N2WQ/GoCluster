//go:build windows

// Portions copyright 2012 The Go Authors. All rights reserved.
// The metadata branch order follows Go 1.26.4 os/stat_windows.go. Its BSD
// license is retained in ../../third_party/go-sqlite3/provenance/GO-LICENSE.txt.

package peerdiag

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
	diagnosticGetAttributes      = syscall.GetFileAttributesEx
	diagnosticMetadataFindFirst  = syscall.FindFirstFile
	diagnosticMetadataFindClose  = syscall.FindClose
	diagnosticMetadataCreateFile = syscall.CreateFile
	diagnosticMetadataFileClose  = (*os.File).Close
	errMetadataRelease           = errors.New("diagnostic metadata release unconfirmed")
)

const metadataFind = 1
const metadataFile = 2

// One concrete slot belongs to the single-threaded helper. Failed native close
// is sticky: no retry or second acquisition is safe after an uncertain close.
// The parent's positively witnessed helper-process retirement is the release
// boundary. No path, FileInfo or variable-size error list is stored here.
type metadataOwner struct {
	handle syscall.Handle
	file   *os.File
	kind   uint8
	failed bool
}

func (s *helperSink) metadataFailed() bool { return s.metadata.failed }

func (s *helperSink) closeMetadata() error {
	owner := &s.metadata
	if owner.failed {
		return errMetadataRelease
	}
	var err error
	switch owner.kind {
	case metadataFind:
		err = diagnosticMetadataFindClose(owner.handle)
	case metadataFile:
		err = diagnosticMetadataFileClose(owner.file)
	}
	if err != nil {
		owner.failed = true
		return errMetadataRelease // cannot unwrap as an intentionally ignored ordinary error
	}
	*owner = metadataOwner{}
	return nil
}

// Plain attributes have no surrogate or GODEBUG-dependent mode interpretation.
// Retain only the logical basename, not temporary native path backing. Handle
// branches return os.File.Stat metadata to preserve Go's effective mode rules.
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
	mode := os.FileMode(0o666)
	if m.data.FileAttributes&syscall.FILE_ATTRIBUTE_READONLY != 0 {
		mode = 0o444
	}
	if m.IsDir() {
		mode |= os.ModeDir | 0o111
	}
	return mode
}
func (m *plainMetadata) Sys() any { data := m.data; return &data }

func (s *helperSink) diagnosticStat(path string) (os.FileInfo, error) {
	return s.metadataStat("Stat", path, true)
}
func (s *helperSink) diagnosticLstat(path string) (os.FileInfo, error) {
	return s.metadataStat("Lstat", path, len(path) != 0 && isPathSeparator(path[len(path)-1]))
}

func (s *helperSink) metadataStat(operation, path string, follow bool) (os.FileInfo, error) {
	if s.metadataFailed() {
		return nil, errMetadataRelease
	}
	if path == "" {
		return nil, &os.PathError{Op: operation, Path: path, Err: syscall.ERROR_PATH_NOT_FOUND}
	}
	name, err := helperOperationPath(path)
	if err != nil {
		return nil, &os.PathError{Op: operation, Path: path, Err: err}
	}
	native, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return nil, &os.PathError{Op: operation, Path: path, Err: err}
	}
	var attributes syscall.Win32FileAttributeData
	err = diagnosticGetAttributes(native, syscall.GetFileExInfoStandard, (*byte)(unsafe.Pointer(&attributes)))
	if errors.Is(err, os.ErrNotExist) {
		return nil, &os.PathError{Op: "GetFileAttributesEx", Path: path, Err: err}
	}
	if err == nil && attributes.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
		return &plainMetadata{name: filepath.Base(path), data: attributes}, nil
	}
	if err == windows.ERROR_SHARING_VIOLATION {
		var data syscall.Win32finddata
		handle, findErr := diagnosticMetadataFindFirst(native, &data)
		if findErr != nil {
			return nil, &os.PathError{Op: "FindFirstFile", Path: path, Err: findErr}
		}
		s.metadata = metadataOwner{handle: handle, kind: metadataFind}
		if err := s.closeMetadata(); err != nil {
			return nil, err
		}
		if data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
			return &plainMetadata{name: filepath.Base(path), data: syscall.Win32FileAttributeData{
				FileAttributes: data.FileAttributes, CreationTime: data.CreationTime,
				LastAccessTime: data.LastAccessTime, LastWriteTime: data.LastWriteTime,
				FileSizeHigh: data.FileSizeHigh, FileSizeLow: data.FileSizeLow,
			}}, nil
		}
	}
	return s.metadataFromHandle(path, native, follow)
}

func (s *helperSink) metadataFromHandle(path string, native *uint16, follow bool) (os.FileInfo, error) {
	flags := uint32(syscall.FILE_FLAG_BACKUP_SEMANTICS | syscall.FILE_FLAG_OPEN_REPARSE_POINT)
	handle, err := diagnosticMetadataCreateFile(native, 0, 0, nil, syscall.OPEN_EXISTING, flags, 0)
	if err == windows.ERROR_INVALID_PARAMETER {
		// Only the existing console branch adds read access; ordinary metadata
		// must remain available without read-data permission.
		handle, err = diagnosticMetadataCreateFile(native, syscall.GENERIC_READ, 0, nil, syscall.OPEN_EXISTING, flags, 0)
	}
	if err != nil {
		return nil, &os.PathError{Op: "CreateFile", Path: path, Err: err}
	}
	s.metadata = metadataOwner{handle: handle, file: os.NewFile(uintptr(handle), path), kind: metadataFile}
	info, statErr := s.metadata.file.Stat()
	var surrogate bool
	if statErr == nil && follow {
		if attributes, ok := info.Sys().(*syscall.Win32FileAttributeData); ok && attributes.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
			// os.FileInfo does not expose the reparse tag. This one fixed tag
			// probe selects Go's name-surrogate follow branch while File.Stat
			// keeps its effective winsymlink mode semantics. No path resolution,
			// buffer growth, extra open or atomic-snapshot promise is introduced.
			var tag struct{ attributes, tag uint32 }
			statErr = windows.GetFileInformationByHandleEx(windows.Handle(handle), windows.FileAttributeTagInfo, (*byte)(unsafe.Pointer(&tag)), uint32(unsafe.Sizeof(tag)))
			if statErr != nil {
				statErr = &os.PathError{Op: "GetFileInformationByHandleEx", Path: path, Err: statErr}
			}
			surrogate = tag.tag&0x20000000 != 0
		}
	}
	if err := s.closeMetadata(); err != nil {
		return nil, err
	}
	if statErr != nil || !surrogate {
		return info, statErr
	}
	handle, err = diagnosticMetadataCreateFile(native, 0, 0, nil, syscall.OPEN_EXISTING, syscall.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return nil, &os.PathError{Op: "CreateFile", Path: path, Err: err}
	}
	s.metadata = metadataOwner{handle: handle, file: os.NewFile(uintptr(handle), path), kind: metadataFile}
	info, statErr = s.metadata.file.Stat()
	if err := s.closeMetadata(); err != nil {
		return nil, err
	}
	return info, statErr
}
