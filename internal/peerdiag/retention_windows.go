//go:build windows

// Portions copyright 2009 The Go Authors. All rights reserved.
// The pinned file-mode predicate uses the BSD-style license in
// ../../third_party/go-sqlite3/provenance/GO-LICENSE.txt.

package peerdiag

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"dxcluster/internal/logutil"
)

var diagnosticFindFirst = syscall.FindFirstFile
var diagnosticFindNext = syscall.FindNextFile
var diagnosticFindClose = syscall.FindClose
var errFindRelease = errors.New("diagnostic directory release unconfirmed")

type directoryScan struct {
	handle syscall.Handle
	active bool
	failed bool
}

func (s *helperSink) scanFailed() bool { return s.scan.failed }

func (s *helperSink) closeScan() error {
	if s.scan.failed {
		return errFindRelease
	}
	if s.scan.active {
		if err := diagnosticFindClose(s.scan.handle); err != nil {
			// One failed close poisons this entire helper generation. The owner
			// stays in the sink, never rescans/retries, and exits after ackFailed.
			// Only the parent's actual process Wait discharges its reservation.
			s.scan.failed = true
			return err
		}
		s.scan = directoryScan{}
	}
	return nil
}

func (s *helperSink) cleanup(day time.Time) (resultErr error) {
	if s.scan.failed {
		return errFindRelease
	}
	if s.options.RetentionDays <= 0 {
		return nil
	}
	if s.options.Directory == "" {
		return &os.PathError{Op: "open", Path: "", Err: syscall.ERROR_FILE_NOT_FOUND}
	}
	pattern, err := helperOperationPath(filepath.Join(s.options.Directory, "*"))
	if err != nil {
		return err
	}
	encoded, err := syscall.UTF16FromString(pattern)
	if err != nil {
		return err
	}
	var data syscall.Win32finddata
	handle, err := diagnosticFindFirst(&encoded[0], &data)
	if err != nil {
		if errors.Is(err, syscall.ERROR_FILE_NOT_FOUND) {
			return nil // empty directory, including roots without dot entries
		}
		return err
	}
	s.scan = directoryScan{handle: handle, active: true}
	defer func() {
		if err := s.closeScan(); err != nil {
			resultErr = err
		}
	}()
	cutoff := day.AddDate(0, 0, -(s.options.RetentionDays - 1))
	for {
		// Match pinned Go1.26 fileStat.mode's directory/name-surrogate rule.
		// A junction/symlink has no ModeDir; Remove acts on its entry, not target.
		isDirectory := data.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0 &&
			!(data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 && data.Reserved0&0x20000000 != 0)
		if !isDirectory {
			if name, ok := diagnosticArchiveName(&data.FileName); ok {
				if date, ok := logutil.ParseDailyArchiveDate(name); ok && date.Before(cutoff) {
					_ = diagnosticRemove(filepath.Join(s.options.Directory, name))
				}
			}
		}
		if err := diagnosticFindNext(handle, &data); err != nil {
			if errors.Is(err, syscall.ERROR_NO_MORE_FILES) {
				return nil
			}
			return err
		}
	}
}

func diagnosticArchiveName(input *[syscall.MAX_PATH - 1]uint16) (string, bool) {
	const length = len("02-Jan-2006.log")
	if input[length] != 0 {
		return "", false
	}
	var name [length]byte
	for i := range name {
		if input[i] == 0 || input[i] > 0x7f {
			return "", false
		}
		name[i] = byte(input[i])
	}
	return string(name[:]), true
}
