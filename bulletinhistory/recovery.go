package bulletinhistory

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Recovery uses two fixed slots, bounded to maxSnapshotBytes. Neither is
// overwritten. A crash or failed copy may leave a partial slot; it must be
// moved aside by the operator before recovery can proceed. The source remains
// authoritative until an independent, complete copy has been committed.
type recoveryFile interface {
	io.Reader
	io.Writer
	Stat() (os.FileInfo, error)
	Sync() error
	Close() error
}

type recoveryOps struct {
	open    func(string, int, os.FileMode) (recoveryFile, error)
	lstat   func(string) (os.FileInfo, error)
	commit  func(string, string) error
	syncDir func(string) error
}

func defaultRecoveryOps() recoveryOps {
	return recoveryOps{
		open: func(path string, flags int, mode os.FileMode) (recoveryFile, error) {
			return os.OpenFile(path, flags, mode)
		},
		lstat: os.Lstat, commit: commitRecoveryFile, syncDir: syncRecoveryDirectory,
	}
}

func preserveFailedSnapshot(path string) error {
	return preserveSnapshot(path, defaultRecoveryOps())
}

func readRecoveryFile(path string, writable bool, ops recoveryOps) ([]byte, os.FileInfo, error) {
	info, err := ops.lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("%s is not a regular file", path)
	}
	flags := os.O_RDONLY
	if writable {
		flags = os.O_RDWR
	}
	f, err := ops.open(path, flags, 0)
	if err != nil {
		return nil, nil, err
	}
	opened, err := f.Stat()
	if err == nil && (!opened.Mode().IsRegular() || !os.SameFile(info, opened)) {
		err = fmt.Errorf("%s changed while opening", path)
	}
	var data []byte
	if err == nil {
		data, err = io.ReadAll(io.LimitReader(f, maxSnapshotBytes+1))
	}
	if err == nil && len(data) > maxSnapshotBytes {
		err = fmt.Errorf("%s exceeds the recovery size limit", path)
	}
	if err == nil && writable {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return data, opened, err
}

func verifyRecoveryCopy(source string, original []byte, originalInfo os.FileInfo, copyPath string, ops recoveryOps) error {
	copyData, copyInfo, err := readRecoveryFile(copyPath, true, ops)
	if err != nil {
		return err
	}
	if os.SameFile(originalInfo, copyInfo) || !bytes.Equal(original, copyData) {
		return fmt.Errorf("%s is not an independent complete copy; move it aside", copyPath)
	}
	current, currentInfo, err := readRecoveryFile(source, false, ops)
	if err != nil {
		return err
	}
	if !os.SameFile(originalInfo, currentInfo) || !bytes.Equal(original, current) {
		return fmt.Errorf("%s changed during recovery", source)
	}
	return nil
}

func preserveSnapshot(path string, ops recoveryOps) error {
	original, sourceInfo, err := readRecoveryFile(path, false, ops)
	if err != nil {
		return fmt.Errorf("read failed snapshot %s: %w", path, err)
	}
	backup, temp := path+".recovery", path+".recovery.tmp"
	if _, err := ops.lstat(backup); err == nil {
		if err := verifyRecoveryCopy(path, original, sourceInfo, backup, ops); err != nil {
			return err
		}
		return ops.syncDir(filepath.Dir(path))
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := ops.lstat(temp); errors.Is(err, os.ErrNotExist) {
		f, createErr := ops.open(temp, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if createErr != nil {
			return createErr
		}
		n, writeErr := f.Write(original)
		if writeErr == nil && n != len(original) {
			writeErr = io.ErrShortWrite
		}
		if writeErr == nil {
			writeErr = f.Sync()
		}
		if closeErr := f.Close(); writeErr == nil {
			writeErr = closeErr
		}
		if writeErr != nil {
			return fmt.Errorf("copy to %s: %w", temp, writeErr)
		}
	} else if err != nil {
		return err
	}
	if err := verifyRecoveryCopy(path, original, sourceInfo, temp, ops); err != nil {
		return err
	}
	if err := ops.commit(temp, backup); err != nil {
		return fmt.Errorf("commit recovery copy %s: %w", backup, err)
	}
	if err := ops.syncDir(filepath.Dir(path)); err != nil {
		return err
	}
	return verifyRecoveryCopy(path, original, sourceInfo, backup, ops)
}
