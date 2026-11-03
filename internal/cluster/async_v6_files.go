package cluster

import (
	"bytes"
	"os"
	"path/filepath"
	"time"

	"dxcluster/internal/asynclog"
	"dxcluster/internal/logfiles"
)

// legacyAsyncFile owns all filesystem work for one destination. Construction,
// rotation and close run only on its destination worker, never on a producer.
type legacyAsyncFile struct {
	config     logfiles.DestinationConfig
	root       *os.Root
	daily      *dailyFileSink
	processor  logfiles.Processor
	processed  [asynclog.MaxRecordBytes]byte
	openFile   func(string, int, os.FileMode) (*os.File, error)
	closeFile  func(*os.File) error
	uncertain  *os.File
	closeErr   error
	closeOwner *logCloseOwner
}

func newLegacyAsyncFile(cfg logfiles.DestinationConfig) *legacyAsyncFile {
	return newLegacyAsyncFileInRoot(cfg, nil)
}

func newLegacyAsyncFileInRoot(cfg logfiles.DestinationConfig, root *os.Root) *legacyAsyncFile {
	return newLegacyAsyncFileInRootOwned(cfg, root, nil)
}

func newLegacyAsyncFileInRootOwned(cfg logfiles.DestinationConfig, root *os.Root, owner *logCloseOwner) *legacyAsyncFile {
	s := &legacyAsyncFile{config: cfg, root: root, closeOwner: owner}
	if cfg.ProcessorFactory != nil {
		s.processor = cfg.ProcessorFactory()
	}
	if cfg.Kind == logfiles.Daily {
		s.openDaily()
	}
	return s
}

func (s *legacyAsyncFile) openDaily() {
	var daily *dailyFileSink
	var err error
	if s.root == nil {
		daily, err = newDailyFileSink(s.config.Path, s.config.RetentionDays)
	} else {
		daily, err = newDailyFileSinkInRootOwned(s.config.Path, s.config.RetentionDays, s.root, s.closeOwner)
	}
	if err != nil {
		return
	}
	if s.config.RotateHook != nil {
		daily.SetRotateHook(s.config.RotateHook)
	}
	daily.checkActive = safeExistingLog
	daily.openFile = s.openFile
	if daily.openFile == nil && s.root == nil {
		daily.openFile = openSafeLog
	}
	daily.workerHook = true
	s.daily = daily
}

func (s *legacyAsyncFile) Write(at time.Time, _ uint8, payload []byte) asynclog.WriteResult {
	if s == nil || s.closeErr != nil || len(payload) > asynclog.MaxRecordBytes {
		return asynclog.NotWritten
	}
	input := payload
	written := false
	if s.processor != nil {
		n, decision := s.processor.Prepare(at, input, &s.processed)
		if decision == logfiles.ProcessSuppress {
			return asynclog.Suppressed
		}
		if decision != logfiles.ProcessWrite || n < 0 || n > len(s.processed) {
			return asynclog.NotWritten
		}
		payload = s.processed[:n]
		defer func() { s.processor.Complete(input, written) }()
	}
	if s.config.Kind == logfiles.Overlong {
		written, s.uncertain, s.closeErr = writeLegacyOverlongWithOwner(s.config.Path, at, payload, s.root, s.openFile, s.closeFile, s.closeOwner)
		if written {
			return asynclog.Written
		}
		return asynclog.NotWritten
	}
	if s.daily == nil {
		s.openDaily()
		if s.daily == nil {
			return asynclog.NotWritten
		}
	}
	if s.config.Stream == streamSystemFile {
		written = true
		for {
			line, rest, more := bytes.Cut(payload, []byte("\n"))
			if !s.daily.writeLine(string(line), at) {
				written = false
				break
			}
			if !more {
				break
			}
			payload = rest
		}
	} else {
		written = s.daily.writeLine(string(payload), at)
	}
	if written {
		return asynclog.Written
	}
	return asynclog.NotWritten
}

func (s *legacyAsyncFile) Close() error {
	if s == nil {
		return nil
	}
	if s.closeErr != nil {
		return s.closeErr
	}
	if s.daily == nil {
		return nil
	}
	return s.daily.Close()
}

// The historical overlong file uses a stable name and two size-limited
// backups. It is disposable and does not block the peer reader on I/O.
func writeLegacyOverlongWithOwner(path string, at time.Time, payload []byte, root *os.Root, openFile func(string, int, os.FileMode) (*os.File, error), closeFile func(*os.File) error, owner *logCloseOwner) (bool, *os.File, error) {
	if owner != nil && owner.err != nil {
		return false, nil, owner.err
	}
	if path == "" || len(payload) > asynclog.MaxRecordBytes {
		return false, nil, nil
	}
	name := path
	if root == nil {
		if err := ensureLogDirectory(filepath.Dir(path)); err != nil {
			return false, nil, nil //nolint:nilerr // An ordinary open failure drops this record and remains retryable.
		}
	} else {
		name = filepath.Base(path)
	}
	safe := safeExistingLog
	stat := os.Stat
	remove := os.Remove
	rename := os.Rename
	if root != nil {
		safe = func(name string) bool { return safeExistingLogInRootOwned(root, name, owner) }
		stat, remove, rename = root.Lstat, root.Remove, root.Rename
	}
	if !safe(name) {
		return false, nil, nil
	}
	if info, err := stat(name); err == nil && info.Size() >= 8<<20 {
		if !safe(name+".1") || !safe(name+".2") {
			return false, nil, nil
		}
		if err := remove(name + ".2"); err != nil && !os.IsNotExist(err) {
			return false, nil, nil
		}
		if err := rename(name+".1", name+".2"); err != nil && !os.IsNotExist(err) {
			return false, nil, nil
		}
		if err := rename(name, name+".1"); err != nil {
			return false, nil, nil //nolint:nilerr // A rotation failure drops the record and remains retryable.
		}
	} else if err != nil && !os.IsNotExist(err) {
		return false, nil, nil
	}
	if openFile == nil {
		if root == nil {
			openFile = openSafeLog
		} else {
			openFile = func(path string, flags int, mode os.FileMode) (*os.File, error) {
				return openSafeLogInRootOwned(root, filepath.Base(path), flags, mode, owner)
			}
		}
	}
	f, err := openFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return false, nil, nil //nolint:nilerr // An ordinary open failure drops this record and remains retryable.
	}
	var storage [asynclog.MaxRecordBytes + 64]byte
	line := at.UTC().AppendFormat(storage[:0], time.RFC3339)
	line = append(line, ' ')
	line = append(line, payload...)
	line = append(line, '\n')
	n, writeErr := f.Write(line)
	if closeFile == nil {
		closeFile = (*os.File).Close
	}
	closeErr := closeFile(f)
	if closeErr != nil {
		return false, f, closeErr
	}
	return writeErr == nil && n == len(line), nil, nil
}
