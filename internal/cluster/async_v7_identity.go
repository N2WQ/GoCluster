package cluster

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/internal/asynclog"
	"dxcluster/internal/logfiles"
	"dxcluster/internal/logidentity"
	"dxcluster/internal/logutil"
)

// A process-wide registry keeps identities claimed by workers that did not
// finish before shutdown. Filesystem operations never hold its mutex.
var sharedLogIdentities logidentity.Registry

// logCloseOwner retains every handle whose close could not be confirmed.
// At most three handles can be uncertain: a cleanup directory, an openat
// directory and the newly opened file. The stream performs no more I/O afterward.
type logCloseOwner struct {
	files     [3]*os.File
	err       error
	closeFile func(*os.File) error // test seam
}

func (o *logCloseOwner) close(file *os.File) error {
	if file == nil {
		return nil
	}
	closeFile := o.closeFile
	if closeFile == nil {
		closeFile = (*os.File).Close
	}
	err := closeFile(file)
	if err != nil {
		for i := range o.files {
			if o.files[i] == nil {
				o.files[i] = file
				break
			}
		}
		o.err = errors.Join(o.err, err)
	}
	return err
}

// startGuard prevents a lease from being stranded if an async slot is stopped
// before its worker invokes the factory.
type logStartGuard struct {
	state atomic.Uint32
	lease logidentity.Lease
	batch *logIdentityBatch
}

func (s *logStartGuard) skip() {
	if s.state.CompareAndSwap(0, 2) {
		_ = s.lease.Release() //nolint:errcheck // A failed release safely keeps the slot unavailable.
		s.batch.completed()
	}
}

func (s *logStartGuard) start(cfg logfiles.DestinationConfig) asynclog.Sink {
	if !s.state.CompareAndSwap(0, 1) {
		return nil
	}
	select {
	case <-s.batch.stop:
		s.batch.completed()
		return &guardedLogFile{lease: s.lease}
	default:
		return newGuardedLogFile(cfg, s.lease, s.batch)
	}
}

type logIdentityBatch struct {
	mu        sync.Mutex
	remaining int
	ready     chan struct{}
	stop      <-chan struct{}
	logging   *v6LogRuntime
	conflict  atomic.Bool
}

func (b *logIdentityBatch) completed() {
	b.mu.Lock()
	b.remaining--
	if b.remaining == 0 {
		close(b.ready)
	}
	b.mu.Unlock()
}

func (b *logIdentityBatch) reportConflict(status logidentity.Status) {
	if status.State == logidentity.Conflict {
		b.conflict.Store(true)
		b.logging.reportConflict(status.First, status.Other)
	}
}

func (b *logIdentityBatch) reportClaimConflict(stream uint8, err error) {
	var conflict *logidentity.ConflictError
	if errors.As(err, &conflict) {
		b.reportConflict(logidentity.Status{State: logidentity.Conflict, First: stream, Other: conflict.Other})
	}
}

// guardedLogFile holds its identity lease until the sink and all file calls
// have really finished. A failed check drops records without blocking callers.
type guardedLogFile struct {
	lease           logidentity.Lease
	stream          uint8
	batch           *logIdentityBatch
	config          logfiles.DestinationConfig
	safe            bool
	sink            *legacyAsyncFile
	folder          *os.File
	root            *os.Root
	folderID        logidentity.DirectoryID
	path            string
	file            *os.File
	uncertainFolder *os.File
	probeErr        error
	closeOwner      logCloseOwner
}

func (s *guardedLogFile) Write(at time.Time, frame uint8, payload []byte) asynclog.WriteResult {
	if s.closeOwner.err != nil {
		return asynclog.NotWritten
	}
	if s.sink == nil && s.batch != nil {
		if !s.safe && s.resetProbe() {
			s.safe = s.inspectDestination(s.config, s.batch)
		}
		s.activate()
	}
	if s.closeOwner.err != nil || s.sink == nil || !s.sameDirectory() {
		return asynclog.NotWritten
	}
	return s.sink.Write(at, frame, payload)
}

func (s *guardedLogFile) Close() error {
	if s.closeOwner.err != nil {
		return s.closeOwner.err
	}
	if s.probeErr != nil {
		return s.probeErr
	}
	if s.uncertainFolder != nil {
		return errors.New("log directory close could not be verified")
	}
	if s.sink != nil {
		if err := s.sink.Close(); err != nil {
			return err
		}
	}
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return err
		}
	}
	if s.folder != nil {
		if err := s.folder.Close(); err != nil {
			return err
		}
	}
	if s.root != nil {
		if err := s.root.Close(); err != nil {
			return err
		}
	}
	if err := s.lease.ReleaseFile(); err != nil {
		return err
	}
	return s.lease.Release()
}

// A failed probe may be retried only after every handle from that attempt
// closed successfully. An uncertain close retains the owner permanently.
func (s *guardedLogFile) resetProbe() bool {
	if s.probeErr != nil || s.uncertainFolder != nil {
		return false
	}
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			s.probeErr = err
			return false
		}
		s.file = nil
	}
	if s.folder != nil {
		if err := s.folder.Close(); err != nil {
			s.probeErr = err
			return false
		}
		s.folder = nil
	}
	if s.root != nil {
		if err := s.root.Close(); err != nil {
			s.probeErr = err
			return false
		}
		s.root = nil
	}
	if err := s.lease.Reset(); err != nil {
		s.probeErr = err
		return false
	}
	s.folderID, s.path = logidentity.DirectoryID{}, ""
	return true
}

func newGuardedLogFile(cfg logfiles.DestinationConfig, lease logidentity.Lease, batch *logIdentityBatch) asynclog.Sink {
	guard := &guardedLogFile{lease: lease, stream: cfg.Stream, batch: batch, config: cfg}
	guard.safe = guard.inspectDestination(cfg, batch)
	batch.completed()
	select {
	case <-batch.stop:
		return guard
	case <-batch.ready:
	}
	// Prefer cancellation even if the last probe and shutdown completed at
	// the same time; a retired worker must not resume writing afterward.
	select {
	case <-batch.stop:
		return guard
	default:
	}
	guard.activate()
	return guard
}

func (guard *guardedLogFile) activate() {
	batch := guard.batch
	select {
	case <-batch.stop:
		return
	default:
	}
	if batch.conflict.Load() {
		return
	}
	dirs, files := sharedLogIdentities.DirectoryStatus(), sharedLogIdentities.FileStatus()
	batch.reportConflict(dirs)
	batch.reportConflict(files)
	if batch.conflict.Load() || !guard.safe || dirs.State != logidentity.Ready || files.State != logidentity.Ready {
		return
	}
	// On Windows, retaining the inspection handle can prevent daily rotation.
	// The writer will replace its identity claim with the actual opened file.
	if guard.file != nil {
		if err := guard.file.Close(); err != nil {
			return
		}
		guard.file = nil
	}
	if !guard.sameDirectory() {
		return
	}
	if cfg := guard.config; cfg.Stream == streamPropagation && batch.logging.registerPropagationRoot != nil {
		batch.logging.registerPropagationRoot(guard.root)
	}
	cfg := guard.config
	if cfg.Stream == streamPropagation && batch.logging.rootedRotation != nil {
		cfg.RotateHook = func(previous time.Time, archive, active string) {
			batch.logging.rootedRotation(previous, archive, active, guard.root)
		}
	} else if hook := cfg.RotateHook; hook != nil {
		cfg.RotateHook = func(previous time.Time, archive, active string) {
			if guard.sameDirectory() {
				hook(previous, archive, active)
			}
		}
	}
	guard.sink = newLegacyAsyncFileInRootOwned(cfg, guard.root, &guard.closeOwner)
	guard.sink.openFile = guard.openActiveFile
	if guard.sink.daily != nil {
		guard.sink.daily.openFile = guard.openActiveFile
	}
}

func (s *guardedLogFile) openActiveFile(path string, flags int, mode os.FileMode) (*os.File, error) {
	file, err := openSafeLogInRootOwned(s.root, filepath.Base(path), flags, mode, &s.closeOwner)
	if err != nil {
		return nil, err
	}
	id, _, err := logidentity.IdentifyFile(file)
	if err == nil {
		err = s.lease.ReplaceFile(id)
	}
	if err != nil {
		if s.batch != nil {
			s.batch.reportClaimConflict(s.stream, err)
		}
		_ = s.closeOwner.close(file)
		return nil, err
	}
	return file, nil
}

func (s *guardedLogFile) inspectDestination(cfg logfiles.DestinationConfig, batch *logIdentityBatch) bool {
	dir := cfg.Path
	name := logutil.DailyActiveFileName(dir)
	if cfg.Kind == logfiles.Overlong {
		name = filepath.Base(cfg.Path)
		dir = filepath.Dir(cfg.Path)
	}
	if dir == "" || ensureLogDirectory(dir) != nil {
		return false
	}
	openRoot := batch.logging.openLogRoot
	if openRoot == nil {
		openRoot = os.OpenRoot
	}
	root, err := openRoot(filepath.Clean(dir))
	if err != nil {
		return false
	}
	s.root = root
	folder, err := root.Open(".")
	if err != nil {
		return false
	}
	s.folder = folder
	id, identifyErr := logidentity.IdentifyDirectory(folder)
	if identifyErr != nil {
		return false
	}
	if err := s.lease.ClaimDirectory(id); err != nil {
		batch.reportClaimConflict(s.stream, err)
		return false
	}
	s.folderID, s.path = id, dir
	entry, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return s.lease.MarkFileAbsent() == nil
	}
	if err != nil {
		return false
	}
	if !entry.Mode().IsRegular() && entry.Mode()&os.ModeSymlink == 0 {
		return false
	}
	var file *os.File
	if entry.Mode()&os.ModeSymlink != 0 {
		// Inspect a link target only to detect a conflict; it is never used
		// for writing and is closed before this worker proceeds.
		file, err = os.Open(strings.Clone(filepath.Join(dir, name)))
	} else {
		file, err = openRootNoFollowOwned(root, name, os.O_RDONLY, 0, &s.closeOwner)
	}
	if err != nil {
		return false
	}
	s.file = file
	fileID, links, identifyErr := logidentity.IdentifyFile(file)
	opened, statErr := file.Stat()
	if identifyErr != nil || statErr != nil {
		if file.Close() == nil {
			s.file = nil
		}
		return false
	}
	claimErr := s.lease.ClaimFile(fileID)
	if claimErr != nil {
		batch.reportClaimConflict(s.stream, claimErr)
	}
	safe := claimErr == nil && entry.Mode().IsRegular() && links == 1 && os.SameFile(entry, opened)
	if !safe {
		// Do not hold an inspection handle on an unrelated symlink target or
		// external hard link for the lifetime of the application.
		if file.Close() == nil {
			s.file = nil
		}
	}
	return safe
}

// Recheck a configured directory on its worker before further filesystem
// operations, since an operator can retarget a directory link while running.
func (s *guardedLogFile) sameDirectory() bool {
	if s.uncertainFolder != nil || (s.batch != nil && s.batch.conflict.Load()) {
		return false
	}
	status := sharedLogIdentities.FileStatus()
	if status.State != logidentity.Ready {
		if s.batch != nil {
			s.batch.reportConflict(status)
		}
		return false
	}
	folder, err := os.Open(filepath.Clean(s.path))
	if err != nil {
		return false
	}
	id, identifyErr := logidentity.IdentifyDirectory(folder)
	closeErr := folder.Close()
	if closeErr != nil {
		s.uncertainFolder = folder
	}
	if identifyErr != nil || closeErr != nil {
		return false
	}
	other, err := s.lease.CheckDirectory(id)
	if errors.Is(err, logidentity.ErrConflict) && s.batch != nil {
		s.batch.reportConflict(logidentity.Status{State: logidentity.Conflict, First: s.stream, Other: other})
	}
	return err == nil && id == s.folderID && (s.batch == nil || !s.batch.conflict.Load())
}

// safeExistingLog refuses links and verifies the identity of the file opened
// for inspection. Missing paths are safe to create exclusively.
func safeExistingLog(path string) bool {
	entry, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	if err != nil || !entry.Mode().IsRegular() {
		return false
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	_, links, identifyErr := logidentity.IdentifyFile(file)
	opened, statErr := file.Stat()
	closeErr := file.Close()
	return identifyErr == nil && statErr == nil && closeErr == nil && links == 1 && os.SameFile(entry, opened)
}

func openSafeLog(path string, flags int, mode os.FileMode) (*os.File, error) {
	entry, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		entry = nil
		flags |= os.O_EXCL
	} else if err != nil {
		return nil, err
	} else if !entry.Mode().IsRegular() {
		return nil, errors.New("log path is not a regular file")
	}
	file, err := os.OpenFile(path, flags, mode)
	if err != nil {
		return nil, err
	}
	_, links, identifyErr := logidentity.IdentifyFile(file)
	opened, statErr := file.Stat()
	if identifyErr != nil || statErr != nil || links != 1 || (entry != nil && !os.SameFile(entry, opened)) {
		_ = file.Close()
		return nil, errors.New("log file identity could not be verified")
	}
	return file, nil
}

// Root-based variants keep every operation beneath the verified directory
// handle even if a configured shortcut is retargeted during rotation.
func safeExistingLogInRootOwned(root *os.Root, name string, owner *logCloseOwner) bool {
	if owner != nil && owner.err != nil {
		return false
	}
	entry, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	if err != nil || !entry.Mode().IsRegular() {
		return false
	}
	file, err := openRootNoFollowOwned(root, name, os.O_RDONLY, 0, owner)
	if err != nil {
		return false
	}
	_, links, identifyErr := logidentity.IdentifyFile(file)
	opened, statErr := file.Stat()
	var closeErr error
	if owner != nil {
		closeErr = owner.close(file)
	} else {
		closeErr = file.Close()
	}
	return identifyErr == nil && statErr == nil && closeErr == nil && links == 1 && os.SameFile(entry, opened)
}

func openSafeLogInRoot(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	return openSafeLogInRootOwned(root, name, flags, mode, nil)
}

func openSafeLogInRootOwned(root *os.Root, name string, flags int, mode os.FileMode, owner *logCloseOwner) (*os.File, error) {
	if owner != nil && owner.err != nil {
		return nil, owner.err
	}
	entry, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		entry = nil
		flags |= os.O_EXCL
	} else if err != nil {
		return nil, err
	} else if !entry.Mode().IsRegular() {
		return nil, errors.New("log path is not a regular file")
	}
	file, err := openRootNoFollowOwned(root, name, flags, mode, owner)
	if err != nil {
		return nil, err
	}
	_, links, identifyErr := logidentity.IdentifyFile(file)
	opened, statErr := file.Stat()
	if identifyErr != nil || statErr != nil || links != 1 || (entry != nil && !os.SameFile(entry, opened)) {
		if owner != nil {
			_ = owner.close(file)
		} else {
			_ = file.Close()
		}
		return nil, errors.New("log file identity could not be verified")
	}
	return file, nil
}
