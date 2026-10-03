package vfs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

type vfsOS struct{}

var openVFSFile = osOpenFile

func (vfsOS) Delete(path string, syncDir bool) error {
	path, err := boundedOSPath(path)
	if err != nil {
		return err
	}
	err = osRemove(path)
	if errors.Is(err, fs.ErrNotExist) {
		return sysError{err, _IOERR_DELETE_NOENT}
	}
	if err != nil {
		return err
	}
	if isUnix && syncDir {
		f, err := os.Open(filepath.Dir(path))
		if err != nil {
			return nil
		}
		return syncAndCloseDirectory(f, SYNC_FULL)
	}
	return nil
}

func (vfsOS) Access(name string, flags AccessFlag) (bool, error) {
	name, err := boundedOSPath(name)
	if err != nil {
		return false, err
	}
	err = osAccess(name, flags)
	if _, failed := retainedCloseOwner(err); failed {
		return false, err
	}
	if flags == ACCESS_EXISTS {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
	} else {
		if errors.Is(err, fs.ErrPermission) {
			return false, nil
		}
	}
	return err == nil, err
}

func (vfsOS) Open(name string, flags OpenFlag) (File, OpenFlag, error) {
	// notest // OpenFilename is called instead
	if name == "" {
		return vfsOS{}.OpenFilename(nil, flags)
	}
	return nil, 0, _CANTOPEN
}

func (vfsOS) OpenFilename(name *Filename, flags OpenFlag) (File, OpenFlag, error) {
	oflags := _O_NOFOLLOW
	if flags&OPEN_EXCLUSIVE != 0 {
		oflags |= os.O_EXCL
	}
	if flags&OPEN_CREATE != 0 {
		oflags |= os.O_CREATE
	}
	if flags&OPEN_READONLY != 0 {
		oflags |= os.O_RDONLY
	}
	if flags&OPEN_READWRITE != 0 {
		oflags |= os.O_RDWR
	}

	isCreate := flags&(OPEN_CREATE) != 0
	isJournl := flags&(OPEN_MAIN_JOURNAL|OPEN_SUPER_JOURNAL|OPEN_WAL) != 0

	var err error
	var f *os.File
	var openedPath string
	if name == nil {
		f, err = osCreateTemp(flags)
	} else {
		path, pathErr := boundedOSPath(name.String())
		if pathErr != nil {
			return nil, flags, pathErr
		}
		f, err = openVFSFile(path, oflags, 0666)
		openedPath = path
		if errors.Is(err, syscall.EISDIR) {
			return nil, flags, sysError{err, _CANTOPEN_ISDIR}
		}
		if isCreate && isJournl && errors.Is(err, fs.ErrPermission) {
			accessErr := osAccess(path, ACCESS_EXISTS)
			if _, failed := retainedCloseOwner(accessErr); failed {
				return nil, flags, accessErr
			}
			if accessErr != nil {
				return nil, flags, sysError{err, _READONLY_DIRECTORY}
			}
		}
	}
	if err != nil {
		return nil, flags, err
	}

	if value := name.callbackParameter("modeof"); len(value) != 0 {
		if len(value) > _MAX_PATHNAME {
			// Metadata reference files share the owned filename budget. This
			// guard precedes OS path conversion/stat, and the wrapper retains
			// the already opened file if cleanup of this refusal fails.
			return &vfsFile{File: f, flags: flags}, flags, _IOERR_NOMEM
		}
		modeof := string(value)
		modeof, err = boundedOSPath(modeof)
		if err == nil {
			err = osSetMode(f, modeof)
		}
		if err != nil {
			// Transfer even this partially initialized file to the wrapper so a
			// failed close cannot disappear with an initialization error.
			if err == _IOERR_NOMEM {
				return &vfsFile{File: f, flags: flags}, flags, err
			}
			return &vfsFile{File: f, flags: flags}, flags, sysError{err, _IOERR_FSTAT}
		}
	}

	file := vfsFile{
		File:  f,
		flags: flags | _FLAG_PSOW,
		mmap:  NewMemoryMapper(f, flags),
	}
	if osBatchAtomic(f) {
		file.flags |= _FLAG_ATOMIC
	}
	if isUnix && isCreate && isJournl {
		file.flags |= _FLAG_SYNC_DIR
	}
	if openedPath != "" {
		file.shm = NewSharedMemory(openedPath+"-shm", flags)
	}
	return &file, flags, nil
}

type vfsFile struct {
	*os.File
	shm         SharedMemory
	mmap        MemoryMapper
	lock        LockLevel
	flags       OpenFlag
	closeFailed bool
}

var (
	// Ensure these interfaces are implemented:
	_ FileLockState          = &vfsFile{}
	_ FileHasMoved           = &vfsFile{}
	_ FileSizeHint           = &vfsFile{}
	_ FilePersistWAL         = &vfsFile{}
	_ FilePowersafeOverwrite = &vfsFile{}
)

func (f *vfsFile) Close() error {
	if f.closeFailed {
		return errFileRelease
	}
	if f.shm != nil {
		if err := f.shm.Close(); err != nil {
			return err
		}
	}
	unlockErr := f.Unlock(LOCK_NONE)
	if unlockErr != nil && runtime.GOOS != "windows" {
		return unlockErr
	}
	// On Windows, a positively closed owning file releases all its locks,
	// including ranges whose individual unlock result was unconfirmed.
	if err := closeOwnedFile(f.File); err != nil {
		if runtime.GOOS == "windows" {
			f.closeFailed = true
		}
		return err
	}
	f.lock = LOCK_NONE
	if !isUnix && f.flags&OPEN_DELETEONCLOSE != 0 {
		osRemove(f.Name())
	}
	return nil
}

func (f *vfsFile) ReadAt(p []byte, off int64) (n int, err error) {
	return osReadAt(f.File, p, off)
}

func (f *vfsFile) WriteAt(p []byte, off int64) (n int, err error) {
	return osWriteAt(f.File, p, off)
}

func (f *vfsFile) Sync(flags SyncFlag) error {
	err := osSync(f.File, f.flags, flags)
	if err != nil {
		return err
	}
	if isUnix && f.flags&_FLAG_SYNC_DIR != 0 {
		f.flags ^= _FLAG_SYNC_DIR
		d, err := os.Open(filepath.Dir(f.File.Name()))
		if err != nil {
			return nil
		}
		return syncAndCloseDirectory(d, flags)
	}
	return nil
}

// A directory is a real native owner even though its lifetime normally fits
// within one callback. Failed close transfers it to the wrapper's fixed table.
func syncAndCloseDirectory(file *os.File, flags SyncFlag) error {
	err := syncDirectory(file, flags)
	if closeErr := closeDirectory(file); closeErr != nil {
		return retainedCloseError{owner: file, error: errors.Join(err, closeErr)}
	}
	if err != nil {
		return sysError{err, _IOERR_DIR_FSYNC}
	}
	return nil
}

type retainedCloseError struct {
	error
	owner io.Closer
}

// Inspect only the private wrappers used by this VFS. The owner is transferred
// before interpreting an ordinary IO code; it must never disappear in modeof.
func retainedCloseOwner(err error) (retainedCloseError, bool) {
	if wrapped, ok := err.(sysError); ok {
		err = wrapped.error
	}
	owner, ok := err.(retainedCloseError)
	return owner, ok
}

var (
	errFileRelease = errors.New("SQLite file release unconfirmed; process restart required")
	closeOwnedFile = (*os.File).Close
	closeDirectory = (*os.File).Close
	syncDirectory  = func(file *os.File, flags SyncFlag) error { return osSync(file, 0, flags) }
)

func (f *vfsFile) Size() (int64, error) {
	return f.Seek(0, io.SeekEnd)
}

func (f *vfsFile) SectorSize() int {
	return _DEFAULT_SECTOR_SIZE
}

func (f *vfsFile) DeviceCharacteristics() DeviceCharacteristic {
	ret := IOCAP_SUBPAGE_READ
	if f.flags&_FLAG_ATOMIC != 0 {
		ret |= IOCAP_BATCH_ATOMIC
	}
	if f.flags&_FLAG_PSOW != 0 {
		ret |= IOCAP_POWERSAFE_OVERWRITE
	}
	if runtime.GOOS == "windows" {
		ret |= IOCAP_UNDELETABLE_WHEN_OPEN
	}
	return ret
}

func (f *vfsFile) SizeHint(size int64) error {
	return osAllocate(f.File, size)
}

func (f *vfsFile) HasMoved() (bool, error) {
	if runtime.GOOS == "windows" {
		return false, nil
	}
	fi, err := f.Stat()
	if err != nil {
		return false, err
	}
	pi, err := os.Stat(f.Name())
	if errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return !os.SameFile(fi, pi), nil
}

func (f *vfsFile) LockState() LockLevel     { return f.lock }
func (f *vfsFile) PowersafeOverwrite() bool { return f.flags&_FLAG_PSOW != 0 }
func (f *vfsFile) PersistWAL() bool         { return f.flags&_FLAG_KEEP_WAL != 0 }

func (f *vfsFile) SetPowersafeOverwrite(psow bool) {
	f.flags &^= _FLAG_PSOW
	if psow {
		f.flags |= _FLAG_PSOW
	}
}

func (f *vfsFile) SetPersistWAL(keepWAL bool) {
	f.flags &^= _FLAG_KEEP_WAL
	if keepWAL {
		f.flags |= _FLAG_KEEP_WAL
	}
}
