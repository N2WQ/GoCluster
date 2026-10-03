//go:build !sqlite3_dotlk

package vfs

import (
	"io"
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/ncruces/go-sqlite3/internal/errutil"
	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
)

const _WALINDEX_PGSZ = 32768

type vfsShm struct {
	*os.File
	path        string
	wrp         *sqlite3_wrap.Wrapper
	regions     [128]*sqlite3_wrap.MappedRegion
	fallback    [64]*sqlite3_wrap.FallbackRegion
	fileLock    bool
	closing     bool
	closeFailed bool
}

func (s *vfsShm) shmOpen() error {
	if s.fileLock {
		return nil
	}
	if s.File == nil {
		f, err := osOpenFile(s.path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			if err == _IOERR_NOMEM {
				return err
			}
			return sysError{err, _CANTOPEN}
		}
		s.fileLock = false
		s.File = f
	}

	// Dead man's switch.
	if osWriteLock(s.File, _SHM_DMS, 1, 0) == nil {
		err := s.Truncate(0)
		if releaseErr := osUnlock(s.File, _SHM_DMS, 1); releaseErr != nil {
			return releaseErr
		}
		if err != nil {
			return sysError{err, _IOERR_SHMOPEN}
		}
	}
	err := osReadLock(s.File, _SHM_DMS, 1, 0)
	s.fileLock = err == nil
	return err
}

func (s *vfsShm) shmMap(wrp *sqlite3_wrap.Wrapper, id, size int32, extend bool) (_ ptr_t, err error) {
	if s.closing {
		return 0, _IOERR_SHMMAP
	}
	// Bound the logical index before arithmetic or allocation. Fallback slots
	// also have a global engine bound shared by all attached databases.
	if size != _WALINDEX_PGSZ || id < 0 || id >= int32(len(s.regions)) {
		return 0, _IOERR_SHMMAP
	}
	if !wrp.CanMapFiles() && id >= int32(len(s.fallback)) {
		return 0, _IOERR_NOMEM
	}
	s.wrp = wrp

	if err := s.shmOpen(); err != nil {
		return 0, err
	}

	// Check if file is big enough.
	o, err := s.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, sysError{err, _IOERR_SHMSIZE}
	}
	if n := (int64(id) + 1) * int64(size); n > o {
		if !extend {
			return 0, nil
		}
		if err := osAllocate(s.File, n); err != nil {
			return 0, sysError{err, _IOERR_SHMSIZE}
		}
	}

	if !wrp.CanMapFiles() {
		defer s.fallbackAcquire(&err)
		if s.fallback[id] == nil {
			r, mapErr := wrp.MapFallback(s.File, int64(id)*int64(size))
			if mapErr != nil {
				return 0, mapErr
			}
			if r == nil {
				return 0, _IOERR_NOMEM
			}
			s.fallback[id] = r
			r.Private = wrp.New(int64(size))
			clear(wrp.Bytes(r.Private, int64(size)))
			// Force the first acquire even for a zero-filled header.
			r.Shadow[4] = 1
		}
		return s.fallback[id].Private, nil
	}
	if s.regions[id] != nil {
		return s.regions[id].Ptr, nil
	}
	r, err := wrp.MapRegion(s.File, int64(id)*int64(size), size, false)
	if err != nil {
		return 0, err
	}
	if r == nil {
		return 0, _IOERR_NOMEM
	}
	s.regions[id] = r
	return r.Ptr, nil
}

func (s *vfsShm) shmLock(offset, n int32, flags _ShmFlag) (err error) {
	if s.File == nil || s.closing {
		return _IOERR_SHMLOCK
	}
	if flags&_SHM_LOCK != 0 {
		defer s.fallbackAcquire(&err)
	} else if flags&_SHM_EXCLUSIVE != 0 {
		s.fallbackRelease()
	}

	switch {
	case flags&_SHM_UNLOCK != 0:
		return osUnlock(s.File, _SHM_BASE+uint32(offset), uint32(n))
	case flags&_SHM_SHARED != 0:
		return osReadLock(s.File, _SHM_BASE+uint32(offset), uint32(n), 0)
	case flags&_SHM_EXCLUSIVE != 0:
		return osWriteLock(s.File, _SHM_BASE+uint32(offset), uint32(n), 0)
	default:
		panic(errutil.AssertErr())
	}
}

func (s *vfsShm) shmUnmap(delete bool) {
	_ = s.shmUnmapError(delete)
}

func (s *vfsShm) shmUnmapError(delete bool) error {
	if s.File == nil {
		return nil
	}
	if err := s.Close(); err != nil {
		return err
	}
	if delete {
		osRemove(s.path)
	}
	return nil
}

// Close owns mappings before the file. Failed mapping/view releases remain
// retriable in their slots. A failed os.File.Close has consumed its descriptor
// wrapper, so closeFailed retains that terminal owner until process exit.
func (s *vfsShm) Close() error {
	if s.closeFailed {
		return errFileRelease
	}
	if !s.closing && s.wrp != nil && !s.wrp.Retiring {
		s.fallbackRelease()
	}
	// A failed close can leave only a mapping handle after its view has gone.
	// Never publish or return that partially retired region to SQLite again.
	s.closing = true
	for i, r := range s.regions {
		if r == nil {
			continue
		}
		if err := r.Unmap(); err != nil {
			return err
		}
		s.regions[i] = nil
	}
	for i, r := range s.fallback {
		if r == nil {
			continue
		}
		if err := r.Close(); err != nil {
			return err
		}
		if !s.wrp.Retiring && !s.wrp.Poisoned {
			s.wrp.Free(r.Private)
		}
		r.Private = 0
		s.fallback[i] = nil
	}
	if s.File != nil {
		if err := closeOwnedFile(s.File); err != nil {
			s.closeFailed = true
			return err
		}
		s.File = nil
	}
	s.fileLock = false
	s.closing = false
	return nil
}

func (s *vfsShm) shmBarrier() {
	if s.closing {
		return
	}
	var b atomic.Bool
	s.fallbackAcquire(nil)
	b.Swap(true)
	s.fallbackRelease()
}

// The shadow algorithm is adapted from upstream v0.30.0 shm_copy.go. Native
// SQLite locks protect writes; aligned atomic words keep header/read-mark
// exchange compatible with other processes. The fixed slot arrays replace the
// upstream growing slices. See the retained source provenance and process tests.
func (s *vfsShm) fallbackAcquire(errp *error) {
	if s.wrp != nil && (s.wrp.Poisoned || s.wrp.Retiring) {
		return
	}
	if errp != nil && *errp != nil {
		return
	}
	for _, r := range s.fallback {
		if r == nil || r.Private == 0 {
			continue
		}
		shared := unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(r.Data))), _WALINDEX_PGSZ/4)
		shadow := unsafe.Slice((*uint32)(unsafe.Pointer(&r.Shadow[0])), _WALINDEX_PGSZ/4)
		private := unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(s.wrp.Bytes(r.Private, _WALINDEX_PGSZ)))), _WALINDEX_PGSZ/4)
		for i := range shared {
			value := atomic.LoadUint32(&shared[i])
			if shadow[i] != value {
				shadow[i], private[i] = value, value
			}
		}
	}
}

func (s *vfsShm) fallbackRelease() {
	if s.wrp != nil && (s.wrp.Poisoned || s.wrp.Retiring) {
		return
	}
	for _, r := range s.fallback {
		if r == nil || r.Private == 0 {
			continue
		}
		shared := unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(r.Data))), _WALINDEX_PGSZ/4)
		shadow := unsafe.Slice((*uint32)(unsafe.Pointer(&r.Shadow[0])), _WALINDEX_PGSZ/4)
		private := unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(s.wrp.Bytes(r.Private, _WALINDEX_PGSZ)))), _WALINDEX_PGSZ/4)
		for i, value := range private {
			if shadow[i] != value {
				atomic.StoreUint32(&shared[i], value)
				shadow[i] = value
			}
		}
	}
}
