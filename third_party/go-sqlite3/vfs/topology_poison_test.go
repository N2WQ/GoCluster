package vfs

import (
	"errors"
	"testing"

	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
)

type metadataTestCloser struct{ calls int }

func (c *metadataTestCloser) Close() error { c.calls++; return nil }

func TestV15SQLiteMetadataCleanupErrorPrecedence(t *testing.T) {
	for _, nested := range []bool{false, true} {
		w := &sqlite3_wrap.Wrapper{}
		owner := &metadataTestCloser{}
		var err error = retainedCloseError{owner: owner, error: errors.New("unconfirmed native release")}
		if nested {
			err = sysError{error: err, code: _IOERR_FSTAT}
		}
		if got := vfsErrorCode(w, err, _CANTOPEN); got != _IOERR_CLOSE || !w.Poisoned || w.GetHandle(^ptr_t(0)) != owner {
			t.Fatal("lost nested owner/poison", got, w.Poisoned)
		}
		// A later benign callback cannot erase the sticky cleanup result.
		if got := vfsErrorCode(w, nil, _OK); got != _IOERR_CLOSE {
			t.Fatal("poison overwritten", got)
		}
		if err := w.Close(); err != nil || owner.calls != 1 {
			t.Fatal("owner not released exactly once", err, owner.calls)
		}
	}
}

func TestV15SQLitePoisonedCallbacksDoNotStartWork(t *testing.T) {
	w := &sqlite3_wrap.Wrapper{Poisoned: true}
	// No memory/file slots exist. Each callback must refuse before looking up
	// a file or entering native I/O, even while the previous SQL call unwinds.
	checks := map[string]func() _ErrorCode{
		"fullpath":            func() _ErrorCode { return vfsFullPathname(w, 0, 0, 0, 0) },
		"delete":              func() _ErrorCode { return vfsDelete(w, 0, 0, 0) },
		"access":              func() _ErrorCode { return vfsAccess(w, 0, 0, ACCESS_EXISTS, 0) },
		"open":                func() _ErrorCode { return vfsOpen(w, 0, 0, 0, 0, 0, 0) },
		"read":                func() _ErrorCode { return vfsRead(w, 0, 0, 0, 0) },
		"write":               func() _ErrorCode { return vfsWrite(w, 0, 0, 0, 0) },
		"truncate":            func() _ErrorCode { return vfsTruncate(w, 0, 0) },
		"sync":                func() _ErrorCode { return vfsSync(w, 0, 0) },
		"seek-size":           func() _ErrorCode { return vfsFileSize(w, 0, 0) },
		"lock":                func() _ErrorCode { return vfsLock(w, 0, LOCK_SHARED) },
		"downgrade-reacquire": func() _ErrorCode { return vfsUnlock(w, 0, LOCK_SHARED) },
		"lock-probe":          func() _ErrorCode { return vfsCheckReservedLock(w, 0, 0) },
		"control":             func() _ErrorCode { return vfsFileControl(w, 0, 0, 0) },
		"map":                 func() _ErrorCode { return vfsShmMap(w, 0, 0, 0, 0, 0) },
		"shm-lock":            func() _ErrorCode { return vfsShmLock(w, 0, 0, 1, _SHM_LOCK|_SHM_EXCLUSIVE) },
		"fetch":               func() _ErrorCode { return vfsFetch(w, 0, 0, 0, 0) },
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			if code := check(); code != _IOERR_CLOSE {
				t.Fatal(code)
			}
		})
	}
	vfsShmBarrier(w, 0)
	if vfsSectorSize(w, 0) != _DEFAULT_SECTOR_SIZE || vfsDeviceCharacteristics(w, 0) != 0 {
		t.Fatal("poisoned scalar callback")
	}
}
