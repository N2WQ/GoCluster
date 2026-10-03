package vfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
	"golang.org/x/sys/windows"
)

func TestV15SQLiteWindowsUnlockFailureStaysOwned(t *testing.T) {
	real := nativeUnlockFile
	defer func() { nativeUnlockFile = real }()
	for _, kind := range []string{"release", "downgrade", "temporary", "probe"} {
		t.Run(kind, func(t *testing.T) {
			file, err := os.OpenFile(filepath.Join(t.TempDir(), "db"), os.O_RDWR|os.O_CREATE, 0600)
			if err != nil {
				t.Fatal(err)
			}
			f := &vfsFile{File: file, lock: LOCK_EXCLUSIVE}
			w := &sqlite3_wrap.Wrapper{}
			w.AddHandle(f)
			nativeUnlockFile = func(windows.Handle, uint32, uint32, uint32, *windows.Overlapped) error {
				return windows.ERROR_ACCESS_DENIED
			}
			switch kind {
			case "release":
				err = f.Unlock(LOCK_NONE)
			case "downgrade":
				err = f.Unlock(LOCK_SHARED)
			case "temporary":
				err = osGetSharedLock(file)
			case "probe":
				_, err = osCheckReservedLock(file)
			}
			if err == nil {
				t.Fatal("native unlock error discarded")
			}
			if code := vfsErrorCode(w, err, _IOERR_LOCK); code != _IOERR_CLOSE || !w.Poisoned {
				t.Fatal("release failure not sticky", code)
			}
			if (kind == "release" || kind == "downgrade") && f.lock != LOCK_EXCLUSIVE {
				t.Fatal("unconfirmed lock state cleared", f.lock)
			}
			if w.GetHandle(^ptr_t(0)) != f {
				t.Fatal("file owner lost")
			}
			nativeUnlockFile = real
			if err = w.Close(); err != nil {
				t.Fatal("known pre-call fault did not retire", err)
			}
		})
	}
}
