//go:build unix

package vfs

import (
	"errors"
	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
	"os"
	"testing"
)

func TestV15SQLiteDirectoryReleaseOwnership(t *testing.T) {
	dir, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	originalClose, originalSync := closeDirectory, syncDirectory
	defer func() { closeDirectory, syncDirectory = originalClose, originalSync; dir.Close() }()
	var syncedDirectory bool
	syncDirectory = func(file *os.File, flags SyncFlag) error {
		info, err := file.Stat()
		syncedDirectory = err == nil && info.IsDir()
		return originalSync(file, flags)
	}
	closeDirectory = func(*os.File) error { return errors.New("injected directory close failure") }
	err = syncAndCloseDirectory(dir, SYNC_FULL)
	if !syncedDirectory || err == nil {
		t.Fatal("directory sync/close fixture ineffective")
	}
	w := new(sqlite3_wrap.Wrapper)
	if code := vfsErrorCode(w, err, _IOERR_DIR_FSYNC); code != _IOERR_CLOSE {
		t.Fatal(code)
	}
	if _, err = dir.Stat(); err != nil {
		t.Fatal("failed close lost real owner", err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = dir.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("wrapper retirement did not close directory", err)
	}
}
