package vfs

import (
	"errors"
	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestV15SQLiteWindowsMetadataParity(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "kept.db")
	if err := os.WriteFile(path, []byte("saved contents"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"winsymlink=0", "winsymlink=1"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("GODEBUG", mode)
			for _, name := range []string{root, path, filepath.Join(root, "absent"), ""} {
				got, err := metadataStat("Stat", name, true)
				want, wantErr := os.Stat(name)
				if (err == nil) != (wantErr == nil) {
					t.Fatal(name, err, wantErr)
				}
				if err == nil && (got.Mode() != want.Mode() || got.IsDir() != want.IsDir() || got.Size() != want.Size() || !got.ModTime().Equal(want.ModTime())) {
					t.Fatal("metadata differs", name, got, want)
				}
			}
		})
	}
}

func TestV15SQLiteMetadataReleaseOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kept.db")
	if err := os.WriteFile(path, []byte("saved"), 0600); err != nil {
		t.Fatal(err)
	}
	realAttributes, realClose := metadataGetAttributes, metadataFileClose
	defer func() { metadataGetAttributes, metadataFileClose = realAttributes, realClose }()
	metadataGetAttributes = func(_ *uint16, _ uint32, out *byte) error {
		(*syscall.Win32FileAttributeData)(unsafe.Pointer(out)).FileAttributes = syscall.FILE_ATTRIBUTE_REPARSE_POINT
		return nil
	}
	closes := 0
	metadataFileClose = func(file *os.File) error {
		closes++
		// A real consumed-handle failure: invalidate the OS handle, then the
		// actual os.File.Close returns its native error and consumes FD state.
		if err := syscall.CloseHandle(syscall.Handle(file.Fd())); err != nil {
			t.Fatal(err)
		}
		return realClose(file)
	}
	_, err := metadataStat("Stat", path, true)
	owner, ok := retainedCloseOwner(err)
	if !ok {
		t.Fatal("actual failed File.Close owner lost", err)
	}
	w := &sqlite3_wrap.Wrapper{}
	if code := vfsErrorCode(w, err, _IOERR_FSTAT); code != _IOERR_CLOSE {
		t.Fatal(code)
	}
	metadataFileClose = realClose
	if owner.owner.Close() == nil || w.Close() == nil || !w.Poisoned || closes != 1 {
		t.Fatal("consumed close was retried or released")
	}
	if code := vfsOpen(w, 0, 0, 0, 0, 0, 0); code != _IOERR_CLOSE {
		t.Fatal("failed retirement admitted another open", code)
	}
	if bytes, err := os.ReadFile(path); err != nil || string(bytes) != "saved" {
		t.Fatal("saved data changed", err)
	}
}

func TestV15SQLitePoisonedFallbackDoesNotPublish(t *testing.T) {
	w := &sqlite3_wrap.Wrapper{Memory: &sqlite3_wrap.Memory{Buf: make([]byte, 32768+16)}, Poisoned: true}
	r := &sqlite3_wrap.FallbackRegion{Data: make([]byte, 32768), Shadow: new([32768]byte), Private: 16}
	w.Buf[16] = 17
	r.Data[0] = 29
	s := &vfsShm{wrp: w}
	s.fallback[0] = r
	s.fallbackRelease()
	s.fallbackAcquire(nil)
	s.shmBarrier()
	if r.Data[0] != 29 || r.Shadow[0] != 0 || w.Buf[16] != 17 {
		t.Fatal("poisoned WAL changed shared/private state")
	}
}

func TestV15SQLiteWindowsMetadataBranchOrder(t *testing.T) {
	realAttributes, realFirst, realClose, realCreate := metadataGetAttributes, metadataFindFirst, metadataFindClose, metadataCreateFile
	defer func() {
		metadataGetAttributes, metadataFindFirst, metadataFindClose = realAttributes, realFirst, realClose
		metadataCreateFile = realCreate
	}()
	metadataGetAttributes = func(_ *uint16, _ uint32, out *byte) error {
		(*syscall.Win32FileAttributeData)(unsafe.Pointer(out)).FileAttributes = syscall.FILE_ATTRIBUTE_NORMAL
		return nil
	}
	metadataFindFirst = func(*uint16, *syscall.Win32finddata) (syscall.Handle, error) {
		t.Fatal("fast attributes opened a handle")
		return 0, nil
	}
	metadataCreateFile = func(*uint16, uint32, uint32, *syscall.SecurityAttributes, uint32, uint32, int32) (syscall.Handle, error) {
		t.Fatal("fast attributes opened exclusive handle")
		return 0, nil
	}
	if _, err := metadataStat("Stat", `C:\kept`, true); err != nil {
		t.Fatal(err)
	}
	metadataGetAttributes = func(*uint16, uint32, *byte) error { return windows.ERROR_SHARING_VIOLATION }
	opened, closed := 0, 0
	metadataFindFirst = func(_ *uint16, out *syscall.Win32finddata) (syscall.Handle, error) {
		opened++
		out.FileAttributes = syscall.FILE_ATTRIBUTE_NORMAL
		return 123, nil
	}
	metadataFindClose = func(h syscall.Handle) error {
		closed++
		if h != 123 {
			t.Fatal(h)
		}
		return nil
	}
	if _, err := metadataStat("Stat", `C:\kept`, true); err != nil || opened != 1 || closed != 1 {
		t.Fatal(err, opened, closed)
	}
	metadataFindClose = func(syscall.Handle) error { closed++; return syscall.ERROR_ACCESS_DENIED }
	_, err := metadataStat("Stat", `C:\kept`, true)
	retained, ok := retainedCloseOwner(err)
	if !ok || errors.Is(err, os.ErrPermission) {
		t.Fatal("failed close owner lost or maskable", err)
	}
	before := closed
	metadataFindClose = realClose
	if retained.owner.Close() == nil || closed != before {
		t.Fatal("terminal native close was retried")
	}
}

func TestV15SQLiteMetadataAccessAndJournalPrecedence(t *testing.T) {
	realAttributes, realFirst, realClose, realOpen := metadataGetAttributes, metadataFindFirst, metadataFindClose, openVFSFile
	defer func() {
		metadataGetAttributes, metadataFindFirst, metadataFindClose, openVFSFile = realAttributes, realFirst, realClose, realOpen
	}()
	metadataGetAttributes = func(*uint16, uint32, *byte) error { return windows.ERROR_SHARING_VIOLATION }
	metadataFindFirst = func(_ *uint16, data *syscall.Win32finddata) (syscall.Handle, error) {
		data.FileAttributes = syscall.FILE_ATTRIBUTE_NORMAL
		return 123, nil
	}
	metadataFindClose = func(syscall.Handle) error { return syscall.ERROR_ACCESS_DENIED }
	for _, flags := range []AccessFlag{ACCESS_EXISTS, ACCESS_READWRITE} {
		if ok, err := (vfsOS{}).Access(`C:\kept`, flags); ok {
			t.Fatal("failed metadata close returned accessible")
		} else if _, retained := retainedCloseOwner(err); !retained {
			t.Fatal("Access masked owner", err)
		}
	}
	openVFSFile = func(path string, _ int, _ os.FileMode) (*os.File, error) {
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.ERROR_ACCESS_DENIED}
	}
	w := &sqlite3_wrap.Wrapper{Memory: &sqlite3_wrap.Memory{Buf: make([]byte, 1024)}}
	w.WriteString(16, `C:\kept-journal`)
	file, _, err := (vfsOS{}).OpenFilename(&Filename{wrp: w, zPath: 16}, OPEN_CREATE|OPEN_READWRITE|OPEN_MAIN_JOURNAL)
	if file != nil {
		t.Fatal("permission failure acquired main file")
	}
	if _, retained := retainedCloseOwner(err); !retained {
		t.Fatal("journal probe masked metadata owner", err)
	}
}
