package vfs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"unsafe"
)

func TestV15SQLiteWindowsFilesystemPaths(t *testing.T) {
	real := nativeLongPaths
	defer func() { nativeLongPaths = real }()
	for _, modern := range []bool{true, false} {
		nativeLongPaths = modern
		for _, path := range []string{"", ".", `C:\short`, `\\?\C:\literal. `, `\??\C:\literal`, `\\.\C:\device`, `\\server\share\short`} {
			got, err := nativeOperationPath(path, 1024)
			if err != nil || got != path {
				t.Fatal(modern, path, got, err)
			}
		}
		root := t.TempDir()
		path := filepath.Join(root, "kept.db")
		f, err := osOpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.Write([]byte("committed")); err != nil {
			t.Fatal(err)
		}
		if err = f.Close(); err != nil {
			t.Fatal(err)
		}
		if got, err := os.ReadFile(path); err != nil || string(got) != "committed" {
			t.Fatal(string(got), err)
		}
		if err = osRemove(path); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := nativeOperationPath(strings.Repeat("x", 1025), 1024); err != _IOERR_NOMEM {
		t.Fatal("oversized name not refused", err)
	}
	if _, err := nativeOperationPath(strings.Repeat("x", 1028), 1028); err != nil && nativeLongPaths {
		t.Fatal("derived SHM name lost admission", err)
	}
}

func TestV15SQLiteWindowsNativeOperationAdmission(t *testing.T) {
	realLong, realFull, realCwd := nativeLongPaths, nativeFullPath, nativeCurrentDirectory
	defer func() { nativeLongPaths, nativeFullPath, nativeCurrentDirectory = realLong, realFull, realCwd }()
	nativeLongPaths = false
	long := `C:\` + strings.Repeat("a", 260)
	for _, tc := range []struct {
		name, reply string
		n           uint32
		err         error
		wantBudget  bool
	}{
		{name: "valid", reply: long, n: uint32(len(long))},
		{name: "zero", wantBudget: true}, {name: "growth", n: nativePathUnits, wantBudget: true}, {name: "overflow", n: ^uint32(0), wantBudget: true},
		{name: "relative", reply: "relative", n: 8, wantBudget: true},
		{name: "interior-null", reply: `C:\`, n: 8, wantBudget: true},
		{name: "native-error", err: syscall.ERROR_ACCESS_DENIED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			nativeFullPath = func(_ *uint16, capacity uint32, out *uint16, _ **uint16) (uint32, error) {
				calls++
				if capacity != nativePathUnits {
					t.Fatal(capacity)
				}
				units, _ := syscall.UTF16FromString(tc.reply)
				copy(unsafe.Slice(out, int(capacity)), units)
				return tc.n, tc.err
			}
			got, err := nativeOperationPath(long, 1024)
			if calls != 1 || (err == _IOERR_NOMEM) != tc.wantBudget {
				t.Fatal(got, err, calls)
			}
			if tc.err != nil && (got != long || err != nil) {
				t.Fatal("ordinary error fallback changed", got, err)
			}
			if tc.name == "valid" && (got != `\\?\`+long || err != nil) {
				t.Fatal(got, err)
			}
		})
	}
	nativeCurrentDirectory = func(uint32, *uint16) (uint32, error) { return nativePathUnits + 1, nil }
	nativeFullPath = func(*uint16, uint32, *uint16, **uint16) (uint32, error) {
		t.Fatal("oversized cwd reached next acquisition")
		return 0, nil
	}
	if _, err := nativeOperationPath("relative", 1024); err != _IOERR_NOMEM {
		t.Fatal(err)
	}
}

func TestV15SQLiteWindowsTemporaryOwnership(t *testing.T) {
	realRandom, realOpen := tempRandom, tempOpenFile
	defer func() { tempRandom, tempOpenFile = realRandom, realOpen }()
	tempRandom = func() uint64 { return 4294967295 }
	root := t.TempDir()
	expected := filepath.Join(root, "4294967295.db")
	calls := 0
	tempOpenFile = func(name string, flags int, mode os.FileMode) (*os.File, error) {
		calls++
		if name != expected || flags != os.O_RDWR|os.O_CREATE|os.O_EXCL || mode != 0600 {
			t.Fatal(name, flags, mode)
		}
		if calls == 1 {
			return nil, syscall.ERROR_FILE_EXISTS
		}
		return realOpen(name, flags, mode)
	}
	f, err := createWindowsTemp(root)
	if err != nil || calls != 2 {
		t.Fatal(err, calls)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if err = osRemove(f.Name()); err != nil {
		t.Fatal(err)
	}
	calls = 0
	tempOpenFile = func(string, int, os.FileMode) (*os.File, error) { calls++; return nil, os.ErrExist }
	if _, err = createWindowsTemp(root); !errors.Is(err, os.ErrExist) || calls != 10000 {
		t.Fatal("collision bound", err, calls)
	}
}

func TestV15SQLiteWindowsNativeBudgetCategory(t *testing.T) {
	realOpen := tempOpenFile
	defer func() { tempOpenFile = realOpen }()
	t.Setenv("SQLITE_TMPDIR", t.TempDir())
	tempOpenFile = func(string, int, os.FileMode) (*os.File, error) { return nil, _IOERR_NOMEM }
	if _, err := osCreateTemp(OPEN_TEMP_DB); err != _IOERR_NOMEM {
		t.Fatal("temporary resource refusal misclassified", err)
	}
	s := &vfsShm{path: strings.Repeat("x", 1029)}
	if err := s.shmOpen(); err != _IOERR_NOMEM {
		t.Fatal("SHM resource refusal misclassified", err)
	}
}
