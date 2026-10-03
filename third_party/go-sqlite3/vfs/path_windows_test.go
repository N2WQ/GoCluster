package vfs

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestV15SQLiteWindowsNativeConversionAdmission(t *testing.T) {
	realInput, realFull, realOutput := pathUTF8ToWide, pathFullName, pathWideToUTF8
	defer func() { pathUTF8ToWide, pathFullName, pathWideToUTF8 = realInput, realFull, realOutput }()
	for _, tc := range []struct {
		name              string
		n                 int32
		err               error
		badTerm, interior bool
	}{
		{name: "zero"}, {name: "negative", n: -1}, {name: "growth", n: 1026},
		{name: "overflow", n: 1<<31 - 1}, {name: "unterminated", n: 2, badTerm: true},
		{name: "interior-null", n: 3, interior: true},
		{name: "insufficient", err: windows.ERROR_INSUFFICIENT_BUFFER},
		{name: "native-error", err: syscall.ERROR_ACCESS_DENIED},
	} {
		t.Run("input/"+tc.name, func(t *testing.T) {
			pathUTF8ToWide = func(cp, flags uint32, _ *byte, length int32, out *uint16, capacity int32) (int32, error) {
				if cp != 65001 || flags != 0 || length != -1 || capacity != 1025 {
					t.Fatal("changed inherited conversion", cp, flags, length, capacity)
				}
				buffer := unsafe.Slice(out, int(capacity))
				buffer[0] = 'x'
				if tc.badTerm {
					buffer[1] = 'x'
				}
				return tc.n, tc.err
			}
			pathFullName = func(*uint16, uint32, *uint16, **uint16) (uint32, error) {
				t.Fatal("invalid conversion reached fullpath")
				return 0, nil
			}
			_, err := boundedWindowsFullPath("relative")
			want := error(_IOERR_NOMEM)
			if tc.err == syscall.ERROR_ACCESS_DENIED {
				want = tc.err
			}
			if !errors.Is(err, want) {
				t.Fatalf("got %v want %v", err, want)
			}
		})
	}
	pathUTF8ToWide = realInput
	pathFullName = func(_ *uint16, capacity uint32, out *uint16, _ **uint16) (uint32, error) {
		copy(unsafe.Slice(out, int(capacity)), []uint16{'C', ':', '\\', 'x', 0})
		return 4, nil
	}
	for _, tc := range []struct {
		name, reply string
		n           int32
		err         error
		badTerm     bool
	}{
		{name: "exact", reply: `C:\` + strings.Repeat("a", 1021), n: 1025},
		{name: "excess", reply: `C:\` + strings.Repeat("a", 1022), n: 1026},
		{name: "zero"}, {name: "negative", n: -1}, {name: "overflow", n: 1<<31 - 1},
		{name: "unterminated", reply: `C:\x`, n: 5, badTerm: true},
		{name: "interior-null", reply: "C:\\x\x00y", n: 7},
		{name: "relative", reply: "relative", n: 9},
		{name: "insufficient", err: windows.ERROR_INSUFFICIENT_BUFFER},
		{name: "native-error", err: syscall.ERROR_ACCESS_DENIED},
	} {
		t.Run("output/"+tc.name, func(t *testing.T) {
			pathWideToUTF8 = func(_ *uint16, out *byte, capacity int32) (int32, error) {
				if capacity != 3073 {
					t.Fatal("output buffer escaped fixed admission", capacity)
				}
				buffer := unsafe.Slice(out, int(capacity))
				copy(buffer, tc.reply)
				if tc.badTerm {
					buffer[tc.n-1] = 'x'
				}
				return tc.n, tc.err
			}
			got, err := boundedWindowsFullPath("relative")
			if tc.name == "exact" {
				if err != nil || got != tc.reply {
					t.Fatal(got, err)
				}
				return
			}
			want := error(_IOERR_NOMEM)
			if tc.err == syscall.ERROR_ACCESS_DENIED {
				want = tc.err
			}
			if !errors.Is(err, want) {
				t.Fatalf("got %q/%v want %v", got, err, want)
			}
		})
	}
}

func TestV15SQLiteWindowsFullPathSpelling(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	for _, path := range []string{".", "..", "relative/file", `C:relative`, `\relative`, `\\server\share\path`, `\\?\C:\path\..\tail`, `\\.\C:\path`, "café/資料", "/C:/path", `/\\?\C:\path\..\tail`, `//server/share/name`, "bad\x00path"} {
		input := path
		// Literal vectors isolate the inherited slash exception; this is not
		// an implementation-derived expected-value helper.
		if path == "/C:/path" || path == `/\\?\C:\path\..\tail` {
			input = path[1:]
		}
		want, wantErr := syscall.FullPath(input)
		got, gotErr := (vfsOS{}).FullPathname(path)
		if want != got || (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("lexical path %q: candidate %q/%v native %q/%v", path, got, gotErr, want, wantErr)
		}
	}
	// A nonexistent parent is a lexical success; CreateFile decides existence.
	path := root + `\missing-parent\new.db`
	if got, err := (vfsOS{}).FullPathname(path); err != nil || got != path {
		t.Fatal("fullpath added a filesystem existence requirement", got, err)
	}
	literal := `\\?\C:\path\..\tail`
	if got, err := boundedOSPath(literal); err != nil || got != literal {
		t.Fatalf("OS path changed literal device spelling: %q/%v", got, err)
	}
}

func TestV15SQLiteWindowsFullPathAdmission(t *testing.T) {
	real := pathFullName
	defer func() { pathFullName = real }()
	for _, tc := range []struct {
		name, reply          string
		n                    uint32
		err                  error
		unterminated, budget bool
	}{
		{name: "exact-ascii", reply: `C:\` + strings.Repeat("a", 1021), n: 1024},
		{name: "exact-utf8", reply: `C:\` + strings.Repeat("é", 510) + "a", n: 514},
		{name: "utf8-excess", reply: `C:\` + strings.Repeat("é", 511), n: 514, budget: true},
		{name: "zero", budget: true}, {name: "growth", n: 1025, budget: true},
		{name: "overflow", n: ^uint32(0), budget: true},
		{name: "relative", reply: "relative", n: 8, budget: true},
		{name: "embedded-null", reply: `C:\`, n: 6, budget: true},
		{name: "unterminated", reply: `C:\a`, n: 4, unterminated: true, budget: true},
		{name: "native-error", err: syscall.ERROR_ACCESS_DENIED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			pathFullName = func(_ *uint16, size uint32, out *uint16, _ **uint16) (uint32, error) {
				calls++
				if size != 1025 {
					t.Fatal("output buffer escaped admission", size)
				}
				units, err := syscall.UTF16FromString(tc.reply)
				if err != nil {
					t.Fatal(err)
				}
				buffer := unsafe.Slice(out, int(size))
				copy(buffer, units)
				if tc.unterminated {
					buffer[tc.n] = 'x'
				}
				return tc.n, tc.err
			}
			got, err := (vfsOS{}).FullPathname("relative")
			if calls != 1 || (err == _IOERR_NOMEM) != tc.budget || tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatal("native reply escaped bounded result", got, err, calls)
			}
			if !tc.budget && tc.err == nil && (err != nil || got != tc.reply) {
				t.Fatal("admitted result changed", got, err)
			}
		})
	}
	pathFullName = func(_ *uint16, _ uint32, _ *uint16, _ **uint16) (uint32, error) {
		t.Fatal("oversized input reached native acquisition")
		return 0, nil
	}
	if _, err := (vfsOS{}).FullPathname(strings.Repeat("x", 1025)); err != _IOERR_NOMEM {
		t.Fatal("input not admitted before conversion", err)
	}
}

func TestV15SQLiteWindowsLongCurrentDirectoryRefusal(t *testing.T) {
	root := t.TempDir()
	kept := filepath.Join(root, "kept")
	if err := os.WriteFile(kept, []byte("committed"), 0o600); err != nil {
		t.Fatal(err)
	}
	deep := root
	for len(deep) <= _MAX_PATHNAME+100 {
		deep = filepath.Join(deep, strings.Repeat("a", 40))
	}
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(deep)
	if _, err := (vfsOS{}).FullPathname("new.db"); err != _IOERR_NOMEM {
		t.Fatal("long cwd lost budget refusal", err)
	}
	if _, err := os.Stat("new.db"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("refusal created a file", err)
	}
	if data, err := os.ReadFile(kept); err != nil || string(data) != "committed" {
		t.Fatal("refusal changed saved bytes", err)
	}
}

func TestV15SQLiteWindowsRelativeTemporaryPath(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.Mkdir("relative", 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SQLITE_TMPDIR", "relative")
	file, err := osCreateTemp(OPEN_TEMP_DB | OPEN_DELETEONCLOSE)
	if err != nil {
		t.Fatal(err)
	}
	name := file.Name()
	if _, err = file.Write([]byte("committed")); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(name)
	if !filepath.IsAbs(name) || filepath.Dir(name) != filepath.Join(dir, "relative") {
		t.Fatal("temporary directory target changed", name)
	}
	if data, err := os.ReadFile(name); err != nil || string(data) != "committed" {
		t.Fatal("temporary file contents", string(data), err)
	}
}
