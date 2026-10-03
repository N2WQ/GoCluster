//go:build windows

package peerdiag

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestV15WindowsOwnedPathAdmission(t *testing.T) {
	oldFull, oldModern := diagnosticGetFullPathName, helperLongPaths
	t.Cleanup(func() { diagnosticGetFullPathName, helperLongPaths = oldFull, oldModern })
	helperLongPaths = false
	path := `\\.\C:\` + strings.Repeat("a", 260)
	for _, value := range []uint32{0, math.MaxUint32, nativePathExpansionReservation} {
		calls := 0
		diagnosticGetFullPathName = func(_ *uint16, n uint32, output *uint16, _ **uint16) (uint32, error) {
			calls++
			if n != 0 || output != nil {
				t.Fatal("invalid declaration reached native output storage")
			}
			return value, nil
		}
		if _, err := helperOperationPath(path); !errors.Is(err, errNativePathBudget) || calls != 1 {
			t.Fatal("resource refusal was retried or fell back", value, calls, err)
		}
	}
	calls := 0
	diagnosticGetFullPathName = func(_ *uint16, n uint32, _ *uint16, _ **uint16) (uint32, error) {
		calls++
		if n == 0 {
			return uint32(len(path) + 1), nil
		}
		return math.MaxUint32, nil
	}
	if _, err := helperOperationPath(path); !errors.Is(err, errNativePathBudget) || calls != 2 {
		t.Fatal("growing second read retried or used ordinary-error fallback", calls, err)
	}
	// Go's legacy normalization returns the original spelling when the native
	// operation fails. Capacity refusal is deliberately distinct from that case.
	diagnosticGetFullPathName = func(*uint16, uint32, *uint16, **uint16) (uint32, error) { return 0, syscall.ERROR_ACCESS_DENIED }
	if got, err := helperOperationPath(path); err != nil || got != path {
		t.Fatal("ordinary native-error fallback changed", got, err)
	}
	// A device spelling must reach the native open unchanged after that fallback.
	file, gotErr := helperOpenFile(`\\.\NUL`, os.O_WRONLY, 0)
	want, wantErr := os.OpenFile(`\\.\NUL`, os.O_WRONLY, 0)
	if file != nil {
		_ = file.Close()
	}
	if want != nil {
		_ = want.Close()
	}
	if (gotErr == nil) != (wantErr == nil) {
		t.Fatal(gotErr, wantErr)
	}
}

func TestV15WindowsOwnedMkdirParity(t *testing.T) {
	for _, tail := range []string{`a\b`, `a\..\c`, `missing\..\lexical`, "trailing. ", "kept", `a\.`} {
		t.Run(tail, func(t *testing.T) {
			root := t.TempDir()
			control, candidate := filepath.Join(root, "control"), filepath.Join(root, "candidate")
			for _, dir := range []string{control, candidate} {
				if err := os.Mkdir(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "kept"), []byte("saved"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			var sink helperSink
			// Do not clean the supplied suffix; dot/space/parent components are
			// part of the native contract being compared. Fresh roots ensure the
			// candidate must create its own parents instead of seeing control work.
			wantErr := os.MkdirAll(control+`\`+tail, 0o700)
			gotErr := sink.diagnosticMkdirAll(candidate+`\`+tail, 0o700)
			if (gotErr == nil) != (wantErr == nil) || errors.Is(gotErr, syscall.ENOTDIR) != errors.Is(wantErr, syscall.ENOTDIR) {
				t.Fatal(gotErr, wantErr)
			}
			inventory := func(dir string) []string {
				var result []string
				err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
					if err != nil {
						return err
					}
					relative, err := filepath.Rel(dir, path)
					if err != nil {
						return err
					}
					result = append(result, relative+":"+entry.Type().String())
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			if a, b := inventory(control), inventory(candidate); !reflect.DeepEqual(a, b) {
				t.Fatal("created different directory trees", a, b)
			}
			data, err := os.ReadFile(filepath.Join(candidate, "kept"))
			if err != nil || string(data) != "saved" {
				t.Fatal("existing contents changed", string(data), err)
			}
		})
	}
}

func TestV15WindowsOwnedMetadataAttributeFastPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kept")
	if err := os.WriteFile(path, []byte("attributes remain available"), 0o600); err != nil {
		t.Fatal(err)
	}
	encoded, _ := syscall.UTF16FromString(path)
	locked, err := syscall.CreateFile(&encoded[0], syscall.GENERIC_READ, 0, nil, syscall.OPEN_EXISTING, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.CloseHandle(locked)
	want, err := os.Stat(path)
	if err != nil {
		t.Fatal("control metadata unavailable", err)
	}
	before := diagnosticMetadataCreateFile
	t.Cleanup(func() { diagnosticMetadataCreateFile = before })
	diagnosticMetadataCreateFile = func(*uint16, uint32, uint32, *syscall.SecurityAttributes, uint32, uint32, int32) (syscall.Handle, error) {
		t.Fatal("attributes fast path attempted exclusive open")
		return syscall.InvalidHandle, syscall.ERROR_ACCESS_DENIED
	}
	var sink helperSink
	got, err := sink.diagnosticStat(path)
	if err != nil || got.Size() != want.Size() || got.IsDir() != want.IsDir() || !got.ModTime().Equal(want.ModTime()) {
		t.Fatal(got, want, err)
	}
}

func TestV15WindowsOwnedMetadataSharingFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kept")
	if err := os.WriteFile(path, []byte("find metadata"), 0o600); err != nil {
		t.Fatal(err)
	}
	want, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	attrs, create, closeFind := diagnosticGetAttributes, diagnosticMetadataCreateFile, diagnosticMetadataFindClose
	t.Cleanup(func() {
		diagnosticGetAttributes, diagnosticMetadataCreateFile, diagnosticMetadataFindClose = attrs, create, closeFind
	})
	diagnosticGetAttributes = func(*uint16, uint32, *byte) error { return windows.ERROR_SHARING_VIOLATION }
	diagnosticMetadataCreateFile = func(*uint16, uint32, uint32, *syscall.SecurityAttributes, uint32, uint32, int32) (syscall.Handle, error) {
		t.Fatal("sharing fallback opened metadata handle")
		return syscall.InvalidHandle, syscall.ERROR_ACCESS_DENIED
	}
	closes := 0
	diagnosticMetadataFindClose = func(h syscall.Handle) error { closes++; return closeFind(h) }
	var sink helperSink
	got, err := sink.diagnosticStat(path)
	if err != nil || closes != 1 || got.Size() != want.Size() || !got.ModTime().Equal(want.ModTime()) || sink.metadataFailed() {
		t.Fatal(got, want, closes, err)
	}
}

func TestV15WindowsOwnedMetadataHandleParity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kept")
	if err := os.WriteFile(path, []byte("handle metadata"), 0o600); err != nil {
		t.Fatal(err)
	}
	attrs, create := diagnosticGetAttributes, diagnosticMetadataCreateFile
	t.Cleanup(func() { diagnosticGetAttributes, diagnosticMetadataCreateFile = attrs, create })
	// Force the existing fallback branch while retaining actual native opens,
	// handle metadata and closes. NUL also exercises console's read-access retry.
	diagnosticGetAttributes = func(*uint16, uint32, *byte) error { return syscall.ERROR_ACCESS_DENIED }
	for _, name := range []string{path, dir, "NUL", `\\.\NUL`} {
		calls := 0
		diagnosticMetadataCreateFile = func(p *uint16, access, share uint32, security *syscall.SecurityAttributes, disposition, flags uint32, template int32) (syscall.Handle, error) {
			calls++
			if share != 0 || disposition != syscall.OPEN_EXISTING || flags != syscall.FILE_FLAG_BACKUP_SEMANTICS|syscall.FILE_FLAG_OPEN_REPARSE_POINT {
				t.Fatal("metadata open flags changed", share, disposition, flags)
			}
			if access != 0 && access != syscall.GENERIC_READ {
				t.Fatal("metadata access changed", access)
			}
			return create(p, access, share, security, disposition, flags, template)
		}
		var sink helperSink
		got, gotErr := sink.diagnosticStat(name)
		want, wantErr := os.Stat(name)
		if calls == 0 || (gotErr == nil) != (wantErr == nil) {
			t.Fatal(name, calls, gotErr, wantErr)
		}
		if gotErr == nil && (got.Size() != want.Size() || got.IsDir() != want.IsDir() || !got.ModTime().Equal(want.ModTime())) {
			t.Fatal("handle metadata changed", name, got, want)
		}
	}
}

func TestV15WindowsOwnedMkdirBacking(t *testing.T) {
	attrs, mkdir, full, cwd, modern := diagnosticGetAttributes, diagnosticCreateDirectory, diagnosticGetFullPathName, diagnosticGetCurrentDirectory, helperLongPaths
	t.Cleanup(func() {
		diagnosticGetAttributes, diagnosticCreateDirectory, diagnosticGetFullPathName, diagnosticGetCurrentDirectory, helperLongPaths = attrs, mkdir, full, cwd, modern
	})
	helperLongPaths = false
	logical := strings.Repeat("a\\", 200) + "leaf"
	cwdUnits, _ := syscall.UTF16FromString(`C:\` + strings.Repeat("c", 297))
	diagnosticGetCurrentDirectory = func(n uint32, output *uint16) (uint32, error) {
		if n == 0 {
			return uint32(len(cwdUnits)), nil
		}
		copy(unsafe.Slice(output, n), cwdUnits)
		return uint32(len(cwdUnits) - 1), nil
	}
	diagnosticGetFullPathName = func(input *uint16, n uint32, output *uint16, _ **uint16) (uint32, error) {
		// A different long native spelling at each level would make recursive
		// retained errors quadratic if operations accidentally returned it.
		encoded, _ := syscall.UTF16FromString(`C:\` + strings.Repeat("b", len(windows.UTF16PtrToString(input))))
		if n == 0 {
			return uint32(len(encoded)), nil
		}
		copy(unsafe.Slice(output, n), encoded)
		return uint32(len(encoded) - 1), nil
	}
	diagnosticGetAttributes = func(*uint16, uint32, *byte) error { return syscall.ERROR_PATH_NOT_FOUND }
	diagnosticCreateDirectory = func(*uint16, *syscall.SecurityAttributes) error { return syscall.ERROR_ACCESS_DENIED }
	var sink helperSink
	// Probe every recursive prefix through the real metadata function. Each
	// error must borrow the logical prefix even though native preparation made
	// a separate spelling. The source recursion retains these same errors.
	for length := 1; length <= len(logical); length += 2 {
		prefix := logical[:length]
		_, err := sink.diagnosticStat(prefix)
		var pe *os.PathError
		if !errors.As(err, &pe) || pe.Path != prefix || unsafe.StringData(pe.Path) != unsafe.StringData(prefix) {
			t.Fatal("metadata error owns normalized/copy backing", length, err)
		}
	}
	err := sink.diagnosticMkdirAll(logical, 0o700)
	var pe *os.PathError
	if !errors.As(err, &pe) || !strings.HasPrefix(logical, pe.Path) || strings.Contains(pe.Path, "bbb") {
		t.Fatal("recursive error retained native spelling", err)
	}
	start := uintptr(unsafe.Pointer(unsafe.StringData(logical)))
	got := uintptr(unsafe.Pointer(unsafe.StringData(pe.Path)))
	if got < start || got+uintptr(len(pe.Path)) > start+uintptr(len(logical)) {
		t.Fatal("recursive error copied its logical path")
	}
}

func TestV15WindowsOwnedOpenRemoveRenameParity(t *testing.T) {
	modern := helperLongPaths
	t.Cleanup(func() { helperLongPaths = modern })
	for _, legacy := range []bool{false, true} {
		helperLongPaths = !legacy
		dir := t.TempDir()
		for _, prefix := range []string{"control", "candidate"} {
			if err := os.WriteFile(filepath.Join(dir, prefix), []byte("first"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, prefix+"-next"), []byte("old destination"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		for _, candidate := range []bool{false, true} {
			base := filepath.Join(dir, "control")
			if candidate {
				base = filepath.Join(dir, "candidate")
			}
			var file *os.File
			var err error
			if candidate {
				file, err = helperOpenFile(base, os.O_APPEND|os.O_WRONLY, 0o600)
			} else {
				file, err = os.OpenFile(base, os.O_APPEND|os.O_WRONLY, 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = file.WriteString("-appended"); err != nil {
				t.Fatal(err)
			}
			if err = file.Close(); err != nil {
				t.Fatal(err)
			}
			if candidate {
				err = diagnosticRename(base, base+"-next")
			} else {
				err = os.Rename(base, base+"-next")
			}
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(base + "-next")
			if err != nil || string(data) != "first-appended" {
				t.Fatal(string(data), err)
			}
			if err = os.Chmod(base+"-next", 0o400); err != nil {
				t.Fatal(err)
			}
			if candidate {
				err = diagnosticRemove(base + "-next")
			} else {
				err = os.Remove(base + "-next")
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestV15WindowsOwnedMetadataBacking(t *testing.T) {
	var sink helperSink
	info, err := os.Stat(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	file := reflect.TypeFor[os.File]().Field(0).Type.Elem()
	if unsafe.Sizeof(metadataOwner{}) > 64 || unsafe.Sizeof(syscall.Win32finddata{}) > 640 || unsafe.Sizeof(plainMetadata{}) > 512 || reflect.TypeOf(info).Elem().Size() > 512 || file.Size()+unsafe.Sizeof(os.File{}) > 512 {
		t.Fatal("fixed metadata inventory exceeded")
	}
	if unsafe.Sizeof(os.PathError{})+16 > 64 {
		t.Fatal("recursive error bucket exceeded")
	}
	if 4*512+2*640+64+64+16+32+64+128 > 4096 || 8192 > directoryBufferReservation {
		t.Fatal("metadata fixed reservation exceeded")
	}
	if sink.metadataFailed() {
		t.Fatal("fresh metadata owner poisoned")
	}
}
