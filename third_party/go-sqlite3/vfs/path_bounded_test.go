//go:build linux

package vfs

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

func requireActualPathSymlink(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	// 1314 is ERROR_PRIVILEGE_NOT_HELD. Keep the ordinary package lane useful
	// on nonprivileged Windows accounts, but never waive the authoritative
	// qualification requirement or hide a different link-creation error.
	if runtime.GOOS == "windows" && errors.Is(err, syscall.Errno(1314)) && os.Getenv("GOCLUSTER_SQLITE_SYMLINK_REQUIRED") != "1" {
		t.Skip("Windows symlink creation privilege unavailable; required qualification sets GOCLUSTER_SQLITE_SYMLINK_REQUIRED=1")
	}
	t.Fatal("actual symlink fixture unavailable", err)
}

func TestV15SQLiteBoundedPathOrdinaryParity(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.MkdirAll("Real/Sub", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("Real/value", []byte("committed"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{".", "Real", "Real/Sub/..", "Real/value/child", "missing", filepath.Join(dir, "Real", "value")} {
		want, wantErr := filepath.EvalSymlinks(path)
		got, gotErr := boundedEvalSymlinks(path)
		if got != want || (gotErr == nil) != (wantErr == nil) || (errors.Is(gotErr, fs.ErrNotExist) != errors.Is(wantErr, fs.ErrNotExist)) {
			t.Fatalf("path %q: candidate %q/%v Go1.26 %q/%v", path, got, gotErr, want, wantErr)
		}
	}
}

func TestV15SQLiteBoundedSymlinkParity(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.MkdirAll("Real/Sub", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("Real/value", []byte("committed"), 0o600); err != nil {
		t.Fatal(err)
	}
	requireActualPathSymlink(t, os.Symlink("Real", "relative"))
	if err := os.Symlink(filepath.Join(dir, "Real"), "absolute"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{".", "Real", "Real/Sub/..", "relative/value", "absolute/Sub/..", "relative/missing", "Real/value/child", "missing", filepath.Join(dir, "absolute", "value")} {
		t.Run(path, func(t *testing.T) {
			want, wantErr := filepath.EvalSymlinks(path)
			got, gotErr := boundedEvalSymlinks(path)
			sameTarget := got == want
			if runtime.GOOS == "windows" && gotErr == nil && wantErr == nil {
				// Windows preserves an absolute NT target's extended namespace.
				// Go's display spelling may differ; compare the actual object.
				a, aErr := os.Stat(got)
				b, bErr := os.Stat(want)
				sameTarget = aErr == nil && bErr == nil && os.SameFile(a, b)
			}
			if !sameTarget || (gotErr == nil) != (wantErr == nil) || (errors.Is(gotErr, fs.ErrNotExist) != errors.Is(wantErr, fs.ErrNotExist)) {
				t.Fatalf("path %q: candidate %q/%v frozen-Go %q/%v", path, got, gotErr, want, wantErr)
			}
		})
	}
	// Keep the VFS's missing-final-component behavior as well as the existing
	// resolved-spelling signal. The parent exists through a real symlink.
	full, err := (vfsOS{}).FullPathname("relative/new.db")
	want, wantErr := filepath.Abs("Real/new.db")
	if full != want || err != _OK_SYMLINK || wantErr != nil {
		t.Fatalf("missing final component: %q/%v want %q/%v", full, err, want, wantErr)
	}
}

func TestV15SQLiteBoundedSymlinkAccumulation(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.MkdirAll("base/x", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("base/kept", []byte("committed"), 0o600); err != nil {
		t.Fatal(err)
	}
	sep := string(os.PathSeparator)
	suffix := strings.Repeat(sep+"x"+sep+"..", 120)
	requireActualPathSymlink(t, os.Symlink("base"+suffix, "second"))
	if err := os.Symlink("second"+suffix, "first"); err != nil {
		t.Fatal(err)
	}
	// Each target fits the individual filename ceiling; their unresolved
	// suffixes do not. The final logical file exists and is left intact.
	if len("second"+suffix) > _MAX_PATHNAME {
		t.Fatal("fixture did not isolate accumulated workspace")
	}
	if _, err := boundedEvalSymlinks("first/kept"); err != _IOERR_NOMEM {
		t.Fatalf("pending suffix was not refused: %v", err)
	}
	data, err := os.ReadFile("base/kept")
	if err != nil || string(data) != "committed" {
		t.Fatalf("refusal changed saved data: %q/%v", data, err)
	}
	// The original standard library remains an oracle for the non-resource
	// behavior. It may traverse a larger workspace than this bounded VFS.
	want, err := filepath.EvalSymlinks("first/kept")
	if err != nil || filepath.Clean(want) != filepath.Clean("base/kept") {
		t.Fatalf("unbounded reference resolution: %q/%v", want, err)
	}
}

func TestV15SQLiteResolverConcatAdmission(t *testing.T) {
	for _, size := range []int{1023, 1024, 1025} {
		part := strings.Repeat("x", size)
		got, err := boundedPathConcat(part)
		if (err == nil) != (size <= _MAX_PATHNAME) || (err == nil && got != part) {
			t.Fatalf("concat length %d: %d/%v", size, len(got), err)
		}
	}
	if _, err := boundedPathConcat(strings.Repeat("a", 700), strings.Repeat("b", 400)); err != _IOERR_NOMEM {
		t.Fatal("aggregate admission missing", err)
	}
}

type resolverLinkInfo struct{ fs.FileInfo }

func (resolverLinkInfo) Mode() fs.FileMode { return fs.ModeSymlink }

func TestV15SQLiteResolverPendingSuffixAdmission(t *testing.T) {
	info, err := os.Stat(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reads := 0
	sep := string(os.PathSeparator)
	target := "loop" + strings.Repeat(sep+"x"+sep+"..", 120)
	_, err = boundedWalkSymlinks("loop", func(path string) (fs.FileInfo, error) {
		if len(path) > _MAX_PATHNAME {
			t.Fatal("oversized lookup reached OS boundary")
		}
		return resolverLinkInfo{info}, nil
	}, func(string) (string, error) {
		reads++
		return target, nil
	})
	if err != _IOERR_NOMEM || reads != 2 {
		t.Fatalf("accumulating pending suffix: reads=%d err=%v", reads, err)
	}
}

func TestV15SQLiteLongCurrentDirectoryRefusal(t *testing.T) {
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
		t.Fatal("long-directory fixture unavailable", err)
	}
	t.Chdir(deep)
	t.Setenv("PWD", "")
	if _, err := boundedPathAbs("new.db"); err != _IOERR_NOMEM {
		t.Fatalf("long CWD was not refused through fixed buffer: %v", err)
	}
	if _, err := (vfsOS{}).FullPathname("new.db"); err != _IOERR_NOMEM {
		t.Fatalf("VFS long-CWD refusal lost category: %v", err)
	}
	if _, err := os.Stat("new.db"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("resource refusal created a file", err)
	}
	if data, err := os.ReadFile(kept); err != nil || string(data) != "committed" {
		t.Fatalf("resource refusal changed data: %q/%v", data, err)
	}
}
