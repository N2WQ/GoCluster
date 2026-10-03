package vfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestV15SQLiteLinuxAbsolutePathParity(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	for _, pwd := range []string{"", dir, filepath.Dir(dir), "relative", strings.Repeat("x", _MAX_PATHNAME+1)} {
		t.Setenv("PWD", pwd)
		for _, path := range []string{"", ".", "..", "relative/file", "/absolute/../file", "資料", "bad\x00path"} {
			want, wantErr := filepath.Abs(path)
			got, gotErr := boundedPathAbs(path)
			if want != got || (wantErr == nil) != (gotErr == nil) {
				t.Fatalf("PWD=%q Abs(%q): candidate %q/%v Go1.26 %q/%v", pwd, path, got, gotErr, want, wantErr)
			}
		}
	}
	link := filepath.Join(filepath.Dir(dir), filepath.Base(dir)+"-link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(link)
	t.Setenv("PWD", link)
	if got, err := boundedPathAbs("file"); err != nil || got != filepath.Join(link, "file") {
		t.Fatalf("same-inode PWD spelling lost: %q/%v", got, err)
	}
	t.Setenv("PWD", "/"+strings.Repeat("x", _MAX_PATHNAME+1))
	if _, err := boundedPathAbs("file"); err != _IOERR_NOMEM {
		t.Fatal("oversized PWD was not refused", err)
	}
}
