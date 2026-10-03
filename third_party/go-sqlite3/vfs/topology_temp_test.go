package vfs

import (
	"os"
	"strings"
	"testing"
)

func TestV15SQLiteTemporaryPathBound(t *testing.T) {
	t.Setenv("SQLITE_TMPDIR", strings.Repeat("x", 2048))
	if f, err := osCreateTemp(OPEN_TEMP_DB | OPEN_DELETEONCLOSE); f != nil || err != _IOERR_NOMEM {
		if f != nil {
			f.Close()
		}
		t.Fatalf("over-bound environment was not refused before allocation: %v", err)
	}
	t.Setenv("SQLITE_TMPDIR", t.TempDir())
	f, err := osCreateTemp(OPEN_TEMP_DB | OPEN_DELETEONCLOSE)
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	os.Remove(name)
	if len(name) > _MAX_PATHNAME {
		t.Fatal("ordinary temporary filename exceeded bound")
	}
}
