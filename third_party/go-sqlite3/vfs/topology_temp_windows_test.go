package vfs

import (
	"os"
	"strings"
	"testing"
)

func TestV15SQLiteWindowsTemporaryDirectoryCompatibility(t *testing.T) {
	for _, value := range []string{`C:\`, t.TempDir(), ""} {
		t.Setenv("SQLITE_TMPDIR", value)
		want := value
		if want == "" {
			want = os.TempDir()
		}
		got, err := boundedTempDirectory()
		if err != nil || got != want {
			t.Fatalf("temporary directory %q got=%q want=%q err=%v", value, got, want, err)
		}
	}
	t.Setenv("SQLITE_TMPDIR", strings.Repeat("x", 32000))
	if _, err := boundedTempDirectory(); err != _IOERR_NOMEM {
		t.Fatalf("large Windows environment not refused through fixed buffer: %v", err)
	}
}
