package fsutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestReplaceFileKeepsDestinationOnWindowsSharingDenial(t *testing.T) {
	dir := t.TempDir()
	dest, candidate := filepath.Join(dir, "active"), filepath.Join(dir, "candidate")
	for path, contents := range map[string]string{dest: "previous", candidate: "next"} {
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	name, err := windows.UTF16PtrFromString(dest)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.CreateFile(name, windows.GENERIC_READ, windows.FILE_SHARE_READ, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	replaceErr := ReplaceFile(candidate, dest)
	if err := windows.CloseHandle(handle); err != nil {
		t.Fatal(err)
	}
	if replaceErr == nil {
		t.Fatal("replacement succeeded while destination denied delete sharing")
	}
	if old, err := os.ReadFile(dest); err != nil || string(old) != "previous" {
		t.Fatalf("previous bytes=%q err=%v", old, err)
	}
	if err := ReplaceFile(candidate, dest); err != nil {
		t.Fatal(err)
	}
	if next, err := os.ReadFile(dest); err != nil || string(next) != "next" {
		t.Fatalf("new bytes=%q err=%v", next, err)
	}
}

func TestReplaceFileAcceptsLongWindowsPaths(t *testing.T) {
	part := strings.Repeat("folder", 15)
	dir := filepath.Join(t.TempDir(), part, part, part)
	if len(dir) <= 260 {
		t.Fatal("fixture did not exceed MAX_PATH")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	dest, candidate := filepath.Join(dir, "active"), filepath.Join(dir, "candidate")
	for path, body := range map[string]string{dest: "previous", candidate: "next"} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := ReplaceFile(candidate, dest); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(dest); err != nil || string(body) != "next" {
		t.Fatalf("published bytes=%q error=%v", body, err)
	}
}
