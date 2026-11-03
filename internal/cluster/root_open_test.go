package cluster

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestRootOpenNoFollow(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := openRootNoFollow(root, "missing.log", os.O_RDONLY, 0); !os.IsNotExist(err) {
		t.Fatalf("missing file was not reported as not found: %v", err)
	}

	file, err := openRootNoFollow(root, "sample.log", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("first"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	file, err = openRootNoFollow(root, "sample.log", os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(" second"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	file, err = openRootNoFollow(root, "sample.log", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || string(data) != "first second" {
		t.Fatalf("read = %q, read error %v, close error %v", data, readErr, closeErr)
	}

	for _, name := range []string{"", ".", "..", "../escape", "sub/file", filepath.Join(dir, "sample.log")} {
		f, err := openRootNoFollow(root, name, os.O_CREATE|os.O_WRONLY, 0600)
		if err == nil {
			_ = f.Close()
			t.Errorf("accepted invalid name %q", name)
		} else if !errors.Is(err, os.ErrInvalid) {
			t.Errorf("invalid name %q: %v", name, err)
		}
	}
	if _, err := openRootNoFollow(nil, "sample.log", os.O_RDONLY, 0); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("nil root: %v", err)
	}
}

func TestRootOpenNoFollowRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.log")
	if err := os.WriteFile(target, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	linkTarget := target
	if runtime.GOOS == "linux" {
		// A relative in-root link would be followed by Root.OpenFile on Unix.
		linkTarget = "target.log"
	}
	if err := os.Symlink(linkTarget, filepath.Join(dir, "link.log")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	file, err := openRootNoFollow(root, "link.log", os.O_WRONLY|os.O_TRUNC, 0)
	if err == nil {
		_ = file.Close()
		t.Fatal("opened a symlink")
	}
	if data, err := os.ReadFile(target); err != nil || string(data) != "unchanged" {
		t.Fatalf("target = %q, %v", data, err)
	}
	if runtime.GOOS != "linux" {
		// The fallback is confined by os.Root but does not reject all internal symlinks.
		t.Log("non-Linux test checks a symlink whose absolute target is outside Root resolution")
	}
}

func TestRootOpenNoFollowStaysAnchoredAfterRetarget(t *testing.T) {
	base := t.TempDir()
	first, second, link := filepath.Join(base, "first"), filepath.Join(base, "second"), filepath.Join(base, "link")
	for _, dir := range []string{first, second} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(first, link); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	root, err := os.OpenRoot(link)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := os.Remove(link); err != nil {
		t.Skipf("directory retarget unavailable: %v", err)
	}
	if err := os.Symlink(second, link); err != nil {
		t.Fatal(err)
	}
	file, err := openRootNoFollow(root, "created.log", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("original"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(first, "created.log")); err != nil || string(data) != "original" {
		t.Fatalf("original file = %q, %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(second, "created.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retargeted folder was modified: %v", err)
	}
}
