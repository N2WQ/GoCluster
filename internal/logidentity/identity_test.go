//go:build windows || linux

package logidentity

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestHandleIdentityDetectsDirectoryAndFileAliases(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "real")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	firstDir, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer firstDir.Close()
	secondDir, err := os.Open(alias)
	if err != nil {
		t.Fatal(err)
	}
	defer secondDir.Close()
	firstID, err := IdentifyDirectory(firstDir)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := IdentifyDirectory(secondDir)
	if err != nil || firstID != secondID {
		t.Fatalf("directory alias IDs differ: %v", err)
	}
	var r Registry
	leases, _, err := r.BeginBatch([]uint8{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := leases[0].ClaimDirectory(firstID); err != nil {
		t.Fatal(err)
	}
	if err := leases[1].ClaimDirectory(secondID); !errors.Is(err, ErrConflict) {
		t.Fatalf("directory alias was not rejected: %v", err)
	}
}

func TestHandleIdentityDetectsHardLinks(t *testing.T) {
	root := t.TempDir()
	firstPath := filepath.Join(root, "first.log")
	if err := os.WriteFile(firstPath, []byte("record"), 0600); err != nil {
		t.Fatal(err)
	}
	secondPath := filepath.Join(root, "second.log")
	if err := os.Link(firstPath, secondPath); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	first, err := os.Open(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := os.Open(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	id1, links1, err := IdentifyFile(first)
	if err != nil {
		t.Fatal(err)
	}
	id2, links2, err := IdentifyFile(second)
	if err != nil || id1 != id2 || links1 < 2 || links2 < 2 {
		t.Fatalf("hard-link identity/link count: %d %d %v", links1, links2, err)
	}
}

func TestIdentityRejectsMissingClosedAndWrongKindHandles(t *testing.T) {
	if _, err := IdentifyDirectory(nil); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("nil directory = %v", err)
	}
	if _, _, err := IdentifyFile(nil); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("nil file = %v", err)
	}
	dir, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := IdentifyFile(dir); !errors.Is(err, ErrWrongKind) {
		t.Fatalf("directory treated as regular file: %v", err)
	}
	if err := dir.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := IdentifyDirectory(dir); err == nil {
		t.Fatal("closed handle returned an identity")
	}
	file, err := os.CreateTemp(t.TempDir(), "file")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := IdentifyDirectory(file); !errors.Is(err, ErrWrongKind) {
		t.Fatalf("file treated as directory: %v", err)
	}
}
