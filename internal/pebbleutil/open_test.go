package pebbleutil

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestPrepareDirRejectsEmptyPath(t *testing.T) {
	if _, err := PrepareDir(" \t "); !errors.Is(err, ErrEmptyPath) {
		t.Fatalf("PrepareDir() error = %v, want ErrEmptyPath", err)
	}
}

func TestPrepareDirRejectsNonDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	if err := os.WriteFile(path, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write file path: %v", err)
	}

	_, err := PrepareDir(path)
	if !errors.Is(err, ErrNotDirectory) {
		t.Fatalf("PrepareDir() error = %v, want ErrNotDirectory", err)
	}
	var notDir *NotDirectoryError
	if !errors.As(err, &notDir) {
		t.Fatalf("PrepareDir() error type = %T, want *NotDirectoryError", err)
	}
	if notDir.Path != path {
		t.Fatalf("NotDirectoryError.Path = %q, want %q", notDir.Path, path)
	}
}

func TestPrepareDirCreatesTrimmedDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "db")

	got, err := PrepareDir(" " + path + " ")
	if err != nil {
		t.Fatalf("PrepareDir() error: %v", err)
	}
	if got != path {
		t.Fatalf("PrepareDir() path = %q, want %q", got, path)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat prepared path: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("prepared path is not a directory")
	}
}

func TestOpenDefaultsNilOptions(t *testing.T) {
	path, err := PrepareDir(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("PrepareDir() error: %v", err)
	}
	db, err := Open(path, nil)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestOpenUsesCallerOptionsWithoutTakingOwnership(t *testing.T) {
	path, err := PrepareDir(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("PrepareDir() error: %v", err)
	}
	cache := pebble.NewCache(1 << 20)
	opts := &pebble.Options{Cache: cache}
	db, err := Open(path, opts)
	if err != nil {
		cache.Unref()
		t.Fatalf("Open() error: %v", err)
	}
	if err := db.Close(); err != nil {
		cache.Unref()
		t.Fatalf("Close() error: %v", err)
	}
	cache.Unref()
}
