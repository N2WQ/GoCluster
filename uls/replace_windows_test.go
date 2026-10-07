package uls

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// Exercise an actual Windows sharing denial, rather than substituting a retry
// callback. Failed replacement preserves old bytes; releasing the owner permits
// a later replacement of that same target without deleting it first.
func TestWindowsReplacementKeepsLastGoodDuringSharingDenial(t *testing.T) {
	dir := t.TempDir()
	target, replacement := filepath.Join(dir, "old.db"), filepath.Join(dir, "new.db")
	if err := os.WriteFile(target, []byte("last good"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(replacement, []byte("new good"), 0600); err != nil {
		t.Fatal(err)
	}
	name, err := syscall.UTF16PtrFromString(target)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := syscall.CreateFile(name, syscall.GENERIC_READ, syscall.FILE_SHARE_READ, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = syscall.CloseHandle(handle)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	err = replaceDBWithRetryContext(ctx, target, replacement)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retry under sharing denial: %v", err)
	}
	old, err := os.ReadFile(target)
	if err != nil || string(old) != "last good" {
		t.Fatalf("old=%q err=%v", old, err)
	}
	if err := syscall.CloseHandle(handle); err != nil {
		t.Fatal(err)
	}
	closed = true
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := replaceDBWithRetryContext(ctx, target, replacement); err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(target)
	if err != nil || string(current) != "new good" {
		t.Fatalf("new=%q err=%v", current, err)
	}
}
