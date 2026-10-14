//go:build windows

package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"dxcluster/config"
	"dxcluster/uls"
)

func TestReplayLicenseCleanupFailureClearsAuthorityAndRetainsRetryOwner(t *testing.T) {
	r := newReplayRunner(replayRequest{})
	r.outDir = t.TempDir()
	r.cfg = &config.Config{FCCULS: config.FCCULSConfig{Enabled: true, DBPath: replayLicenseDB(t, "K1ABC")}}
	t.Cleanup(r.close)
	if err := r.configureExternalDependencies(); err != nil {
		t.Fatal(err)
	}
	dir := r.licenseInputsDir
	ptr, err := syscall.UTF16PtrFromString(filepath.Join(dir, "fcc.db"))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := syscall.CreateFile(ptr, syscall.GENERIC_READ, syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if handle != syscall.InvalidHandle {
			_ = syscall.CloseHandle(handle)
		}
	}()
	cleanupErr := r.closeWithError()
	if cleanupErr == nil || !r.ownsLicenseInputs || r.licenseInputsDir != dir {
		t.Fatalf("failed deletion lost retry owner: err=%v owned=%t path=%s", cleanupErr, r.ownsLicenseInputs, r.licenseInputsDir)
	}
	if got := uls.LookupUS("K1ABC"); got.Available || uls.LicenseChecksEnabled() || uls.LookupStats().Entries != 0 {
		t.Fatalf("cleanup failure retained authority: %+v", got)
	}
	if err = syscall.CloseHandle(handle); err != nil {
		t.Fatal(err)
	}
	handle = syscall.InvalidHandle
	if err = r.closeWithError(); err != nil {
		t.Fatalf("cleanup retry: %v", err)
	}
	if _, err = os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retry did not remove owned files: %v", err)
	}
}
