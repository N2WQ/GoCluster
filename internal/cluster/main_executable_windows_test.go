package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

const mainTestImageIdentity = "GOCLUSTER_TEST_MAIN_EXECUTABLE"

func mainExecutableForFixture(t *testing.T, repo, _ string) string {
	t.Helper()
	current, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	image, err := validatePreparedMainImage(repo, current, os.Getenv(mainTestImageIdentity))
	if err != nil {
		t.Fatalf("Windows real-main fixture requires scripts/test-windows.ps1: %v", err)
	}
	return image
}

// Freshness comes from the runner's successful build while its lock is held.
// This validates that private handoff, not an arbitrary manual image selector.
func validatePreparedMainImage(repo, current, supplied string) (string, error) {
	parent := filepath.Join(repo, ".tmp", "windows-tests", "dxcluster", "internal", "cluster", "package.test.exe")
	image := filepath.Join(filepath.Dir(parent), "main", "gocluster.exe")
	if !filepath.IsAbs(supplied) || !diagEqualPath(image, supplied) || !diagEqualPath(parent, current) {
		return "", fmt.Errorf("missing or mismatched prepared main executable")
	}
	for _, path := range []string{parent, image} {
		// Inspect every object directly: resolving the final filename alone
		// does not reliably expose an ancestor junction on every Windows host.
		for ancestor := path; !diagEqualPath(ancestor, repo); ancestor = filepath.Dir(ancestor) {
			name, err := windows.UTF16PtrFromString(ancestor)
			if err != nil {
				return "", err
			}
			attributes, err := windows.GetFileAttributes(name)
			if err != nil {
				return "", err
			}
			if attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
				return "", fmt.Errorf("prepared executable traverses a reparse point: %s", ancestor)
			}
		}
		info, err := os.Lstat(path)
		if err != nil {
			return "", err
		}
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("prepared executable is not a regular file: %s", path)
		}
	}
	return image, nil
}

func observeMainFixtureImage(t *testing.T, process *os.Process, expected string) {
	t.Helper()
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := windows.CloseHandle(handle); err != nil {
			t.Errorf("close main image query handle: %v", err)
		}
	}()
	if err := diagVerifyImage(handle, expected); err != nil {
		t.Fatal(err)
	}
	t.Logf("MAIN-CHILD verified image=%s pid=%d", expected, process.Pid)
}
