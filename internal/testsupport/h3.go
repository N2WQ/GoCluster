// Package testsupport owns shared test fixture discovery helpers.
package testsupport

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// H3TableDir returns the checked-in H3 fixture directory from any package test
// working directory. The tables are required fixtures, so missing files fail
// the test instead of silently skipping H3-backed coverage.
func H3TableDir(t testing.TB) string {
	t.Helper()
	root := RepoRoot(t)
	dir := filepath.Join(root, "data", "h3")
	for _, name := range []string{"res1.bin", "res2.bin"} {
		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("required H3 fixture %s unavailable: %v", path, err)
		}
		if info.IsDir() {
			t.Fatalf("required H3 fixture %s is a directory", path)
		}
	}
	return dir
}

// RepoRoot walks upward from the current test working directory until it finds
// go.mod. Package tests run from their package directory, not the repository
// root, so relative fixture paths must be anchored explicitly.
func RepoRoot(t testing.TB) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get test working directory: %v", err)
	}
	root, err := findRepoRoot(wd)
	if err != nil {
		t.Fatalf("find repository root from %s: %v", wd, err)
	}
	return root
}

func findRepoRoot(start string) (string, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found")
		}
		dir = parent
	}
}
