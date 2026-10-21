package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"dxcluster/internal/testsupport"
)

func ordinaryHarnessRequireExecutable(t testing.TB) {
	t.Helper()
	repo := testsupport.RepoRoot(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("get ordinary fixture executable: %v", err)
	}
	if err := ordinaryHarnessCheckExecutable(repo, executable); err != nil {
		t.Fatal(err)
	}
}

func ordinaryHarnessExecutablePath(repo string) string {
	return filepath.Join(repo, ".tmp", "windows-tests", "dxcluster", "internal", "cluster", "package.test.exe")
}

func ordinaryHarnessCheckExecutable(repo, executable string) error {
	expected := ordinaryHarnessExecutablePath(repo)
	actual := filepath.Clean(executable)
	equal := actual == expected
	if runtime.GOOS == "windows" {
		equal = strings.EqualFold(actual, expected)
	}
	if !filepath.IsAbs(executable) || !equal {
		return fmt.Errorf("ordinary fixtures require checkout-local executable %s; got %q", expected, executable)
	}
	return nil
}

func ordinaryHarnessArtifactDir(t testing.TB, prefix string) string {
	t.Helper()
	dir, err := ordinaryHarnessCreateArtifactDir(testsupport.RepoRoot(t), os.Getenv("GOCLUSTER_PATH_SHARDING_ARTIFACT_DIR"), prefix)
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func ordinaryHarnessCreateArtifactDir(repo, requested, prefix string) (string, error) {
	root, err := ordinaryHarnessArtifactRoot(repo, requested)
	if err != nil {
		return "", err
	}
	if err := ordinaryHarnessCheckAncestors(root, os.Lstat); err != nil {
		return "", err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return "", fmt.Errorf("create ordinary artifact root %s: %w", root, err)
	}
	dir, err := os.MkdirTemp(root, prefix)
	if err != nil {
		return "", fmt.Errorf("create ordinary artifact directory in %s: %w", root, err)
	}
	return dir, nil
}

func ordinaryHarnessArtifactRoot(repo, requested string) (string, error) {
	repo, err := filepath.Abs(repo)
	if err != nil {
		return "", fmt.Errorf("resolve ordinary checkout: %w", err)
	}
	allowed := filepath.Join(repo, ".tmp", "path-reliability-first-wave")
	root := filepath.Join(allowed, "s4")
	if requested != "" {
		for _, part := range strings.FieldsFunc(requested, func(r rune) bool { return r == '/' || r == '\\' }) {
			// Reject traversal before Clean can hide it, including Win32
			// trailing-dot/space aliases of path components.
			if part == ".." || runtime.GOOS == "windows" && part != "." && strings.TrimRight(part, " .") != part {
				return "", fmt.Errorf("ordinary artifact path contains traversal or a noncanonical component: %q", requested)
			}
		}
		root, err = filepath.Abs(requested)
		if err != nil {
			return "", fmt.Errorf("resolve ordinary artifact path %q: %w", requested, err)
		}
	}
	relative, err := filepath.Rel(allowed, root)
	if err != nil {
		return "", fmt.Errorf("ordinary artifact path must stay inside %s: %s (%w)", allowed, root, err)
	}
	if !filepath.IsLocal(relative) {
		return "", fmt.Errorf("ordinary artifact path must stay inside %s: %s", allowed, root)
	}
	return root, nil
}

// Absolute containment plus nonredirecting ancestors establishes the canonical
// boundary even when the leaf does not exist. The runner owns this private tree;
// this does not promise protection from hostile concurrent path replacement.
func ordinaryHarnessCheckAncestors(path string, inspect func(string) (os.FileInfo, error)) error {
	for {
		info, err := inspect(path)
		switch {
		case err != nil && !errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("inspect ordinary artifact ancestor %s: %w", path, err)
		case err == nil:
			redirected, err := ordinaryHarnessRedirected(info)
			if err != nil {
				return fmt.Errorf("inspect ordinary artifact ancestor %s: %w", path, err)
			}
			if redirected {
				return fmt.Errorf("ordinary artifact ancestor redirects through a link or reparse point: %s", path)
			}
			if !info.IsDir() {
				return fmt.Errorf("ordinary artifact ancestor is not a directory: %s", path)
			}
		}
		parent := filepath.Dir(path)
		if parent == path {
			return nil
		}
		path = parent
	}
}
