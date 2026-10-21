//go:build !windows

package cluster

import (
	"os"
	"path/filepath"
	"testing"
)

func ordinaryHarnessRedirected(info os.FileInfo) (bool, error) {
	return info.Mode()&os.ModeSymlink != 0, nil
}

func ordinaryHarnessBoundaryLink(t *testing.T, link, target string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(link); err != nil {
			t.Error(err)
		}
	})
}

func ordinaryHarnessPlatformPathBoundaries(t *testing.T, directory string) {
	t.Helper()
	t.Run("symlink_metadata", func(t *testing.T) {
		info, err := os.Lstat(directory)
		if err != nil {
			t.Fatal(err)
		}
		if redirected, err := ordinaryHarnessRedirected(info); err != nil || redirected {
			t.Fatalf("ordinary directory misclassified: redirected=%v error=%v", redirected, err)
		}
		link := filepath.Join(directory, "metadata-link")
		ordinaryHarnessBoundaryLink(t, link, directory)
		info, err = os.Lstat(link)
		if err != nil || info.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("native symlink metadata missing: %v", err)
		}
		if redirected, err := ordinaryHarnessRedirected(info); err != nil || !redirected {
			t.Fatalf("symlink metadata accepted: redirected=%v error=%v", redirected, err)
		}
	})
}
