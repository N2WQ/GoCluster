package cluster

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRootedLegacyArchiveHardLinkIsNotAdopted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(filepath.Dir(dir), "external")
	if err := os.WriteFile(external, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	archive := archiveLogPathForDate(dir, now)
	if err := os.Link(external, archive); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	sink, err := newDailyFileSinkInRoot(dir, 7, root)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if sink.writeLine("must not be written", now) {
		t.Fatal("adopted an externally linked archive")
	}
	if data, err := os.ReadFile(archive); err != nil || string(data) != "keep" {
		t.Fatalf("legacy archive changed: %q, %v", data, err)
	}
	if _, err := os.Stat(activeLogPathForDir(dir)); !os.IsNotExist(err) {
		t.Fatalf("active log unexpectedly created: %v", err)
	}
}

func TestRootedDailySinkStaysInOriginalDirectoryAfterRetarget(t *testing.T) {
	for _, collision := range []bool{false, true} {
		t.Run(map[bool]string{false: "rename", true: "archive collision"}[collision], func(t *testing.T) {
			parent := t.TempDir()
			original := filepath.Join(parent, "original")
			other := filepath.Join(parent, "other")
			for _, dir := range []string{original, other} {
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			link := filepath.Join(parent, "system")
			if err := os.Symlink(original, link); err != nil {
				t.Skipf("directory symlinks unavailable: %v", err)
			}
			root, err := os.OpenRoot(link)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			sink, err := newDailyFileSinkInRoot(link, 7, root)
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			sink.workerHook = true
			now := time.Now().UTC()
			previous := now.AddDate(0, 0, -1)
			stale := now.AddDate(0, 0, -20)
			activeName := activeLogFileNameForDir(link)
			archiveName := logFileNameForDate(previous)
			staleName := logFileNameForDate(stale)
			oldLine := formatLogTimestamp(previous) + " previous\n"
			files := map[string]string{
				filepath.Join(original, activeName): oldLine,
				filepath.Join(original, staleName):  "old archive\n",
				filepath.Join(other, activeName):    "other active\n",
				filepath.Join(other, archiveName):   "other archive\n",
				filepath.Join(other, staleName):     "other old archive\n",
			}
			if collision {
				files[filepath.Join(original, archiveName)] = "existing archive\n"
			}
			for path, contents := range files {
				if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Remove(link); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(other, link); err != nil {
				t.Fatal(err)
			}
			var gotPath string
			sink.SetRotateHook(func(_ time.Time, previousPath, _ string) { gotPath = previousPath })
			if !sink.writeLine("current", now) {
				t.Fatal("rooted write failed")
			}
			if gotPath != filepath.Join(link, archiveName) {
				t.Fatalf("rotation path = %q", gotPath)
			}
			if err := sink.Close(); err != nil {
				t.Fatal(err)
			}
			wantArchive := oldLine
			if collision {
				wantArchive = "existing archive\n"
			}
			if data, err := os.ReadFile(filepath.Join(original, archiveName)); err != nil || string(data) != wantArchive {
				t.Fatalf("original archive = %q, %v", data, err)
			}
			if data, err := os.ReadFile(filepath.Join(original, activeName)); err != nil || !strings.Contains(string(data), "current") {
				t.Fatalf("original active = %q, %v", data, err)
			}
			if _, err := os.Stat(filepath.Join(original, staleName)); !os.IsNotExist(err) {
				t.Fatalf("original old archive was not cleaned: %v", err)
			}
			for name, want := range map[string]string{activeName: "other active\n", archiveName: "other archive\n", staleName: "other old archive\n"} {
				if data, err := os.ReadFile(filepath.Join(other, name)); err != nil || string(data) != want {
					t.Fatalf("retargeted %s = %q, %v", name, data, err)
				}
			}
			if _, err := root.Stat(activeName); err != nil {
				t.Fatalf("sink closed its borrowed root: %v", err)
			}
		})
	}
}

func TestRootedDailySinkPreservesOpenAndCleanupHooks(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	sink, err := newDailyFileSinkInRoot(dir, 7, root)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	now := time.Now().UTC()
	var cleanups int
	sink.cleanupFn = func(got string, _ time.Time, days int) error {
		if got != dir || days != 7 {
			t.Fatalf("cleanup args = %q, %d", got, days)
		}
		cleanups++
		return nil
	}
	sink.openFile = func(string, int, os.FileMode) (*os.File, error) {
		return nil, errors.New("injected open failure")
	}
	if sink.writeLine("failed", now) {
		t.Fatal("injected open failure reported success")
	}
	sink.openFile = func(path string, flags int, mode os.FileMode) (*os.File, error) {
		return openSafeLogInRoot(root, filepath.Base(path), flags, mode)
	}
	if !sink.writeLine("recovered", now) || cleanups != 1 {
		t.Fatalf("rooted retry/cleanup failed: cleanups=%d", cleanups)
	}
}
