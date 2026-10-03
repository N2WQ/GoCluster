package peer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestV15TopologyDirectoryCleanupOwnership(t *testing.T) {
	if topologyReservation.Load() != nil {
		t.Fatal("fixture starts with another persistence owner")
	}
	realAttributes, realFirst, realClose := topologyMetadataGetAttributes, topologyMetadataFindFirst, topologyMetadataFindClose
	defer func() {
		topologyMetadataGetAttributes, topologyMetadataFindFirst, topologyMetadataFindClose = realAttributes, realFirst, realClose
	}()
	root := t.TempDir()
	kept := filepath.Join(root, "kept")
	if err := os.WriteFile(kept, []byte("saved"), 0600); err != nil {
		t.Fatal(err)
	}
	topologyMetadataGetAttributes = func(*uint16, uint32, *byte) error { return windows.ERROR_SHARING_VIOLATION }
	topologyMetadataFindFirst = func(_ *uint16, out *syscall.Win32finddata) (syscall.Handle, error) {
		out.FileAttributes = syscall.FILE_ATTRIBUTE_DIRECTORY
		return 123, nil
	}
	closes := 0
	topologyMetadataFindClose = func(syscall.Handle) error { closes++; return syscall.ERROR_ACCESS_DENIED }
	db, err := openTopologyDatabase(context.Background(), filepath.Join(root, "new.db"))
	if err == nil || db != nil {
		t.Fatal("failed directory cleanup admitted engine", db, err)
	}
	owner := topologyReservation.Load()
	if owner == nil || owner.conn != nil || !owner.failedCleanup.Load() || !owner.directoryOwner.terminal {
		t.Fatal("nil-Conn constructor forgot native owner", owner)
	}
	defer func() {
		// Only this fixture's fake123 handle is cleared: no native acquisition
		// occurred. Production terminal owners cannot use this test escape.
		owner.directoryOwner = topologyDirectoryOwner{}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err = openTopologyDatabase(context.Background(), filepath.Join(root, "second.db")); !errors.Is(err, errTopologyReservation) {
		t.Fatal("replacement ignored retained constructor", err)
	}
	if closes != 1 {
		t.Fatal("terminal close retried", closes)
	}
	if bytes, err := os.ReadFile(kept); err != nil || string(bytes) != "saved" {
		t.Fatal(string(bytes), err)
	}
	if _, err = os.Stat(filepath.Join(root, "new.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("constructor created DB after failed metadata close", err)
	}
}

func TestV15TopologyDirectoryNativeCleanupOwnership(t *testing.T) {
	const child = "GOCLUSTER_SQLITE_DIRECTORY_CLOSE_CHILD"
	if os.Getenv(child) != "1" {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestV15TopologyDirectoryNativeCleanupOwnership$", "-test.v")
		command.Env = append(os.Environ(), child+"=1")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("constructor native owner: %v\n%s", err, output)
		}
		return
	}
	root := t.TempDir()
	kept := filepath.Join(root, "kept")
	if err := os.WriteFile(kept, []byte("saved"), 0600); err != nil {
		t.Fatal(err)
	}
	realAttributes, realClose := topologyMetadataGetAttributes, topologyMetadataFindClose
	defer func() { topologyMetadataGetAttributes, topologyMetadataFindClose = realAttributes, realClose }()
	topologyMetadataGetAttributes = func(*uint16, uint32, *byte) error { return windows.ERROR_SHARING_VIOLATION }
	closes := 0
	topologyMetadataFindClose = func(h syscall.Handle) error {
		closes++
		if err := realClose(h); err != nil {
			t.Fatal("actual FindFirst owner not acquired", err)
		}
		return realClose(h) // actual native failure after the handle was consumed
	}
	if db, err := openTopologyDatabase(t.Context(), filepath.Join(root, "new.db")); err == nil || db != nil {
		t.Fatal(db, err)
	}
	owner := topologyReservation.Load()
	if owner == nil || owner.conn != nil || !owner.failedCleanup.Load() || !owner.directoryOwner.terminal {
		t.Fatal("native constructor owner lost")
	}
	topologyMetadataFindClose = realClose
	if _, err := openTopologyDatabase(t.Context(), filepath.Join(root, "other.db")); !errors.Is(err, errTopologyReservation) || closes != 1 {
		t.Fatal("terminal constructor retried/replaced", err, closes)
	}
	if data, err := os.ReadFile(kept); err != nil || string(data) != "saved" {
		t.Fatal(string(data), err)
	}
}

func TestV15TopologyDirectoryMkdirParity(t *testing.T) {
	for _, mode := range []string{"winsymlink=0", "winsymlink=1"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("GODEBUG", mode)
			for _, relative := range []string{`a\b`, `a\b\.`, `a\b\..\c`, `a\b\`, `a\b. `} {
				priorRoot, candidateRoot := t.TempDir(), t.TempDir()
				wantErr := os.MkdirAll(filepath.Join(priorRoot, relative), 0755)
				db := &topologyDatabase{}
				gotErr := db.makeTopologyDirectory(filepath.Join(candidateRoot, relative))
				if (gotErr == nil) != (wantErr == nil) {
					t.Fatal(relative, gotErr, wantErr)
				}
				for _, pair := range [][2]string{{priorRoot, candidateRoot}, {filepath.Join(priorRoot, relative), filepath.Join(candidateRoot, relative)}} {
					want, err1 := os.Stat(pair[0])
					got, err2 := os.Stat(pair[1])
					if (err1 == nil) != (err2 == nil) || err1 == nil && want.IsDir() != got.IsDir() {
						t.Fatal(relative, err1, err2)
					}
				}
			}
		})
	}
}

func TestV15TopologyDirectoryJunctionParity(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	link := filepath.Join(root, "junction")
	for _, path := range []string{target, link} {
		relative, err := filepath.Rel(root, path)
		if err != nil || !filepath.IsAbs(path) || !filepath.IsLocal(relative) || relative == "." {
			t.Fatal("fixture escaped root", path, err)
		}
	}
	if err := os.Mkdir(target, 0755); err != nil {
		t.Fatal(err)
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	command := exec.CommandContext(t.Context(), "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", "$ErrorActionPreference='Stop'; New-Item -ItemType Junction -Path "+quote(link)+" -Target "+quote(target)+" | Out-Null")
	command.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("actual junction fixture: %v %s", err, output)
	}
	t.Cleanup(func() {
		if err := os.Remove(link); err != nil {
			t.Error("nonrecursive junction removal", err)
		}
	})
	want, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.Stat(link)
	if err != nil || !os.SameFile(want, got) {
		t.Fatal("fixture does not reach actual target", err)
	}
	kept := filepath.Join(target, "kept")
	if err = os.WriteFile(kept, []byte("saved"), 0600); err != nil {
		t.Fatal(err)
	}
	for i, mode := range []string{"winsymlink=0", "winsymlink=1"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("GODEBUG", mode)
			db := &topologyDatabase{}
			name := fmt.Sprintf("tree%d", i)
			if err := os.MkdirAll(filepath.Join(link, "prior", name), 0755); err != nil {
				t.Fatal(err)
			}
			if err := db.makeTopologyDirectory(filepath.Join(link, "candidate", name)); err != nil {
				t.Fatal(err)
			}
			for _, base := range []string{"prior", "candidate"} {
				if info, err := os.Stat(filepath.Join(target, base, name)); err != nil || !info.IsDir() {
					t.Fatal("wrong junction target", base, err)
				}
			}
		})
	}
	if data, err := os.ReadFile(kept); err != nil || string(data) != "saved" {
		t.Fatal(string(data), err)
	}
}
