//go:build windows

package peerdiag

import (
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestV15WindowsRetentionSemantics(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"30-Sep-2026.log", "01-Oct-2026.log", "02-Oct-2026.log", "invalid-logfile", "ééééééé.log"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("kept"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "29-Sep-2026.log"), 0o700); err != nil {
		t.Fatal(err)
	}
	// A real junction has no ModeDir under pinned Go's name-surrogate rule.
	// Deleting its archive-named entry must not walk or delete its target.
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "28-Sep-2026.log")
	for _, path := range []string{target, link} {
		rel, err := filepath.Rel(dir, path)
		if err != nil || !filepath.IsAbs(path) || !filepath.IsLocal(rel) || rel == "." {
			t.Fatal("fixture path escaped owned root", path, err)
		}
	}
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "kept"), []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", `$ErrorActionPreference = 'Stop'
New-Item -ItemType Junction -Path $env:GOCLUSTER_TEST_JUNCTION_LINK -Target $env:GOCLUSTER_TEST_JUNCTION_TARGET | Out-Null`)
	cmd.Env = append(os.Environ(), "GOCLUSTER_TEST_JUNCTION_LINK="+link, "GOCLUSTER_TEST_JUNCTION_TARGET="+target)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create actual junction: %v %s", err, output)
	}
	defer func() {
		if err := os.Remove(link); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Error(err)
		}
	}()
	info, err := os.Lstat(link)
	if err != nil || info.IsDir() {
		t.Fatal("pinned Go junction IsDir control differs", info, err)
	}
	locked := filepath.Join(dir, "27-Sep-2026.log")
	if err := os.WriteFile(locked, []byte("locked"), 0o600); err != nil {
		t.Fatal(err)
	}
	encoded, _ := syscall.UTF16PtrFromString(locked)
	handle, err := syscall.CreateFile(encoded, syscall.GENERIC_READ, 0, nil, syscall.OPEN_EXISTING, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.CloseHandle(handle)
	s := helperSink{options: Options{Directory: dir, RetentionDays: 2}}
	if err := s.cleanup(time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"30-Sep-2026.log", "28-Sep-2026.log"} {
		if _, err := os.Lstat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("expired non-directory entry retained", name, err)
		}
	}
	for _, name := range []string{"01-Oct-2026.log", "02-Oct-2026.log", "29-Sep-2026.log", "27-Sep-2026.log", "invalid-logfile", "ééééééé.log", "target"} {
		if _, err := os.Lstat(filepath.Join(dir, name)); err != nil {
			t.Fatal("retention removed protected/invalid/locked entry", name, err)
		}
	}
	if value, err := os.ReadFile(filepath.Join(target, "kept")); err != nil || string(value) != "target" {
		t.Fatal("junction removal changed target", string(value), err)
	}
	if s.scan.active || s.scan.failed {
		t.Fatal("completed directory scan retained native owner")
	}
}

func TestV15WindowsFindOwnership(t *testing.T) {
	realFirst, realNext, realClose := diagnosticFindFirst, diagnosticFindNext, diagnosticFindClose
	defer func() { diagnosticFindFirst, diagnosticFindNext, diagnosticFindClose = realFirst, realNext, realClose }()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "unrelated"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	diagnosticFindFirst = func(*uint16, *syscall.Win32finddata) (syscall.Handle, error) {
		return syscall.InvalidHandle, syscall.ERROR_ACCESS_DENIED
	}
	s := helperSink{options: Options{Directory: dir, RetentionDays: 1}}
	if err := s.cleanup(time.Now()); !errors.Is(err, syscall.ERROR_ACCESS_DENIED) || s.scan.active {
		t.Fatal("failed first scan created/lost ownership", err)
	}
	diagnosticFindFirst = realFirst
	closeCount := 0
	diagnosticFindClose = func(h syscall.Handle) error { closeCount++; return realClose(h) }
	diagnosticFindNext = func(syscall.Handle, *syscall.Win32finddata) error { return syscall.ERROR_ACCESS_DENIED }
	if err := s.cleanup(time.Now()); !errors.Is(err, syscall.ERROR_ACCESS_DENIED) || closeCount != 1 || s.scan.active {
		t.Fatal("failed enumeration did not release exactly once", err, closeCount)
	}
	diagnosticFindNext = realNext
	closeCount = 0
	diagnosticFindClose = func(syscall.Handle) error { closeCount++; return syscall.ERROR_ACCESS_DENIED }
	if err := s.cleanup(time.Now()); !errors.Is(err, syscall.ERROR_ACCESS_DENIED) || !s.scan.active || !s.scan.failed || closeCount != 1 {
		t.Fatal("failed native release lost owner", err, s.scan, closeCount)
	}
	handle := s.scan.handle
	defer func() {
		if err := realClose(handle); err != nil {
			t.Error("fault-control actual release", err)
		}
	}()
	diagnosticFindFirst = func(*uint16, *syscall.Win32finddata) (syscall.Handle, error) {
		t.Fatal("poisoned helper rescanned")
		return syscall.InvalidHandle, nil
	}
	if err := s.cleanup(time.Now()); !errors.Is(err, errFindRelease) {
		t.Fatal("uncertain owner admitted another scan", err)
	}
	if err := s.close(); !errors.Is(err, errFindRelease) || closeCount != 1 {
		t.Fatal("sink close retried uncertain native release", closeCount, err)
	}
	if s.write(testEvent(1, time.Now(), Overlong, "must-not-write")) != ackFailed {
		t.Fatal("poisoned sink accepted a later write")
	}
}

func TestV15WindowsFindFailureChild(t *testing.T) {
	address := os.Getenv("PEERDIAG_FIND_ADDRESS")
	if address == "" {
		return
	}
	closes := 0
	diagnosticFindClose = func(syscall.Handle) error { closes++; return syscall.ERROR_ACCESS_DENIED }
	err := RunHelper(address, strings.Repeat("0", 64))
	if err == nil || closes != 1 {
		os.Exit(3)
	}
	os.Exit(0)
}

func TestV15WindowsFindFailureRetiresHelper(t *testing.T) {
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := listener.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p, err := startCompanion(executable, []string{helperExecutable, "-test.run=^TestV15WindowsFindFailureChild$"}, &os.ProcAttr{Env: append(helperEnvironment(helperEnvironmentRoot()), "PEERDIAG_FIND_ADDRESS="+listener.Addr().String()), Files: []*os.File{file, file, file}, Sys: helperProcessAttributes()})
	if err != nil {
		if p != nil {
			_ = p.release()
		}
		t.Fatal(err)
	}
	child := &helperProcess{process: p, exited: make(chan struct{})}
	go func() {
		var waitErr error
		child.waitSucceeded, waitErr = p.Wait()
		child.failedExit = waitErr != nil
		close(child.exited)
	}()
	s := New(Options{Enabled: true, Directory: t.TempDir(), RetentionDays: 1})
	t.Cleanup(func() { s.retire(child); s.Stop() })
	s.Mailbox.mu.Lock()
	s.stats.ChargedBytes = BackingLimit
	s.Mailbox.mu.Unlock()
	child.conn, err = listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	var auth [32]byte
	if _, err := io.ReadFull(child.conn, auth[:]); err != nil || auth != ([32]byte{}) {
		t.Fatal("fault companion auth", auth, err)
	}
	if err := sendOptions(child.conn, s.options); err != nil {
		t.Fatal(err)
	}
	s.Emit(Diagnostic, Fields{Action: "find_close_failure"})
	s.exchange(child)
	if stats := s.Snapshot(); stats.Unconfirmed != 1 || stats.Written != 0 || stats.ChargedBytes != BackingLimit {
		t.Fatal("failed native release lost generation charge or ACK classification", stats)
	}
	select {
	case <-child.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("failed native release helper did not exit")
	}
	var exitCode uint32
	if err := syscall.GetExitCodeProcess(p.handle, &exitCode); err != nil || exitCode != 0 || !child.waitSucceeded {
		t.Fatal("actual fault path/Wait witness absent", exitCode, err)
	}
	if !s.retire(child) || s.Snapshot().ChargedBytes != parentReservation {
		t.Fatal("actual process retirement did not discharge failed native owner")
	}
}
