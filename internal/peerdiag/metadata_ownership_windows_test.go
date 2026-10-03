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
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestV15WindowsOwnedMetadataJunctionRelease(t *testing.T) {
	dir := t.TempDir()
	target, link := filepath.Join(dir, "target"), filepath.Join(dir, "alias")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", `$ErrorActionPreference='Stop'
New-Item -ItemType Junction -Path $env:PEERDIAG_TEST_LINK -Target $env:PEERDIAG_TEST_TARGET | Out-Null`)
	cmd.Env = append(os.Environ(), "PEERDIAG_TEST_LINK="+link, "PEERDIAG_TEST_TARGET="+target)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("actual junction: %v %s", err, output)
	}
	defer os.Remove(link)
	create, closeFile := diagnosticMetadataCreateFile, diagnosticMetadataFileClose
	t.Cleanup(func() { diagnosticMetadataCreateFile, diagnosticMetadataFileClose = create, closeFile })
	for _, failedClose := range []int{1, 2} {
		var sink helperSink
		opens, closes := 0, 0
		diagnosticMetadataCreateFile = func(p *uint16, access, share uint32, security *syscall.SecurityAttributes, disposition, flags uint32, template int32) (syscall.Handle, error) {
			opens++
			wantFlags := uint32(syscall.FILE_FLAG_BACKUP_SEMANTICS)
			if opens == 1 {
				wantFlags |= syscall.FILE_FLAG_OPEN_REPARSE_POINT
			}
			if access != 0 || share != 0 || flags != wantFlags {
				t.Fatal("junction follow flags changed", opens, access, share, flags)
			}
			return create(p, access, share, security, disposition, flags, template)
		}
		diagnosticMetadataFileClose = func(file *os.File) error {
			closes++
			if closes == failedClose {
				return syscall.ERROR_ACCESS_DENIED
			}
			return closeFile(file)
		}
		if _, err := sink.diagnosticStat(link); !errors.Is(err, errMetadataRelease) || opens != failedClose || closes != failedClose {
			t.Fatal("failed close did not stop before next acquisition", failedClose, opens, closes, err)
		}
		if _, err := sink.diagnosticStat(link); !errors.Is(err, errMetadataRelease) || opens != failedClose || closes != failedClose {
			t.Fatal("failed generation retried", err, opens, closes)
		}
		if err := closeFile(sink.metadata.file); err != nil {
			t.Fatal("fixture-owned handle cleanup", err)
		}
	}
	diagnosticMetadataCreateFile, diagnosticMetadataFileClose = create, closeFile
	// Lstat must leave the final junction un-followed unless a trailing
	// separator requests the inherited POSIX-style follow behavior.
	for _, path := range []string{link, link + `\`} {
		var sink helperSink
		got, gotErr := sink.diagnosticLstat(path)
		want, wantErr := os.Lstat(path)
		if (gotErr == nil) != (wantErr == nil) || gotErr == nil && (got.Mode() != want.Mode() || got.IsDir() != want.IsDir()) {
			t.Fatal("Lstat surrogate policy changed", path, got, gotErr, want, wantErr)
		}
	}
}

func TestV15WindowsOwnedMetadataReleaseFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kept")
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	attrs, closeFind, closeFile, create := diagnosticGetAttributes, diagnosticMetadataFindClose, diagnosticMetadataFileClose, diagnosticMetadataCreateFile
	t.Cleanup(func() {
		diagnosticGetAttributes, diagnosticMetadataFindClose, diagnosticMetadataFileClose, diagnosticMetadataCreateFile = attrs, closeFind, closeFile, create
	})
	for _, kind := range []string{"find", "file"} {
		t.Run(kind, func(t *testing.T) {
			var sink helperSink
			closes, opens := 0, 0
			if kind == "find" {
				diagnosticGetAttributes = func(*uint16, uint32, *byte) error { return windows.ERROR_SHARING_VIOLATION }
				diagnosticMetadataFindClose = func(syscall.Handle) error { closes++; return syscall.ERROR_ACCESS_DENIED }
				diagnosticMetadataFileClose = closeFile
			} else {
				diagnosticGetAttributes = func(*uint16, uint32, *byte) error { return syscall.ERROR_ACCESS_DENIED }
				diagnosticMetadataFindClose = closeFind
				diagnosticMetadataFileClose = func(*os.File) error { closes++; return syscall.ERROR_ACCESS_DENIED }
			}
			diagnosticMetadataCreateFile = func(p *uint16, access, share uint32, security *syscall.SecurityAttributes, disposition, flags uint32, template int32) (syscall.Handle, error) {
				opens++
				return create(p, access, share, security, disposition, flags, template)
			}
			_, err := sink.diagnosticStat(path)
			if !errors.Is(err, errMetadataRelease) || !sink.metadataFailed() || closes != 1 {
				t.Fatal("failed owner escaped", kind, closes, err)
			}
			previousOpens := opens
			if _, err := sink.diagnosticStat(path); !errors.Is(err, errMetadataRelease) {
				t.Fatal(err)
			}
			if err := sink.diagnosticMkdirAll(filepath.Dir(path), 0o700); !errors.Is(err, errMetadataRelease) {
				t.Fatal(err)
			}
			if err := sink.close(); !errors.Is(err, errMetadataRelease) {
				t.Fatal(err)
			}
			if closes != 1 || opens != previousOpens {
				t.Fatal("uncertain handle retried or replaced", closes, opens, previousOpens)
			}
			// The fault was injected BEFORE release. Only this fixture knows that
			// fact and can safely close the retained handle for test cleanup.
			if kind == "find" {
				if err := closeFind(sink.metadata.handle); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := closeFile(sink.metadata.file); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestV15WindowsMetadataFailureStopsSink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "overlong.log")
	if err := os.WriteFile(path, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	attrs, closeFind := diagnosticGetAttributes, diagnosticMetadataFindClose
	t.Cleanup(func() { diagnosticGetAttributes, diagnosticMetadataFindClose = attrs, closeFind })
	diagnosticGetAttributes = func(p *uint16, level uint32, data *byte) error {
		// Directory metadata succeeds; only the intentionally ignored overlong
		// file Stat reaches the sharing fallback and failed close.
		if err := attrs(p, level, data); err != nil {
			return err
		}
		if (*syscall.Win32FileAttributeData)(unsafe.Pointer(data)).FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY == 0 {
			return windows.ERROR_SHARING_VIOLATION
		}
		return nil
	}
	diagnosticMetadataFindClose = func(syscall.Handle) error { return syscall.ERROR_ACCESS_DENIED }
	sink := helperSink{options: Options{OverlongPath: path}}
	if ack := sink.write(testEvent(1, time.Unix(0, 0), Overlong, "must-not-write")); ack != ackFailed || !sink.metadataFailed() {
		t.Fatal("ordinary Stat-ignore path hid failed release", ack)
	}
	if err := closeFind(sink.metadata.handle); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "unchanged" {
		t.Fatal("write followed ownership failure", string(data), err)
	}
}

func TestV15WindowsMetadataFailureChild(t *testing.T) {
	address := os.Getenv("PEERDIAG_METADATA_ADDRESS")
	if address == "" {
		return
	}
	diagnosticGetAttributes = func(*uint16, uint32, *byte) error { return windows.ERROR_SHARING_VIOLATION }
	closes := 0
	diagnosticMetadataFindClose = func(syscall.Handle) error { closes++; return syscall.ERROR_ACCESS_DENIED }
	if err := RunHelper(address, strings.Repeat("0", 64)); err == nil || closes != 1 {
		os.Exit(3)
	}
	os.Exit(0)
}

func TestV15WindowsMetadataFailureRetiresHelper(t *testing.T) {
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err = listener.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
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
	p, err := startCompanion(executable, []string{helperExecutable, "-test.run=^TestV15WindowsMetadataFailureChild$"}, &os.ProcAttr{Env: append(helperEnvironment(helperEnvironmentRoot()), "PEERDIAG_METADATA_ADDRESS="+listener.Addr().String()), Files: []*os.File{file, file, file}, Sys: helperProcessAttributes()})
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
	s := New(Options{Enabled: true, Directory: t.TempDir()})
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
		t.Fatal("fault companion authentication", err)
	}
	if err := sendOptions(child.conn, s.options); err != nil {
		t.Fatal(err)
	}
	s.Emit(Diagnostic, Fields{Action: "metadata_close_failure"})
	s.exchange(child)
	if stats := s.Snapshot(); stats.Unconfirmed != 1 || stats.Written != 0 || stats.ChargedBytes != BackingLimit {
		t.Fatal("failed release lost generation charge or ACK classification", stats)
	}
	select {
	case <-child.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata failure helper did not exit")
	}
	var exitCode uint32
	if err := syscall.GetExitCodeProcess(p.handle, &exitCode); err != nil || exitCode != 0 || !child.waitSucceeded {
		t.Fatal("actual fault/Wait witness absent", exitCode, err)
	}
	if !s.retire(child) || s.Snapshot().ChargedBytes != parentReservation {
		t.Fatal("process retirement did not discharge owner")
	}
}
