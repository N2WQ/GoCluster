//go:build windows

package peerdiag

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestV15WindowsNativeAttributeOwnership(t *testing.T) {
	for _, flags := range []uint32{0, 1} {
		p := &companionProcess{}
		err := p.allocateAttributes(flags)
		if (err == nil) != (flags == 0) {
			t.Fatalf("flags=%d initialization err=%v", flags, err)
		}
		if p.attributes == 0 || p.attributeBytes == 0 || p.attributeBytes > nativeAttributeLimit {
			t.Fatal("native allocation escaped partial owner or reservation")
		}
		t.Logf("native attribute bytes=%d flags=%d", p.attributeBytes, flags)
		if err = p.release(); err != nil {
			t.Fatal(err)
		}
		if p.attributes != 0 || p.attributeBytes != 0 {
			t.Fatal("partial initialized native attribute allocation retained")
		}
	}
}

func TestV15WindowsStartupTemporaryBacking(t *testing.T) {
	// Pinned internal/poll.InitWSA uses these two native structures. They
	// occupy the startup phase allowance, before any log/directory file work.
	bytes := 32*unsafe.Sizeof(syscall.WSAProtocolInfo{}) + unsafe.Sizeof(syscall.WSAData{})
	if bytes > 32<<10 {
		t.Fatalf("Winsock capability structures exceed phase allowance: %d", bytes)
	}
	t.Logf("Winsock startup structure backing=%d", bytes)
}

func TestV15WindowsFailedCreateReleasesNativeStartup(t *testing.T) {
	file, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for range 100 {
		p, err := startCompanion(filepath.Join(t.TempDir(), "missing.exe"), []string{helperExecutable}, &os.ProcAttr{Env: helperEnvironment(helperEnvironmentRoot()), Files: []*os.File{file, file, file}})
		if err == nil || p == nil || p.started() {
			t.Fatal("missing executable did not fail after owned native setup")
		}
		if err = p.release(); err != nil {
			t.Fatal(err)
		}
		if p.handle != 0 || p.thread != 0 || p.attributes != 0 || p.inherited != ([3]syscall.Handle{}) {
			t.Fatal("failed CreateProcess retained startup allocation/handle")
		}
	}
}

func TestV15WindowsIsolatedHandleChild(t *testing.T) {
	value := os.Getenv("PEERDIAG_FORBIDDEN_HANDLE")
	if value == "" {
		return
	}
	handle, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		os.Exit(2)
	}
	if err = windows.SetEvent(windows.Handle(handle)); !errors.Is(err, windows.ERROR_INVALID_HANDLE) {
		os.Exit(3)
	}
	_, _ = os.Stderr.WriteString("isolated\n")
	os.Exit(0)
}

func TestV15WindowsInheritsOnlyOwnedHandles(t *testing.T) {
	attributes := windows.SecurityAttributes{InheritHandle: 1}
	attributes.Length = uint32(unsafe.Sizeof(attributes))
	forbidden, err := windows.CreateEvent(&attributes, 1, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(forbidden)
	file, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p, err := startCompanion(executable, []string{helperExecutable, "-test.run=^TestV15WindowsIsolatedHandleChild$"}, &os.ProcAttr{Env: append(helperEnvironment(helperEnvironmentRoot()), "PEERDIAG_FORBIDDEN_HANDLE="+strconv.FormatUint(uint64(forbidden), 10)), Files: []*os.File{file, file, writer}})
	if err != nil {
		if p != nil {
			_ = p.release()
		}
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	line, readErr := bufio.NewReader(reader).ReadString('\n')
	waited, waitErr := p.Wait()
	releaseErr := p.release()
	if readErr != nil || line != "isolated\n" || !waited || waitErr != nil || releaseErr != nil {
		t.Fatalf("isolation line=%q read=%v waited=%t wait=%v release=%v", line, readErr, waited, waitErr, releaseErr)
	}
}
