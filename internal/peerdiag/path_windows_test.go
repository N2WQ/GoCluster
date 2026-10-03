//go:build windows

package peerdiag

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"dxcluster/internal/logutil"
	"golang.org/x/sys/windows"
)

func TestV15WindowsWTF8ByteCount(t *testing.T) {
	for _, vector := range [][]uint16{{}, {'a', 0, 'b'}, {0x7f, 0x80, 0x7ff, 0x800, 0xffff}, {0xd800}, {0xdc00}, {0xd800, 0xdc00}, {0xdbff, 0xdfff}, {0xd800, 'a', 0xdc00}, {0xdc00, 0xd800}} {
		if got, want := helperWTF8Bytes(vector), uint64(len(syscall.UTF16ToString(vector))); got != want {
			t.Fatalf("%x: byte count=%d pinned syscall=%d", vector, got, want)
		}
	}
}

func TestV15WindowsCwdAdmissionBoundary(t *testing.T) {
	t.Setenv("SYSTEMROOT", `C:\Windows`)
	// Pinned Windows/amd64 fixed owners plus32 environment bytes leave
	// 452960 bytes after two200-byte options: 7077 cwd bytes fit,7078 do not.
	// These literals protect the independent source inventory at its boundary.
	if !helperOptionsFit(200, 200, 7077) || helperOptionsFit(200, 200, 7078) || helperOptionsFit(200, 200, 8000) {
		t.Fatal("legacy absolute-mkdir cwd backing was not admitted at64 bytes/byte")
	}
}

func TestV15WindowsLegacyParentPathCharge(t *testing.T) {
	before := helperLongPaths
	defer func() { helperLongPaths = before }()
	helperLongPaths = false
	// Parent fixed/control594312 + two100-byte executable and three100-byte
	// sibling terms + native229376 leaves224388 bytes for five environment
	// copies. The next byte crosses the unchanged one-MiB ceiling.
	if !parentLaunchFits(100, 100, 44877) || parentLaunchFits(100, 100, 44878) {
		t.Fatal("legacy devnull/native workspace was not charged before launch")
	}
	helperLongPaths = true
	if !parentLaunchFits(100, 100, 44878) {
		t.Fatal("legacy reservation narrowed modern Windows admission")
	}
}

func TestV15WindowsNativePathAdmission(t *testing.T) {
	realCwd, realFull := diagnosticGetCurrentDirectory, diagnosticGetFullPathName
	defer func() { diagnosticGetCurrentDirectory, diagnosticGetFullPathName = realCwd, realFull }()
	for _, size := range []uint32{0, math.MaxUint32, nativePathExpansionReservation} {
		calls := 0
		diagnosticGetCurrentDirectory = func(n uint32, p *uint16) (uint32, error) {
			calls++
			if n != 0 || p != nil {
				t.Fatal("oversized cwd allocated/read before admission")
			}
			return size, nil
		}
		if _, err := helperCurrentDirectoryBytes(0, 0); !errors.Is(err, errNativePathBudget) || calls != 1 {
			t.Fatalf("size=%d calls=%d err=%v", size, calls, err)
		}
	}
	diagnosticGetCurrentDirectory = func(uint32, *uint16) (uint32, error) { return math.MaxUint32, nil }
	if allocations := testing.AllocsPerRun(100, func() { _, _ = helperCurrentDirectoryBytes(0, 0) }); allocations != 0 {
		t.Fatal("rejected native size allocated", allocations)
	}
	diagnosticGetCurrentDirectory = func(n uint32, p *uint16) (uint32, error) {
		if n == 0 {
			return 4, nil
		}
		return n + 1, nil
	}
	if _, err := helperCurrentDirectoryBytes(0, 0); !errors.Is(err, errNativePathBudget) {
		t.Fatal("cwd growth retried instead of refusing", err)
	}
	diagnosticGetCurrentDirectory = func(uint32, *uint16) (uint32, error) { return 0, syscall.ERROR_ACCESS_DENIED }
	if _, err := helperCurrentDirectoryBytes(0, 0); !errors.Is(err, syscall.ERROR_ACCESS_DENIED) {
		t.Fatal("native cwd error changed", err)
	}
	fullCalls := 0
	diagnosticGetFullPathName = func(_ *uint16, n uint32, _ *uint16, _ **uint16) (uint32, error) {
		fullCalls++
		if n != 0 {
			t.Fatal("oversized full path allocated/read before admission")
		}
		return math.MaxUint32, nil
	}
	if _, err := helperBoundedFullPath("relative", ""); !errors.Is(err, errNativePathBudget) || fullCalls != 1 {
		t.Fatal("native full path declaration bypassed bound", fullCalls, err)
	}
	diagnosticGetFullPathName = func(_ *uint16, n uint32, _ *uint16, _ **uint16) (uint32, error) {
		if n == 0 {
			return 4, nil
		}
		return n, nil
	}
	if _, err := helperBoundedFullPath("relative", ""); !errors.Is(err, errNativePathBudget) {
		t.Fatal("native full path growth retried", err)
	}
}

func TestV15WindowsExecutableBound(t *testing.T) {
	fixed := unsafe.Sizeof(Service{}) + unsafe.Sizeof(Mailbox{}) + QueueSize*RecordBytes + unsafe.Sizeof(helperProcess{}) + 2*RecordBytes + ackBytes
	acquisition := unsafe.Sizeof([executableNativeUnits]uint16{}) + 3*executableNativeUnits
	if fixed+processMetadataReservation+acquisition > parentReservation {
		t.Fatal("executable native/conversion phase exceeds parent reservation", fixed, acquisition)
	}
	want, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := helperExecutablePath(0); err != nil || got != want {
		t.Fatal("native executable differs from pinned Go", got, want, err)
	}
	real := diagnosticGetModuleFileName
	defer func() { diagnosticGetModuleFileName = real }()
	for _, n := range []uint32{0, 64 << 10, math.MaxUint32} {
		diagnosticGetModuleFileName = func(_ windows.Handle, _ *uint16, size uint32) (uint32, error) {
			if size != 64<<10 {
				t.Fatal("module filename grew native storage", size)
			}
			return n, nil
		}
		if _, err := helperExecutablePath(0); !errors.Is(err, errNativePathBudget) {
			t.Fatal("invalid/truncated module name accepted", n, err)
		}
	}
	diagnosticGetModuleFileName = func(_ windows.Handle, output *uint16, _ uint32) (uint32, error) {
		copy(unsafe.Slice(output, 3), []uint16{'a', 'b', 0})
		return 2, nil
	}
	if _, err := helperExecutablePath(math.MaxInt); !errors.Is(err, errNativePathBudget) {
		t.Fatal("conversion proceeded without launch admission", err)
	}
	diagnosticGetModuleFileName = func(windows.Handle, *uint16, uint32) (uint32, error) { return 0, syscall.ERROR_ACCESS_DENIED }
	if _, err := helperExecutablePath(0); !errors.Is(err, syscall.ERROR_ACCESS_DENIED) {
		t.Fatal(err)
	}
}

func TestV15WindowsPreparedPathParity(t *testing.T) {
	before := helperLongPaths
	defer func() { helperLongPaths = before }()
	for _, input := range []string{"", ".", `C:\`, `C:relative`, `\relative`, `\\server\share\item`, `\\?\C:\literal. `, `\\.\NUL`, "NUL", "trailing. ", "café/資料", string([]byte{0xed, 0xa0, 0x80})} {
		helperLongPaths = true
		if got, err := helperOSPath(input); err != nil || got != input {
			t.Fatalf("modern OS spelling changed %q to %q/%v", input, got, err)
		}
	}
	dir := t.TempDir()
	t.Chdir(dir)
	helperLongPaths = false
	for _, input := range []string{".", "plain", "trailing.", "trailing ", `child\..\kept`, "NUL", `\\.\NUL`} {
		beforeActive, beforeArchive := logutil.DailyActivePath(input), logutil.DailyArchivePath(input, time.Unix(0, 0))
		path, err := helperOSPath(input)
		if err != nil {
			t.Fatal(input, err)
		}
		if logutil.DailyActivePath(input) != beforeActive || logutil.DailyArchivePath(input, time.Unix(0, 0)) != beforeArchive {
			t.Fatal("logical name changed")
		}
		if input == "." && (beforeActive != "current.log" || filepath.Base(path) != filepath.Base(dir)) {
			t.Fatal("dot naming or OS target changed", beforeActive, path)
		}
		// Compare real native file outcomes for reserved and trailing-dot/space
		// spellings; successful writes are observed through the original name.
		if input == "plain" || strings.HasPrefix(input, "trailing") {
			if err := os.WriteFile(input, []byte("kept"), 0o600); err != nil {
				t.Fatal(err)
			}
			if got, err := os.ReadFile(path); err != nil || string(got) != "kept" {
				t.Fatal("prepared path changed real file target", input, path, string(got), err)
			}
		}
		if input == "NUL" || input == `\\.\NUL` {
			old, oldErr := os.OpenFile(input, os.O_WRONLY, 0)
			next, nextErr := os.OpenFile(path, os.O_WRONLY, 0)
			if old != nil {
				_ = old.Close()
			}
			if next != nil {
				_ = next.Close()
			}
			if (oldErr == nil) != (nextErr == nil) {
				t.Fatal("reserved/device open changed", input, oldErr, nextErr)
			}
		}
	}
	if got, err := helperOSPath(""); err != nil || got != "" {
		t.Fatal("empty path became cwd", got, err)
	}
	if err := (&helperSink{}).diagnosticMkdirAll("", 0o755); err == nil {
		t.Fatal("empty directory failure disappeared")
	}
	if _, err := helperLegacyOSPath("relative", math.MaxUint64); !errors.Is(err, errNativePathBudget) {
		t.Fatal("cwd arithmetic overflow", err)
	}
	for _, literal := range []string{`\\?\C:\literal. `, `\??\C:\literal. `} {
		if got, err := helperLegacyOSPath(literal, 0); err != nil || got != literal {
			t.Fatal("legacy extended spelling changed", got, err)
		}
	}
	long := filepath.Join(dir, strings.Repeat("segment\\", 40), "kept")
	want, err := filepath.Abs(long)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := helperLegacyOSPath(long, uint64(len(dir))); err != nil || got != `\\?\`+want {
		t.Fatal("legacy long path prefix differs", got, want, err)
	}
	unc := `\\server\share\` + strings.Repeat("segment\\", 40) + "kept"
	if got, err := helperLegacyOSPath(unc, 0); err != nil || got != `\\?\UNC\`+unc[2:] {
		t.Fatal("legacy UNC path prefix differs", got, err)
	}
	s := helperSink{options: Options{Enabled: true, Directory: "."}}
	if result := s.write(testEvent(1, time.Unix(0, 0), Diagnostic, "dot-name")); result != ackWritten {
		t.Fatal("legacy dot write failed", result)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	if value, err := os.ReadFile("current.log"); err != nil || !strings.Contains(string(value), "dot-name") {
		t.Fatal("OS preparation changed active-log naming", string(value), err)
	}
}
