//go:build windows

package peerdiag

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"dxcluster/internal/logutil"
	"golang.org/x/sys/windows"
)

func TestV15WindowsMetadataSurrogateBacking(t *testing.T) {
	real := diagnosticGetFullPathName
	defer func() { diagnosticGetFullPathName = real }()
	// The returned rune has four bytes but UTF16ToString owns six capacity
	// bytes. This exercises both exact-length refusal and admitted overcapacity.
	diagnosticGetFullPathName = func(_ *uint16, n uint32, output *uint16, _ **uint16) (uint32, error) {
		if n == 0 {
			return 3, nil
		}
		copy(unsafe.Slice(output, n), []uint16{0xd800, 0xdc00, 0})
		return 2, nil
	}
	if _, err := helperBoundedFullPathWithin("x", "", 3); !errors.Is(err, errNativePathBudget) {
		t.Fatal("result exceeded retained byte credit", err)
	}
	if got, err := helperBoundedFullPathWithin("x", "", 4); err != nil || got != "\U00010000" {
		t.Fatal("paired-surrogate output changed", got, err)
	}
}

func TestV15WindowsMetadataBoundaryOnly(t *testing.T) {
	var sink helperSink
	real, modern := diagnosticGetFullPathName, helperLongPaths
	defer func() { diagnosticGetFullPathName, helperLongPaths = real, modern }()
	helperLongPaths = true
	diagnosticGetFullPathName = func(*uint16, uint32, *uint16, **uint16) (uint32, error) {
		t.Fatal("nonmetadata operation reached metadata native preparation")
		return 0, syscall.ERROR_ACCESS_DENIED
	}
	for _, path := range []string{"", `C:\literal. `, `\\server\share\item`, `\\?\C:\literal. `, `\??\C:\literal. `, `\\.\NUL`} {
		if got, err := helperOperationPath(path); err != nil || got != path {
			t.Fatal("empty/absolute/device spelling changed", path, got, err)
		}
	}
	file, err := diagnosticOpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal("parent NUL opener changed", err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	t.Chdir(dir)
	file, err = diagnosticOpenFile("first", os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	if err = diagnosticRename("first", "second"); err != nil {
		t.Fatal(err)
	}
	if err = diagnosticRemove("second"); err != nil {
		t.Fatal(err)
	}
	calls := 0
	diagnosticGetFullPathName = func(path *uint16, n uint32, output *uint16, final **uint16) (uint32, error) {
		calls++
		return real(path, n, output, final)
	}
	if _, err = sink.diagnosticStat("."); err != nil || calls != 0 {
		t.Fatal("modern Stat unexpectedly resolved a native path", calls, err)
	}
	calls = 0
	if err = sink.diagnosticMkdirAll("child", 0o700); err != nil || calls != 0 {
		t.Fatal("modern MkdirAll unexpectedly resolved a native path", calls, err)
	}
}

func TestV15WindowsOwnedFilesystemTargetParity(t *testing.T) {
	var sink helperSink
	actualName := captureMetadataName(t)
	modern := helperLongPaths
	defer func() { helperLongPaths = modern }()
	helperLongPaths = true
	dir := t.TempDir()
	t.Chdir(dir)
	for _, name := range []string{"kept", "trailing", "café資料", "\U00010000"} {
		if err := os.WriteFile(name, []byte("native-target:"+name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir("child", 0o700); err != nil {
		t.Fatal(err)
	}
	volume := filepath.VolumeName(dir)
	paths := []string{".", "kept", `child\..\kept`, "trailing.", "trailing ", "café資料", "\U00010000", volume + "kept", strings.TrimPrefix(filepath.Join(dir, "kept"), volume), "missing", "NUL", `\\.\NUL`, ""}
	for _, path := range paths {
		want, wantErr := os.Stat(path)
		got, gotErr := sink.diagnosticStat(path)
		if (gotErr == nil) != (wantErr == nil) || errors.Is(gotErr, os.ErrNotExist) != errors.Is(wantErr, os.ErrNotExist) {
			t.Fatal("metadata outcome changed", path, gotErr, wantErr)
		}
		if gotErr == nil && (got.IsDir() != want.IsDir() || got.Size() != want.Size() || !got.ModTime().Equal(want.ModTime())) {
			t.Fatal("consumed metadata changed", path, got, want)
		}
		if wantErr == nil && path != "NUL" && path != `\\.\NUL` && !sameNativeIdentity(t, *actualName, path) {
			t.Fatal("metadata selected another native target", path)
		}
	}
	for _, path := range []string{`created\deep`, `created\..\other`, `absent-parent\..\lexical-dir`, "trailing-directory. ", "kept", ""} { //nolint:misspell // This Windows parent-traversal path is a lexical parity fixture, not prose.
		wantErr := os.MkdirAll(path, 0o700)
		gotErr := sink.diagnosticMkdirAll(path, 0o700)
		if (gotErr == nil) != (wantErr == nil) || errors.Is(gotErr, syscall.ENOTDIR) != errors.Is(wantErr, syscall.ENOTDIR) {
			t.Fatal("mkdir outcome changed", path, gotErr, wantErr)
		}
	}
	sink.options = Options{Enabled: true, Directory: "."}
	if active := logutil.DailyActivePath(sink.options.Directory); active != "current.log" {
		t.Fatal("logical active name changed", active)
	}
	if ack := sink.write(testEvent(1, time.Unix(0, 0), Diagnostic, "metadata-boundary")); ack != ackWritten {
		t.Fatal("relative-directory diagnostic failed", ack)
	}
	if err := sink.close(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile("current.log"); err != nil || !strings.Contains(string(data), "metadata-boundary") {
		t.Fatal("logical log target changed", string(data), err)
	}
}

func TestV15WindowsMetadataJunctionParentParity(t *testing.T) {
	var sink helperSink
	actualName := captureMetadataName(t)
	dir := t.TempDir()
	work, target := filepath.Join(dir, "work"), filepath.Join(dir, "target", "nested")
	for _, path := range []string{work, target} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(work, "alias")
	for _, path := range []string{link, target} {
		rel, err := filepath.Rel(dir, path)
		if err != nil || !filepath.IsLocal(rel) || rel == "." {
			t.Fatal("junction fixture escaped owned root", path, err)
		}
	}
	cmd := exec.CommandContext(t.Context(), "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", `$ErrorActionPreference = 'Stop'
New-Item -ItemType Junction -Path $env:GOCLUSTER_TEST_JUNCTION_LINK -Target $env:GOCLUSTER_TEST_JUNCTION_TARGET | Out-Null`)
	cmd.Env = append(os.Environ(), "GOCLUSTER_TEST_JUNCTION_LINK="+link, "GOCLUSTER_TEST_JUNCTION_TARGET="+target)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create actual junction: %v %s", err, output)
	}
	defer func() {
		if err := os.Remove(link); err != nil {
			t.Error(err)
		}
	}()
	for path, data := range map[string]string{filepath.Join(work, "marker"): "lexical", filepath.Join(dir, "target", "marker"): "resolved-junction-parent"} {
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(work)
	for _, path := range []string{`alias\..\marker`, `alias\.`} {
		want, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		got, err := sink.diagnosticStat(path)
		if err != nil || got.Size() != want.Size() || got.IsDir() != want.IsDir() || !sameNativeIdentity(t, *actualName, path) {
			t.Fatal("metadata changed junction/dot-dot native target", path, got, want, err)
		}
	}
	const created = `alias\..\created`
	if err := os.MkdirAll(created, 0o700); err != nil {
		t.Fatal("original junction-parent mkdir", err)
	}
	if err := sink.diagnosticMkdirAll(created, 0o700); err != nil {
		t.Fatal("prepared junction-parent mkdir", err)
	}
	want, err := os.Stat(created)
	if err != nil {
		t.Fatal(err)
	}
	got, err := sink.diagnosticStat(created)
	if err != nil || !sameNativeIdentity(t, *actualName, created) {
		t.Fatal("mkdir selected another junction-parent target", got, want, err)
	}
}

// Capture the actual name passed to the native API, then open that name and
// the control independently to compare real file IDs. The custom fast-path
// FileInfo deliberately does not implement os's private SameFile protocol.
func captureMetadataName(t *testing.T) *string {
	t.Helper()
	original := diagnosticGetAttributes
	var name string
	diagnosticGetAttributes = func(p *uint16, level uint32, data *byte) error {
		name = windows.UTF16PtrToString(p)
		return original(p, level, data)
	}
	t.Cleanup(func() { diagnosticGetAttributes = original })
	return &name
}

func sameNativeIdentity(t *testing.T, actual, control string) bool {
	t.Helper()
	first, err := os.Open(actual)
	if err != nil {
		t.Fatal("open captured native name", actual, err)
	}
	defer first.Close()
	second, err := os.Open(control)
	if err != nil {
		t.Fatal("open independent control", control, err)
	}
	defer second.Close()
	a, err := first.Stat()
	if err != nil {
		t.Fatal(err)
	}
	b, err := second.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return os.SameFile(a, b)
}
