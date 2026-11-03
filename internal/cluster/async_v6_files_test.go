package cluster

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"dxcluster/internal/asynclog"
	"dxcluster/internal/logfiles"
)

func TestLegacyAsyncFileUsesDailyNamesAndRotation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "peer_connections")
	s := newLegacyAsyncFile(logfiles.DestinationConfig{Kind: logfiles.Daily, Path: dir, RetentionDays: 7})
	first := time.Date(2026, time.November, 1, 23, 59, 0, 0, time.UTC)
	s.Write(first, 0, []byte("event=peer_connection action=connected"))
	s.Write(first.Add(time.Minute), 0, []byte("event=peer_connection action=closed"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	archived, err := os.ReadFile(filepath.Join(dir, "01-Nov-2026.log"))
	if err != nil || !strings.Contains(string(archived), "action=connected") {
		t.Fatalf("previous day archive: %q, %v", archived, err)
	}
	active, err := os.ReadFile(filepath.Join(dir, "peer_connections.log"))
	if err != nil || !strings.Contains(string(active), "action=closed") {
		t.Fatalf("current log: %q, %v", active, err)
	}
}

func TestLegacyAsyncOverlongFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "peering_overlong.log")
	s := newLegacyAsyncFile(logfiles.DestinationConfig{Kind: logfiles.Overlong, Path: path})
	s.Write(time.Date(2026, time.November, 2, 0, 0, 0, 0, time.UTC), 0, []byte("host=peer reason=too_long"))
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "2026-11-02T00:00:00Z host=peer reason=too_long\n" {
		t.Fatalf("overlong sample: %q, %v", data, err)
	}
}

func TestLegacyOverlongCloseFailureRetainsOneOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "overlong.log")
	s := newLegacyAsyncFile(logfiles.DestinationConfig{Kind: logfiles.Overlong, Path: path})
	failure := errors.New("close failed")
	var opens int
	s.openFile = func(path string, flags int, mode os.FileMode) (*os.File, error) {
		opens++
		return openSafeLog(path, flags, mode)
	}
	s.closeFile = func(*os.File) error { return failure }
	if got := s.Write(time.Now(), 0, []byte("first")); got != asynclog.NotWritten {
		t.Fatalf("write with uncertain close = %v", got)
	}
	if s.uncertain == nil {
		t.Fatal("uncertain file was not retained")
	}
	defer s.uncertain.Close()
	s.Write(time.Now(), 0, []byte("second"))
	if opens != 1 || !errors.Is(s.Close(), failure) {
		t.Fatalf("opens = %d, close error = %v", opens, s.Close())
	}
}

func TestLegacyAsyncOverlongDoesNotGrowWhenRotationFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peering_overlong.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(8 << 20); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(path+".2", 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(path+".2", "occupied"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	s := newLegacyAsyncFile(logfiles.DestinationConfig{Kind: logfiles.Overlong, Path: path})
	if s.Write(time.Now(), 0, []byte("sample")) == asynclog.Written {
		t.Fatal("write succeeded despite failed rotation")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != 8<<20 {
		t.Fatalf("active size = %v, %v", info, err)
	}
}

func TestLegacyAsyncSystemMultilineTimestampsEachLine(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	s := newLegacyAsyncFile(logfiles.DestinationConfig{Stream: streamSystemFile, Kind: logfiles.Daily, Path: dir, RetentionDays: 7})
	at := time.Date(2026, 11, 2, 12, 30, 0, 0, time.UTC)
	if got := s.Write(at, 0, []byte("first\nsecond")); got != asynclog.Written {
		t.Fatalf("write = %v", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "system.log"))
	if err != nil || string(data) != "2026/11/02 12:30:00 first\n2026/11/02 12:30:00 second\n" {
		t.Fatalf("multiline output %q: %v", data, err)
	}
}

func TestLegacyAsyncFailedOpenDoesNotSuppressLaterRecord(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "peers")
	if err := os.WriteFile(dir, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	s := newLegacyAsyncFile(logfiles.DestinationConfig{Kind: logfiles.Daily, Path: dir, RetentionDays: 7,
		ProcessorFactory: func() logfiles.Processor { return logfiles.NewPeerProcessor(time.Minute) }})
	at := time.Now().UTC()
	s.Write(at, 0, []byte("event=peer_connection action=connected"))
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	s.Write(at.Add(time.Second), 0, []byte("event=peer_connection action=connected"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "peers.log"))
	if err != nil || !strings.Contains(string(data), "action=connected") {
		t.Fatalf("record after recovery = %q, %v", data, err)
	}
}

func TestEnsureLogDirectoryCreatesParentsAndRejectsFile(t *testing.T) {
	root := t.TempDir()
	dir := root
	for range 30 {
		dir = filepath.Join(dir, "nested")
	}
	if err := ensureLogDirectory(dir); err != nil {
		t.Fatal(err)
	}
	if err := ensureLogDirectory(dir); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ensureLogDirectory(filepath.Join(file, "child")); err == nil {
		t.Fatal("accepted a file as a parent directory")
	}
}

func TestArchiveCollisionWithSameFileDoesNotGrow(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	sink, err := newDailyFileSink(dir, 7)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Now().UTC().Add(-24 * time.Hour)
	sink.WriteLine("previous", day)
	active := filepath.Join(dir, "system.log")
	archive := archiveLogPathForDate(dir, day)
	if err := os.Link(active, archive); err != nil {
		_ = sink.Close()
		t.Skipf("hard links unavailable: %v", err)
	}
	sink.WriteLine("next", day.Add(24*time.Hour))
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	oldData, err := os.ReadFile(archive)
	if err != nil || strings.Count(string(oldData), "previous") != 1 {
		t.Fatalf("archive after collision = %q, %v", oldData, err)
	}
	newData, err := os.ReadFile(active)
	if err != nil || !strings.Contains(string(newData), "next") {
		t.Fatalf("active after collision = %q, %v", newData, err)
	}
}

func TestArchiveCollisionPreservesExternalHardLink(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "system")
	sink, err := newDailyFileSink(dir, 7)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Now().UTC().Add(-24 * time.Hour)
	sink.WriteLine("disposable active", day)
	external := filepath.Join(root, "external")
	if err := os.WriteFile(external, []byte("do not change\n"), 0600); err != nil {
		t.Fatal(err)
	}
	archive := archiveLogPathForDate(dir, day)
	if err := os.Link(external, archive); err != nil {
		_ = sink.Close()
		t.Skipf("hard links unavailable: %v", err)
	}
	sink.WriteLine("next", day.Add(24*time.Hour))
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(external)
	if err != nil || string(data) != "do not change\n" {
		t.Fatalf("external link changed: %q, %v", data, err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "system.log"))
	if err != nil || !strings.Contains(string(data), "next") {
		t.Fatalf("logging did not resume: %q, %v", data, err)
	}
}

func TestArchiveCollisionWithDirectoryKeepsLogging(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	sink, err := newDailyFileSink(dir, 7)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Now().UTC().Add(-24 * time.Hour)
	sink.WriteLine("disposable active", day)
	archive := archiveLogPathForDate(dir, day)
	if err := os.Mkdir(archive, 0700); err != nil {
		t.Fatal(err)
	}
	sink.WriteLine("next", day.Add(24*time.Hour))
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(archive)
	if err != nil || !info.IsDir() {
		t.Fatalf("archive entry changed: %v, %v", info, err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "system.log"))
	if err != nil || !strings.Contains(string(data), "next") {
		t.Fatalf("logging did not resume: %q, %v", data, err)
	}
}

func TestArchiveCollisionReportsPreservedArchive(t *testing.T) {
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		t.Skip("archive link count is unavailable on this platform")
	}
	dir := filepath.Join(t.TempDir(), "propagation")
	sink, err := newDailyFileSink(dir, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	sink.workerHook = true
	day := time.Now().UTC().Add(-24 * time.Hour)
	var reported string
	sink.SetRotateHook(func(_ time.Time, previousPath, _ string) { reported = previousPath })
	sink.WriteLine("discarded", day)
	archive := archiveLogPathForDate(dir, day)
	if err := os.WriteFile(archive, []byte("existing\n"), 0600); err != nil {
		t.Fatal(err)
	}
	sink.WriteLine("next", day.Add(24*time.Hour))
	data, err := os.ReadFile(archive)
	if err != nil || string(data) != "existing\n" || reported != archive {
		t.Fatalf("archive=%q reported=%q err=%v", data, reported, err)
	}
}

func TestArchiveCollisionRetryDoesNotDuplicate(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("this case exercises Windows delete sharing")
	}
	dir := filepath.Join(t.TempDir(), "system")
	sink, err := newDailyFileSink(dir, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	day := time.Now().UTC().Add(-24 * time.Hour)
	sink.WriteLine("old active", day)
	archive := archiveLogPathForDate(dir, day)
	if err := os.WriteFile(archive, []byte("existing\n"), 0600); err != nil {
		t.Fatal(err)
	}
	held, err := os.Open(activeLogPathForDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		sink.WriteLine("retry", day.Add(24*time.Hour))
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	sink.WriteLine("next", day.Add(24*time.Hour))
	data, err := os.ReadFile(archive)
	if err != nil || string(data) != "existing\n" {
		t.Fatalf("archive changed across retries: %q, %v", data, err)
	}
	data, err = os.ReadFile(activeLogPathForDir(dir))
	if err != nil || !strings.Contains(string(data), "next") {
		t.Fatalf("logging did not resume: %q, %v", data, err)
	}
}

func TestArchiveCollisionDoesNotReportUnrelatedFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "propagation")
	sink, err := newDailyFileSink(dir, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	sink.workerHook = true
	day := time.Now().UTC().Add(-24 * time.Hour)
	var reportPath string
	sink.SetRotateHook(func(_ time.Time, previousPath, _ string) { reportPath = previousPath })
	sink.WriteLine("disposable", day)
	archive := archiveLogPathForDate(dir, day)
	if err := os.Mkdir(archive, 0700); err != nil {
		t.Fatal(err)
	}
	sink.WriteLine("next", day.Add(24*time.Hour))
	if reportPath != "" {
		t.Fatalf("unavailable archive was reported as %q", reportPath)
	}
}

func TestArchiveCollisionWithSymlinkDoesNotReportTarget(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "propagation")
	sink, err := newDailyFileSink(dir, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	sink.workerHook = true
	day := time.Now().UTC().Add(-24 * time.Hour)
	var reportPath string
	sink.SetRotateHook(func(_ time.Time, previousPath, _ string) { reportPath = previousPath })
	sink.WriteLine("disposable", day)
	target := filepath.Join(root, "unrelated.log")
	if err := os.WriteFile(target, []byte("unrelated"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, archiveLogPathForDate(dir, day)); err != nil {
		t.Skipf("symbolic links unavailable: %v", err)
	}
	sink.WriteLine("next", day.Add(24*time.Hour))
	if reportPath != "" {
		t.Fatalf("symlink archive was reported as %q", reportPath)
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "unrelated" {
		t.Fatalf("symlink target changed: %q, %v", data, err)
	}
}
