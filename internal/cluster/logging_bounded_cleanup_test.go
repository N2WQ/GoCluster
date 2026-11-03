package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/internal/asynclog"
)

func TestDailyFileSinkStalledCleanupHasOneWorker(t *testing.T) {
	sink, err := newDailyFileSink(t.TempDir(), 2)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var calls atomic.Int32
	sink.cleanupFn = func(string, time.Time, int) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return nil
	}
	day := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	h, err := asynclog.StartSingle(28, func() asynclog.Sink { return &legacyAsyncFile{daily: sink} })
	if err != nil {
		t.Fatal(err)
	}
	if !h.EmitString(day, 0, "first") {
		t.Fatal("first record refused")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup did not start")
	}
	for i := 1; i <= 3; i++ {
		if !h.EmitString(day.AddDate(0, 0, i), 0, "next") {
			t.Fatal("producer blocked or record refused")
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("started %d cleanup workers, want one", got)
	}
	h.Stop()
	select {
	case <-h.Done():
		t.Fatal("sink retired before cleanup completed")
	case <-time.After(50 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-h.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("sink did not retire after cleanup returned")
	}
}

func TestCleanupOldLogsScansBeyondOneBatch(t *testing.T) {
	dir := t.TempDir()
	for year := 1800; year < 2000; year++ {
		name := fmt.Sprintf("01-Jan-%04d.log", year)
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := cleanupOldLogs(dir, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 2); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("retention left %d expired archives", len(entries))
	}
}

func TestCleanupOldLogsUsesCleanedDirectoryForScanAndRemoval(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"logs", "real/child", "real/logs"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(filepath.Join(root, "real", "child"), link); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	archive := "01-Jan-2000.log"
	for _, dir := range []string{"logs", "real/logs"} {
		if err := os.WriteFile(filepath.Join(root, dir, archive), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	unclean := link + string(os.PathSeparator) + ".." + string(os.PathSeparator) + "logs"
	if err := cleanupOldLogs(unclean, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "logs", archive)); !os.IsNotExist(err) {
		t.Fatalf("archive in cleaned directory was not removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "real", "logs", archive)); err != nil {
		t.Fatalf("archive in symlink-resolved directory was changed: %v", err)
	}
}

func TestDailyFileSinkStalledRotationHookHasOneWorker(t *testing.T) {
	sink, err := newDailyFileSink(t.TempDir(), 2)
	if err != nil {
		t.Fatal(err)
	}
	sink.cleanupFn = func(string, time.Time, int) error { return nil }
	sink.workerHook = true
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var calls atomic.Int32
	sink.SetRotateHook(func(time.Time, string, string) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
	})
	day := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	h, err := asynclog.StartSingle(27, func() asynclog.Sink { return &legacyAsyncFile{daily: sink} })
	if err != nil {
		t.Fatal(err)
	}
	if !h.EmitString(day, 0, "first") || !h.EmitString(day.AddDate(0, 0, 1), 0, "second") {
		t.Fatal("records refused")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("rotation hook did not start")
	}
	if !h.EmitString(day.AddDate(0, 0, 2), 0, "third") || !h.EmitString(day.AddDate(0, 0, 3), 0, "fourth") {
		t.Fatal("later records refused")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("started %d rotation hooks, want one", got)
	}
	h.Stop()
	select {
	case <-h.Done():
		t.Fatal("sink retired before rotation hook completed")
	case <-time.After(50 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-h.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("sink did not retire after rotation hook returned")
	}
}

func TestParseLogDateFromNewlineHeavyTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "active.log")
	data := "2026/01/22 23:59:59 last dated line\n" + strings.Repeat("\n", activeLogTailBytes-64)
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	got, ok := parseLogDateFromTail(path, int64(len(data)))
	want := time.Date(2026, 1, 22, 0, 0, 0, 0, time.UTC)
	if !ok || !got.Equal(want) {
		t.Fatalf("date = %v, %v; want %v, true", got, ok, want)
	}
}
