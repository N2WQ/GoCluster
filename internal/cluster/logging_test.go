package cluster

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLogFileNameForDate(t *testing.T) {
	when := time.Date(2026, time.January, 22, 12, 0, 0, 0, time.UTC)
	if got := logFileNameForDate(when); got != "22-Jan-2026.log" {
		t.Fatalf("expected log filename to be 22-Jan-2026.log, got %q", got)
	}
}

func TestActiveLogFileNameForDir(t *testing.T) {
	if got := activeLogFileNameForDir(filepath.Join("data", "logs", "system")); got != "system.log" {
		t.Fatalf("expected active filename system.log, got %q", got)
	}
	if got := activeLogFileNameForDir(filepath.Join("data", "logs", "propagation")); got != "propagation.log" {
		t.Fatalf("expected active filename propagation.log, got %q", got)
	}
}

func TestParseLogFileDate(t *testing.T) {
	parsed, ok := parseLogFileDate("22-Jan-2026.log")
	if !ok {
		t.Fatalf("expected parse to succeed")
	}
	if parsed.Year() != 2026 || parsed.Month() != time.January || parsed.Day() != 22 {
		t.Fatalf("unexpected parsed date: %s", parsed.Format(time.RFC3339))
	}
	if _, ok := parseLogFileDate("notes.txt"); ok {
		t.Fatalf("expected non-log file to be rejected")
	}
	if _, ok := parseLogFileDate("system.log"); ok {
		t.Fatalf("expected active log file name to be rejected")
	}
}

func TestCleanupOldLogs(t *testing.T) {
	dir := t.TempDir()
	files := []string{
		"20-Jan-2026.log",
		"21-Jan-2026.log",
		"22-Jan-2026.log",
		"system.log",
		"notes.txt",
	}
	for _, name := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	now := time.Date(2026, time.January, 22, 12, 0, 0, 0, time.UTC)
	if err := cleanupOldLogs(dir, now, 2); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	expectMissing := []string{"20-Jan-2026.log"}
	for _, name := range expectMissing {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			t.Fatalf("expected %s to be removed", name)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat %s: %v", name, err)
		}
	}
	expectPresent := []string{"21-Jan-2026.log", "22-Jan-2026.log", "system.log", "notes.txt"}
	for _, name := range expectPresent {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected %s to remain: %v", name, err)
		}
	}
}

func TestDailyFileSinkWritesStableActiveFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	sink, err := newDailyFileSink(dir, 1)
	if err != nil {
		t.Fatalf("newDailyFileSink: %v", err)
	}
	defer sink.Close()

	day := time.Date(2026, time.January, 22, 12, 0, 0, 0, time.UTC)
	sink.WriteLine("first", day)

	activePath := filepath.Join(dir, "system.log")
	data, err := os.ReadFile(activePath)
	if err != nil {
		t.Fatalf("read active log: %v", err)
	}
	if !strings.Contains(string(data), "first") {
		t.Fatalf("expected active log to contain first line, got %q", data)
	}
	if _, err := os.Stat(filepath.Join(dir, "22-Jan-2026.log")); !os.IsNotExist(err) {
		t.Fatalf("did not expect archive before rotation, stat err=%v", err)
	}
}

func TestDailyFileSinkRotateHook(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "propagation")
	sink, err := newDailyFileSink(dir, 1)
	if err != nil {
		t.Fatalf("newDailyFileSink: %v", err)
	}
	defer sink.Close()
	sink.cleanupFn = func(string, time.Time, int) error { return nil }

	var gotPrevDate time.Time
	var gotPrevPath string
	var gotNewPath string
	hookDone := make(chan struct{})
	var hookOnce sync.Once
	sink.SetRotateHook(func(prevDate time.Time, prevPath, newPath string) {
		gotPrevDate = prevDate
		gotPrevPath = prevPath
		gotNewPath = newPath
		hookOnce.Do(func() { close(hookDone) })
	})

	day1 := time.Date(2026, time.January, 22, 12, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)

	sink.WriteLine("first", day1)
	sink.WriteLine("second", day2)

	select {
	case <-hookDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("rotate hook did not complete")
	}
	if gotPrevDate.IsZero() {
		t.Fatalf("expected rotate hook to capture previous date")
	}
	if gotPrevDate.Year() != day1.Year() || gotPrevDate.Month() != day1.Month() || gotPrevDate.Day() != day1.Day() {
		t.Fatalf("unexpected prev date: %s", gotPrevDate.Format(time.RFC3339))
	}
	if gotPrevPath == "" || gotNewPath == "" {
		t.Fatalf("expected prev/new log paths to be set")
	}
	if filepath.Base(gotPrevPath) != "22-Jan-2026.log" {
		t.Fatalf("unexpected prev log path: %s", gotPrevPath)
	}
	if filepath.Base(gotNewPath) != "propagation.log" {
		t.Fatalf("unexpected new log path: %s", gotNewPath)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	archiveData, err := os.ReadFile(filepath.Join(dir, "22-Jan-2026.log"))
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if !strings.Contains(string(archiveData), "first") {
		t.Fatalf("expected archive to contain first line, got %q", archiveData)
	}
	activeData, err := os.ReadFile(filepath.Join(dir, "propagation.log"))
	if err != nil {
		t.Fatalf("read active: %v", err)
	}
	if strings.Contains(string(activeData), "first") || !strings.Contains(string(activeData), "second") {
		t.Fatalf("expected active to contain only new-day line, got %q", activeData)
	}
}

func TestDailyFileSinkCleanupAsync(t *testing.T) {
	dir := t.TempDir()
	sink, err := newDailyFileSink(dir, 1)
	if err != nil {
		t.Fatalf("newDailyFileSink: %v", err)
	}
	defer sink.Close()

	cleanupStarted := make(chan struct{})
	cleanupBlock := make(chan struct{})
	sink.cleanupFn = func(dir string, now time.Time, retentionDays int) error {
		close(cleanupStarted)
		<-cleanupBlock
		return nil
	}

	done := make(chan struct{})
	go func() {
		sink.WriteLine("line", time.Now().UTC())
		close(done)
	}()

	select {
	case <-cleanupStarted:
	case <-time.After(2 * time.Second):
		t.Fatalf("cleanup did not start")
	}

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("WriteLine blocked on cleanup; expected async cleanup")
	}

	close(cleanupBlock)
}

func TestRotateHookLoggingDoesNotDeadlock(t *testing.T) {
	dir := t.TempDir()
	sink, err := newDailyFileSink(dir, 1)
	if err != nil {
		t.Fatalf("newDailyFileSink: %v", err)
	}
	defer sink.Close()

	fanout := newLogFanout(nil, sink)
	logger := log.New(fanout, "", 0)

	now := time.Now().UTC()
	sink.WriteLine("prime", now)

	// Force the next log write to rotate without relying on wall-clock midnight.
	sink.mu.Lock()
	sink.currentDate = dateOnly(now.Add(-24 * time.Hour)).Format(logDateKeyLayout)
	sink.mu.Unlock()

	hookDone := make(chan struct{})
	var hookOnce sync.Once
	sink.SetRotateHook(func(prevDate time.Time, prevPath, newPath string) {
		logger.Printf("rotate hook for %s", prevDate.Format(time.RFC3339))
		hookOnce.Do(func() { close(hookDone) })
	})

	done := make(chan struct{})
	go func() {
		logger.Print("trigger rotation")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("logger.Print deadlocked during rotate hook logging")
	}
	select {
	case <-hookDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("rotate hook did not complete")
	}
}

func TestDailyFileSinkAdoptsCurrentLegacyLog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	day := time.Date(2026, time.January, 22, 12, 0, 0, 0, time.UTC)
	legacyPath := filepath.Join(dir, "22-Jan-2026.log")
	if err := os.WriteFile(legacyPath, []byte("legacy\n"), 0644); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	sink, err := newDailyFileSink(dir, 365)
	if err != nil {
		t.Fatalf("newDailyFileSink: %v", err)
	}
	defer sink.Close()

	sink.WriteLine("current", day)

	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("expected legacy current-day file to be adopted, stat err=%v", err)
	}
	activeData, err := os.ReadFile(filepath.Join(dir, "system.log"))
	if err != nil {
		t.Fatalf("read active: %v", err)
	}
	if !strings.Contains(string(activeData), "legacy") || !strings.Contains(string(activeData), "current") {
		t.Fatalf("expected active to contain adopted and current lines, got %q", activeData)
	}
}

func TestDailyFileSinkArchivesStaleActiveOnStartup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	activePath := filepath.Join(dir, "system.log")
	if err := os.WriteFile(activePath, []byte("2026/01/22 23:59:59 stale\n"), 0644); err != nil {
		t.Fatalf("write active: %v", err)
	}
	sink, err := newDailyFileSink(dir, 1)
	if err != nil {
		t.Fatalf("newDailyFileSink: %v", err)
	}
	defer sink.Close()

	sink.WriteLine("fresh", time.Date(2026, time.January, 23, 0, 0, 1, 0, time.UTC))

	archiveData, err := os.ReadFile(filepath.Join(dir, "22-Jan-2026.log"))
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if !strings.Contains(string(archiveData), "stale") {
		t.Fatalf("expected archive to contain stale line, got %q", archiveData)
	}
	activeData, err := os.ReadFile(activePath)
	if err != nil {
		t.Fatalf("read active: %v", err)
	}
	if strings.Contains(string(activeData), "stale") || !strings.Contains(string(activeData), "fresh") {
		t.Fatalf("expected active to contain only fresh line, got %q", activeData)
	}
}

func TestDailyFileSinkArchiveCollisionMergesWithoutOverwrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	sink, err := newDailyFileSink(dir, 1)
	if err != nil {
		t.Fatalf("newDailyFileSink: %v", err)
	}
	defer sink.Close()

	day1 := time.Date(2026, time.January, 22, 12, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	sink.WriteLine("from-active", day1)
	archivePath := filepath.Join(dir, "22-Jan-2026.log")
	if err := os.WriteFile(archivePath, []byte("existing\n"), 0644); err != nil {
		t.Fatalf("write archive: %v", err)
	}

	sink.WriteLine("new-day", day2)
	if err := sink.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	archiveData, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if !strings.Contains(string(archiveData), "existing") || !strings.Contains(string(archiveData), "from-active") {
		t.Fatalf("expected archive merge without overwrite, got %q", archiveData)
	}
	activeData, err := os.ReadFile(filepath.Join(dir, "system.log"))
	if err != nil {
		t.Fatalf("read active: %v", err)
	}
	if strings.Contains(string(activeData), "from-active") || !strings.Contains(string(activeData), "new-day") {
		t.Fatalf("expected active to contain only new-day line, got %q", activeData)
	}
}
