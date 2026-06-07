// File role: Owns daily file sinks and process log fanout for live runtime logs.
// Crawler notes: Start here for system-log, propagation-log, rotation hook,
// and file-only line behavior before tracing report scheduling.
// Related docs: docs/OPERATOR_GUIDE.md, data/config/README.md.
// Related tests: internal/cluster/logging_test.go, internal/cluster/main_runtime_test.go.
package cluster

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"dxcluster/config"
	"dxcluster/internal/linebuffer"
	"dxcluster/internal/logutil"
)

const (
	logTimestampLayout = "2006/01/02 15:04:05"
	logFileDateLayout  = logutil.DailyArchiveDateLayout
	logDateKeyLayout   = "2006-01-02"
	maxLogBufferBytes  = 16 * 1024
	activeLogTailBytes = 64 * 1024
)

var errPropagationLoggingDisabled = errors.New("propagation logging disabled")

type lineSink interface {
	WriteLine(line string, now time.Time)
	Close() error
}

type ioLineSink struct {
	w             io.Writer
	withTimestamp bool
}

// WriteLine writes log lines to an io.Writer with optional timestamp prefix.
// Key aspects: Adds local time prefix and always terminates with newline.
// Upstream: logFanout line dispatch.
// Downstream: io.Writer.Write.
func (s *ioLineSink) WriteLine(line string, now time.Time) {
	if s == nil || s.w == nil {
		return
	}
	if s.withTimestamp {
		line = formatLogTimestamp(now) + " " + line
	}
	if _, err := io.WriteString(s.w, line+"\n"); err != nil {
		return
	}
}

func (s *ioLineSink) Close() error {
	return nil
}

type dailyFileSink struct {
	dir           string
	retentionDays int
	currentDate   string
	currentPath   string
	file          *os.File
	lastErrorAt   time.Time
	rotateHook    logRotateHook
	cleanupFn     func(string, time.Time, int) error
	mu            sync.Mutex
}

// Purpose: Initialize a daily file sink with directory creation and cleanup.
// Key aspects: Ensures directory exists and bounds retention by date-based cleanup.
// Upstream: setupLogging.
// Downstream: os.MkdirAll and cleanupOldLogs.
func newDailyFileSink(dir string, retentionDays int) (*dailyFileSink, error) {
	trimmed := strings.TrimSpace(dir)
	if trimmed == "" {
		return nil, fmt.Errorf("log directory is empty")
	}
	if retentionDays <= 0 {
		retentionDays = 7
	}
	if err := os.MkdirAll(trimmed, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory %q: %w", trimmed, err)
	}
	sink := &dailyFileSink{
		dir:           trimmed,
		retentionDays: retentionDays,
		cleanupFn:     cleanupOldLogs,
	}
	if err := sink.cleanupFn(trimmed, time.Now().UTC(), retentionDays); err != nil {
		fmt.Fprintf(os.Stderr, "Logging: cleanup failed for %s: %v\n", trimmed, err)
	}
	return sink, nil
}

func newDailyLogSink(dir string, retentionDays int) (lineSink, error) {
	return newDailyFileSink(dir, retentionDays)
}

// WriteLine appends a timestamped line to the stable current-day log file.
// Key aspects: Archives before first new-day write and logs file errors to stderr (rate-limited).
// Upstream: logFanout line dispatch.
// Downstream: os.OpenFile and file.WriteString.
func (s *dailyFileSink) WriteLine(line string, now time.Time) {
	if s == nil {
		return
	}
	now = now.UTC()
	day := dateOnly(now)
	dateKey := day.Format(logDateKeyLayout)

	var hook logRotateHook
	var prevDate time.Time
	var prevPath string
	var newPath string

	s.mu.Lock()

	if s.file == nil || s.currentDate != dateKey {
		hook, prevDate, prevPath, newPath = s.rotateLocked(dateKey, day, now)
	}
	if s.file == nil {
		s.mu.Unlock()
		return
	}
	if _, err := s.file.WriteString(formatLogTimestamp(now) + " " + line + "\n"); err != nil {
		s.reportErrorLocked(now, fmt.Errorf("write failed: %w", err))
	}
	s.mu.Unlock()

	if hook != nil && !prevDate.IsZero() {
		prevDateCopy := prevDate
		prevPathCopy := prevPath
		newPathCopy := newPath
		// Invoke asynchronously to avoid re-entering the logger while its mutex is held.
		go hook(prevDateCopy, prevPathCopy, newPathCopy)
	}
}

// Close closes the currently open log file (if any).
// Key aspects: Safe for repeated calls and nil receivers.
// Upstream: main shutdown path.
// Downstream: os.File.Close.
func (s *dailyFileSink) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	s.currentDate = ""
	s.currentPath = ""
	return err
}

type logRotateHook func(prevDate time.Time, prevPath, newPath string)

func (s *dailyFileSink) SetRotateHook(hook logRotateHook) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.rotateHook = hook
	s.mu.Unlock()
}

func (s *dailyFileSink) rotateLocked(dateKey string, day, now time.Time) (logRotateHook, time.Time, string, string) {
	var hook logRotateHook
	var prevDate time.Time
	var prevPath string
	var newPath string
	if s.currentDate != "" && s.currentDate != dateKey {
		parsed, err := time.ParseInLocation(logDateKeyLayout, s.currentDate, time.UTC)
		if err == nil {
			prevDate = parsed
		}
		hook = s.rotateHook
	}
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		s.reportErrorLocked(now, fmt.Errorf("failed to create log directory %q: %w", s.dir, err))
		return nil, time.Time{}, "", ""
	}
	if !prevDate.IsZero() {
		archivedPath, err := s.archiveActiveLogLocked(prevDate)
		if err != nil {
			s.reportErrorLocked(now, err)
			return nil, time.Time{}, "", ""
		}
		prevPath = archivedPath
	} else if s.currentDate == "" {
		activePath := activeLogPathForDir(s.dir)
		if activeDate, ok := inferActiveLogDate(activePath); ok {
			if !dateOnly(activeDate).Equal(day) {
				archivedPath, err := s.archiveActiveLogLocked(dateOnly(activeDate))
				if err != nil {
					s.reportErrorLocked(now, err)
					return nil, time.Time{}, "", ""
				}
				prevDate = dateOnly(activeDate)
				prevPath = archivedPath
				hook = s.rotateHook
			}
		} else if err := s.adoptLegacyCurrentLogLocked(day); err != nil {
			s.reportErrorLocked(now, err)
			return nil, time.Time{}, "", ""
		}
	}

	path := activeLogPathForDir(s.dir)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		s.reportErrorLocked(now, fmt.Errorf("open failed for %s: %w", path, err))
		return nil, time.Time{}, "", ""
	}
	s.file = file
	s.currentDate = dateKey
	s.currentPath = path
	newPath = path
	if s.cleanupFn != nil {
		cleanupDir := s.dir
		cleanupRetention := s.retentionDays
		cleanupNow := now
		cleanupFn := s.cleanupFn
		// Cleanup can be slow on large directories; keep it off the hot path.
		go func() {
			if err := cleanupFn(cleanupDir, cleanupNow, cleanupRetention); err != nil {
				s.reportError(time.Now().UTC(), fmt.Errorf("cleanup failed: %w", err))
			}
		}()
	}
	return hook, prevDate, prevPath, newPath
}

func (s *dailyFileSink) archiveActiveLogLocked(date time.Time) (string, error) {
	activePath := activeLogPathForDir(s.dir)
	archivePath := archiveLogPathForDate(s.dir, date)
	info, err := os.Stat(activePath)
	if err != nil {
		if os.IsNotExist(err) {
			return archivePath, nil
		}
		return "", fmt.Errorf("stat active log %s: %w", activePath, err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("active log path is a directory: %s", activePath)
	}
	if info.Size() == 0 {
		if err := os.Remove(activePath); err != nil && !os.IsNotExist(err) {
			return "", fmt.Errorf("remove empty active log %s: %w", activePath, err)
		}
		return archivePath, nil
	}
	if _, err := os.Stat(archivePath); err != nil {
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("stat archive log %s: %w", archivePath, err)
		}
		if err := os.Rename(activePath, archivePath); err != nil {
			return "", fmt.Errorf("archive active log %s to %s: %w", activePath, archivePath, err)
		}
		return archivePath, nil
	}
	if err := appendFileAndRemove(activePath, archivePath); err != nil {
		return "", err
	}
	return archivePath, nil
}

func (s *dailyFileSink) adoptLegacyCurrentLogLocked(day time.Time) error {
	legacyPath := archiveLogPathForDate(s.dir, day)
	activePath := activeLogPathForDir(s.dir)
	if _, err := os.Stat(legacyPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat current legacy log %s: %w", legacyPath, err)
	}
	if _, err := os.Stat(activePath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat active log %s: %w", activePath, err)
	}
	if err := os.Rename(legacyPath, activePath); err != nil {
		return fmt.Errorf("adopt current legacy log %s to %s: %w", legacyPath, activePath, err)
	}
	return nil
}

func appendFileAndRemove(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open active log for archive merge %s: %w", srcPath, err)
	}

	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		_ = src.Close()
		return fmt.Errorf("open archive log for merge %s: %w", dstPath, err)
	}
	_, copyErr := io.Copy(dst, src)
	closeDstErr := dst.Close()
	closeSrcErr := src.Close()
	if copyErr != nil {
		return fmt.Errorf("merge active log %s into %s: %w", srcPath, dstPath, copyErr)
	}
	if closeDstErr != nil {
		return fmt.Errorf("close archive log %s after merge: %w", dstPath, closeDstErr)
	}
	if closeSrcErr != nil {
		return fmt.Errorf("close active log %s after merge: %w", srcPath, closeSrcErr)
	}
	if err := os.Remove(srcPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove merged active log %s: %w", srcPath, err)
	}
	return nil
}

func inferActiveLogDate(path string) (time.Time, bool) {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return time.Time{}, false
	}
	if date, ok := parseLogDateFromTail(path, info.Size()); ok {
		return date, true
	}
	return dateOnly(info.ModTime().UTC()), true
}

func parseLogDateFromTail(path string, size int64) (time.Time, bool) {
	if size <= 0 {
		return time.Time{}, false
	}
	readSize := size
	if readSize > activeLogTailBytes {
		readSize = activeLogTailBytes
	}
	file, err := os.Open(path)
	if err != nil {
		return time.Time{}, false
	}
	defer file.Close()
	buf := make([]byte, int(readSize))
	if _, err := file.ReadAt(buf, size-readSize); err != nil && !errors.Is(err, io.EOF) {
		return time.Time{}, false
	}
	lines := strings.Split(string(buf), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if len(line) < len(logTimestampLayout) {
			continue
		}
		parsed, err := time.ParseInLocation(logTimestampLayout, line[:len(logTimestampLayout)], time.UTC)
		if err == nil {
			return dateOnly(parsed.UTC()), true
		}
	}
	return time.Time{}, false
}

func (s *dailyFileSink) reportErrorLocked(now time.Time, err error) {
	if err == nil {
		return
	}
	if !s.lastErrorAt.IsZero() && now.Sub(s.lastErrorAt) < time.Minute {
		return
	}
	s.lastErrorAt = now
	fmt.Fprintf(os.Stderr, "Logging: %v\n", err)
}

func (s *dailyFileSink) reportError(now time.Time, err error) {
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	s.reportErrorLocked(now, err)
	s.mu.Unlock()
}

type logFanout struct {
	mu      sync.Mutex
	buf     []byte
	console lineSink
	file    lineSink
}

// Purpose: Create the log fanout writer for console/file duplication.
// Key aspects: Caller decides which sinks are active.
// Upstream: setupLogging.
// Downstream: log.SetOutput.
func newLogFanout(console lineSink, file lineSink) *logFanout {
	return &logFanout{
		console: console,
		file:    file,
	}
}

// Purpose: Wire logging based on config without blocking startup.
// Key aspects: Returns a fanout writer even when file logging fails.
// Upstream: main startup.
// Downstream: newDailyFileSink and log.SetOutput.
func setupLogging(cfg config.LoggingConfig, console io.Writer) (*logFanout, error) {
	fanout := newLogFanout(&ioLineSink{w: console, withTimestamp: true}, nil)
	if !cfg.Enabled {
		return fanout, nil
	}
	fileSink, err := newDailyLogSink(cfg.Dir, cfg.RetentionDays)
	if err != nil {
		return fanout, err
	}
	fanout.SetFileSink(fileSink)
	return fanout, nil
}

// newPropagationLogSink builds the file-only propagation aggregate sink.
// Keeping this separate from the system log lets report generation read one
// purpose-built daily file without duplicating path lines into console/UI logs.
func newPropagationLogSink(cfg config.PropagationLoggingConfig) (lineSink, error) {
	if !cfg.Enabled {
		return nil, errPropagationLoggingDisabled
	}
	sink, err := newDailyLogSink(cfg.Dir, cfg.RetentionDays)
	if err != nil {
		return nil, fmt.Errorf("propagation logging setup: %w", err)
	}
	return sink, nil
}

// SetConsoleSink swaps the console sink (e.g., to a UI writer).
// Key aspects: Updates the sink atomically with the line buffer.
// Upstream: main after UI initialization.
// Downstream: None.
func (f *logFanout) SetConsoleSink(writer io.Writer, withTimestamp bool) {
	if f == nil {
		return
	}
	var sink lineSink
	if writer != nil {
		sink = &ioLineSink{w: writer, withTimestamp: withTimestamp}
	}
	f.mu.Lock()
	f.console = sink
	f.mu.Unlock()
}

// SetFileSink attaches or replaces the file sink.
// Key aspects: Allows setupLogging to install a daily sink after creation.
// Upstream: setupLogging.
// Downstream: None.
func (f *logFanout) SetFileSink(sink lineSink) {
	if f == nil {
		return
	}
	f.mu.Lock()
	f.file = sink
	f.mu.Unlock()
}

type rotateHookSetter interface {
	SetRotateHook(hook logRotateHook)
}

// SetRotateHook attaches a rotate hook to the file sink if supported.
// Key aspects: No-op when file logging is disabled or sink does not support hooks.
// Upstream: main when enabling async prop report generation on rotation.
// Downstream: dailyFileSink.SetRotateHook.
func (f *logFanout) SetRotateHook(hook logRotateHook) {
	if f == nil {
		return
	}
	f.mu.Lock()
	sink := f.file
	f.mu.Unlock()
	if setter, ok := sink.(rotateHookSetter); ok {
		setter.SetRotateHook(hook)
	}
}

// Write fans out log output to console/UI and file sinks.
// Key aspects: Line-buffered with bounded internal storage.
// Upstream: log.Logger output.
// Downstream: lineSink.WriteLine.
func (f *logFanout) Write(p []byte) (int, error) {
	if f == nil {
		return len(p), nil
	}
	f.mu.Lock()
	remaining, lines := linebuffer.AppendAndExtractLines(f.buf, p, maxLogBufferBytes)
	f.buf = remaining
	console := f.console
	file := f.file
	f.mu.Unlock()

	if len(lines) == 0 {
		return len(p), nil
	}
	now := time.Now().UTC()
	for _, line := range lines {
		if console != nil {
			console.WriteLine(line, now)
		}
		if file != nil {
			file.WriteLine(line, now)
		}
	}
	return len(p), nil
}

// Close closes all sinks owned by the fanout writer.
// Key aspects: Best-effort cleanup for process shutdown.
// Upstream: main shutdown.
// Downstream: lineSink.Close.
func (f *logFanout) Close() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	console := f.console
	file := f.file
	f.mu.Unlock()

	var firstErr error
	if console != nil {
		if err := console.Close(); err != nil {
			firstErr = err
		}
	}
	if file != nil {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// WriteFileOnlyLine writes a single line only to the file sink (no console/UI output).
// Key aspects: Safe when file logging is disabled.
// Upstream: periodic background loggers that should not spam the console.
// Downstream: lineSink.WriteLine.
func (f *logFanout) WriteFileOnlyLine(line string, now time.Time) {
	if f == nil {
		return
	}
	f.mu.Lock()
	file := f.file
	f.mu.Unlock()
	if file != nil {
		file.WriteLine(line, now)
	}
}

func formatLogTimestamp(now time.Time) string {
	return now.UTC().Format(logTimestampLayout)
}

func activeLogFileNameForDir(dir string) string {
	return logutil.DailyActiveFileName(dir)
}

func activeLogPathForDir(dir string) string {
	return logutil.DailyActivePath(dir)
}

func logFileNameForDate(now time.Time) string {
	return logutil.DailyArchiveFileName(now)
}

func archiveLogPathForDate(dir string, now time.Time) string {
	return logutil.DailyArchivePath(dir, now)
}

func parseLogFileDate(name string) (time.Time, bool) {
	return logutil.ParseDailyArchiveDate(name)
}

func cleanupOldLogs(dir string, now time.Time, retentionDays int) error {
	if retentionDays <= 0 {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	cutoff := dateOnly(now.UTC()).AddDate(0, 0, -(retentionDays - 1))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		date, ok := parseLogFileDate(entry.Name())
		if !ok {
			continue
		}
		if date.Before(cutoff) {
			_ = os.Remove(filepath.Join(dir, entry.Name()))
		}
	}
	return nil
}

func dateOnly(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}
