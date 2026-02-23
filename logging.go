package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"dxcluster/config"
	"dxcluster/internal/linebuffer"
)

const (
	logTimestampLayout = "2006/01/02 15:04:05"
	logFileDateLayout  = "02-Jan-2006"
	maxLogBufferBytes  = 16 * 1024
)

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

// WriteLine appends a timestamped line to the current daily log file.
// Key aspects: Rotates on day change and logs file errors to stderr (rate-limited).
// Upstream: logFanout line dispatch.
// Downstream: os.OpenFile and file.WriteString.
func (s *dailyFileSink) WriteLine(line string, now time.Time) {
	if s == nil {
		return
	}
	now = now.UTC()
	date := now.Format(logFileDateLayout)

	var hook logRotateHook
	var prevDate time.Time
	var prevPath string
	var newPath string

	s.mu.Lock()

	if s.file == nil || s.currentDate != date {
		hook, prevDate, prevPath, newPath = s.rotateLocked(date, now)
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

func (s *dailyFileSink) rotateLocked(date string, now time.Time) (logRotateHook, time.Time, string, string) {
	var hook logRotateHook
	var prevDate time.Time
	var prevPath string
	var newPath string
	if s.currentDate != "" && s.currentDate != date {
		parsed, err := time.ParseInLocation(logFileDateLayout, s.currentDate, time.UTC)
		if err == nil {
			prevDate = parsed
		}
		prevPath = s.currentPath
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
	path := filepath.Join(s.dir, logFileNameForDate(now))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		s.reportErrorLocked(now, fmt.Errorf("open failed for %s: %w", path, err))
		return nil, time.Time{}, "", ""
	}
	s.file = file
	s.currentDate = date
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
	fileSink, err := newDailyFileSink(cfg.Dir, cfg.RetentionDays)
	if err != nil {
		return fanout, err
	}
	fanout.SetFileSink(fileSink)
	return fanout, nil
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

func logFileNameForDate(now time.Time) string {
	return now.UTC().Format(logFileDateLayout) + ".log"
}

func parseLogFileDate(name string) (time.Time, bool) {
	if filepath.Ext(name) != ".log" {
		return time.Time{}, false
	}
	base := strings.TrimSuffix(name, ".log")
	parsed, err := time.ParseInLocation(logFileDateLayout, base, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
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
