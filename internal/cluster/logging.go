// File role: Provides shared log naming helpers and daily file sinks.
// Crawler notes: Runtime destinations invoke these synchronous operations from
// bounded workers; callers must not run file operations on producer goroutines.
// Related docs: docs/OPERATOR_GUIDE.md, data/config/README.md.
// Related tests: internal/cluster/logging_test.go, internal/cluster/main_runtime_test.go.
package cluster

import (
	"bytes"
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
	activePath    string
	root          *os.Root // Borrowed from the log worker; that worker closes it.
	closeOwner    *logCloseOwner
	retentionDays int
	currentDate   string
	currentPath   string
	file          *os.File
	rotateHook    logRotateHook
	workerHook    bool
	cleanupFn     func(string, time.Time, int) error
	openFile      func(string, int, os.FileMode) (*os.File, error)
	checkActive   func(string) bool
	hookDone      <-chan struct{}
	closed        bool
	closeErr      error
	mu            sync.Mutex
}

// Purpose: Initialize a daily file sink with directory creation and cleanup.
// Key aspects: Ensures directory exists and bounds retention by date-based cleanup.
// Upstream: setupLogging.
// Downstream: os.MkdirAll and cleanupOldLogs.
func newDailyFileSink(dir string, retentionDays int) (*dailyFileSink, error) {
	return newDailyFileSinkInRoot(dir, retentionDays, nil)
}

// newDailyFileSinkInRoot uses an already-owned directory root when supplied.
// It never closes the root; the caller owns its lifetime and any close errors.
func newDailyFileSinkInRoot(dir string, retentionDays int, root *os.Root) (*dailyFileSink, error) {
	return newDailyFileSinkInRootOwned(dir, retentionDays, root, nil)
}

func newDailyFileSinkInRootOwned(dir string, retentionDays int, root *os.Root, owner *logCloseOwner) (*dailyFileSink, error) {
	trimmed := strings.TrimSpace(dir)
	if trimmed == "" {
		return nil, fmt.Errorf("log directory is empty")
	}
	if retentionDays <= 0 {
		retentionDays = 7
	}
	if root == nil {
		if err := ensureLogDirectory(trimmed); err != nil {
			return nil, fmt.Errorf("failed to create log directory %q: %w", trimmed, err)
		}
	}
	sink := &dailyFileSink{
		dir:           trimmed,
		activePath:    strings.Clone(activeLogPathForDir(trimmed)),
		root:          root,
		closeOwner:    owner,
		retentionDays: retentionDays,
		cleanupFn:     cleanupOldLogs,
	}
	if root != nil {
		sink.cleanupFn = func(_ string, now time.Time, days int) error {
			return cleanupOldLogsInRootOwned(root, activeLogFileNameForDir(trimmed), now, days, owner)
		}
	}
	_ = sink.cleanupFn(trimmed, time.Now().UTC(), retentionDays) //nolint:errcheck // Logs are disposable; startup remains available.
	return sink, nil
}

func newDailyLogSink(dir string, retentionDays int) (lineSink, error) {
	return newDailyFileSink(dir, retentionDays)
}

// WriteLine appends a timestamped line to the stable current-day log file.
// Key aspects: Archives before the first new-day write; file errors drop the record.
// Upstream: logFanout line dispatch.
// Downstream: os.OpenFile and file.WriteString.
func (s *dailyFileSink) WriteLine(line string, now time.Time) {
	s.writeLine(line, now)
}

// writeLine lets the async worker commit deduplication only after the full
// record was written. Its filesystem operations run on that worker.
func (s *dailyFileSink) writeLine(line string, now time.Time) bool {
	if s == nil {
		return false
	}
	if s.closeOwner != nil && s.closeOwner.err != nil {
		return false
	}
	now = now.UTC()
	day := dateOnly(now)
	dateKey := day.Format(logDateKeyLayout)

	var hook logRotateHook
	var prevDate time.Time
	var prevPath string
	var newPath string

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}

	if s.file == nil || s.currentDate != dateKey {
		hook, prevDate, prevPath, newPath = s.rotateLocked(dateKey, day, now)
	}
	if s.closeOwner != nil && s.closeOwner.err != nil {
		s.mu.Unlock()
		return false
	}
	if s.file == nil {
		directHook := s.prepareRotateHookLocked(hook, prevDate, prevPath, newPath)
		s.mu.Unlock()
		if directHook {
			hook(prevDate, prevPath, newPath)
		}
		return false
	}
	record := formatLogTimestamp(now) + " " + line + "\n"
	n, err := s.file.WriteString(record)
	directHook := s.prepareRotateHookLocked(hook, prevDate, prevPath, newPath)
	s.mu.Unlock()
	if directHook {
		// Runtime logging reaches this sink only through its dedicated worker.
		// The report callback can run there without a second retained owner.
		hook(prevDate, prevPath, newPath)
	}
	return err == nil && n == len(record)
}

// prepareRotateHookLocked schedules legacy hooks without re-entering the
// writer. Runtime hooks run on the destination worker after the mutex is free.
func (s *dailyFileSink) prepareRotateHookLocked(hook logRotateHook, prevDate time.Time, prevPath, newPath string) bool {
	if hook == nil || prevDate.IsZero() || prevPath == "" {
		return false
	}
	if s.workerHook {
		return true
	}
	if finished(s.hookDone) {
		done := make(chan struct{})
		s.hookDone = done
		go func() { defer close(done); hook(prevDate, prevPath, newPath) }()
	}
	return false
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
	if !s.closed {
		s.closed = true
		if s.file != nil {
			s.closeErr = s.closeTemp(s.file)
			if s.closeErr == nil {
				s.file = nil
			}
		}
		s.currentDate = ""
		s.currentPath = ""
	}
	err, hook := s.closeErr, s.hookDone
	s.mu.Unlock()
	if hook != nil {
		<-hook
	}
	return err
}

func finished(done <-chan struct{}) bool {
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

type logRotateHook func(prevDate time.Time, prevPath, newPath string)

func (s *dailyFileSink) SetRotateHook(hook logRotateHook) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if !s.closed {
		s.rotateHook = hook
	}
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
		if err := s.closeTemp(s.file); err != nil {
			s.closed = true
			s.closeErr = err
			return nil, time.Time{}, "", ""
		}
		s.file = nil
	}
	if s.root == nil {
		if err := ensureLogDirectory(s.dir); err != nil {
			return nil, time.Time{}, "", ""
		}
		if s.checkActive != nil && !s.checkActive(s.activePath) {
			return nil, time.Time{}, "", ""
		}
	} else {
		name, err := rootLogName(s.activePath)
		if err != nil || !safeExistingLogInRootOwned(s.root, name, s.closeOwner) {
			return nil, time.Time{}, "", ""
		}
	}
	if !prevDate.IsZero() {
		archivedPath, err := s.archiveActiveLogLocked(prevDate)
		if err != nil {
			return nil, time.Time{}, "", ""
		}
		prevPath = archivedPath
	} else if s.currentDate == "" {
		activePath := s.activePath
		activeDate, ok := s.inferActiveLogDate(activePath)
		if s.closeOwner != nil && s.closeOwner.err != nil {
			return nil, time.Time{}, "", ""
		}
		if ok {
			if !dateOnly(activeDate).Equal(day) {
				archivedPath, err := s.archiveActiveLogLocked(dateOnly(activeDate))
				if err != nil {
					return nil, time.Time{}, "", ""
				}
				prevDate = dateOnly(activeDate)
				prevPath = archivedPath
				hook = s.rotateHook
			}
		} else if err := s.adoptLegacyCurrentLogLocked(day); err != nil {
			return nil, time.Time{}, "", ""
		}
	}
	if s.closeOwner != nil && s.closeOwner.err != nil {
		return nil, time.Time{}, "", ""
	}

	path := s.activePath
	if s.workerHook && s.root != nil && hook != nil && !prevDate.IsZero() && prevPath != "" {
		// The report takes a rooted archive snapshot before retention can
		// remove it. This callback runs only on the destination worker.
		hook(prevDate, prevPath, path)
		hook = nil
	}
	openFile := s.openFile
	if openFile == nil {
		if s.root == nil {
			openFile = os.OpenFile
		} else {
			openFile = func(path string, flags int, mode os.FileMode) (*os.File, error) {
				name, err := rootLogName(path)
				if err != nil {
					return nil, err
				}
				return openSafeLogInRootOwned(s.root, name, flags, mode, s.closeOwner)
			}
		}
	}
	file, err := openFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return hook, prevDate, prevPath, path
	}
	s.file = file
	s.currentDate = dateKey
	s.currentPath = path
	newPath = path
	if s.cleanupFn != nil {
		// Cleanup may block this destination, but the shared queue keeps its
		// producers and every other destination moving.
		_ = s.cleanupFn(s.dir, now, s.retentionDays) //nolint:errcheck // Retention failure does not stop logging.
	}
	return hook, prevDate, prevPath, newPath
}

func (s *dailyFileSink) archiveActiveLogLocked(date time.Time) (string, error) {
	activePath := s.activePath
	archivePath := strings.Clone(archiveLogPathForDate(s.dir, date))
	info, err := s.statLog(activePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("stat active log %s: %w", activePath, err)
	}
	if info.IsDir() || (s.root != nil && !info.Mode().IsRegular()) {
		return "", fmt.Errorf("active log path is not a regular file: %s", activePath)
	}
	if info.Size() == 0 {
		if err := s.removeLog(activePath); err != nil && !os.IsNotExist(err) {
			return "", fmt.Errorf("remove empty active log %s: %w", activePath, err)
		}
		return "", nil
	}
	archiveEntry, err := s.lstatLog(archivePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", s.discardActiveLog(activePath)
		}
		if err := s.renameLog(activePath, archivePath); err != nil {
			return "", fmt.Errorf("archive active log %s to %s: %w", activePath, archivePath, err)
		}
		return archivePath, nil
	}
	// A partial append or interruption makes a later merge indistinguishable
	// from a fresh one. Preserve the existing archive and discard the active
	// log instead; logs are disposable and retries must not duplicate records.
	usableArchive := false
	if archiveEntry.Mode().IsRegular() && !os.SameFile(info, archiveEntry) {
		archive, openErr := s.openLog(archivePath)
		if openErr == nil {
			openedInfo, statErr := archive.Stat()
			singleLink, linkErr := logArchiveSingleLink(archive)
			closeErr := s.closeTemp(archive)
			if closeErr != nil && s.closeOwner != nil {
				return "", closeErr
			}
			usableArchive = statErr == nil && linkErr == nil && closeErr == nil && singleLink && os.SameFile(archiveEntry, openedInfo)
		}
	}
	if s.closeOwner != nil && s.closeOwner.err != nil {
		return "", s.closeOwner.err
	}
	if err := s.discardActiveLog(activePath); err != nil {
		return "", err
	}
	if usableArchive {
		return archivePath, nil
	}
	return "", nil
}

func (s *dailyFileSink) adoptLegacyCurrentLogLocked(day time.Time) error {
	legacyPath := strings.Clone(archiveLogPathForDate(s.dir, day))
	activePath := s.activePath
	if info, err := s.statLog(legacyPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat current legacy log %s: %w", legacyPath, err)
	} else if s.root != nil && !info.Mode().IsRegular() {
		return fmt.Errorf("current legacy log is not a regular file: %s", legacyPath)
	}
	if s.root != nil {
		name, err := rootLogName(legacyPath)
		if err != nil || !safeExistingLogInRootOwned(s.root, name, s.closeOwner) {
			return fmt.Errorf("current legacy log identity could not be verified: %s", legacyPath)
		}
	}
	if _, err := s.statLog(activePath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat active log %s: %w", activePath, err)
	}
	if err := s.renameLog(legacyPath, activePath); err != nil {
		return fmt.Errorf("adopt current legacy log %s to %s: %w", legacyPath, activePath, err)
	}
	return nil
}

// rootLogName derives and validates one final name beneath an owned root. The
// full configured path is retained only for output and test-injection hooks.
func rootLogName(path string) (string, error) {
	name := filepath.Base(path)
	if name == "." || name == ".." || name == "" || name == string(filepath.Separator) || !filepath.IsLocal(name) || filepath.VolumeName(name) != "" {
		return "", fmt.Errorf("invalid rooted log name %q", name)
	}
	return name, nil
}

func (s *dailyFileSink) statLog(path string) (os.FileInfo, error) {
	if s.root == nil {
		return os.Stat(path)
	}
	name, err := rootLogName(path)
	if err != nil {
		return nil, err
	}
	// Rooted callers must not follow a last-component symlink between their
	// active-file guard and archive/adoption work.
	return s.root.Lstat(name)
}

func (s *dailyFileSink) lstatLog(path string) (os.FileInfo, error) {
	if s.root == nil {
		return os.Lstat(path)
	}
	name, err := rootLogName(path)
	if err != nil {
		return nil, err
	}
	return s.root.Lstat(name)
}

func (s *dailyFileSink) openLog(path string) (*os.File, error) {
	if s.root == nil {
		return os.Open(path)
	}
	name, err := rootLogName(path)
	if err != nil {
		return nil, err
	}
	return openRootNoFollowOwned(s.root, name, os.O_RDONLY, 0, s.closeOwner)
}

func (s *dailyFileSink) closeTemp(file *os.File) error {
	if s.closeOwner != nil {
		return s.closeOwner.close(file)
	}
	return file.Close()
}

func (s *dailyFileSink) removeLog(path string) error {
	if s.root == nil {
		return os.Remove(path)
	}
	name, err := rootLogName(path)
	if err != nil {
		return err
	}
	return s.root.Remove(name)
}

func (s *dailyFileSink) renameLog(oldPath, newPath string) error {
	if s.root == nil {
		return os.Rename(oldPath, newPath)
	}
	oldName, err := rootLogName(oldPath)
	if err != nil {
		return err
	}
	newName, err := rootLogName(newPath)
	if err != nil {
		return err
	}
	return s.root.Rename(oldName, newName)
}

func (s *dailyFileSink) discardActiveLog(path string) error {
	if err := s.removeLog(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *dailyFileSink) inferActiveLogDate(path string) (time.Time, bool) {
	info, err := s.statLog(path)
	if err != nil || info.IsDir() || (s.root != nil && !info.Mode().IsRegular()) {
		return time.Time{}, false
	}
	file, err := s.openLog(path)
	if err == nil {
		defer s.closeTemp(file) //nolint:errcheck // A failed close is retained by closeOwner.
		if date, ok := parseLogDateFromFile(file, info.Size()); ok {
			return date, true
		}
	}
	return dateOnly(info.ModTime().UTC()), true
}

func parseLogDateFromTail(path string, size int64) (time.Time, bool) {
	file, err := os.Open(path)
	if err != nil {
		return time.Time{}, false
	}
	defer file.Close()
	return parseLogDateFromFile(file, size)
}

func parseLogDateFromFile(file *os.File, size int64) (time.Time, bool) {
	if size <= 0 {
		return time.Time{}, false
	}
	readSize := size
	if readSize > activeLogTailBytes {
		readSize = activeLogTailBytes
	}
	buf := make([]byte, int(readSize))
	if _, err := file.ReadAt(buf, size-readSize); err != nil && !errors.Is(err, io.EOF) {
		return time.Time{}, false
	}
	// Scan the bounded tail in place. Splitting a newline-heavy file would
	// retain one slice header per byte despite the fixed 64 KiB input limit.
	for end := len(buf); ; {
		separator := bytes.LastIndexByte(buf[:end], '\n')
		line := bytes.TrimSpace(buf[separator+1 : end])
		if len(line) >= len(logTimestampLayout) {
			parsed, err := time.ParseInLocation(logTimestampLayout, string(line[:len(logTimestampLayout)]), time.UTC)
			if err == nil {
				return dateOnly(parsed.UTC()), true
			}
		}
		if separator < 0 {
			break
		}
		end = separator
	}
	return time.Time{}, false
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

// ensureLogDirectory creates parents iteratively. Unlike recursive MkdirAll,
// an unusually deep configured path cannot retain one error per parent while
// a lower filesystem operation is stalled.
func ensureLogDirectory(path string) error {
	path = filepath.Clean(path)
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return nil
	}
	start := len(filepath.VolumeName(path))
	for start < len(path) && os.IsPathSeparator(path[start]) {
		start++
	}
	for i := start; i <= len(path); i++ {
		if i != len(path) && !os.IsPathSeparator(path[i]) {
			continue
		}
		part := path[:i]
		if part == "" {
			continue
		}
		if err := os.Mkdir(part, 0755); err != nil {
			if info, statErr := os.Stat(part); statErr != nil || !info.IsDir() {
				return err
			}
		}
	}
	return nil
}

func cleanupOldLogs(dir string, now time.Time, retentionDays int) error {
	if retentionDays <= 0 {
		return nil
	}
	// Scan the same cleaned directory used by log creation and removal.
	dir = filepath.Clean(dir)
	directory, err := os.Open(dir) // #nosec G703 -- Directory is operator configuration.
	if err != nil {
		return err
	}
	defer directory.Close()
	cutoff := dateOnly(now.UTC()).AddDate(0, 0, -(retentionDays - 1))
	for {
		// Keep just one directory entry live while removing an archive, since
		// that filesystem operation can stall indefinitely.
		entries, readErr := directory.ReadDir(1)
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			date, ok := parseLogFileDate(entry.Name())
			if ok && date.Before(cutoff) {
				path := filepath.Join(dir, entry.Name())
				if safeExistingLog(path) {
					_ = os.Remove(path) // #nosec G703 -- ReadDir supplies a single name beneath the configured directory.
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

// cleanupOldLogsInRoot scans and removes entries relative to the borrowed
// directory handle. A retargeted configuration path cannot redirect cleanup.
func cleanupOldLogsInRootOwned(root *os.Root, activeName string, now time.Time, retentionDays int, owner *logCloseOwner) error {
	if retentionDays <= 0 {
		return nil
	}
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() {
		if owner != nil {
			_ = owner.close(directory)
		} else {
			_ = directory.Close()
		}
	}()
	cutoff := dateOnly(now.UTC()).AddDate(0, 0, -(retentionDays - 1))
	for {
		entries, readErr := directory.ReadDir(1)
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || strings.EqualFold(name, activeName) {
				continue
			}
			validName, nameErr := rootLogName(name)
			if nameErr != nil || validName != name {
				continue
			}
			date, ok := parseLogFileDate(name)
			if ok && date.Before(cutoff) && safeExistingLogInRootOwned(root, name, owner) {
				_ = root.Remove(name) //nolint:errcheck // Logs are disposable; cleanup is best effort.
			}
		}
		if owner != nil && owner.err != nil {
			return owner.err
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func dateOnly(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}
