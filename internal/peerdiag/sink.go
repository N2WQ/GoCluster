package peerdiag

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"dxcluster/internal/logutil"
)

const timestampLayout = "2006/01/02 15:04:05"

type helperSink struct {
	options  Options
	entries  [512]dedupeEntry
	used     int
	file     *os.File
	day      time.Time
	line     [4096]byte
	scratch  [64 << 10]byte
	scan     directoryScan
	metadata metadataOwner
}

func (s *helperSink) close() error {
	scanErr := errors.Join(s.closeScan(), s.closeMetadata())
	if s.file != nil {
		file := s.file
		s.file = nil
		return errors.Join(scanErr, file.Close())
	}
	return scanErr
}

func (s *helperSink) write(event Event) uint32 {
	if s.scanFailed() || s.metadataFailed() {
		return ackFailed
	}
	now := time.Unix(0, event.UnixNano).UTC()
	line := event.Data[:event.Length]
	if event.Kind == Overlong {
		if s.writeOverlong(now, line) != nil {
			return ackFailed
		}
		return ackWritten
	}
	if !s.options.Enabled {
		return ackSuppressed
	}
	if event.Kind != LossSummary {
		var emit bool
		line, emit = s.deduplicate(line, now)
		if !emit {
			return ackSuppressed
		}
	}
	if err := s.openDaily(now); err != nil {
		return ackFailed
	}
	// Header and body fit fixed scratch even when a dedupe summary is appended.
	output := now.AppendFormat(s.scratch[:0], timestampLayout)
	output = append(output, ' ')
	output = append(output, line...)
	output = append(output, '\n')
	if writeFull(s.file, output) != nil {
		return ackFailed
	}
	return ackWritten
}

func dateOnly(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func (s *helperSink) openDaily(now time.Time) error {
	day := dateOnly(now)
	if s.file != nil && s.day.Equal(day) {
		return nil
	}
	if err := s.close(); err != nil {
		return err
	}
	if err := s.diagnosticMkdirAll(s.options.Directory, 0755); err != nil {
		return err
	}
	active := logutil.DailyActivePath(s.options.Directory)
	previous := s.day
	if previous.IsZero() {
		info, err := s.diagnosticStat(active)
		if err == nil && !info.IsDir() {
			previous, err = s.activeDate(active, info)
			if err != nil {
				return err
			}
		}
		if errors.Is(err, os.ErrNotExist) {
			legacy := logutil.DailyArchivePath(s.options.Directory, day)
			if err = diagnosticRename(legacy, active); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	if !previous.IsZero() && !previous.Equal(day) {
		if err := s.archive(active, logutil.DailyArchivePath(s.options.Directory, previous)); err != nil {
			return err
		}
	}
	file, err := helperOpenFile(active, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	s.file, s.day = file, day
	return s.cleanup(day)
}

func (s *helperSink) activeDate(path string, info os.FileInfo) (date time.Time, resultErr error) {
	file, err := diagnosticOpen(path)
	if err != nil {
		return dateOnly(info.ModTime()), nil
	}
	defer func() {
		if err := file.Close(); resultErr == nil {
			resultErr = err
		}
	}()
	size := min(info.Size(), int64(len(s.scratch)))
	data := s.scratch[:size]
	if _, err = file.ReadAt(data, info.Size()-size); err != nil && !errors.Is(err, io.EOF) {
		return dateOnly(info.ModTime()), nil
	}
	// Scan existing bounded tail in place; Split would retain an additional
	// pointer array proportional to the number of lines in that tail.
	for end := len(data); end > 0; {
		start := bytes.LastIndexByte(data[:end], '\n') + 1
		line := bytes.TrimSpace(data[start:end])
		if len(line) >= len(timestampLayout) {
			if parsed, err := time.ParseInLocation(timestampLayout, string(line[:len(timestampLayout)]), time.UTC); err == nil {
				return dateOnly(parsed), nil
			}
		}
		if start == 0 {
			break
		}
		end = start - 1
	}
	return dateOnly(info.ModTime()), nil
}

func (s *helperSink) archive(active, archive string) error {
	info, err := s.diagnosticStat(active)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return diagnosticRemove(active)
	}
	if _, err = s.diagnosticStat(archive); errors.Is(err, os.ErrNotExist) {
		return diagnosticRename(active, archive)
	} else if err != nil {
		return err
	}
	source, err := diagnosticOpen(active)
	if err != nil {
		return err
	}
	defer source.Close()
	destination, err := helperOpenFile(archive, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	// Manual bounded copy avoids io.Copy selecting a hidden WriterTo/ReaderFrom
	// path with unaccounted backing allocations.
	for {
		n, readErr := source.Read(s.scratch[:])
		if n > 0 {
			if err = writeFull(destination, s.scratch[:n]); err != nil {
				break
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				err = readErr
			}
			break
		}
	}
	closeErr := destination.Close()
	sourceCloseErr := source.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if sourceCloseErr != nil {
		return sourceCloseErr
	}
	return diagnosticRemove(active)
}

func (s *helperSink) writeOverlong(now time.Time, line []byte) error {
	path := s.options.OverlongPath
	if path == "" {
		return errors.New("missing overlong diagnostic path")
	}
	if err := s.diagnosticMkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	info, err := s.diagnosticStat(path)
	if s.metadataFailed() || isFilesystemBudget(err) {
		return err
	}
	if err == nil && info.Size() >= 8<<20 {
		_ = diagnosticRemove(path + ".2")
		for i := 1; i >= 1; i-- {
			if err = diagnosticRename(path+"."+strconv.Itoa(i), path+"."+strconv.Itoa(i+1)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if err = diagnosticRename(path, path+".1"); err != nil {
			return err
		}
	}
	file, err := helperOpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	output := now.AppendFormat(s.scratch[:0], time.RFC3339)
	output = append(output, ' ')
	output = append(output, line...)
	output = append(output, '\n')
	err = writeFull(file, output)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}
