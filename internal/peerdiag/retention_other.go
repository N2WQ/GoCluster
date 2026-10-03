//go:build !windows

package peerdiag

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"dxcluster/internal/logutil"
)

type directoryScan struct{}

func (s *helperSink) scanFailed() bool { return false }
func (s *helperSink) closeScan() error { return nil }

// Preserve the existing non-Windows directory API and deletion behavior.
func (s *helperSink) cleanup(day time.Time) (resultErr error) {
	if s.options.RetentionDays <= 0 {
		return nil
	}
	directory, err := os.Open(s.options.Directory)
	if err != nil {
		return err
	}
	defer func() {
		if err := directory.Close(); resultErr == nil {
			resultErr = err
		}
	}()
	cutoff := day.AddDate(0, 0, -(s.options.RetentionDays - 1))
	for {
		entries, readErr := directory.ReadDir(1)
		for _, entry := range entries {
			if !entry.IsDir() && len(entry.Name()) == len(logutil.DailyArchiveDateLayout)+len(".log") {
				if date, ok := logutil.ParseDailyArchiveDate(entry.Name()); ok && date.Before(cutoff) {
					_ = os.Remove(filepath.Join(s.options.Directory, entry.Name()))
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
