// Package logutil contains small shared logging helpers.
package logutil

import (
	"path/filepath"
	"strings"
	"time"
)

const DailyArchiveDateLayout = "02-Jan-2006"

// SafePrintf calls logger.Printf when logger is non-nil.
func SafePrintf(logger interface{ Printf(string, ...any) }, format string, args ...any) {
	if logger == nil {
		return
	}
	logger.Printf(format, args...)
}

// DailyLogBase returns the stable log identity derived from its directory.
func DailyLogBase(dir string) string {
	base := filepath.Base(filepath.Clean(strings.TrimSpace(dir)))
	if base == "." || base == string(filepath.Separator) || base == "" {
		return "current"
	}
	return base
}

// DailyActiveFileName returns the permanent current-day file name for a log dir.
func DailyActiveFileName(dir string) string {
	return DailyLogBase(dir) + ".log"
}

// DailyActivePath returns the permanent current-day file path for a log dir.
func DailyActivePath(dir string) string {
	return filepath.Join(dir, DailyActiveFileName(dir))
}

// DailyArchiveFileName returns the existing date-only archive file name.
func DailyArchiveFileName(date time.Time) string {
	return date.UTC().Format(DailyArchiveDateLayout) + ".log"
}

// DailyArchivePath returns the date-only archive path for a log dir.
func DailyArchivePath(dir string, date time.Time) string {
	return filepath.Join(dir, DailyArchiveFileName(date))
}

// ParseDailyArchiveDate parses date-only archive names and rejects active names.
func ParseDailyArchiveDate(name string) (time.Time, bool) {
	if filepath.Ext(name) != ".log" {
		return time.Time{}, false
	}
	base := strings.TrimSuffix(name, ".log")
	parsed, err := time.ParseInLocation(DailyArchiveDateLayout, base, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}
