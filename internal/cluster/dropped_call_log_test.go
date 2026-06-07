package cluster

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

func TestDroppedCallLoggerDisabledReturnsNil(t *testing.T) {
	logger, err := newDroppedCallLogger(config.DroppedCallLoggingConfig{})
	if err != nil {
		t.Fatalf("newDroppedCallLogger() error: %v", err)
	}
	if logger != nil {
		t.Fatalf("expected nil logger when disabled")
	}
}

func TestDroppedCallLoggerWritesSeparateDailyFiles(t *testing.T) {
	dir := t.TempDir()
	logger, err := newDroppedCallLogger(config.DroppedCallLoggingConfig{
		Enabled:             true,
		Dir:                 dir,
		RetentionDays:       1,
		DedupeWindowSeconds: 0,
		BadDEDX:             true,
		NoLicense:           true,
		Harmonics:           true,
	})
	if err != nil {
		t.Fatalf("newDroppedCallLogger() error: %v", err)
	}

	logger.LogBadCall("rbn", "DX", "invalid_callsign", "BAD!", "N0CALL", "BAD!", "FT8", "source_parser")
	logger.LogNoLicense("pskreporter", "DE", "K1ABC", "K1ABC", "DL1ABC", "FT8", "fcc_uls")
	logger.LogHarmonic("rbn", "K1ABC", "W1XYZ", "K1ABC", "CW", "corroborators=2_delta_db=18")
	if err := logger.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	assertDroppedLogLine(t, dir, "bad_de_dx", "source=rbn role=DX reason=invalid_callsign call=BAD! de=N0CALL dx=BAD! mode=FT8 detail=source_parser")
	assertDroppedLogLine(t, dir, "no_license", "source=pskreporter role=DE reason=unlicensed_us call=K1ABC de=K1ABC dx=DL1ABC mode=FT8 detail=fcc_uls")
	assertDroppedLogLine(t, dir, "harmonics", "source=rbn role=DX reason=harmonic call=K1ABC de=W1XYZ dx=K1ABC mode=CW detail=corroborators=2_delta_db=18")
}

func TestDroppedCallLoggerDedupesWithinWindow(t *testing.T) {
	dir := t.TempDir()
	logger, err := newDroppedCallLogger(config.DroppedCallLoggingConfig{
		Enabled:             true,
		Dir:                 dir,
		RetentionDays:       1,
		DedupeWindowSeconds: 60,
		BadDEDX:             true,
	})
	if err != nil {
		t.Fatalf("newDroppedCallLogger() error: %v", err)
	}
	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)
	logger.badDEDXDedupe.now = func() time.Time { return now }

	logger.LogBadCall("rbn", "DX", "invalid_callsign", "BAD!", "N0CALL", "BAD!", "FT8", "source_parser")
	logger.LogBadCall("rbn", "DX", "invalid_callsign", "BAD!", "N0CALL", "BAD!", "FT8", "source_parser")
	now = now.Add(61 * time.Second)
	logger.LogBadCall("rbn", "DX", "invalid_callsign", "BAD!", "N0CALL", "BAD!", "FT8", "source_parser")
	if err := logger.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	path := droppedLogPath(dir, "bad_de_dx")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected two emitted lines after dedupe, got %d: %q", len(lines), string(data))
	}
	if !strings.Contains(lines[1], "detail=source_parser,suppressed=1_window=1m0s") {
		t.Fatalf("expected suppressed summary on second emitted line, got %q", lines[1])
	}
}

func assertDroppedLogLine(t *testing.T, dir, subdir, wantBody string) {
	t.Helper()
	path := droppedLogPath(dir, subdir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	line := strings.TrimSpace(string(data))
	if strings.Contains(line, "Dropped call:") || strings.Contains(line, "freq_khz=") || strings.Contains(line, "category=") {
		t.Fatalf("line contains excluded fields: %q", line)
	}
	pattern := regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ` + regexp.QuoteMeta(wantBody) + `$`)
	if !pattern.MatchString(line) {
		t.Fatalf("unexpected line in %s:\ngot  %q\nwant timestamp + %q", path, line, wantBody)
	}
}

func droppedLogPath(dir, subdir string) string {
	return filepath.Join(dir, subdir, activeLogFileNameForDir(filepath.Join(dir, subdir)))
}
