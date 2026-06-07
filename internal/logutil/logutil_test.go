package logutil

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDailyActiveFileNameDerivesFromDirectory(t *testing.T) {
	if got := DailyActiveFileName(filepath.Join("data", "logs", "system")); got != "system.log" {
		t.Fatalf("expected system.log, got %q", got)
	}
	if got := DailyActiveFileName(filepath.Join("data", "logs", "bad_de_dx")); got != "bad_de_dx.log" {
		t.Fatalf("expected bad_de_dx.log, got %q", got)
	}
}

func TestDailyArchiveFileNameKeepsDateOnlyFormat(t *testing.T) {
	when := time.Date(2026, time.June, 7, 12, 0, 0, 0, time.UTC)
	if got := DailyArchiveFileName(when); got != "07-Jun-2026.log" {
		t.Fatalf("expected date-only archive filename, got %q", got)
	}
}

func TestParseDailyArchiveDateRejectsActiveNames(t *testing.T) {
	if _, ok := ParseDailyArchiveDate("07-Jun-2026.log"); !ok {
		t.Fatalf("expected date-only archive name to parse")
	}
	if _, ok := ParseDailyArchiveDate("system.log"); ok {
		t.Fatalf("expected active name to be rejected")
	}
}
