package spot

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCleanupDecisionLogRetentionRemovesOldDBs(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "callcorr_debug_modified.log")
	now := time.Date(2025, 12, 13, 12, 0, 0, 0, time.UTC)

	keepDates := []string{"2025-12-13", "2025-12-12", "2025-12-11"}
	dropDates := []string{"2025-12-10", "2025-12-09"}

	mustWrite := func(path string) {
		t.Helper()
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	for _, dayStr := range append(append([]string{}, keepDates...), dropDates...) {
		day, err := time.ParseInLocation("2006-01-02", dayStr, time.UTC)
		if err != nil {
			t.Fatalf("parse day %q: %v", dayStr, err)
		}
		dbPath := DecisionLogPath(basePath, day)
		mustWrite(dbPath)
		mustWrite(dbPath + "-wal")
		mustWrite(dbPath + "-shm")
	}

	if err := cleanupDecisionLogRetention(basePath, now, 3, ""); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	assertExists := func(path string) {
		t.Helper()
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to exist: %v", path, err)
		}
	}
	assertMissing := func(path string) {
		t.Helper()
		if _, err := os.Stat(path); err == nil {
			t.Fatalf("expected %s to be removed", path)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat %s: %v", path, err)
		}
	}

	for _, dayStr := range keepDates {
		day, err := time.ParseInLocation("2006-01-02", dayStr, time.UTC)
		if err != nil {
			t.Fatalf("parse keep day %q: %v", dayStr, err)
		}
		dbPath := DecisionLogPath(basePath, day)
		assertExists(dbPath)
		assertExists(dbPath + "-wal")
		assertExists(dbPath + "-shm")
	}

	for _, dayStr := range dropDates {
		day, err := time.ParseInLocation("2006-01-02", dayStr, time.UTC)
		if err != nil {
			t.Fatalf("parse drop day %q: %v", dayStr, err)
		}
		dbPath := DecisionLogPath(basePath, day)
		assertMissing(dbPath)
		assertMissing(dbPath + "-wal")
		assertMissing(dbPath + "-shm")
	}
}

func TestCleanupDecisionLogRetentionKeepsActivePath(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "callcorr_debug_modified.log")
	now := time.Date(2025, 12, 13, 12, 0, 0, 0, time.UTC)
	older := time.Date(2025, 12, 9, 12, 0, 0, 0, time.UTC)
	keepPath := DecisionLogPath(basePath, older)

	if err := os.WriteFile(keepPath, []byte("x"), 0o644); err != nil {
		t.Fatalf("write %s: %v", keepPath, err)
	}
	if err := os.WriteFile(keepPath+"-wal", []byte("x"), 0o644); err != nil {
		t.Fatalf("write %s-wal: %v", keepPath, err)
	}
	if err := os.WriteFile(keepPath+"-shm", []byte("x"), 0o644); err != nil {
		t.Fatalf("write %s-shm: %v", keepPath, err)
	}

	if err := cleanupDecisionLogRetention(basePath, now, 3, keepPath); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, err := os.Stat(keepPath); err != nil {
		t.Fatalf("expected active db %s to remain: %v", keepPath, err)
	}
	if _, err := os.Stat(keepPath + "-wal"); err != nil {
		t.Fatalf("expected active wal %s-wal to remain: %v", keepPath, err)
	}
	if _, err := os.Stat(keepPath + "-shm"); err != nil {
		t.Fatalf("expected active shm %s-shm to remain: %v", keepPath, err)
	}
}
