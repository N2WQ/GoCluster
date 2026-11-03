package cluster

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/internal/asynclog"
)

func TestRootedLogCloseFailureRetainsOwnerAndStopsStream(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "active.log"), []byte("entry\n"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	closeFailure := errors.New("injected close failure")
	calls := 0
	owner := &logCloseOwner{closeFile: func(*os.File) error { calls++; return closeFailure }}
	if safeExistingLogInRootOwned(root, "active.log", owner) {
		t.Fatal("failed close reported safe")
	}
	if !errors.Is(owner.err, closeFailure) || owner.files[0] == nil {
		t.Fatal("uncertain handle not retained")
	}
	before := calls
	if _, err := openSafeLogInRootOwned(root, "new.log", os.O_CREATE|os.O_WRONLY, 0600, owner); !errors.Is(err, closeFailure) {
		t.Fatalf("later open error: %v", err)
	}
	if calls != before {
		t.Fatal("poisoned owner performed another open")
	}
	if _, err := os.Stat(filepath.Join(dir, "new.log")); !os.IsNotExist(err) {
		t.Fatalf("poisoned owner created file: %v", err)
	}
	guard := &guardedLogFile{closeOwner: *owner}
	if got := guard.Write(time.Now(), 0, []byte("x")); got != asynclog.NotWritten {
		t.Fatalf("write: %v", got)
	}
	if !errors.Is(guard.Close(), closeFailure) {
		t.Fatal("guard released uncertain ownership")
	}
	for _, f := range owner.files {
		if f != nil {
			_ = f.Close()
		}
	}
}

func TestDailySinkRetainsActiveFileWhenCloseFails(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "active")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	failure := errors.New("injected close failure")
	owner := &logCloseOwner{closeFile: func(*os.File) error { return failure }}
	sink := &dailyFileSink{file: file, closeOwner: owner}
	if err := sink.Close(); !errors.Is(err, failure) {
		t.Fatalf("close error = %v", err)
	}
	if sink.file != file || owner.files[0] != file {
		t.Fatal("uncertain active file was not retained")
	}
}

func TestArchiveCollisionDoesNotDiscardAfterCloseFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	day := time.Now().UTC().AddDate(0, 0, -1)
	active := activeLogPathForDir(dir)
	archive := archiveLogPathForDate(dir, day)
	for _, path := range []string{active, archive} {
		if err := os.WriteFile(path, []byte("preserve"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	failure := errors.New("injected close failure")
	owner := &logCloseOwner{closeFile: func(*os.File) error { return failure }}
	sink := &dailyFileSink{dir: dir, activePath: active, root: root, closeOwner: owner}
	if _, err := sink.archiveActiveLogLocked(day); !errors.Is(err, failure) {
		t.Fatalf("archive error = %v", err)
	}
	for _, f := range owner.files {
		if f != nil {
			_ = f.Close()
		}
	}
	if data, err := os.ReadFile(active); err != nil || string(data) != "preserve" {
		t.Fatalf("active file = %q, %v", data, err)
	}
}

func TestRootedLogTailAndCleanupRetainFailedCloses(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "active.log"), []byte("2026-01-02 12:00:00 record\n"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	failure := errors.New("injected close failure")
	for _, operation := range []string{"tail", "cleanup"} {
		t.Run(operation, func(t *testing.T) {
			owner := &logCloseOwner{closeFile: func(*os.File) error { return failure }}
			if operation == "tail" {
				s := &dailyFileSink{root: root, closeOwner: owner}
				s.inferActiveLogDate("active.log")
			} else {
				_ = cleanupOldLogsInRootOwned(root, "active.log", time.Now(), 1, owner)
			}
			if !errors.Is(owner.err, failure) || owner.files[0] == nil {
				t.Fatal("close failure was not retained")
			}
			for _, f := range owner.files {
				if f != nil {
					_ = f.Close()
				}
			}
		})
	}
}

func TestReportArchiveRetainsUncertainAcquisition(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "archive")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	owner := &logCloseOwner{closeFile: func(*os.File) error { return errors.New("failed close") }}
	_ = owner.close(file)
	lease := reserveReportArchive()
	if lease.generation == 0 {
		t.Fatal("no report lease")
	}
	lease.retainUncertain(owner)
	_ = lease.release(false)
	reportArchiveOwners.Lock()
	s := &reportArchiveOwners.slots[lease.index]
	if s.state != 5 || s.uncertain != owner {
		reportArchiveOwners.Unlock()
		t.Fatal("uncertain acquisition was released")
	}
	// This fake failure left the file open; clean the test-only reservation.
	s.state, s.uncertain = 0, nil
	reportArchiveOwners.Unlock()
}
