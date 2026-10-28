package bulletinhistory

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestFailedLoadPreservesOriginalAcrossRestart(t *testing.T) {
	for _, original := range [][]byte{[]byte("not json"), []byte(`{"version":99}`)} {
		t.Run(string(original), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.json")
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatal(err)
			}
			s := Open(path, time.Minute, 8)
			now := time.Now().UTC()
			s.Record("WWV", "fresh", now)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			backup, err := os.ReadFile(path + ".recovery")
			if err != nil || !bytes.Equal(backup, original) {
				t.Fatalf("backup %q: %v", backup, err)
			}
			a, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			b, err := os.Stat(path + ".recovery")
			if err != nil {
				t.Fatal(err)
			}
			if os.SameFile(a, b) {
				t.Fatal("backup shares original inode")
			}
			r := Open(path, time.Minute, 8)
			defer r.Close()
			if got := r.Render("WWV", true, now); !strings.Contains(got, "fresh") || !strings.Contains(got, "may be incomplete") {
				t.Fatalf("restart: %q", got)
			}
			backup, err = os.ReadFile(path + ".recovery")
			if err != nil || !bytes.Equal(backup, original) {
				t.Fatalf("restart backup %q: %v", backup, err)
			}
		})
	}
}

func TestFailedLoadWithoutRecordsStillPreservesOriginal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	original := []byte("corrupt with no new records")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	s := Open(path, 0, 0)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path + ".recovery")
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("original not preserved: %q %v", got, err)
	}
}

func TestRecoveryResumesCompleteInterruptedCopies(t *testing.T) {
	for _, suffix := range []string{".recovery.tmp", ".recovery"} {
		t.Run(suffix, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.json")
			original := []byte("corrupt original")
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path+suffix, original, 0o600); err != nil {
				t.Fatal(err)
			}
			s := Open(path, 0, 0)
			s.Record("WWV", "new report", time.Now().UTC())
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(path + ".recovery")
			if err != nil || !bytes.Equal(got, original) {
				t.Fatalf("recovery: %q %v", got, err)
			}
			r := Open(path, 0, 0)
			defer r.Close()
			if got := r.Render("WWV", true, time.Now().UTC()); !strings.Contains(got, "new report") {
				t.Fatalf("new report missing on restart: %q", got)
			}
		})
	}
}

func TestRecoveryObstructionsKeepOriginalAndMemoryHistory(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		original, backup, temp []byte
	}{
		{"different backup", []byte("bad original"), []byte("other backup"), nil},
		{"partial temp", []byte("bad original"), nil, []byte("bad")},
		{"oversized original", bytes.Repeat([]byte("x"), maxSnapshotBytes+1), nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.json")
			if err := os.WriteFile(path, tc.original, 0o600); err != nil {
				t.Fatal(err)
			}
			if tc.backup != nil {
				if err := os.WriteFile(path+".recovery", tc.backup, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.temp != nil {
				if err := os.WriteFile(path+".recovery.tmp", tc.temp, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			s := Open(path, 0, 0)
			now := time.Now().UTC()
			s.Record("WCY", "memory report", now)
			if got := s.Render("WCY", true, now); !strings.Contains(got, "memory report") || !strings.Contains(got, "may be incomplete") {
				t.Fatalf("memory/warning: %q", got)
			}
			if err := s.Close(); err == nil {
				t.Fatal("close unexpectedly saved history")
			}
			got, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(got, tc.original) {
				t.Fatalf("original replaced: %v", err)
			}
			if tc.backup != nil {
				got, err := os.ReadFile(path + ".recovery")
				if err != nil || !bytes.Equal(got, tc.backup) {
					t.Fatalf("backup replaced: %v", err)
				}
			}
		})
	}
}

func TestRecoveryRetriesAfterObstructionRemoved(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	original := []byte("bad original")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".recovery", []byte("conflict"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := Open(path, 0, 0)
	now := time.Now().UTC()
	s.Record("WWV", "new report", now)
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(s.Stats().StorageError, "cannot preserve") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !strings.Contains(s.Stats().StorageError, "cannot preserve") {
		t.Fatalf("preservation failure not reported: %+v", s.Stats())
	}
	if err := os.Remove(path + ".recovery"); err != nil {
		t.Fatal(err)
	}
	// Wake the worker without relying on its five-second ticker.
	s.signal()
	for !s.Stats().StorageHealthy && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !s.Stats().StorageHealthy {
		t.Fatalf("retry did not recover: %+v", s.Stats())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	backup, err := os.ReadFile(path + ".recovery")
	if err != nil || !bytes.Equal(backup, original) {
		t.Fatalf("backup: %q %v", backup, err)
	}
	r := Open(path, 0, 0)
	defer r.Close()
	if got := r.Render("WWV", true, now); !strings.Contains(got, "new report") || !strings.Contains(got, "may be incomplete") {
		t.Fatalf("retry/restart: %q", got)
	}
}

func TestRecoveryRefusesLinkedBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	original := []byte("failed snapshot")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(path, path+".recovery"); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	if err := preserveFailedSnapshot(path); err == nil {
		t.Fatal("hard-linked recovery was accepted")
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("source changed: %v", err)
	}
}

type faultRecoveryFile struct {
	recoveryFile
	stage string
	fail  string
}

var errInjected = errors.New("injected recovery failure")

func (f faultRecoveryFile) Read(p []byte) (int, error) {
	if f.fail == f.stage+"-read" {
		return 0, errInjected
	}
	return f.recoveryFile.Read(p)
}
func (f faultRecoveryFile) Write(p []byte) (int, error) {
	if f.fail == f.stage+"-write" {
		return 0, errInjected
	}
	if f.fail == f.stage+"-short" {
		n, err := f.recoveryFile.Write(p[:len(p)/2])
		if err != nil {
			return n, err
		}
		return n, nil
	}
	return f.recoveryFile.Write(p)
}
func (f faultRecoveryFile) Sync() error {
	if f.fail == f.stage+"-sync" {
		return errInjected
	}
	return f.recoveryFile.Sync()
}
func (f faultRecoveryFile) Close() error {
	err := f.recoveryFile.Close()
	if f.fail == f.stage+"-close" {
		return errInjected
	}
	return err
}

func recoveryOpsWithFault(fail string) recoveryOps {
	ops := defaultRecoveryOps()
	open := ops.open
	ops.open = func(name string, flags int, mode os.FileMode) (recoveryFile, error) {
		stage := "source"
		if strings.HasSuffix(name, ".recovery.tmp") {
			stage = "temp"
		}
		if strings.HasSuffix(name, ".recovery") {
			stage = "backup"
		}
		if fail == stage+"-open" {
			return nil, errInjected
		}
		f, err := open(name, flags, mode)
		if err != nil {
			return nil, err
		}
		return faultRecoveryFile{f, stage, fail}, nil
	}
	if fail == "commit" {
		ops.commit = func(string, string) error { return errInjected }
	}
	if fail == "directory-sync" {
		ops.syncDir = func(string) error { return errInjected }
	}
	return ops
}

func TestRecoveryFaultsNeverReplaceOriginal(t *testing.T) {
	for _, fail := range []string{"source-open", "source-read", "source-close", "temp-open", "temp-write", "temp-short", "temp-sync", "temp-close", "commit", "directory-sync", "backup-sync"} {
		t.Run(fail, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.json")
			original := []byte("unreadable or damaged snapshot")
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatal(err)
			}
			ops := recoveryOpsWithFault(fail)
			want := errInjected
			if fail == "temp-short" {
				want = io.ErrShortWrite
			}
			if err := preserveSnapshot(path, ops); !errors.Is(err, want) {
				t.Fatalf("expected injected error: %v", err)
			}
			got, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(got, original) {
				t.Fatalf("source changed: %v", err)
			}
		})
	}
}

func TestWorkerRecoveryFaultsGateAllWrites(t *testing.T) {
	for _, fail := range []string{"source-read", "temp-short", "temp-sync", "commit", "directory-sync", "backup-sync"} {
		t.Run(fail, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.json")
			original := []byte("damaged original snapshot")
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatal(err)
			}
			ops := recoveryOpsWithFault(fail)
			attempts := make(chan struct{}, 3)
			s := &Store{path: path, needsRecovery: true, dirty: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}), preserveSnapshot: func(path string) error {
				err := preserveSnapshot(path, ops)
				select {
				case attempts <- struct{}{}:
				default:
				}
				return err
			}}
			s.state.Version = 1
			go s.worker()
			t.Cleanup(func() { _ = s.Close() })
			s.Record("WWV", "in memory", time.Now().UTC())
			if got := s.Render("WWV", true, time.Now().UTC()); !strings.Contains(got, "in memory") || !strings.Contains(got, "may be incomplete") {
				t.Fatalf("missing memory/warning: %q", got)
			}
			waitAttempt := func() {
				t.Helper()
				select {
				case <-attempts:
				case <-time.After(5 * time.Second):
					t.Fatal("worker recovery attempt did not finish")
				}
			}
			assertOriginal := func() {
				t.Helper()
				got, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(got, original) {
					t.Fatalf("worker replaced original: %v", err)
				}
			}
			// Wait for dirty handling, then request a retry before shutdown. This
			// prevents the stop case from being the only exercised write gate.
			waitAttempt()
			assertOriginal()
			s.signal()
			waitAttempt()
			assertOriginal()
			if err := s.Close(); err == nil {
				t.Fatal("worker close unexpectedly saved")
			}
			assertOriginal()
		})
	}
}

func TestMissingSourceAfterFailedLoadDoesNotCreateSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json")
	s := &Store{path: path, needsRecovery: true}
	if err := s.save(false); err == nil {
		t.Fatal("missing failed source unexpectedly allowed save")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source recreated: %v", err)
	}
}

func TestMissingSnapshotRequiresConfirmedAbsentEntry(t *testing.T) {
	for _, tc := range []struct {
		name        string
		statErr     error
		wantFailure bool
	}{
		{"absent", os.ErrNotExist, false},
		{"non-directory parent", syscall.ENOTDIR, false},
		{"existing dangling link", nil, true},
		{"metadata failure", os.ErrPermission, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := distinguishMissingSnapshot("history.json", os.ErrNotExist, func(string) (os.FileInfo, error) { return nil, tc.statErr })
			if (err != nil) != tc.wantFailure {
				t.Fatalf("classification error = %v", err)
			}
		})
	}
}

func TestDanglingSnapshotLinkIsNotReplaced(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "history.json")
	if err := os.Symlink(filepath.Join(dir, "absent-target"), path); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	s := Open(path, 0, 0)
	s.Record("WWV", "memory report", time.Now().UTC())
	if err := s.Close(); err == nil {
		t.Fatal("dangling link unexpectedly replaced")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("link not preserved: %v %v", info, err)
	}
}
