package peerdiag

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/internal/logutil"
)

func testEvent(sequence uint64, at time.Time, kind Kind, line string) Event {
	event := Event{Sequence: sequence, UnixNano: at.UnixNano(), Kind: kind, Length: uint16(len(line))}
	copy(event.Data[:], line)
	return event
}

func TestV15HelperDailyRotationAndRetention(t *testing.T) {
	dir := t.TempDir()
	day := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	s := helperSink{options: Options{Enabled: true, Directory: dir, RetentionDays: 2}}
	defer func() {
		if err := s.close(); err != nil {
			t.Error(err)
		}
	}()
	old := logutil.DailyArchivePath(dir, day.AddDate(0, 0, -3))
	if err := os.WriteFile(old, []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	if s.write(testEvent(1, day, Diagnostic, "event=first")) != ackWritten {
		t.Fatal("first write failed")
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("expired archive retained")
	}
	if s.write(testEvent(2, day.AddDate(0, 0, 1), Diagnostic, "event=second")) != ackWritten {
		t.Fatal("rollover write failed")
	}
	archive, err := os.ReadFile(logutil.DailyArchivePath(dir, day))
	if err != nil || string(archive) != "2026/10/02 00:00:00 event=first\n" {
		t.Fatalf("archive=%q err=%v", archive, err)
	}
	active, err := os.ReadFile(logutil.DailyActivePath(dir))
	if err != nil || string(active) != "2026/10/03 00:00:00 event=second\n" {
		t.Fatalf("active=%q err=%v", active, err)
	}
}

func TestV15HelperOverlongRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "overlong.log")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Truncate(8 << 20); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if err = os.WriteFile(path+".1", []byte("older"), 0644); err != nil {
		t.Fatal(err)
	}
	s := helperSink{options: Options{OverlongPath: path}}
	if s.write(testEvent(1, time.Now(), Overlong, "event=peer_overlong detail=preview")) != ackWritten {
		t.Fatal("sample write failed")
	}
	if info, err := os.Stat(path + ".1"); err != nil || info.Size() != 8<<20 {
		t.Fatal("current file not rotated")
	}
	if data, err := os.ReadFile(path + ".2"); err != nil || string(data) != "older" {
		t.Fatal("backup rotation changed")
	}
}

func TestV15HelperLargeDirectoryBounded(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 2000; i++ {
		file, err := os.CreateTemp(dir, "unrelated-")
		if err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}
	s := helperSink{options: Options{Directory: dir, RetentionDays: 1}}
	if err := s.cleanup(time.Now()); err != nil {
		t.Fatal(err)
	}
	// Existing malformed/timestamp-free tails use modification date, and
	// newline-dense tails are scanned without allocating a per-line slice.
	path := filepath.Join(dir, "dense.log")
	if err := os.WriteFile(path, []byte(strings.Repeat("\n", 64<<10)), 0644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.activeDate(path, info); err != nil || !got.Equal(dateOnly(info.ModTime())) {
		t.Fatal("timestamp-free fallback changed")
	}
}

func TestV15HelperCloseFailureStopsRollover(t *testing.T) {
	day := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	s := helperSink{options: Options{Enabled: true, Directory: t.TempDir()}}
	if s.write(testEvent(1, day, Diagnostic, "first")) != ackWritten {
		t.Fatal("initial write failed")
	}
	// Closing the actual file supplies a deterministic native Close failure
	// at rollover without an arbitrary callback in the production sink.
	if err := s.file.Close(); err != nil {
		t.Fatal(err)
	}
	if s.write(testEvent(2, day.AddDate(0, 0, 1), Diagnostic, "second")) != ackFailed || s.file != nil {
		t.Fatal("uncertain close allowed a replacement file")
	}
}
