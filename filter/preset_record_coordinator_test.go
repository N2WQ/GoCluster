package filter

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSavePresetAndRecordAdmissionAndOrder(t *testing.T) {
	usePresetTestDir(t)
	set := testSavedPreset()
	prepared, err := PrepareConfiguration("N2WQ-1", ConfigurationFromPreset(set), &PresetReference{Name: "TEST", Baseline: set}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		name                      string
		prepare                   func() (*PreparedUserRecord, error)
		libraryError, recordError error
		wantSaved                 bool
		want                      []string
	}{
		{"nil preparation", nil, nil, nil, false, nil},
		{"preparation refusal", func() (*PreparedUserRecord, error) { return nil, ErrStorageAdmission }, nil, nil, false, []string{"prepare"}},
		{"nil payload", func() (*PreparedUserRecord, error) {
			return nil, nil //nolint:nilnil // Deliberately invalid callback result must be refused before either writer.
		}, nil, nil, false, []string{"prepare"}},
		{"zero payload", func() (*PreparedUserRecord, error) { return &PreparedUserRecord{}, nil }, nil, nil, false, []string{"prepare"}},
		{"oversized payload", func() (*PreparedUserRecord, error) {
			return &PreparedUserRecord{path: prepared.path, data: make([]byte, MaxUserRecordBytes+1)}, nil
		}, nil, nil, false, []string{"prepare"}},
		{"library failure", func() (*PreparedUserRecord, error) { return prepared, nil }, os.ErrPermission, nil, false, []string{"prepare", "library"}},
		{"record failure", func() (*PreparedUserRecord, error) { return prepared, nil }, nil, os.ErrPermission, true, []string{"prepare", "library", "record"}},
		{"success", func() (*PreparedUserRecord, error) { return prepared, nil }, nil, nil, true, []string{"prepare", "library", "record"}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			var events []string
			store := presetStore{write: func(_ string, _ []byte) error { events = append(events, "library"); return fixture.libraryError }}
			prepare := fixture.prepare
			if prepare != nil {
				original := prepare
				prepare = func() (*PreparedUserRecord, error) { events = append(events, "prepare"); return original() }
			}
			saved, err := store.saveWithRecord("N2WQ-1", "TEST", set, prepare, func(payload *PreparedUserRecord) error {
				events = append(events, "record")
				if payload != prepared {
					t.Fatal("commit received different payload")
				}
				return fixture.recordError
			}, true)
			if saved != fixture.wantSaved || !reflect.DeepEqual(events, fixture.want) || (fixture.name == "success") != (err == nil) {
				t.Fatalf("saved=%t events=%v err=%v", saved, events, err)
			}
		})
	}
}

func TestSavePresetAndRecordCommitsCapturedBytes(t *testing.T) {
	usePresetTestDir(t)
	set := testSavedPreset()
	var preparedBytes, libraryBytes []byte
	var target string
	store := presetStore{write: func(path string, data []byte) error {
		libraryBytes = bytes.Clone(data)
		return writeAtomicUserFile(path, data)
	}}
	saved, err := store.saveWithRecord("N2WQ-1", "TEST", set, func() (*PreparedUserRecord, error) {
		prepared, err := PrepareConfiguration("N2WQ-1", ConfigurationFromPreset(set), &PresetReference{Name: "TEST", Baseline: set}, []string{"old"})
		if err != nil {
			return nil, err
		}
		preparedBytes, target = bytes.Clone(prepared.data), prepared.path
		// Library bytes already exist; association preparation has also captured
		// its own immutable bytes before this callback changes the source.
		set.Grid = "CHANGED"
		set.DXCallsigns = []string{"CHANGED"}
		return prepared, nil
	}, nil, true)
	if err != nil || !saved {
		t.Fatalf("coordinated commit: %v", err)
	}
	disk, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(disk, preparedBytes) {
		t.Fatal("record commit rematerialized inputs")
	}
	libraryPath, err := presetCollectionPath("N2WQ")
	if err != nil {
		t.Fatal(err)
	}
	disk, err = os.ReadFile(libraryPath)
	if err != nil || !bytes.Equal(disk, libraryBytes) || bytes.Contains(disk, []byte("CHANGED")) {
		t.Fatal("library commit changed prepared bytes")
	}
}

func TestSavePresetAndRecordPublicDefaultCommit(t *testing.T) {
	usePresetTestDir(t)
	set := testSavedPreset()
	saved, err := SavePresetAndRecord("N2WQ-1", "TEST", set, func() (*PreparedUserRecord, error) {
		return PrepareConfiguration("N2WQ-1", ConfigurationFromPreset(set), &PresetReference{Name: "TEST", Baseline: set}, nil)
	}, nil)
	if err != nil || !saved {
		t.Fatal("public coordinator failed", err)
	}
	record, err := LoadUserRecord("N2WQ-1")
	if err != nil || record.Preset == nil || record.Preset.Name != "TEST" {
		t.Fatal("record not committed", err)
	}
	if _, err := LoadPreset("N2WQ-2", "TEST"); err != nil {
		t.Fatal("shared library not committed", err)
	}
}

func TestSavePresetAndRecordHoldsCollectionOwnership(t *testing.T) {
	usePresetTestDir(t)
	if err := SavePreset("N2WQ-2", "OLD", testSavedPreset()); err != nil {
		t.Fatal(err)
	}
	path, err := presetCollectionPath("N2WQ-1")
	if err != nil {
		t.Fatal(err)
	}
	mu := presetCollectionLock(path)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	saved := make(chan error, 1)
	savedDone := make(chan struct{})
	var deletedDone chan struct{}
	join := func(label string, done <-chan struct{}) bool {
		select {
		case <-done:
			return true
		case <-time.After(5 * time.Second):
			t.Errorf("%s worker did not join", label)
			return false
		}
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		join("SAVE", savedDone)
		if deletedDone != nil {
			join("DELETE", deletedDone)
		}
	})
	go func() {
		defer close(savedDone)
		_, err := SavePresetAndRecord("N2WQ-1", "NEW", testSavedPreset(), func() (*PreparedUserRecord, error) {
			close(entered)
			<-release
			return PrepareUserRecord("N2WQ-1", &UserRecord{})
		}, func(record *PreparedUserRecord) error {
			if mu.TryLock() {
				mu.Unlock()
				return errors.New("record commit did not retain collection ownership")
			}
			return record.Commit()
		})
		saved <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("preparation did not start")
	}
	if mu.TryLock() {
		mu.Unlock()
		t.Fatal("preparation did not retain collection ownership")
	}
	deleted := make(chan error, 1)
	deletedDone = make(chan struct{})
	go func() { defer close(deletedDone); deleted <- DeletePreset("N2WQ-2", "OLD") }()
	releaseOnce.Do(func() { close(release) })
	if !join("SAVE", savedDone) || !join("DELETE", deletedDone) {
		t.FailNow()
	}
	if err := <-saved; err != nil {
		t.Fatal(err)
	}
	if err := <-deleted; err != nil {
		t.Fatal(err)
	}
	names, err := ListPresets("N2WQ-2")
	if err != nil || !reflect.DeepEqual(names, []string{"NEW"}) {
		t.Fatalf("lost concurrent update: %v %v", names, err)
	}
}

func TestSavePresetAndRecordRealReplacementFailure(t *testing.T) {
	usePresetTestDir(t)
	// An existing nonempty directory permits preparation and temp-file writes,
	// then rejects the real record replacement after the library was committed.
	target := userRecordPath("N2WQ-1")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "retained"), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	saved, err := SavePresetAndRecord("N2WQ-1", "TEST", testSavedPreset(), func() (*PreparedUserRecord, error) { return PrepareUserRecord("N2WQ-1", &UserRecord{}) }, nil)
	if !saved || err == nil {
		t.Fatalf("real association replacement fault missed: saved=%t err=%v", saved, err)
	}
	if _, err := LoadPreset("N2WQ-2", "TEST"); err != nil {
		t.Fatal("partial success lost library", err)
	}
	retained, err := os.ReadFile(filepath.Join(target, "retained"))
	if err != nil || string(retained) != "old" {
		t.Fatal("failed association replacement changed target")
	}
}
