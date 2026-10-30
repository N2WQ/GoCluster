package telnet

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/filter"
)

func exhaustConfigurationPreparationBudget(t *testing.T) {
	t.Helper()
	input, output := configurationTransferBudgetStats()
	if output != 0 {
		t.Fatalf("prior owner retained %d configuration output bytes", output)
	}
	for range configurationOutputBudget / configurationOutputReservationBytes {
		owner, err := (&Client{}).reserveConfigurationOutput()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(owner.Release)
	}
	// This cleanup runs after the per-owner cleanups registered above only when
	// the caller explicitly registers its final assertion before this helper.
	assertConfigurationTransferStats(t, input, configurationOutputBudget)
}

func TestConfigurationPreparationBudgetRefusesAcknowledgementAndFinalSaveBeforeWork(t *testing.T) {
	for _, operation := range []string{"acknowledgement", "final-save"} {
		t.Run(operation, func(t *testing.T) {
			s, c, _, path := v7NoticeFixture(t)
			input, output := configurationTransferBudgetStats()
			t.Cleanup(func() { assertConfigurationTransferStats(t, input, output) })
			// An attempted strict load would return a document error. Budget
			// refusal must precede it, not merely prevent the eventual commit.
			original := []byte("configuration_version: [invalid metadata\n")
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatal(err)
			}
			commits := 0
			s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
			c.configurationInitialized = false // Capture would initialize it.
			exhaustConfigurationPreparationBudget(t)
			var err error
			if operation == "acknowledgement" {
				err = s.acknowledgeV7ResetNotice(c, v7ResetNoticeEvents{profileGeneration: 7})
			} else {
				c.interrupt()
				release, leaseErr := s.acquireConfiguration(c, true, false, time.Time{})
				if leaseErr != nil {
					t.Fatal(leaseErr)
				}
				err = c.saveFilterOwned()
				release()
			}
			if !errors.Is(err, errConfigurationTransferBusy) || commits != 0 || c.configurationInitialized {
				t.Fatalf("exhausted preparation performed capture/load/commit: err=%v commits=%d initialized=%t", err, commits, c.configurationInitialized)
			}
			if got, readErr := os.ReadFile(path); readErr != nil || !bytes.Equal(got, original) {
				t.Fatalf("preparation refusal changed current storage: %v", readErr)
			}
			if c.v7Notice.ProfileAcknowledgedGeneration != 0 || c.configurationOutputHeld {
				t.Fatal("refusal changed receipt metadata or retained a partial owner")
			}
		})
	}
}

func TestConfigurationPreparationBudgetRefusesEveryPresetVerbBeforeStorage(t *testing.T) {
	for _, verb := range []string{"LIST", "DELETE", "SAVE", "LOAD"} {
		t.Run(verb, func(t *testing.T) {
			s, c, _, profilePath := v7NoticeFixture(t)
			input, output := configurationTransferBudgetStats()
			t.Cleanup(func() { assertConfigurationTransferStats(t, input, output) })
			libraryPath := filepath.Join(filter.UserDataDir, "presets", hex.EncodeToString([]byte("W1ABC"))+".yaml")
			if err := os.MkdirAll(filepath.Dir(libraryPath), 0o700); err != nil {
				t.Fatal(err)
			}
			// Reading this trap would fail strict decoding. Budget refusal must
			// occur before any library decode/reset or active-bank capture.
			original := []byte("presets: [invalid library\n")
			if err := os.WriteFile(libraryPath, original, 0o600); err != nil {
				t.Fatal(err)
			}
			profile := presetDiskBytes(t, profilePath)
			c.configurationInitialized = false
			exhaustConfigurationPreparationBudget(t)
			command := verb + " PRESET"
			if verb != "LIST" {
				command += " FRESH"
			}
			response, handled := s.handlePresetCommand(c, command)
			if !handled || !strings.Contains(response, errConfigurationTransferBusy.Error()) || c.configurationInitialized || c.configurationOutputHeld {
				t.Fatalf("exhausted preset performed work: handled=%t response=%q initialized=%t held=%t", handled, response, c.configurationInitialized, c.configurationOutputHeld)
			}
			if !bytes.Equal(presetDiskBytes(t, libraryPath), original) || !bytes.Equal(presetDiskBytes(t, profilePath), profile) {
				t.Fatal("budget refusal changed profile or library")
			}
			if _, err := os.Stat(libraryPath + ".pre-v7.bak"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("budget refusal entered library backup: %v", err)
			}
		})
	}
}

func TestConfigurationPreparationClosedCurrentOwnerAndRetirement(t *testing.T) {
	s, c, _, _ := v7NoticeFixture(t)
	input, output := configurationTransferBudgetStats()
	t.Cleanup(func() { assertConfigurationTransferStats(t, input, output) })
	s.shutdown = make(chan struct{})
	c.interrupt()
	close(s.shutdown)
	release, err := s.acquireConfiguration(c, true, false, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	finishLease := func() { releaseOnce.Do(release) }
	t.Cleanup(finishLease)
	owner, err := c.reserveConfigurationPreparation()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Release)
	if !owner.preparationOnly {
		t.Fatal("closed preparation acquired queue authority")
	}
	if queued, err := c.reserveConfigurationOutput(); queued != nil || !errors.Is(err, errClientClosed) {
		t.Fatalf("closed public output admitted: owner=%v err=%v", queued != nil, err)
	}
	if second, err := c.reserveConfigurationPreparation(); second != nil || !errors.Is(err, errConfigurationTransferBusy) {
		t.Fatalf("second preparation owner admitted: owner=%v err=%v", second != nil, err)
	}
	owner.Release()
	owner.Release()
	assertConfigurationTransferStats(t, input, output)
	commits := 0
	s.commitConfigurationFn = func(record *filter.PreparedUserRecord) error {
		commits++
		assertConfigurationTransferStats(t, input, output+configurationOutputReservationBytes)
		return record.Commit()
	}
	c.configurationUnsaved = true
	if err := c.saveFilterOwned(); err != nil {
		t.Fatal(err)
	}
	finishLease()
	if commits != 1 || c.configurationUnsaved {
		t.Fatal("closed current owner failed to finish its bounded save")
	}
	assertConfigurationTransferStats(t, input, output)
	// The preparation helper grants no ownership itself: the enclosing final
	// lease still refuses a displaced session before it can enter the helper.
	s.clientsMutex.Lock()
	s.clients[c.callsign] = configurationTestClient(s, c.callsign)
	s.clientsMutex.Unlock()
	if release, err := s.acquireConfiguration(c, true, false, time.Time{}); release != nil || !errors.Is(err, errRetiredConfiguration) {
		if release != nil {
			release()
		}
		t.Fatalf("retired final owner acquired configuration authority: %v", err)
	}
	assertConfigurationTransferStats(t, input, output)
}

func TestConfigurationPreparationAndOutputMutuallyExcludeAndCannotQueue(t *testing.T) {
	for _, human := range []bool{false, true} {
		t.Run(map[bool]string{false: "machine", true: "human"}[human], func(t *testing.T) {
			input, output := configurationTransferBudgetStats()
			c := &Client{done: make(chan struct{}), controlChan: make(chan controlMessage, 1)}
			owner, err := c.reserveConfigurationPreparation()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(owner.Release)
			if second, err := c.reserveConfigurationOutput(); second != nil || !errors.Is(err, errConfigurationTransferBusy) {
				t.Fatalf("queued output bypassed preparation owner: %v", err)
			}
			if human {
				err = c.enqueueHumanReadback(owner, []byte("human frame\r\n"), readbackCompletion{epoch: 1})
			} else {
				err = c.enqueueConfigurationResponse(owner, []byte("---\r\n...\r\n"), true)
			}
			if !errors.Is(err, errConfigurationPreparationOnly) || len(c.controlChan) != 0 || c.preserveRecordOnExit.Load() {
				t.Fatalf("preparation owner entered queue/terminal state: err=%v queued=%d", err, len(c.controlChan))
			}
			assertConfigurationTransferStats(t, input, output)
			queued, err := c.reserveConfigurationOutput()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(queued.Release)
			if second, err := c.reserveConfigurationPreparation(); second != nil || !errors.Is(err, errConfigurationTransferBusy) {
				t.Fatalf("preparation bypassed queued output owner: %v", err)
			}
			queued.Release()
			assertConfigurationTransferStats(t, input, output)
		})
	}
}
