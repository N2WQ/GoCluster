package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/filter"
)

func v7NoticeFixture(t *testing.T) (*Server, *Client, *filter.V7UserRecord, string) {
	t.Helper()
	s := presetTestServer(t)
	configuration, err := filter.DefaultV7Configuration(filter.SettingsConfiguration{Dialect: "cc", Grid: "FN31", NoiseClass: "URBAN", DedupePolicy: "FAST"})
	if err != nil {
		t.Fatal(err)
	}
	configuration.Filters.CC.Modes = []string{"CW"}
	configuration.Filters.CC.Sources = []string{"HUMAN"}
	configuration, _, cc, err := filter.PrepareV7Configuration(configuration)
	if err != nil {
		t.Fatal(err)
	}
	c := configurationTestClient(s, "W1ABC-1")
	c.retired = make(chan struct{})
	c.filter = &cc
	c.filterBanks = &configuration.Filters
	c.configuredSettings = configuration.Settings
	c.configurationInitialized = true
	c.dialect = DialectCC
	c.v7Notice = filter.V7NoticeState{ProfileResetGeneration: 7}
	record := &filter.V7UserRecord{ConfigurationVersion: 7, Configuration: configuration, Notice: c.v7Notice, RecentIPs: []string{"192.0.2.1"}, LastLoginUTC: time.Unix(1700000000, 0).UTC()}
	payload, err := filter.PrepareV7UserRecord(c.callsign, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := payload.Commit(); err != nil {
		t.Fatal(err)
	}
	s.registerClient(c)
	t.Cleanup(c.interrupt)
	return s, c, record, filepath.Join(filter.UserDataDir, c.callsign+".yaml")
}

func writeV7NoticeLibrary(t *testing.T, c *Client, generation int) {
	t.Helper()
	data, err := filter.EncodeV7PresetCollection(&filter.V7PresetCollection{ConfigurationVersion: 7, ResetGeneration: generation, Presets: map[string]*filter.V7SavedPreset{}})
	if err != nil {
		t.Fatal(err)
	}
	path := presetLibraryPathFor(c.callsign)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestV7ResetNoticeAcknowledgementOnlyUpdatesFreshMetadata(t *testing.T) {
	s, c, record, _ := v7NoticeFixture(t)
	// A complete restore reached disk after the notice snapshot was captured.
	record.Configuration.Settings.Grid = "EM10"
	record.Configuration.Filters.CC.Modes = []string{"RTTY"}
	payload, err := filter.PrepareV7UserRecord(c.callsign, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := payload.Commit(); err != nil {
		t.Fatal(err)
	}
	// Independent unsaved live edits must neither be repaired nor overwritten.
	live, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	live.Filters.CC.Modes = []string{"FT8"}
	canonical, _, cc, err := filter.PrepareV7Configuration(live)
	if err != nil {
		t.Fatal(err)
	}
	c.filterBanks = &canonical.Filters
	*c.filter = cc
	c.configurationUnsaved = true
	before, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	revision, history := c.configurationRevision, c.historyGeneration
	if err := s.acknowledgeV7ResetNotice(c, v7ResetNoticeEvents{profileGeneration: 7}); err != nil {
		t.Fatal(err)
	}
	after, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	current, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	if !after.Configuration.Equal(record.Configuration) || !current.Equal(before) || !c.configurationUnsaved || c.configurationRevision != revision || c.historyGeneration != history || after.Notice.ProfileAcknowledgedGeneration != 7 || c.v7Notice != after.Notice {
		t.Fatal("acknowledgement changed preferences/dirty state or lost fresh metadata")
	}
}

func TestV7ResetNoticeStrictAcknowledgementNeverCreatesOrResets(t *testing.T) {
	for _, fault := range []string{"missing", "malformed", "pre7", "different-event", "commit"} {
		t.Run(fault, func(t *testing.T) {
			s, c, record, path := v7NoticeFixture(t)
			switch fault {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "malformed":
				if err := os.WriteFile(path, []byte("[broken"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "pre7":
				if err := os.WriteFile(path, []byte("configuration_version: 6\nconfiguration: {filters: {rules: []}, settings: {}}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "different-event":
				record.Notice = filter.V7NoticeState{}
				payload, err := filter.PrepareV7UserRecord(c.callsign, record)
				if err != nil {
					t.Fatal(err)
				}
				if err := payload.Commit(); err != nil {
					t.Fatal(err)
				}
			case "commit":
				s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { return os.ErrPermission }
			}
			before, readErr := os.ReadFile(path)
			if err := s.acknowledgeV7ResetNotice(c, v7ResetNoticeEvents{profileGeneration: 7}); err == nil {
				t.Fatal("failed acknowledgement was accepted")
			}
			after, afterErr := os.ReadFile(path)
			if errors.Is(readErr, os.ErrNotExist) {
				if !errors.Is(afterErr, os.ErrNotExist) {
					t.Fatal("acknowledgement created missing profile")
				}
			} else if afterErr != nil || !bytes.Equal(before, after) {
				t.Fatal("failed acknowledgement changed original bytes")
			}
			if c.v7Notice.ProfileAcknowledgedGeneration != 0 {
				t.Fatal("failed acknowledgement cleared in-memory notice")
			}
		})
	}
}

func TestV7ResetNoticeAcknowledgesOnlyCapturedComponents(t *testing.T) {
	s, c, _, _ := v7NoticeFixture(t)
	writeV7NoticeLibrary(t, c, 7)
	// The library completed after this profile-only notice was captured.
	if err := s.acknowledgeV7ResetNotice(c, v7ResetNoticeEvents{profileGeneration: 7}); err != nil {
		t.Fatal(err)
	}
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	if record.Notice.LibraryAcknowledgedGeneration != 0 {
		t.Fatal("undelivered library event was acknowledged")
	}
	c.v7LibraryResetGeneration = 7
	if pending := pendingV7ResetNotice(c); pending.profileGeneration != 0 || pending.libraryGeneration != 7 {
		t.Fatal("later library event is not pending")
	}
	if err := s.acknowledgeV7ResetNotice(c, v7ResetNoticeEvents{libraryGeneration: 7}); err != nil {
		t.Fatal(err)
	}
	if !pendingV7ResetNotice(c).empty() {
		t.Fatal("successfully delivered library event remains pending")
	}
}

func TestV7ResetNoticeWaitReleasesLeaseAndPreparation(t *testing.T) {
	s, c, _, _ := v7NoticeFixture(t)
	result := make(chan bool, 1)
	go func() { result <- s.sendV7ResetNotice(c) }()
	var message controlMessage
	select {
	case message = <-c.controlChan:
	case <-time.After(3 * time.Second):
		t.Fatal("notice was not queued")
	}
	if message.resetNotice == nil || !strings.Contains(string(message.raw), "earlier upgrade") {
		t.Fatal("missing historical notice receipt/text")
	}
	_, output := configurationTransferBudgetStats()
	if output != 0 {
		t.Fatal("notice waits while retaining preparation charge")
	}
	acquired := make(chan struct{})
	go func() {
		release, err := s.acquireConfiguration(c, false, false, time.Time{})
		if err == nil {
			release()
		}
		close(acquired)
	}()
	waitConfigurationTest(t, acquired)
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	if record.Notice.ProfileAcknowledgedGeneration != 0 {
		t.Fatal("enqueue was mistaken for delivery")
	}
	_, _ = s.handleReadPauseCommand(c, "PAUSE 5")
	_, _ = s.handleReadPauseCommand(c, "RESUME")
	message.resetNotice.signal()
	message.resetNotice.signal()
	select {
	case ok := <-result:
		if !ok {
			t.Fatal("successful receipt did not continue connection")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("receipt owner did not finish")
	}
	message.resetNotice.signal()
	if len(message.resetNotice.delivered) != 0 {
		t.Fatal("receipt signaled more than once")
	}
	record, err = filter.LoadV7UserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	if record.Notice.ProfileAcknowledgedGeneration != 7 {
		t.Fatal("successful flush opportunity was not acknowledged")
	}
}

func TestV7ResetNoticeWriterRequiresCompleteFlush(t *testing.T) {
	for _, behavior := range []string{"success", "write_failure", "flush_failure"} {
		t.Run(behavior, func(t *testing.T) {
			s, c, _, _ := v7NoticeFixture(t)
			s.writerBatchMaxBytes = 2048
			s.writerBatchWait = time.Millisecond
			conn := &readbackDeliveryConn{entered: make(chan struct{}), release: make(chan struct{})}
			buffer := 4096
			if behavior == "write_failure" {
				buffer = 128
			}
			if behavior != "success" {
				conn.err = errors.New("injected notice delivery failure")
			}
			c.conn = conn
			c.writer = bufio.NewWriterSize(conn, buffer)
			c.spotChan = make(chan *spotEnvelope, 1)
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(conn.release) }) })
			writerDone := writerV15Start(c)
			result := make(chan bool, 1)
			go func() { result <- s.sendV7ResetNotice(c) }()
			writerV15Wait(t, conn.entered, "notice write entered")
			record, err := filter.LoadV7UserRecord(c.callsign)
			if err != nil {
				t.Fatal(err)
			}
			if record.Notice.ProfileAcknowledgedGeneration != 0 {
				t.Fatal("notice acknowledged before flush")
			}
			once.Do(func() { close(conn.release) })
			select {
			case ok := <-result:
				if ok != (behavior == "success") {
					t.Fatalf("connection result=%v for %s", ok, behavior)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("notice sender did not complete")
			}
			c.interrupt()
			writerV15Wait(t, writerDone, "notice writer stopped")
			record, err = filter.LoadV7UserRecord(c.callsign)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if behavior == "success" {
				want = 7
			}
			if record.Notice.ProfileAcknowledgedGeneration != want {
				t.Fatal("notice acknowledgement did not follow complete flush")
			}
		})
	}
}

func TestV7ResetNoticeRetirementAndShutdownKeepPending(t *testing.T) {
	for _, cause := range []string{"retired", "shutdown"} {
		t.Run(cause, func(t *testing.T) {
			s, c, _, _ := v7NoticeFixture(t)
			s.shutdown = make(chan struct{})
			retirement := make(chan struct{})
			s.testCommandPacingRetired = func(*Client) { close(retirement) }
			result := make(chan bool, 1)
			go func() { result <- s.sendV7ResetNotice(c) }()
			select {
			case <-c.controlChan:
			case <-time.After(3 * time.Second):
				t.Fatal("notice not queued")
			}
			if cause == "retired" {
				close(c.retired)
				waitConfigurationTest(t, retirement)
				select {
				case <-result:
					t.Fatal("retired reader did not wait for duplicate-notice writer")
				default:
				}
				c.interrupt()
			} else {
				close(s.shutdown)
			}
			select {
			case ok := <-result:
				if ok {
					t.Fatal("departed notice owner continued")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("notice wait did not cancel")
			}
			record, err := filter.LoadV7UserRecord(c.callsign)
			if err != nil {
				t.Fatal(err)
			}
			if record.Notice.ProfileAcknowledgedGeneration != 0 {
				t.Fatal("canceled notice was acknowledged")
			}
		})
	}
}

func TestV7ResetNoticeRetiredOwnerCannotAcknowledge(t *testing.T) {
	s, c, _, path := v7NoticeFixture(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	replacement := configurationTestClient(s, c.callsign)
	s.clientsMutex.Lock()
	s.clients[c.callsign] = replacement
	s.clientsMutex.Unlock()
	if err := s.acknowledgeV7ResetNotice(c, v7ResetNoticeEvents{profileGeneration: 7}); !errors.Is(err, errRetiredConfiguration) {
		t.Fatalf("stale owner ack: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("stale owner changed profile")
	}
}

func TestV7ResetNoticeBusyPreparationIsTruthfulAndUnacknowledged(t *testing.T) {
	s, c, _, _ := v7NoticeFixture(t)
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Release()
	if !s.sendV7ResetNotice(c) {
		t.Fatal("busy fallback unnecessarily closed connection")
	}
	message := <-c.controlChan
	if message.resetNotice != nil || !strings.Contains(string(message.raw), "still pending") {
		t.Fatal("busy fallback pretended completed notice delivery")
	}
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	if record.Notice.ProfileAcknowledgedGeneration != 0 {
		t.Fatal("busy fallback acknowledged notice")
	}
}
