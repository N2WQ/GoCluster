package telnet

import (
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

func configurationTestClient(s *Server, call string) *Client {
	c := newTestClient()
	c.server, c.callsign, c.address = s, call, "192.0.2.8:8000"
	c.done = make(chan struct{})
	c.controlChan = make(chan controlMessage, 8)
	return c
}

func waitConfigurationTest(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("configuration operation did not complete")
	}
}

func TestMachineGETOverflowReleasesOwnershipBeforeBlockedReporter(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	s.registerClient(c)
	for range cap(c.controlChan) {
		c.controlChan <- controlMessage{line: "occupied\n"}
	}
	reportEntered, releaseReport := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseReport) }) })
	s.connectionReporter = func(event ConnectionEvent) {
		if event.Action == "disconnect" {
			close(reportEntered)
			<-releaseReport
		}
	}
	getDone := make(chan struct{})
	var queued bool
	go func() {
		queued = s.getMachineConfiguration(c, machineCommand{Verb: "GET", Resource: "SETTINGS", RequestID: "read-1"})
		close(getDone)
	}()
	waitConfigurationTest(t, reportEntered)
	select {
	case <-c.done:
	default:
		t.Fatal("overflow reporting began before mandatory connection cleanup")
	}
	cleanupDone := make(chan struct{})
	go func() { s.unregisterClient(c); close(cleanupDone) }()
	waitConfigurationTest(t, cleanupDone)
	next := configurationTestClient(s, c.callsign)
	_, err := s.restoreAndRegisterClient(next, time.Now().UTC(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("reconnect waited on optional overflow reporter: %v", err)
	}
	releaseOnce.Do(func() { close(releaseReport) })
	waitConfigurationTest(t, getDone)
	if queued {
		t.Fatal("saturated control queue reported GET success")
	}
}

func TestConfigurationSaveAndReconnectHandoff(t *testing.T) {
	s := presetTestServer(t)
	old := configurationTestClient(s, "W1ABC-1")
	old.configuredSettings = filter.SettingsConfiguration{Dialect: "go", NoiseClass: "URBAN", DedupePolicy: "FAST"}
	old.configurationInitialized = true
	old.presetReference = &filter.PresetReference{Name: "CONTEST", Baseline: &filter.SavedPreset{Filter: *filter.NewFilter(), Dialect: "go", NoiseClass: "QUIET", DedupePolicy: "FAST"}}
	s.registerClient(old)
	entered, finish := make(chan struct{}), make(chan struct{})
	s.saveConfigurationFn = func(call string, cfg filter.Configuration, ref *filter.PresetReference, ips []string) error {
		close(entered)
		<-finish
		return filter.SaveConfiguration(call, cfg, ref, ips)
	}
	saved := make(chan struct{})
	var saveErr error
	go func() { saveErr = old.saveFilter(); close(saved) }()
	waitConfigurationTest(t, entered)
	// The old save already owns its transaction. Reconnect must read AFTER its
	// commit, not restore an earlier file and then overwrite the old transaction.
	next := configurationTestClient(s, old.callsign)
	restored := make(chan struct{})
	var restoreErr error
	go func() {
		_, restoreErr = s.restoreAndRegisterClient(next, time.Now().UTC(), time.Now().Add(time.Minute))
		close(restored)
	}()
	// Persistence runs outside all client/registry locks while owning its stripe.
	if !s.clientsMutex.TryLock() {
		t.Fatal("old disk save holds registry lock")
	}
	s.clientsMutex.Unlock()
	if !old.pathMu.TryLock() {
		t.Fatal("old disk save holds path lock")
	}
	old.pathMu.Unlock()
	if !old.filterMu.TryLock() {
		t.Fatal("old disk save holds filter lock")
	}
	old.filterMu.Unlock()
	close(finish)
	waitConfigurationTest(t, saved)
	waitConfigurationTest(t, restored)
	if saveErr != nil || restoreErr != nil {
		t.Fatalf("save=%v restore=%v", saveErr, restoreErr)
	}
	if next.configuredSettings.NoiseClass != "URBAN" || next.presetReference == nil || next.presetReference.Name != "CONTEST" || next.presetReference.Baseline.NoiseClass != "QUIET" {
		t.Fatal("handoff did not restore the committed preferences and baseline together")
	}
	s.saveConfigurationFn = nil
	// Neither a later retired save nor its unconditional teardown can overwrite
	// the successor. Inspect the literal durable bytes after both paths.
	path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	old.configuredSettings.NoiseClass = "INDUSTRIAL"
	if err := old.saveFilter(); !errors.Is(err, errRetiredConfiguration) {
		t.Fatalf("retired save=%v", err)
	}
	s.unregisterClient(old)
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("retired session changed durable state")
	}
	if s.clients[next.callsign] != next {
		t.Fatal("retired teardown removed successor")
	}
	if !strings.Contains(string(after), "noise_class: URBAN") || !strings.Contains(string(after), "name: CONTEST") || !strings.Contains(string(after), "noise_class: QUIET") {
		t.Fatal("disk does not independently contain current preferences and original baseline")
	}
}

func TestConfigurationClosedOwnerFinalSave(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-2")
	c.configuredSettings = filter.SettingsConfiguration{NoiseClass: "URBAN"}
	c.configurationInitialized = true
	s.registerClient(c)
	c.interrupt()
	s.unregisterClient(c)
	data, err := os.ReadFile(filepath.Join(filter.UserDataDir, "W1ABC-2.yaml"))
	if err != nil || !strings.Contains(string(data), "noise_class: URBAN") {
		t.Fatalf("closed current-owner save: %s, %v", data, err)
	}
	if s.clients[c.callsign] != nil {
		t.Fatal("final owner remains registered")
	}
}

func TestConfigurationProtectedTemporaryDefaults(t *testing.T) {
	for _, fixture := range []string{"[broken", "configuration_version: 99\nnoise_class: URBAN\n"} {
		t.Run(fixture, func(t *testing.T) {
			s := presetTestServer(t)
			path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
			if err := os.WriteFile(path, []byte(fixture), 0o644); err != nil {
				t.Fatal(err)
			}
			c := configurationTestClient(s, "W1ABC-1")
			result, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(time.Minute))
			if err != nil || !c.recordProtected || !strings.Contains(result.warning, "temporary defaults") {
				t.Fatalf("restore=%+v, %v", result, err)
			}
			response, _ := s.handlePathSettingsCommand(c, "SET NOISE URBAN")
			if !strings.Contains(response, "warning: failed to persist") {
				t.Fatalf("human temporary mutation=%q", response)
			}
			if c.noiseClass != "URBAN" {
				t.Fatal("temporary human preferences were not applied")
			}
			if err := c.saveFilter(); !errors.Is(err, errProtectedRecord) {
				t.Fatalf("ordinary save=%v", err)
			}
			c.interrupt()
			s.unregisterClient(c)
			data, err := os.ReadFile(path)
			if err != nil || string(data) != fixture {
				t.Fatalf("protected bytes=%q, %v", data, err)
			}
		})
	}
}

func TestConfigurationLoginMetadataFailureRestoresPreferences(t *testing.T) {
	s := presetTestServer(t)
	call := "W1ABC-1"
	if err := filter.SaveUserRecord(call, &filter.UserRecord{Filter: *filter.NewFilter(), Dialect: "cc", Grid: "FN31", NoiseClass: "URBAN", DedupePolicy: "SLOW"}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filter.UserDataDir, call+".yaml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s.saveLoginRecordFn = func(string, *filter.UserRecord) error { return os.ErrPermission }
	c := configurationTestClient(s, call)
	result, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(time.Minute))
	if err != nil || c.recordProtected || !strings.Contains(result.warning, "Restored your saved configuration") {
		t.Fatalf("metadata failure=%+v, %v", result, err)
	}
	if c.configuredSettings.Grid != "FN31" || c.configuredSettings.NoiseClass != "URBAN" || c.dialect != DialectCC || c.getDedupePolicy() != dedupePolicySlow {
		t.Fatal("successful read lost saved preferences")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed metadata save changed old disk state")
	}
}

func TestConfigurationWaitCancellation(t *testing.T) {
	for _, reason := range []string{"client", "shutdown", "deadline"} {
		t.Run(reason, func(t *testing.T) {
			s := &Server{shutdown: make(chan struct{})}
			owner := configurationTestClient(s, "W1ABC-1")
			release, err := s.acquireConfiguration(owner, false, true, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			waiting := configurationTestClient(s, owner.callsign)
			deadline := time.Time{}
			switch reason {
			case "client":
				close(waiting.done)
			case "shutdown":
				close(s.shutdown)
			case "deadline":
				deadline = time.Now().Add(-time.Second)
			}
			_, err = s.acquireConfiguration(waiting, false, true, deadline)
			if err == nil {
				t.Fatal("canceled waiter acquired ownership")
			}
			if !s.clientsMutex.TryLock() || !waiting.pathMu.TryLock() || !waiting.filterMu.TryLock() {
				t.Fatal("canceled wait retained another lock")
			}
			waiting.filterMu.Unlock()
			waiting.pathMu.Unlock()
			s.clientsMutex.Unlock()
		})
	}
}
