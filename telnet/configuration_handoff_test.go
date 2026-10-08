package telnet

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func TestConfigurationHandoffDeadlineBehindOldOwnershipCheck(t *testing.T) {
	s := presetTestServer(t)
	old := configurationTestClient(s, "W1ABC-1")
	old.filter = &filter.Filter{Bands: map[string]bool{"20m": true, "40m": false}}
	old.configuredSettings = filter.SettingsConfiguration{
		Dialect: "go", NoiseClass: "URBAN", DedupePolicy: "FAST", PathMinObservationCount: 25, SolarSummaryMinutes: 15,
	}
	old.configurationInitialized = true
	old.presetReference = &filter.PresetReference{Name: "CONTEST", Baseline: &filter.SavedPreset{
		ConfigurationVersion: filter.CurrentConfigurationVersion, Filter: filter.Filter{Bands: map[string]bool{"20m": true, "40m": false}},
		Dialect: "go", NoiseClass: "QUIET", DedupePolicy: "FAST",
	}}
	s.registerClient(old)
	// The registry write lock blocks the old save at its ownership check, after
	// it obtains the fixed stripe. Reconnect must wait on that stripe first.
	s.clientsMutex.Lock()
	var releaseRegistry sync.Once
	unlockRegistry := func() { releaseRegistry.Do(s.clientsMutex.Unlock) }
	saved := make(chan struct{})
	var saveErr error
	go func() { saveErr = old.saveFilter(); close(saved) }()
	t.Cleanup(func() {
		unlockRegistry()
		select {
		case <-saved:
		case <-time.After(3 * time.Second):
			t.Error("old save did not finish during test cleanup")
		}
	})
	stripe := s.configurationTxnStripes[configurationStripe(old.callsign)]
	observeUntil := time.Now().Add(3 * time.Second)
	for len(stripe) != 1 && time.Now().Before(observeUntil) {
		runtime.Gosched()
	}
	if len(stripe) != 1 {
		t.Fatal("old save never obtained transaction ownership")
	}
	waiting := configurationTestClient(s, old.callsign)
	loginTime := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	originalDeadline := time.Now().Add(25 * time.Millisecond)
	reconnectDone := make(chan struct{})
	var reconnectErr error
	go func() {
		_, reconnectErr = s.restoreAndRegisterClient(waiting, loginTime, originalDeadline)
		close(reconnectDone)
	}()
	waitUntil := time.Now().Add(3 * time.Second)
waitReconnect:
	for {
		select {
		case <-reconnectDone:
			break waitReconnect
		default:
		}
		// Sample throughout the ownership wait, rather than only after its
		// cancellation has already released any accidentally retained lock.
		assertHandoffClientLocksAvailable(t, old, waiting)
		if time.Now().After(waitUntil) {
			waiting.interrupt()
			t.Fatal("reconnect could not expire while the old save waited for registry ownership")
		}
		runtime.Gosched()
	}
	if reconnectErr == nil || reconnectErr.Error() != "configuration handoff exceeded login deadline" || time.Now().Before(originalDeadline) {
		t.Fatalf("reconnect did not retain its original deadline: %v", reconnectErr)
	}
	if len(stripe) != 1 {
		t.Fatal("expired reconnect released or replaced the old save's stripe ownership")
	}
	assertHandoffClientLocksAvailable(t, old, waiting)
	path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("blocked ownership or expired reconnect wrote a user record")
	}
	unlockRegistry()
	waitConfigurationTest(t, saved)
	if saveErr != nil {
		t.Fatalf("old current-owner save failed after registry release: %v", saveErr)
	}
	assertHandoffDiskLiteral(t, path)
	next := configurationTestClient(s, old.callsign)
	if _, err := s.restoreAndRegisterClient(next, loginTime, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("later reconnect did not restore the completed save: %v", err)
	}
	if next.configuredSettings != (filter.SettingsConfiguration{Dialect: "go", NoiseClass: "URBAN", DedupePolicy: "FAST", PathMinObservationCount: 25, SolarSummaryMinutes: 15}) {
		t.Fatalf("reconnect restored different preferences: %+v", next.configuredSettings)
	}
	if len(next.filter.Bands) != 2 || !next.filter.Bands["20m"] || next.filter.Bands["40m"] {
		t.Fatal("reconnect lost literal enabled/disabled filter entries")
	}
	ref := next.presetReference
	if ref == nil || ref.Name != "CONTEST" || ref.Baseline == nil || ref.Baseline.NoiseClass != "QUIET" || ref.Baseline.Dialect != "go" || ref.Baseline.DedupePolicy != "FAST" || len(ref.Baseline.Bands) != 2 || !ref.Baseline.Bands["20m"] || ref.Baseline.Bands["40m"] {
		t.Fatal("reconnect did not restore the same independent preset reference")
	}
	assertHandoffDiskLiteral(t, path)
}

func assertHandoffClientLocksAvailable(t *testing.T, clients ...*Client) {
	t.Helper()
	for i, c := range clients {
		for _, lock := range []struct {
			name string
			mu   *sync.RWMutex
		}{{"path", &c.pathMu}, {"filter", &c.filterMu}} {
			if !lock.mu.TryLock() {
				t.Fatalf("client %d %s lock held while awaiting transaction ownership", i, lock.name)
			}
			lock.mu.Unlock()
		}
		if !c.writeMu.TryLock() {
			t.Fatalf("client %d writer lock held while awaiting transaction ownership", i)
		}
		c.writeMu.Unlock()
	}
}

func assertHandoffDiskLiteral(t *testing.T, path string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	// Read nodes directly; UserRecord/SavedPreset normalizers cannot turn a
	// malformed or incomplete persisted tuple into the expected result.
	for _, want := range []struct {
		path       []string
		tag, value string
	}{
		{[]string{"configuration_version"}, "!!int", "3"},
		{[]string{"noise_class"}, "!!str", "URBAN"},
		{[]string{"dedupe_policy"}, "!!str", "FAST"},
		{[]string{"path_min_observation_count"}, "!!int", "25"},
		{[]string{"solar_summary_minutes"}, "!!int", "15"},
		{[]string{"bands", "20m"}, "!!bool", "true"},
		{[]string{"bands", "40m"}, "!!bool", "false"},
		{[]string{"preset", "name"}, "!!str", "CONTEST"},
		{[]string{"preset", "baseline", "configuration_version"}, "!!int", "3"},
		{[]string{"preset", "baseline", "noise_class"}, "!!str", "QUIET"},
		{[]string{"preset", "baseline", "bands", "40m"}, "!!bool", "false"},
	} {
		node := doc.Content[0]
		for _, field := range want.path {
			var found *yaml.Node
			if node.Kind == yaml.MappingNode {
				for i := 0; i+1 < len(node.Content); i += 2 {
					if node.Content[i].Value == field {
						found = node.Content[i+1]
						break
					}
				}
			}
			if found == nil {
				t.Fatalf("durable tuple missing literal path %v", want.path)
			}
			node = found
		}
		if node.Tag != want.tag || node.Value != want.value {
			t.Fatalf("durable path %v = %s %q, want %s %q", want.path, node.Tag, node.Value, want.tag, want.value)
		}
	}
}
