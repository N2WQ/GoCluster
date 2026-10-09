package telnet

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
)

func TestHumanPreferenceCoordinatorReconnectFence(t *testing.T) {
	s := presetTestServer(t)
	old := configurationTestClient(s, "W1ABC-1")
	old.noiseClass = "QUIET"
	if err := old.saveFilter(); err != nil {
		t.Fatal(err)
	}
	s.registerClient(old)
	entered, finish := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	joined := make([]<-chan struct{}, 0, 2)
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(finish) })
		close(old.done)
		for _, done := range joined {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("human handoff goroutine did not join before fixture cleanup")
			}
		}
	})
	var attempts atomic.Int32
	s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error {
		attempts.Add(1)
		close(entered)
		<-finish
		return prepared.Commit()
	}
	saved := make(chan struct{})
	joined = append(joined, saved)
	var response string
	go func() { response, _ = s.handlePathSettingsCommand(old, "SET NOISE URBAN"); close(saved) }()
	waitConfigurationTest(t, entered)
	if len(s.configurationTxnStripes[configurationStripe(old.callsign)]) != 1 {
		t.Fatal("stalled human coordinator released its transaction stripe")
	}
	// This synchronous bounded reconnect probe actually attempts the occupied
	// lease. A started goroutine plus a nonblocking receive would not prove that.
	probe := configurationTestClient(s, old.callsign)
	if _, err := s.restoreAndRegisterClient(probe, time.Now().UTC(), time.Now().Add(30*time.Millisecond)); err == nil || !strings.Contains(err.Error(), "login deadline") {
		t.Fatalf("reconnect bypassed the active human save lease: %v", err)
	}
	s.clientsMutex.RLock()
	current := s.clients[old.callsign]
	s.clientsMutex.RUnlock()
	if current != old {
		t.Fatal("blocked reconnect changed current membership")
	}
	// Exercise fan-out lock progress while the real human save is stalled.
	progress := make(chan struct{})
	joined = append(joined, progress)
	go func() {
		state := old.pathSnapshot()
		old.filterMu.RLock()
		available := old.filter != nil
		old.filterMu.RUnlock()
		if state.noiseClass != "URBAN" || !available {
			t.Error("accepted human edit was not published before its save")
		}
		close(progress)
	}()
	waitConfigurationTest(t, progress)
	releaseOnce.Do(func() { close(finish) })
	waitConfigurationTest(t, saved)
	next := configurationTestClient(s, old.callsign)
	_, restoreErr := s.restoreAndRegisterClient(next, time.Now().UTC(), time.Now().Add(time.Minute))
	if response != "Noise class set to URBAN\n" || restoreErr != nil || attempts.Load() != 1 || next.configuredSettings.NoiseClass != "URBAN" {
		t.Fatalf("human coordinator handoff failed: %q, %v, saves=%d", response, restoreErr, attempts.Load())
	}
	s.commitConfigurationFn = nil
	path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if response, handled := s.handlePathSettingsCommand(old, "SET NOISE INDUSTRIAL"); !handled || response == "Noise class set to INDUSTRIAL\n" || old.noiseClass != "URBAN" {
		t.Fatal("retired coordinator published another edit")
	}
	s.unregisterClient(old)
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("retired human command/teardown changed successor bytes")
	}
}
