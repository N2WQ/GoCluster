package telnet

import (
	"bytes"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/filter"
)

func TestV5MachineCoordinatorConcurrentCommitAndFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "commit"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			s, c, cfg := v5CoordinatorFixture(t)
			disk := v5CoordinatorSeed(t, c, cfg)
			before := snapshotBooleanCommand(c)
			cmd, request := v5CoordinatorProposal(t, c, "PATCH", "FILTER", map[string]any{
				"rules": []filter.RuleDefinition{{Slot: 3, Action: filter.RuleReject, Expr: "MODE=FT8"}},
			})
			entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(resume) }); waitConfigurationTest(t, done) })
			s.commitConfigurationFn = func(payload *filter.PreparedUserRecord) error {
				close(entered)
				<-resume
				if fail {
					return os.ErrPermission
				}
				return payload.Commit()
			}
			var response string
			go func() { response = s.applyV5MachineRequest(c, cmd, request); close(done) }()
			waitConfigurationTest(t, entered)
			// Broadcast-style readers do not own the stripe. They see old state
			// throughout disk IO and one complete publication after the commit.
			c.pathMu.RLock()
			c.filterMu.RLock()
			observed := filter.V5ConfigurationFromFilter(c.filter, c.configuredSettings)
			c.filterMu.RUnlock()
			c.pathMu.RUnlock()
			if !observed.Equal(cfg) || !bytes.Equal(disk, v5CoordinatorDisk(t, c)) {
				t.Fatal("machine state published before durable commit")
			}
			c.advanceSolarSummaryAt(time.Now().Add(20 * time.Minute))
			c.solarMu.Lock()
			clock := c.solarNextSummaryAt
			c.solarMu.Unlock()
			readDone := make(chan struct{})
			go func() {
				defer close(readDone)
				for range 1000 {
					c.pathMu.RLock()
					c.filterMu.RLock()
					_ = filter.V5ConfigurationFromFilter(c.filter, c.configuredSettings).Fingerprint()
					c.filterMu.RUnlock()
					c.pathMu.RUnlock()
				}
			}()
			once.Do(func() { close(resume) })
			waitConfigurationTest(t, done)
			waitConfigurationTest(t, readDone)
			if fail {
				before.assertUnchanged(t, c)
				if !strings.Contains(response, "code: persistence_failed") || !bytes.Equal(disk, v5CoordinatorDisk(t, c)) ||
					c.configurationRevision != 0 {
					t.Fatal("failed commit changed state or claimed success")
				}
			} else {
				v5CoordinatorSuccess(t, response, true)
				if c.configurationRevision != 1 || c.filter != before.pointer ||
					c.filter.PreparedRules() == before.programs {
					t.Fatal("successful commit failed single atomic publication")
				}
			}
			if c.solarNextSummaryAt != clock {
				t.Fatal("publication lost a broadcaster tick during disk IO")
			}
		})
	}
}

func TestV5MachineCoordinatorConcurrentRevisionFence(t *testing.T) {
	s, c, cfg := v5CoordinatorFixture(t)
	v5CoordinatorSeed(t, c, cfg)
	cmd, request := v5CoordinatorProposal(t, c, "PATCH", "SETTINGS", map[string]any{"noise_class": "URBAN"})
	start := make(chan struct{})
	results := make(chan string, 2)
	for range 2 {
		go func() { <-start; results <- s.applyV5MachineRequest(c, cmd, request) }()
	}
	close(start)
	success, conflict := 0, 0
	for range 2 {
		select {
		case reply := <-results:
			switch {
			case strings.Contains(reply, "persisted: true"):
				success++
			case strings.Contains(reply, "code: revision_conflict"):
				conflict++
			default:
				t.Fatal(reply)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent revision transaction stalled")
		}
	}
	if success != 1 || conflict != 1 || c.configurationRevision != 1 {
		t.Fatal("same-revision requests escaped the full-SSID lease")
	}
	c.preserveRecordOnExit.Store(true)
	s.unregisterClient(c)
	replacement := configurationTestClient(s, c.callsign)
	s.registerClient(replacement)
	disk := v5CoordinatorDisk(t, c)
	response := s.applyV5MachineRequest(c, cmd, request)
	if !strings.Contains(response, "code: session_unavailable") || !bytes.Equal(disk, v5CoordinatorDisk(t, c)) {
		t.Fatal("retired registered session overwrote its replacement")
	}
}
