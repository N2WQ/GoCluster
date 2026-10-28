package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
)

// The second exact-identity reconnect begins while the first owns the stripe.
// Its capacity check must use the owner and IP state current after that wait.
func TestIPSessionQuotaConcurrentCrossIPReplacements(t *testing.T) {
	for _, destinationFills := range []bool{false, true} {
		t.Run(fmt.Sprintf("destination_fills_%t", destinationFills), func(t *testing.T) {
			s := newIPQuotaServer(t, 0, 1, 1)
			old := ipQuotaLogin(t, s, "K1ABC-1", "192.0.2.1:1001")
			first := ipQuotaClient(s, old.callsign, "192.0.2.2:1002")
			second := ipQuotaClient(s, old.callsign, "192.0.2.3:1003")
			entered, releaseFirst := ceilingBlockedRestore(t, s)
			probeWaiting, allowProbe := make(chan struct{}), make(chan struct{})
			var probeCount atomic.Int32
			var probeOnce sync.Once
			releaseProbe := func() { probeOnce.Do(func() { close(allowProbe) }) }
			s.testLoginCapacityProbe = func(c *Client, checked, full bool) {
				if c == second && checked && !full && probeCount.Add(1) == 2 {
					close(probeWaiting)
					<-allowProbe
				}
			}
			firstDone, firstFinished := ceilingRestoreAsync(s, first, time.Now().Add(10*time.Second))
			var secondFinished <-chan struct{}
			t.Cleanup(func() {
				releaseFirst()
				releaseProbe()
				first.interrupt()
				second.interrupt()
				waitConfigurationTest(t, firstFinished)
				if secondFinished != nil {
					waitConfigurationTest(t, secondFinished)
				}
				s.clientsMutex.RLock()
				owners := make([]*Client, 0, len(s.clients))
				for _, c := range s.clients {
					owners = append(owners, c)
				}
				s.clientsMutex.RUnlock()
				for _, c := range owners {
					s.unregisterClient(c)
				}
			})
			waitConfigurationTest(t, entered)
			secondDone, finished := ceilingRestoreAsync(s, second, time.Now().Add(10*time.Second))
			secondFinished = finished
			waitConfigurationTest(t, probeWaiting)
			ipQuotaCurrent(t, s, old.callsign, old)
			ipQuotaCounts(t, s, "192.0.2.1", 1, 0)
			ipQuotaCounts(t, s, "192.0.2.2", 0, 1)
			ipQuotaCounts(t, s, "192.0.2.3", 0, 0)
			releaseFirst()
			if err := ceilingResult(t, firstDone); err != nil {
				t.Fatalf("first replacement: %v", err)
			}
			ipQuotaCurrent(t, s, old.callsign, first)
			profile := filepath.Join(filter.UserDataDir, old.callsign+".yaml")
			before, err := os.ReadFile(profile)
			if err != nil {
				t.Fatal(err)
			}
			if destinationFills {
				// The competitor is a different base, so only destination-IP
				// capacity determines the second replacement's outcome.
				ipQuotaLogin(t, s, "W1XYZ-1", "192.0.2.3:1004")
			}
			releaseProbe()
			err = ceilingResult(t, secondDone)
			if destinationFills {
				if !errors.Is(err, errIPSessionFull) {
					t.Fatalf("second replacement at newly full destination: %v", err)
				}
				ipQuotaCurrent(t, s, old.callsign, first)
				after, readErr := os.ReadFile(profile)
				if readErr != nil || !bytes.Equal(before, after) {
					t.Fatalf("rejected replacement changed current profile: %v", readErr)
				}
				ipQuotaCounts(t, s, "192.0.2.2", 1, 0)
				ipQuotaCounts(t, s, "192.0.2.3", 1, 0)
			} else {
				if err != nil {
					t.Fatalf("second replacement at free destination: %v", err)
				}
				ipQuotaCurrent(t, s, old.callsign, second)
				s.unregisterClient(first)
				ipQuotaCounts(t, s, "192.0.2.2", 0, 0)
				ipQuotaCounts(t, s, "192.0.2.3", 1, 0)
			}
			s.unregisterClient(old)
			ipQuotaCounts(t, s, "192.0.2.1", 0, 0)
		})
	}
}
