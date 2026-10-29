package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"dxcluster/filter"
)

// A cross-IP replacement can lose its old owner while it waits for a pending
// destination slot. On resumption it must be admitted as a new identity.
func TestIPSessionQuotaWaitingReplacementOwnerDeparts(t *testing.T) {
	for _, competitorWins := range []bool{false, true} {
		t.Run(fmt.Sprintf("competitor_wins_%t", competitorWins), func(t *testing.T) {
			s := newIPQuotaServer(t, 3, 1, 1)
			owner := ipQuotaLogin(t, s, "K1ABC-1", "192.0.2.1:1001")
			owner.configuredSettings.NoiseClass = "URBAN"
			baseline := testSavedPreset(t, filter.V6Configuration{Settings: filter.SettingsConfiguration{Dialect: "go", NoiseClass: "QUIET", DedupePolicy: "FAST"}})
			if err := filter.SavePreset(owner.callsign, "CONTEST", baseline); err != nil {
				t.Fatal(err)
			}
			profile := filepath.Join(filter.UserDataDir, owner.callsign+".yaml")
			library := presetLibraryPathFor(owner.callsign)
			for _, path := range []string{profile + ".pre-v6.bak", library + ".pre-v6.bak"} {
				if err := os.WriteFile(path, []byte("existing backup "+filepath.Base(path)), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			holderCall := recheckDifferentStripeCall(t, owner.callsign)
			holder := ipQuotaClient(s, holderCall, "192.0.2.2:1002")
			replacement := ipQuotaClient(s, owner.callsign, "192.0.2.2:1003")
			entered, releaseHolder := ceilingBlockedRestore(t, s)
			waiting, allowWait := make(chan struct{}), make(chan struct{})
			var waitOnce, releaseWaitOnce sync.Once
			releaseWait := func() { releaseWaitOnce.Do(func() { close(allowWait) }) }
			s.testLoginCapacityWait = func(c *Client) {
				if c == replacement {
					waitOnce.Do(func() {
						close(waiting)
						<-allowWait
					})
				}
			}
			holderDone, holderFinished := ceilingRestoreAsync(s, holder, time.Now().Add(10*time.Second))
			var replacementFinished <-chan struct{}
			t.Cleanup(func() {
				releaseHolder()
				releaseWait()
				holder.interrupt()
				replacement.interrupt()
				waitConfigurationTest(t, holderFinished)
				if replacementFinished != nil {
					waitConfigurationTest(t, replacementFinished)
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
			ipQuotaCounts(t, s, "192.0.2.2", 0, 1)
			done, finished := ceilingRestoreAsync(s, replacement, time.Now().Add(10*time.Second))
			replacementFinished = finished
			// The test hook runs only after capacity admission releases its
			// stripe, allowing the old owner to perform final teardown.
			waitConfigurationTest(t, waiting)
			ipQuotaCurrent(t, s, owner.callsign, owner)
			s.unregisterClient(owner)
			ipQuotaCounts(t, s, "192.0.2.1", 0, 0)
			var competitor *Client
			if competitorWins {
				var call string
				for i := 2; i <= 99; i++ {
					candidate := fmt.Sprintf("K1ABC-%d", i)
					if configurationStripe(candidate) != configurationStripe(holder.callsign) {
						call = candidate
						break
					}
				}
				if call == "" {
					t.Fatal("could not choose a different stripe from the pending holder")
				}
				competitor = ipQuotaLogin(t, s, call, "192.0.2.3:1004")
			}
			// Record all files after the owner's legitimate final save and any
			// competitor login, before the blocked replacement can recheck.
			paths := []string{profile, library, profile + ".pre-v6.bak", library + ".pre-v6.bak"}
			before := make(map[string][]byte, len(paths))
			for _, path := range paths {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("read post-teardown %s: %v", path, err)
				}
				before[path] = data
			}
			if !bytes.Contains(before[profile], []byte("noise_class: URBAN")) {
				t.Fatal("departing owner did not save before replacement rechecked")
			}
			holder.interrupt()
			releaseHolder()
			if err := ceilingResult(t, holderDone); !errors.Is(err, errClientClosed) {
				t.Fatalf("canceled pending holder: %v", err)
			}
			ipQuotaCounts(t, s, "192.0.2.2", 0, 0)
			releaseWait()
			err := ceilingResult(t, done)
			if competitorWins {
				if !errors.Is(err, errBaseSessionFull) {
					t.Fatalf("replacement after station slot was taken: %v", err)
				}
				for _, path := range paths {
					after, readErr := os.ReadFile(path)
					if readErr != nil || !bytes.Equal(before[path], after) {
						t.Fatalf("rejected replacement changed %s: %v", path, readErr)
					}
				}
				ipQuotaCurrent(t, s, competitor.callsign, competitor)
				ipQuotaCurrent(t, s, replacement.callsign, nil)
				ipQuotaCounts(t, s, "192.0.2.2", 0, 0)
			} else {
				if err != nil || replacement.configuredSettings.NoiseClass != "URBAN" {
					t.Fatalf("replacement failed to restore departing owner: %v", err)
				}
				ipQuotaCurrent(t, s, replacement.callsign, replacement)
				ipQuotaCounts(t, s, "192.0.2.2", 1, 0)
			}
		})
	}
}
