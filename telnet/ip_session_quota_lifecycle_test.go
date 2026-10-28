package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
)

func TestIPSessionQuotaPendingResolution(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_%t", cancel), func(t *testing.T) {
			s := newIPQuotaServer(t, 0, 0, 1)
			first := ipQuotaClient(s, "K1ABC-1", "192.0.2.1:1001")
			waiter := ipQuotaClient(s, "W1XYZ-1", "192.0.2.1:1002")
			entered, release := ceilingBlockedRestore(t, s)
			firstDone, firstFinished := ceilingRestoreAsync(s, first, time.Now().Add(10*time.Second))
			var waiterFinished <-chan struct{}
			t.Cleanup(func() {
				release()
				first.interrupt()
				waiter.interrupt()
				waitConfigurationTest(t, firstFinished)
				if waiterFinished != nil {
					waitConfigurationTest(t, waiterFinished)
				}
			})
			waitConfigurationTest(t, entered)
			ipQuotaCounts(t, s, "192.0.2.1", 0, 1)
			waiterDone, finished := ceilingRestoreAsync(s, waiter, time.Now().Add(10*time.Second))
			waiterFinished = finished
			ceilingCapacityWaitObserved(t, s)
			select {
			case err := <-waiterDone:
				t.Fatalf("pending IP slot was treated as registered fullness: %v", err)
			default:
			}
			if cancel {
				first.interrupt()
			}
			release()
			firstErr, waiterErr := ceilingResult(t, firstDone), ceilingResult(t, waiterDone)
			if cancel {
				if !errors.Is(firstErr, errClientClosed) || waiterErr != nil {
					t.Fatalf("canceled holder=%v, waiter=%v", firstErr, waiterErr)
				}
				s.unregisterClient(waiter)
			} else {
				if firstErr != nil || !errors.Is(waiterErr, errIPSessionFull) {
					t.Fatalf("holder=%v, IP waiter=%v", firstErr, waiterErr)
				}
				recheckNoProfile(t, waiter.callsign)
				s.unregisterClient(first)
			}
			ipQuotaCounts(t, s, "192.0.2.1", 0, 0)
		})
	}
}

func TestIPSessionQuotaCrossIPReservationKeepsSourceOccupied(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_%t", cancel), func(t *testing.T) {
			s := newIPQuotaServer(t, 0, 0, 1)
			owner := ipQuotaLogin(t, s, "K1ABC-1", "192.0.2.1:1001")
			replacement := ipQuotaClient(s, owner.callsign, "192.0.2.2:1002")
			waiter := ipQuotaClient(s, "W1XYZ-1", "192.0.2.2:1003")
			entered, release := ceilingBlockedRestore(t, s)
			replaceDone, replaceFinished := ceilingRestoreAsync(s, replacement, time.Now().Add(10*time.Second))
			var waiterFinished <-chan struct{}
			t.Cleanup(func() {
				release()
				replacement.interrupt()
				waiter.interrupt()
				waitConfigurationTest(t, replaceFinished)
				if waiterFinished != nil {
					waitConfigurationTest(t, waiterFinished)
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
			ipQuotaCurrent(t, s, owner.callsign, owner)
			ipQuotaCounts(t, s, "192.0.2.1", 1, 0)
			ipQuotaCounts(t, s, "192.0.2.2", 0, 1)
			// The cross-IP reservation is not an extra global session.
			ceilingOccupancy(t, s, 1, 0)
			fromSource := ipQuotaClient(s, "N1ABC-1", "192.0.2.1:1004")
			if err := ipQuotaTry(s, fromSource); !errors.Is(err, errIPSessionFull) {
				t.Fatalf("source slot freed before transfer: %v", err)
			}
			recheckNoProfile(t, fromSource.callsign)
			waiterDone, finished := ceilingRestoreAsync(s, waiter, time.Now().Add(10*time.Second))
			waiterFinished = finished
			ceilingCapacityWaitObserved(t, s)
			select {
			case err := <-waiterDone:
				t.Fatalf("destination contender did not wait for transfer: %v", err)
			default:
			}
			if cancel {
				replacement.interrupt()
			}
			release()
			replaceErr, waiterErr := ceilingResult(t, replaceDone), ceilingResult(t, waiterDone)
			if cancel {
				if !errors.Is(replaceErr, errClientClosed) || waiterErr != nil {
					t.Fatalf("canceled transfer=%v, destination waiter=%v", replaceErr, waiterErr)
				}
				ipQuotaCurrent(t, s, owner.callsign, owner)
				ipQuotaCounts(t, s, "192.0.2.1", 1, 0)
				ipQuotaCounts(t, s, "192.0.2.2", 1, 0)
			} else {
				if replaceErr != nil || !errors.Is(waiterErr, errIPSessionFull) {
					t.Fatalf("transfer=%v, destination waiter=%v", replaceErr, waiterErr)
				}
				recheckNoProfile(t, waiter.callsign)
				ipQuotaCurrent(t, s, owner.callsign, replacement)
				s.unregisterClient(owner) // Retired owner must not release the new IP.
				ipQuotaCounts(t, s, "192.0.2.1", 0, 0)
				ipQuotaCounts(t, s, "192.0.2.2", 1, 0)
				newSource := ipQuotaLogin(t, s, "N1ABC-1", "192.0.2.1:1005")
				s.unregisterClient(newSource)
			}
		})
	}
}

func TestIPSessionQuotaWaiterDeadlineCancellationAndShutdown(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			s := newIPQuotaServer(t, 0, 0, 1)
			holder := ipQuotaClient(s, "K1ABC-1", "192.0.2.1:1001")
			waiter := ipQuotaClient(s, "W1XYZ-1", "192.0.2.1:1002")
			entered, release := ceilingBlockedRestore(t, s)
			holderDone, holderFinished := ceilingRestoreAsync(s, holder, time.Now().Add(10*time.Second))
			var waiterFinished <-chan struct{}
			t.Cleanup(func() {
				release()
				holder.interrupt()
				waiter.interrupt()
				waitConfigurationTest(t, holderFinished)
				if waiterFinished != nil {
					waitConfigurationTest(t, waiterFinished)
				}
			})
			waitConfigurationTest(t, entered)
			deadline := time.Now().Add(10 * time.Second)
			if mode == "deadline" {
				deadline = time.Now().Add(time.Second)
			}
			waiterDone, finished := ceilingRestoreAsync(s, waiter, deadline)
			waiterFinished = finished
			ceilingCapacityWaitObserved(t, s)
			switch mode {
			case "cancel":
				waiter.interrupt()
			case "shutdown":
				close(s.shutdown)
			}
			err := ceilingResult(t, waiterDone)
			if mode == "deadline" {
				if err == nil || !strings.Contains(err.Error(), "login deadline") {
					t.Fatalf("waiting deadline: %v", err)
				}
			} else if !errors.Is(err, errClientClosed) {
				t.Fatalf("waiting %s: %v", mode, err)
			}
			recheckNoProfile(t, waiter.callsign)
			ipQuotaCounts(t, s, "192.0.2.1", 0, 1)
			release()
			holderErr := ceilingResult(t, holderDone)
			if mode == "shutdown" {
				if !errors.Is(holderErr, errClientClosed) {
					t.Fatalf("reserved login on shutdown: %v", holderErr)
				}
			} else {
				if holderErr != nil {
					t.Fatalf("holder after waiter %s: %v", mode, holderErr)
				}
				s.unregisterClient(holder)
			}
			ipQuotaCounts(t, s, "192.0.2.1", 0, 0)
		})
	}
}

func TestIPSessionQuotaConcurrentNewCalls(t *testing.T) {
	const limit, attempts = 3, 8
	s := newIPQuotaServer(t, 0, 0, limit)
	entered := make(chan struct{}, attempts)
	release := make(chan struct{})
	var releaseOnce sync.Once
	allowCommits := func() { releaseOnce.Do(func() { close(release) }) }
	s.commitLoginRecordFn = func(prepared *filter.PreparedUserRecord) error {
		entered <- struct{}{}
		<-release
		return prepared.Commit()
	}
	type result struct {
		client *Client
		err    error
	}
	results := make(chan result, attempts)
	waiting := make(chan string, attempts)
	var observedWaiters sync.Map
	s.testLoginCapacityWait = func(c *Client) {
		if _, seen := observedWaiters.LoadOrStore(c.callsign, struct{}{}); !seen {
			waiting <- c.callsign
		}
	}
	used := make(map[uint64]bool)
	var workers sync.WaitGroup
	for i := 1; len(used) < attempts && i < 10000; i++ {
		call := fmt.Sprintf("K%dABC", i)
		stripe := configurationStripe(call)
		if used[stripe] {
			continue
		}
		used[stripe] = true
		c := ipQuotaClient(s, call, fmt.Sprintf("192.0.2.1:%d", 1000+i))
		workers.Add(1)
		go func() {
			defer workers.Done()
			results <- result{c, ipQuotaTry(s, c)}
		}()
	}
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	t.Cleanup(func() {
		allowCommits()
		waitConfigurationTest(t, finished)
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
	if len(used) != attempts {
		t.Fatal("could not select enough independent stripes")
	}
	for i := 0; i < limit; i++ {
		waitConfigurationTest(t, entered)
	}
	// Each excess login must reach its own capacity wait. Merely observing the
	// shared notification channel cannot prove that all contenders arrived.
	for i := 0; i < attempts-limit; i++ {
		select {
		case <-waiting:
		case <-time.After(5 * time.Second):
			t.Fatal("not every excess contender reached the capacity wait")
		}
	}
	ipQuotaCounts(t, s, "192.0.2.1", 0, limit)
	select {
	case got := <-results:
		t.Fatalf("contender returned before reservations resolved: %v", got.err)
	default:
	}
	allowCommits()
	waitConfigurationTest(t, finished)
	var admitted, rejected int
	for i := 0; i < attempts; i++ {
		got := <-results
		switch {
		case got.err == nil:
			admitted++
		case errors.Is(got.err, errIPSessionFull):
			rejected++
			recheckNoProfile(t, got.client.callsign)
		default:
			t.Fatalf("contender %s: %v", got.client.callsign, got.err)
		}
	}
	if admitted != limit || rejected != attempts-limit || s.GetClientCount() != limit {
		t.Fatalf("admitted=%d rejected=%d registered=%d", admitted, rejected, s.GetClientCount())
	}
	ipQuotaCounts(t, s, "192.0.2.1", limit, 0)
}

func TestIPSessionQuotaDepartingOwnerReplacement(t *testing.T) {
	for _, competitorWins := range []bool{false, true} {
		t.Run(fmt.Sprintf("competitor_wins_%t", competitorWins), func(t *testing.T) {
			s := newIPQuotaServer(t, 0, 0, 1)
			owner := ipQuotaLogin(t, s, "K1ABC-1", "192.0.2.1:1001")
			baseline := testSavedPreset(t, filter.V5Configuration{Settings: filter.SettingsConfiguration{Dialect: "go", NoiseClass: "QUIET", DedupePolicy: "FAST"}})
			if err := filter.SavePreset(owner.callsign, "CONTEST", baseline); err != nil {
				t.Fatal(err)
			}
			profile := filepath.Join(filter.UserDataDir, owner.callsign+".yaml")
			library := presetLibraryPathFor(owner.callsign)
			for _, path := range []string{profile + ".pre-v5.bak", library + ".pre-v5.bak"} {
				if err := os.WriteFile(path, []byte("existing backup "+filepath.Base(path)), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			owner.configuredSettings.NoiseClass = "URBAN"
			enteredSave, allowSave := make(chan struct{}), make(chan struct{})
			removed, allowStripe := make(chan struct{}), make(chan struct{})
			var saveOnce, saveReleaseOnce, removalOnce, stripeReleaseOnce sync.Once
			releaseSave := func() { saveReleaseOnce.Do(func() { close(allowSave) }) }
			releaseStripe := func() { stripeReleaseOnce.Do(func() { close(allowStripe) }) }
			s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error {
				saveOnce.Do(func() { close(enteredSave); <-allowSave })
				return prepared.Commit()
			}
			s.testAfterClientRemoval = func(c *Client) {
				if c == owner {
					removalOnce.Do(func() { close(removed); <-allowStripe })
				}
			}
			replacement := ipQuotaClient(s, owner.callsign, "192.0.2.2:1002")
			probed := make(chan struct{})
			var probes atomic.Int32
			s.testLoginCapacityProbe = func(c *Client, checked, full bool) {
				if c == replacement && checked && !full && probes.Add(1) == 2 {
					// The second observation is a periodic probe: the first
					// completed and this login is waiting on the old stripe.
					close(probed)
				}
			}
			ownerDone := make(chan struct{})
			go func() { s.unregisterClient(owner); close(ownerDone) }()
			var replacementFinished <-chan struct{}
			t.Cleanup(func() {
				releaseSave()
				releaseStripe()
				replacement.interrupt()
				waitConfigurationTest(t, ownerDone)
				if replacementFinished != nil {
					waitConfigurationTest(t, replacementFinished)
				}
			})
			waitConfigurationTest(t, enteredSave)
			done, finished := ceilingRestoreAsync(s, replacement, time.Now().Add(10*time.Second))
			replacementFinished = finished
			waitConfigurationTest(t, probed)
			releaseSave()
			waitConfigurationTest(t, removed)
			// Snapshot after the old owner's legitimate final save, so only
			// writes caused by a subsequently rejected replacement fail this test.
			paths := []string{profile, library, profile + ".pre-v5.bak", library + ".pre-v5.bak"}
			before := make(map[string][]byte, len(paths))
			for _, path := range paths {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("read post-teardown %s: %v", path, err)
				}
				before[path] = data
			}
			if !bytes.Contains(before[profile], []byte("noise_class: URBAN")) {
				t.Fatal("departing owner did not save")
			}
			var competitor *Client
			if competitorWins {
				call := recheckDifferentStripeCall(t, owner.callsign)
				competitor = ipQuotaLogin(t, s, call, "192.0.2.2:1003")
			}
			releaseStripe()
			waitConfigurationTest(t, ownerDone)
			err := ceilingResult(t, done)
			if competitorWins {
				if !errors.Is(err, errIPSessionFull) {
					t.Fatalf("replacement after destination filled: %v", err)
				}
				for _, path := range paths {
					after, readErr := os.ReadFile(path)
					if readErr != nil || !bytes.Equal(before[path], after) {
						t.Fatalf("rejected replacement changed %s: %v", path, readErr)
					}
				}
				s.unregisterClient(competitor)
			} else {
				if err != nil || replacement.configuredSettings.NoiseClass != "URBAN" {
					t.Fatalf("replacement failed to restore departing owner profile: %v", err)
				}
				s.unregisterClient(replacement)
			}
			ipQuotaCounts(t, s, "192.0.2.1", 0, 0)
			ipQuotaCounts(t, s, "192.0.2.2", 0, 0)
		})
	}
}
