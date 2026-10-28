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

func newBaseQuotaServer(t *testing.T, global, station int) *Server {
	t.Helper()
	s := newSessionCeilingServer(t, global)
	s.maxSessionsPerBaseCall = station
	return s
}

func baseQuotaCounts(t *testing.T, s *Server, base string, registered, pending int) {
	t.Helper()
	s.clientsMutex.RLock()
	got := s.baseSessionCounts[base]
	s.clientsMutex.RUnlock()
	if got.registered != registered || got.pending != pending {
		t.Fatalf("station %s registered=%d pending=%d, want %d and %d", base, got.registered, got.pending, registered, pending)
	}
}

func baseQuotaRestore(t *testing.T, s *Server, call string) *Client {
	t.Helper()
	c := configurationTestClient(s, call)
	if _, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(5*time.Second)); err != nil {
		t.Fatalf("login %s: %v", call, err)
	}
	return c
}

func TestBaseSessionQuotaPendingResolution(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_%t", cancel), func(t *testing.T) {
			s := newBaseQuotaServer(t, 0, 1)
			first := configurationTestClient(s, "K1ABC-1")
			waiter := configurationTestClient(s, "K1ABC-2")
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
			baseQuotaCounts(t, s, "K1ABC", 0, 1)
			waiterDone, finished := ceilingRestoreAsync(s, waiter, time.Now().Add(10*time.Second))
			waiterFinished = finished
			ceilingCapacityWaitObserved(t, s)
			select {
			case err := <-waiterDone:
				t.Fatalf("station reservation was treated as registered fullness: %v", err)
			default:
			}
			other := baseQuotaRestore(t, s, "W1XYZ-1")
			s.unregisterClient(other)
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
				if firstErr != nil || !errors.Is(waiterErr, errBaseSessionFull) {
					t.Fatalf("holder=%v, quota waiter=%v", firstErr, waiterErr)
				}
				if _, err := os.Stat(filepath.Join(filter.UserDataDir, waiter.callsign+".yaml")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("station-rejected waiter changed profile: %v", err)
				}
				s.unregisterClient(first)
			}
			baseQuotaCounts(t, s, "K1ABC", 0, 0)
		})
	}
}

func TestBaseSessionQuotaConcurrentSSIDs(t *testing.T) {
	const limit, attempts = 3, 8
	s := newBaseQuotaServer(t, 6, limit)
	entered := make(chan struct{}, attempts)
	release, start := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseCommits := func() { releaseOnce.Do(func() { close(release) }) }
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
	usedStripes := make(map[uint64]bool)
	var workers sync.WaitGroup
	for i, created := 0, 0; created < attempts && i <= 99; i++ {
		call := fmt.Sprintf("K1ABC-%d", i)
		stripe := configurationStripe(call)
		if usedStripes[stripe] {
			continue
		}
		usedStripes[stripe] = true
		created++
		c := configurationTestClient(s, call)
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(10*time.Second))
			results <- result{c, err}
		}()
	}
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	t.Cleanup(func() {
		releaseCommits()
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
	close(start)
	if len(usedStripes) != attempts {
		t.Fatal("not enough distinct configuration stripes")
	}
	for i := 0; i < limit; i++ {
		waitConfigurationTest(t, entered)
	}
	ceilingCapacityWaitObserved(t, s)
	baseQuotaCounts(t, s, "K1ABC", 0, limit)
	select {
	case got := <-results:
		t.Fatalf("contender returned before reservations resolved: %v", got.err)
	default:
	}
	releaseCommits()
	waitConfigurationTest(t, finished)
	var admitted, rejected int
	for i := 0; i < attempts; i++ {
		got := <-results
		switch {
		case got.err == nil:
			admitted++
		case errors.Is(got.err, errBaseSessionFull):
			rejected++
			recheckNoProfile(t, got.client.callsign)
		default:
			t.Fatalf("contender %s: %v", got.client.callsign, got.err)
		}
	}
	if admitted != limit || rejected != attempts-limit || s.GetClientCount() != limit {
		t.Fatalf("admitted=%d rejected=%d registered=%d", admitted, rejected, s.GetClientCount())
	}
	baseQuotaCounts(t, s, "K1ABC", limit, 0)
}

func TestBaseSessionQuotaLimitsAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		name            string
		global, station int
		want            error
	}{
		{"station_only", 0, 1, errBaseSessionFull},
		{"global_only", 1, 0, errSessionFull},
		{"both_full_global_first", 1, 1, errSessionFull},
		{"both_disabled", 0, 0, nil},
		{"negative_disabled_api", -1, -1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newBaseQuotaServer(t, tc.global, tc.station)
			old := baseQuotaRestore(t, s, "K1ABC-1")
			replacement := baseQuotaRestore(t, s, "K1ABC-1")
			s.unregisterClient(old)
			other := configurationTestClient(s, "K1ABC-01")
			_, err := s.restoreAndRegisterClient(other, time.Now().UTC(), time.Now().Add(5*time.Second))
			if !errors.Is(err, tc.want) {
				t.Fatalf("different spelling: got %v, want %v", err, tc.want)
			}
			if err == nil {
				s.unregisterClient(other)
			}
			s.clientsMutex.RLock()
			current := s.clients[replacement.callsign]
			s.clientsMutex.RUnlock()
			if current != replacement {
				t.Fatal("retired owner removed replacement")
			}
			s.unregisterClient(replacement)
			baseQuotaCounts(t, s, "K1ABC", 0, 0)
		})
	}

	s := newBaseQuotaServer(t, 10, 2)
	one := baseQuotaRestore(t, s, "K1ABC-1")
	two := baseQuotaRestore(t, s, "K1ABC-01")
	portable := baseQuotaRestore(t, s, "VE3/K1ABC-1")
	other := configurationTestClient(s, "K1ABC/P-1")
	if _, err := s.restoreAndRegisterClient(other, time.Now().UTC(), time.Now().Add(5*time.Second)); !errors.Is(err, errBaseSessionFull) {
		t.Fatalf("portable suffix must share K1ABC quota: %v", err)
	}
	baseQuotaCounts(t, s, "K1ABC", 2, 0)
	baseQuotaCounts(t, s, "VE3/K1ABC", 1, 0)
	for _, c := range []*Client{one, two, portable} {
		s.unregisterClient(c)
	}
}

func TestBaseSessionQuotaBusyRegistryAndStripe(t *testing.T) {
	s := newBaseQuotaServer(t, 0, 1)
	owner := baseQuotaRestore(t, s, "K1ABC-1")
	contender := configurationTestClient(s, "K1ABC-2")
	release, err := s.acquireConfiguration(contender, false, true, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var stripeOnce, registryOnce, probeOnce sync.Once
	releaseStripe := func() { stripeOnce.Do(release) }
	s.clientsMutex.Lock()
	unlockRegistry := func() { registryOnce.Do(s.clientsMutex.Unlock) }
	failed := make(chan struct{})
	s.testLoginCapacityProbe = func(c *Client, checked, _ bool) {
		if c == contender && !checked {
			probeOnce.Do(func() { close(failed) })
		}
	}
	done, finished := ceilingRestoreAsync(s, contender, time.Now().Add(5*time.Second))
	t.Cleanup(func() {
		unlockRegistry()
		releaseStripe()
		contender.interrupt()
		waitConfigurationTest(t, finished)
		s.unregisterClient(owner)
	})
	waitConfigurationTest(t, failed)
	unlockRegistry()
	if err := ceilingResult(t, done); !errors.Is(err, errBaseSessionFull) {
		t.Fatalf("station full hidden behind stripe: %v", err)
	}
	if len(s.configurationTxnStripes[configurationStripe(contender.callsign)]) != 1 {
		t.Fatal("station rejection waited until stripe was released")
	}
	recheckNoProfile(t, contender.callsign)
}

func TestBaseSessionQuotaRetiredOwnerAndChurn(t *testing.T) {
	s := newBaseQuotaServer(t, 0, 1)
	for i := 1; i <= 25; i++ {
		call := fmt.Sprintf("K%dABC-1", i)
		old := baseQuotaRestore(t, s, call)
		replacement := baseQuotaRestore(t, s, call)
		s.unregisterClient(old)
		other := configurationTestClient(s, fmt.Sprintf("K%dABC-2", i))
		if _, err := s.restoreAndRegisterClient(other, time.Now().UTC(), time.Now().Add(5*time.Second)); !errors.Is(err, errBaseSessionFull) {
			t.Fatalf("retired owner changed current quota at %s: %v", call, err)
		}
		s.unregisterClient(replacement)
		s.clientsMutex.RLock()
		bases := len(s.baseSessionCounts)
		s.clientsMutex.RUnlock()
		if bases != 0 {
			t.Fatalf("base entries after churn %d: %d", i, bases)
		}
		if got := s.PreloginAdmissionMetricSnapshot().TrackedSessionBases; got != 0 {
			t.Fatalf("reported base entries after churn %d: %d", i, got)
		}
	}
}

func TestBaseSessionQuotaDepartingOwner(t *testing.T) {
	for _, competitorWins := range []bool{false, true} {
		t.Run(fmt.Sprintf("competitor_wins_%t", competitorWins), func(t *testing.T) {
			s := newBaseQuotaServer(t, 0, 1)
			owner := baseQuotaRestore(t, s, "K1ABC-1")
			owner.configuredSettings.NoiseClass = "URBAN"
			enteredSave, allowSave := make(chan struct{}), make(chan struct{})
			removed, allowRemoval := make(chan struct{}), make(chan struct{})
			var saveOnce, removalOnce, probeOnce sync.Once
			releaseSave := func() { saveOnce.Do(func() { close(allowSave) }) }
			releaseRemoval := func() { removalOnce.Do(func() { close(allowRemoval) }) }
			s.commitConfigurationFn = func(prepared *filter.PreparedUserRecord) error {
				close(enteredSave)
				<-allowSave
				return prepared.Commit()
			}
			s.testAfterClientRemoval = func(c *Client) {
				if c == owner {
					close(removed)
					<-allowRemoval
				}
			}
			replacement := configurationTestClient(s, owner.callsign)
			probed := make(chan struct{})
			s.testLoginCapacityProbe = func(c *Client, checked, full bool) {
				if c == replacement && checked && !full {
					probeOnce.Do(func() { close(probed) })
				}
			}
			ownerDone := make(chan struct{})
			go func() { s.unregisterClient(owner); close(ownerDone) }()
			var replacementFinished <-chan struct{}
			t.Cleanup(func() {
				releaseSave()
				releaseRemoval()
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
			path := filepath.Join(filter.UserDataDir, owner.callsign+".yaml")
			before, err := os.ReadFile(path)
			if err != nil || !bytes.Contains(before, []byte("noise_class: URBAN")) {
				t.Fatalf("departing owner did not save: %v", err)
			}
			var competitor *Client
			if competitorWins {
				var call string
				for i := 2; i <= 99; i++ {
					candidate := fmt.Sprintf("K1ABC-%d", i)
					if configurationStripe(candidate) != configurationStripe(owner.callsign) {
						call = candidate
						break
					}
				}
				if call == "" {
					t.Fatal("no different stripe for competing SSID")
				}
				competitor = baseQuotaRestore(t, s, call)
			}
			releaseRemoval()
			waitConfigurationTest(t, ownerDone)
			err = ceilingResult(t, done)
			if competitorWins {
				if !errors.Is(err, errBaseSessionFull) {
					t.Fatalf("replacement after competitor filled quota: %v", err)
				}
				after, readErr := os.ReadFile(path)
				if readErr != nil || !bytes.Equal(before, after) {
					t.Fatalf("rejected replacement changed saved owner profile: %v", readErr)
				}
				s.commitConfigurationFn = nil
				s.unregisterClient(competitor)
			} else {
				if err != nil || replacement.configuredSettings.NoiseClass != "URBAN" {
					t.Fatalf("replacement did not restore departing owner profile: %v", err)
				}
				s.commitConfigurationFn = nil
				s.unregisterClient(replacement)
			}
			baseQuotaCounts(t, s, "K1ABC", 0, 0)
		})
	}
}
