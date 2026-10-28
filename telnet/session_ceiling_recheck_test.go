package telnet

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
)

func recheckCollidingCall(t *testing.T, original string) string {
	t.Helper()
	for i := 2; i < 10000; i++ {
		candidate := fmt.Sprintf("K%dABC", i)
		if candidate != original && configurationStripe(candidate) == configurationStripe(original) {
			return candidate
		}
	}
	t.Fatal("could not find a different identity on the requested stripe")
	return ""
}

func recheckDifferentStripeCall(t *testing.T, original string) string {
	t.Helper()
	for i := 2; i < 10000; i++ {
		candidate := fmt.Sprintf("K%dABC", i)
		if configurationStripe(candidate) != configurationStripe(original) {
			return candidate
		}
	}
	t.Fatal("could not find an identity on another stripe")
	return ""
}

func recheckNoProfile(t *testing.T, call string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(filter.UserDataDir, call+".yaml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected identity %s changed its profile: %v", call, err)
	}
}

func TestSessionCeilingRecheckBusyRegistryAndStripe(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	owner := configurationTestClient(s, "K1ABC-1")
	s.registerClient(owner)
	contender := configurationTestClient(s, recheckCollidingCall(t, owner.callsign))
	release, err := s.acquireConfiguration(owner, false, true, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var stripeOnce, registryOnce, probeOnce sync.Once
	releaseStripe := func() { stripeOnce.Do(release) }
	s.clientsMutex.Lock()
	unlockRegistry := func() { registryOnce.Do(s.clientsMutex.Unlock) }
	failedProbe := make(chan struct{})
	s.testLoginCapacityProbe = func(c *Client, checked, _ bool) {
		if c == contender && !checked {
			probeOnce.Do(func() { close(failedProbe) })
		}
	}
	result, finished := ceilingRestoreAsync(s, contender, time.Now().Add(5*time.Second))
	t.Cleanup(func() {
		unlockRegistry()
		releaseStripe()
		contender.interrupt()
		waitConfigurationTest(t, finished)
	})
	waitConfigurationTest(t, failedProbe)
	unlockRegistry()
	if err := ceilingResult(t, result); !errors.Is(err, errSessionFull) {
		t.Fatalf("full registry remained hidden behind stripe: %v", err)
	}
	waitConfigurationTest(t, finished)
	// The stripe remains held through the rejection; otherwise a successful
	// stripe acquisition could mask the missing periodic registry recheck.
	if got := len(s.configurationTxnStripes[configurationStripe(owner.callsign)]); got != 1 {
		t.Fatalf("held stripe was unexpectedly released: %d", got)
	}
	recheckNoProfile(t, contender.callsign)
}

func TestSessionCeilingRecheckRegistryFillsWhileStripeBusy(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	holder := configurationTestClient(s, "K1ABC-1")
	contender := configurationTestClient(s, recheckCollidingCall(t, holder.callsign))
	release, err := s.acquireConfiguration(holder, false, true, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var stripeOnce, probeOnce sync.Once
	releaseStripe := func() { stripeOnce.Do(release) }
	initial := make(chan struct{})
	s.testLoginCapacityProbe = func(c *Client, checked, full bool) {
		if c == contender && checked && !full {
			probeOnce.Do(func() { close(initial) })
		}
	}
	result, finished := ceilingRestoreAsync(s, contender, time.Now().Add(5*time.Second))
	t.Cleanup(func() { releaseStripe(); contender.interrupt(); waitConfigurationTest(t, finished) })
	waitConfigurationTest(t, initial)
	other := configurationTestClient(s, recheckDifferentStripeCall(t, holder.callsign))
	s.registerClient(other)
	t.Cleanup(func() { s.unregisterClient(other) })
	if err := ceilingResult(t, result); !errors.Is(err, errSessionFull) {
		t.Fatalf("newly full registry did not reject blocked contender: %v", err)
	}
	waitConfigurationTest(t, finished)
	if got := len(s.configurationTxnStripes[configurationStripe(holder.callsign)]); got != 1 {
		t.Fatalf("held stripe was unexpectedly released: %d", got)
	}
	recheckNoProfile(t, contender.callsign)
}

func TestSessionCeilingRecheckSameIdentityAtCapacity(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	owner := configurationTestClient(s, "K1ABC-1")
	s.registerClient(owner)
	contender := configurationTestClient(s, owner.callsign)
	release, err := s.acquireConfiguration(owner, false, true, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var stripeOnce, registryOnce, failedOnce, checkedOnce sync.Once
	releaseStripe := func() { stripeOnce.Do(release) }
	s.clientsMutex.Lock()
	unlockRegistry := func() { registryOnce.Do(s.clientsMutex.Unlock) }
	failed, checked := make(chan struct{}), make(chan struct{})
	s.testLoginCapacityProbe = func(c *Client, ok, full bool) {
		if c != contender {
			return
		}
		if !ok {
			failedOnce.Do(func() { close(failed) })
		} else if !full {
			checkedOnce.Do(func() { close(checked) })
		}
	}
	result, finished := ceilingRestoreAsync(s, contender, time.Now().Add(5*time.Second))
	t.Cleanup(func() { unlockRegistry(); releaseStripe(); contender.interrupt(); waitConfigurationTest(t, finished) })
	waitConfigurationTest(t, failed)
	unlockRegistry()
	waitConfigurationTest(t, checked)
	select {
	case err := <-result:
		t.Fatalf("same-identity replacement returned before handoff: %v", err)
	default:
	}
	releaseStripe()
	if err := ceilingResult(t, result); err != nil {
		t.Fatalf("same-identity replacement at capacity: %v", err)
	}
	s.clientsMutex.RLock()
	current, count, reserved := s.clients[owner.callsign], len(s.clients), s.sessionReservations
	s.clientsMutex.RUnlock()
	if current != contender || count != 1 || reserved != 0 {
		t.Fatalf("replacement ownership/count/reservations: %p %d %d", current, count, reserved)
	}
	s.unregisterClient(owner)
	s.clientsMutex.RLock()
	current = s.clients[owner.callsign]
	s.clientsMutex.RUnlock()
	if current != contender {
		t.Fatal("retired owner removed its replacement")
	}
	s.unregisterClient(contender)
}

func TestSessionCeilingRecheckReservedButNotFull(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	holder := configurationTestClient(s, "K1ABC-1")
	contender := configurationTestClient(s, recheckCollidingCall(t, holder.callsign))
	reservedClient := configurationTestClient(s, recheckDifferentStripeCall(t, holder.callsign))
	releaseReservationStripe, reserved, err := s.acquireLoginCapacity(reservedClient, time.Now().Add(5*time.Second))
	if err != nil || !reserved {
		t.Fatalf("setup did not reserve last slot: reserved=%t err=%v", reserved, err)
	}
	var reservationOnce sync.Once
	releaseReservation := func() { reservationOnce.Do(func() { s.releaseSessionReservation(); releaseReservationStripe() }) }
	t.Cleanup(releaseReservation)
	release, err := s.acquireConfiguration(holder, false, true, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var stripeOnce sync.Once
	releaseStripe := func() { stripeOnce.Do(release) }
	var probes atomic.Int64
	rechecked := make(chan struct{})
	s.testLoginCapacityProbe = func(c *Client, checked, full bool) {
		if c == contender && checked && !full && probes.Add(1) == 2 {
			close(rechecked)
		}
	}
	result, finished := ceilingRestoreAsync(s, contender, time.Now().Add(5*time.Second))
	t.Cleanup(func() {
		releaseReservation()
		releaseStripe()
		contender.interrupt()
		waitConfigurationTest(t, finished)
	})
	waitConfigurationTest(t, rechecked)
	select {
	case err := <-result:
		t.Fatalf("reservation was mistaken for a full registry: %v", err)
	default:
	}
	recheckNoProfile(t, contender.callsign)
	releaseStripe()
	ceilingCapacityWaitObserved(t, s)
	releaseReservation()
	if err := ceilingResult(t, result); err != nil {
		t.Fatalf("waiting login could not take released reservation: %v", err)
	}
	s.unregisterClient(contender)
	ceilingOccupancy(t, s, 0, 0)
}

func TestSessionCeilingRecheckCancellationUnderRegistryContention(t *testing.T) {
	for _, mode := range []string{"deadline", "client", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			s := newSessionCeilingServer(t, 1)
			owner := configurationTestClient(s, "K1ABC-1")
			s.registerClient(owner)
			contender := configurationTestClient(s, recheckCollidingCall(t, owner.callsign))
			release, err := s.acquireConfiguration(owner, false, true, time.Time{})
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
			deadline := time.Now().Add(5 * time.Second)
			if mode == "deadline" {
				deadline = time.Now().Add(300 * time.Millisecond)
			}
			result, finished := ceilingRestoreAsync(s, contender, deadline)
			t.Cleanup(func() { unlockRegistry(); releaseStripe(); contender.interrupt(); waitConfigurationTest(t, finished) })
			waitConfigurationTest(t, failed)
			switch mode {
			case "client":
				contender.interrupt()
			case "shutdown":
				close(s.shutdown)
			}
			err = ceilingResult(t, result)
			if mode == "deadline" {
				if err == nil || time.Now().Before(deadline) {
					t.Fatalf("login did not expire at its original deadline: %v", err)
				}
			} else if !errors.Is(err, errClientClosed) {
				t.Fatalf("%s did not end blocked login: %v", mode, err)
			}
			waitConfigurationTest(t, finished)
			recheckNoProfile(t, contender.callsign)
		})
	}
}

func TestSessionCeilingRecheckPollIsBounded(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	owner := configurationTestClient(s, "K1ABC-1")
	s.registerClient(owner)
	contender := configurationTestClient(s, recheckCollidingCall(t, owner.callsign))
	release, err := s.acquireConfiguration(owner, false, true, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var stripeOnce, registryOnce sync.Once
	releaseStripe := func() { stripeOnce.Do(release) }
	s.clientsMutex.Lock()
	unlockRegistry := func() { registryOnce.Do(s.clientsMutex.Unlock) }
	var calls atomic.Int64
	first, third := make(chan struct{}), make(chan struct{})
	s.testLoginCapacityProbe = func(c *Client, _, _ bool) {
		if c == contender {
			switch calls.Add(1) {
			case 1:
				close(first)
			case 3:
				close(third)
			}
		}
	}
	result, finished := ceilingRestoreAsync(s, contender, time.Now().Add(5*time.Second))
	t.Cleanup(func() { unlockRegistry(); releaseStripe(); contender.interrupt(); waitConfigurationTest(t, finished) })
	waitConfigurationTest(t, first)
	waitConfigurationTest(t, third)
	// A held registry and stripe create sustained contention. Polling must make
	// progress without an unbounded spin. Scale the bound by actual elapsed
	// time, so a busy test host cannot make a delayed timer look like a spin.
	before, started := calls.Load(), time.Now()
	timer := time.NewTimer(5 * sessionCapacityRecheckInterval)
	<-timer.C
	elapsed := time.Since(started)
	count := calls.Load() - before
	maximum := int64(elapsed/sessionCapacityRecheckInterval) + 2
	if count > maximum {
		t.Fatalf("registry probes during %s = %d, bounded by %d", elapsed, count, maximum)
	}
	contender.interrupt()
	if err := ceilingResult(t, result); !errors.Is(err, errClientClosed) {
		t.Fatalf("blocked recheck did not stop on interruption: %v", err)
	}
	waitConfigurationTest(t, finished)
}

func TestSessionCeilingRecheckRealListenerFullResponse(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	call := recheckCollidingCall(t, "K1ABC-1")
	failed := make(chan struct{})
	var stripeOnce, registryOnce, probeOnce sync.Once
	s.testLoginCapacityProbe = func(c *Client, checked, _ bool) {
		if c.callsign == call && !checked {
			probeOnce.Do(func() { close(failed) })
		}
	}
	address := startSessionCeilingListener(t, s)
	ownerConn := ceilingConnect(t, address, "K1ABC-1")
	ceilingReadThrough(t, ownerConn, "READY\r\n")
	s.clientsMutex.RLock()
	owner := s.clients["K1ABC-1"]
	s.clientsMutex.RUnlock()
	if owner == nil {
		t.Fatal("listener did not register the owner")
	}
	release, err := s.acquireConfiguration(owner, false, true, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	releaseStripe := func() { stripeOnce.Do(release) }
	defer releaseStripe()
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(t.Context(), "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ceilingReadThrough(t, conn, "login: ")
	s.clientsMutex.Lock()
	unlockRegistry := func() { registryOnce.Do(s.clientsMutex.Unlock) }
	defer unlockRegistry()
	if _, err := io.WriteString(conn, call+"\r\n"); err != nil {
		t.Fatal(err)
	}
	waitConfigurationTest(t, failed)
	unlockRegistry()
	ceilingReadThrough(t, conn, "Server full")
	ceilingWaitEOF(t, conn)
	waitSessionCeiling(t, func() bool {
		active, _, _, _, _, _, _ := s.PreloginMetricSnapshot()
		return active == 0
	})
	recheckNoProfile(t, call)
}
