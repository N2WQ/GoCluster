package telnet

import (
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

func TestSessionCeilingFullRejectDoesNotWaitForCollidingStripe(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	owner := configurationTestClient(s, "K1ABC-1")
	s.registerClient(owner)
	var call string
	for i := 2; i < 10000; i++ {
		candidate := fmt.Sprintf("K%dABC", i)
		if configurationStripe(candidate) == configurationStripe(owner.callsign) {
			call = candidate
			break
		}
	}
	if call == "" {
		t.Fatal("could not find a different callsign on the owner's stripe")
	}
	release, err := s.acquireConfiguration(owner, false, true, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	contender := configurationTestClient(s, call)
	result, finished := ceilingRestoreAsync(s, contender, time.Now().Add(5*time.Second))
	if err := ceilingResult(t, result); !errors.Is(err, errSessionFull) {
		t.Fatalf("full server did not reject different identity: %v", err)
	}
	waitConfigurationTest(t, finished)
	if _, err := os.Stat(filepath.Join(filter.UserDataDir, call+".yaml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected login wrote its profile: %v", err)
	}
}

// ceilingBlockedRestore holds one admitted login inside its first record commit.
// Other commits remain independent, so they can prove progress while it waits.
func ceilingBlockedRestore(t *testing.T, s *Server) (<-chan struct{}, func()) {
	t.Helper()
	entered, unblock := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var releaseOnce sync.Once
	s.commitLoginRecordFn = func(prepared *filter.PreparedUserRecord) error {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-unblock
		}
		return prepared.Commit()
	}
	release := func() { releaseOnce.Do(func() { close(unblock) }) }
	t.Cleanup(release)
	return entered, release
}

func ceilingCapacityWaitObserved(t *testing.T, s *Server) {
	t.Helper()
	waitSessionCeiling(t, func() bool {
		s.clientsMutex.RLock()
		defer s.clientsMutex.RUnlock()
		return s.sessionCapacityChanged != nil
	})
}

func ceilingResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("login operation did not finish")
		return nil
	}
}

func ceilingOccupancy(t *testing.T, s *Server, wantRegistered, wantReserved int) {
	t.Helper()
	s.clientsMutex.RLock()
	registered, reserved := len(s.clients), s.sessionReservations
	s.clientsMutex.RUnlock()
	if registered != wantRegistered || reserved != wantReserved {
		t.Fatalf("registered=%d reserved=%d, want %d and %d", registered, reserved, wantRegistered, wantReserved)
	}
}

func ceilingRestoreAsync(s *Server, c *Client, deadline time.Time) (<-chan error, <-chan struct{}) {
	result, finished := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(finished)
		_, err := s.restoreAndRegisterClient(c, time.Now().UTC(), deadline)
		result <- err
	}()
	return result, finished
}

func TestSessionCeilingWaitReleasesCollidingOwnerStripe(t *testing.T) {
	s := newSessionCeilingServer(t, 2)
	ownerCall := "K1ABC-1"
	var waitingCall, firstCall string
	for i := 2; i < 10000 && (waitingCall == "" || firstCall == ""); i++ {
		call := fmt.Sprintf("K%dABC", i)
		if configurationStripe(call) == configurationStripe(ownerCall) {
			waitingCall = call
		} else if firstCall == "" {
			firstCall = call
		}
	}
	if waitingCall == "" || firstCall == "" {
		t.Fatal("could not find test identities with required stripe placement")
	}
	owner := configurationTestClient(s, ownerCall)
	s.registerClient(owner)
	ceilingOccupancy(t, s, 1, 0)
	first := configurationTestClient(s, firstCall)
	waiter := configurationTestClient(s, waitingCall)
	entered, release := ceilingBlockedRestore(t, s)
	deadline := time.Now().Add(10 * time.Second)
	firstDone, firstFinished := ceilingRestoreAsync(s, first, deadline)
	var waitingFinished, ownerFinished <-chan struct{}
	t.Cleanup(func() {
		release()
		first.interrupt()
		waiter.interrupt()
		waitConfigurationTest(t, firstFinished)
		if waitingFinished != nil {
			waitConfigurationTest(t, waitingFinished)
		}
		if ownerFinished != nil {
			waitConfigurationTest(t, ownerFinished)
		}
	})
	waitConfigurationTest(t, entered)
	ceilingOccupancy(t, s, 1, 1)
	waitingDone, finished := ceilingRestoreAsync(s, waiter, deadline)
	waitingFinished = finished
	ceilingCapacityWaitObserved(t, s)
	select {
	case err := <-waitingDone:
		t.Fatalf("waiter returned before owner departure: %v", err)
	default:
	}
	ownerDone := make(chan struct{})
	ownerFinished = ownerDone
	go func() { s.unregisterClient(owner); close(ownerDone) }()
	// This must complete while the unrelated first login remains blocked. The
	// waiting callsign deliberately shares the old owner's stripe.
	waitConfigurationTest(t, ownerDone)
	if err := ceilingResult(t, waitingDone); err != nil {
		t.Fatalf("waiter did not take the departing owner's capacity: %v", err)
	}
	ceilingOccupancy(t, s, 1, 1)
	release()
	if err := ceilingResult(t, firstDone); err != nil {
		t.Fatalf("blocked login did not complete: %v", err)
	}
	ceilingOccupancy(t, s, 2, 0)
	s.unregisterClient(waiter)
	s.unregisterClient(first)
	ceilingOccupancy(t, s, 0, 0)
}

func TestSessionCeilingWaitingLoginCancellation(t *testing.T) {
	for _, mode := range []string{"deadline", "client", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			s := newSessionCeilingServer(t, 1)
			first := configurationTestClient(s, "K1ABC-1")
			waiter := configurationTestClient(s, "K1ABC-2")
			entered, release := ceilingBlockedRestore(t, s)
			firstDone, firstFinished := ceilingRestoreAsync(s, first, time.Now().Add(10*time.Second))
			var waitingFinished <-chan struct{}
			t.Cleanup(func() {
				release()
				first.interrupt()
				waiter.interrupt()
				waitConfigurationTest(t, firstFinished)
				if waitingFinished != nil {
					waitConfigurationTest(t, waitingFinished)
				}
			})
			waitConfigurationTest(t, entered)
			deadline := time.Now().Add(10 * time.Second)
			if mode == "deadline" {
				deadline = time.Now().Add(time.Second)
			}
			waitingDone, finished := ceilingRestoreAsync(s, waiter, deadline)
			waitingFinished = finished
			ceilingCapacityWaitObserved(t, s)
			switch mode {
			case "client":
				waiter.interrupt()
			case "shutdown":
				close(s.shutdown)
			}
			err := ceilingResult(t, waitingDone)
			if mode == "deadline" {
				if err == nil || !strings.Contains(err.Error(), "login deadline") || time.Now().Before(deadline) {
					t.Fatalf("deadline result = %v", err)
				}
			} else if !errors.Is(err, errClientClosed) {
				t.Fatalf("%s result = %v", mode, err)
			}
			ceilingOccupancy(t, s, 0, 1)
			release()
			err = ceilingResult(t, firstDone)
			if mode == "shutdown" {
				if !errors.Is(err, errClientClosed) {
					t.Fatalf("shutdown blocked login = %v", err)
				}
				ceilingOccupancy(t, s, 0, 0)
				return
			}
			if err != nil {
				t.Fatalf("first login after waiter cancellation = %v", err)
			}
			ceilingOccupancy(t, s, 1, 0)
			s.unregisterClient(first)
			ceilingOccupancy(t, s, 0, 0)
			later := configurationTestClient(s, "K1ABC-3")
			if _, err := s.restoreAndRegisterClient(later, time.Now().UTC(), time.Now().Add(5*time.Second)); err != nil {
				t.Fatalf("capacity not reusable after cancellation: %v", err)
			}
			s.unregisterClient(later)
			ceilingOccupancy(t, s, 0, 0)
		})
	}
}

func TestSessionCeilingBlockedReservationCancellation(t *testing.T) {
	for _, mode := range []string{"deadline", "client", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			s := newSessionCeilingServer(t, 1)
			first := configurationTestClient(s, "K1ABC-1")
			entered, release := ceilingBlockedRestore(t, s)
			deadline := time.Now().Add(10 * time.Second)
			if mode == "deadline" {
				deadline = time.Now().Add(time.Second)
			}
			firstDone, firstFinished := ceilingRestoreAsync(s, first, deadline)
			t.Cleanup(func() { release(); first.interrupt(); waitConfigurationTest(t, firstFinished) })
			waitConfigurationTest(t, entered)
			ceilingOccupancy(t, s, 0, 1)
			switch mode {
			case "deadline":
				timer := time.NewTimer(time.Until(deadline))
				<-timer.C
			case "client":
				first.interrupt()
			case "shutdown":
				close(s.shutdown)
			}
			release()
			err := ceilingResult(t, firstDone)
			if mode == "deadline" {
				if err == nil || !strings.Contains(err.Error(), "login deadline") {
					t.Fatalf("deadline result = %v", err)
				}
			} else if !errors.Is(err, errClientClosed) {
				t.Fatalf("%s result = %v", mode, err)
			}
			ceilingOccupancy(t, s, 0, 0)
			if mode == "shutdown" {
				return
			}
			later := configurationTestClient(s, "K1ABC-2")
			if _, err := s.restoreAndRegisterClient(later, time.Now().UTC(), time.Now().Add(5*time.Second)); err != nil {
				t.Fatalf("released reservation was not reusable: %v", err)
			}
			s.unregisterClient(later)
			ceilingOccupancy(t, s, 0, 0)
		})
	}
}
