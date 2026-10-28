package telnet

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
)

func TestBaseSessionQuotaDeadlineAndShutdownCleanup(t *testing.T) {
	for _, mode := range []string{"waiting_deadline", "holder_deadline", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			s := newBaseQuotaServer(t, 0, 1)
			holder := configurationTestClient(s, "K1ABC-1")
			waiter := configurationTestClient(s, "K1ABC-2")
			entered, release := ceilingBlockedRestore(t, s)
			holderDeadline := time.Now().Add(10 * time.Second)
			if mode == "holder_deadline" {
				holderDeadline = time.Now().Add(time.Second)
			}
			holderDone, holderFinished := ceilingRestoreAsync(s, holder, holderDeadline)
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
			waiterDeadline := time.Now().Add(10 * time.Second)
			if mode == "waiting_deadline" {
				waiterDeadline = time.Now().Add(time.Second)
			}
			waiterDone, finished := ceilingRestoreAsync(s, waiter, waiterDeadline)
			waiterFinished = finished
			ceilingCapacityWaitObserved(t, s)
			baseQuotaCounts(t, s, "K1ABC", 0, 1)

			switch mode {
			case "waiting_deadline":
				if err := ceilingResult(t, waiterDone); err == nil || !strings.Contains(err.Error(), "login deadline") {
					t.Fatalf("waiting login deadline = %v", err)
				}
				baseQuotaCounts(t, s, "K1ABC", 0, 1)
				recheckNoProfile(t, waiter.callsign)
				release()
				if err := ceilingResult(t, holderDone); err != nil {
					t.Fatalf("holder after waiter deadline: %v", err)
				}
				s.unregisterClient(holder)
				later := baseQuotaRestore(t, s, "K1ABC-3")
				s.unregisterClient(later)
			case "holder_deadline":
				timer := time.NewTimer(time.Until(holderDeadline))
				<-timer.C
				release()
				if err := ceilingResult(t, holderDone); err == nil || !strings.Contains(err.Error(), "login deadline") {
					t.Fatalf("holder deadline = %v", err)
				}
				if err := ceilingResult(t, waiterDone); err != nil {
					t.Fatalf("waiter after holder deadline: %v", err)
				}
				s.unregisterClient(waiter)
			case "shutdown":
				close(s.shutdown)
				if err := ceilingResult(t, waiterDone); !errors.Is(err, errClientClosed) {
					t.Fatalf("waiting login on shutdown = %v", err)
				}
				baseQuotaCounts(t, s, "K1ABC", 0, 1)
				release()
				if err := ceilingResult(t, holderDone); !errors.Is(err, errClientClosed) {
					t.Fatalf("reserved login on shutdown = %v", err)
				}
			}
			baseQuotaCounts(t, s, "K1ABC", 0, 0)
			s.clientsMutex.RLock()
			registered, reserved, bases := len(s.clients), s.sessionReservations, len(s.baseSessionCounts)
			s.clientsMutex.RUnlock()
			if registered != 0 || reserved != 0 || bases != 0 {
				t.Fatalf("after %s: registered=%d reserved=%d bases=%d", mode, registered, reserved, bases)
			}
		})
	}
}

func TestBaseSessionQuotaGlobalAndStationPending(t *testing.T) {
	s := newBaseQuotaServer(t, 2, 1)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	s.commitLoginRecordFn = func(prepared *filter.PreparedUserRecord) error {
		entered <- struct{}{}
		<-release
		return prepared.Commit()
	}
	first := configurationTestClient(s, "K1ABC-1")
	second := configurationTestClient(s, "W1XYZ-1")
	same := configurationTestClient(s, "K1ABC-2")
	other := configurationTestClient(s, "N1XYZ-1")
	firstDone, firstFinished := ceilingRestoreAsync(s, first, time.Now().Add(10*time.Second))
	secondDone, secondFinished := ceilingRestoreAsync(s, second, time.Now().Add(10*time.Second))
	var sameFinished, otherFinished <-chan struct{}
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		for _, c := range []*Client{first, second, same, other} {
			c.interrupt()
		}
		waitConfigurationTest(t, firstFinished)
		waitConfigurationTest(t, secondFinished)
		if sameFinished != nil {
			waitConfigurationTest(t, sameFinished)
		}
		if otherFinished != nil {
			waitConfigurationTest(t, otherFinished)
		}
	})
	waitConfigurationTest(t, entered)
	waitConfigurationTest(t, entered)
	baseQuotaCounts(t, s, "K1ABC", 0, 1)
	baseQuotaCounts(t, s, "W1XYZ", 0, 1)
	ceilingOccupancy(t, s, 0, 2)
	sameDone, sameCompletion := ceilingRestoreAsync(s, same, time.Now().Add(10*time.Second))
	sameFinished = sameCompletion
	otherDone, otherCompletion := ceilingRestoreAsync(s, other, time.Now().Add(10*time.Second))
	otherFinished = otherCompletion
	ceilingCapacityWaitObserved(t, s)
	select {
	case err := <-sameDone:
		t.Fatalf("station waiter rejected before reservations resolved: %v", err)
	case err := <-otherDone:
		t.Fatalf("global waiter rejected before reservations resolved: %v", err)
	default:
	}
	close(release)
	released = true
	if err := ceilingResult(t, firstDone); err != nil {
		t.Fatalf("first reserved login: %v", err)
	}
	if err := ceilingResult(t, secondDone); err != nil {
		t.Fatalf("second reserved login: %v", err)
	}
	// The station may fill before the second registration takes the global
	// slot; either reason is then valid. The other base can only hit global.
	if err := ceilingResult(t, sameDone); !errors.Is(err, errBaseSessionFull) && !errors.Is(err, errSessionFull) {
		t.Fatalf("full quota for %s: %v", same.callsign, err)
	}
	if err := ceilingResult(t, otherDone); !errors.Is(err, errSessionFull) {
		t.Fatalf("full global quota for %s: %v", other.callsign, err)
	}
	recheckNoProfile(t, same.callsign)
	recheckNoProfile(t, other.callsign)
	baseQuotaCounts(t, s, "K1ABC", 1, 0)
	baseQuotaCounts(t, s, "W1XYZ", 1, 0)
	ceilingOccupancy(t, s, 2, 0)
	s.unregisterClient(first)
	s.unregisterClient(second)
	ceilingOccupancy(t, s, 0, 0)
}

func TestBaseSessionQuotaSampledDiagnosticCardinality(t *testing.T) {
	s := newBaseQuotaServer(t, 0, 1)
	first := baseQuotaRestore(t, s, "K1ABC-1")
	second := baseQuotaRestore(t, s, "W1XYZ-1")
	t.Cleanup(func() { s.unregisterClient(first); s.unregisterClient(second) })
	s.admissionLogSample = 1
	s.admissionLogMaxLines = 1
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	s.logAdmissionReject("max_sessions_per_base_call", "203.0.113.5")
	s.logAdmissionReject("max_sessions_per_base_call", "203.0.113.5")
	log.SetOutput(previous)
	got := output.String()
	if !strings.Contains(got, "reason=max_sessions_per_base_call") || !strings.Contains(got, "tracked_session_bases=2") {
		t.Fatalf("sample lacks station reason or cardinality: %q", got)
	}
	if count := strings.Count(got, "Admission reject sample:"); count != 1 {
		t.Fatalf("bounded sample count=%d, output=%q", count, got)
	}
}
