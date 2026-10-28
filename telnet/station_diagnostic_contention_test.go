package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
)

// The reporter must finish its snapshot while Stop is still invoking it. The
// callback watchdog also lets the old lock cycle unwind rather than leaking a
// permanently blocked shutdown goroutine when this regression fails.
func TestStationDiagnosticSnapshotInShutdownReporter(t *testing.T) {
	s := newBaseQuotaServer(t, 0, 1)
	owner := baseQuotaRestore(t, s, "K1ABC-1")
	stopped := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-stopped:
			s.unregisterClient(owner)
		default:
			// Do not strand the test process if an unrelated shutdown failure
			// prevents the watchdog-protected callback from returning.
		}
	})

	type reportResult struct {
		completed bool
		count     int
	}
	reported := make(chan reportResult, 1)
	snapshotDone := make(chan struct{})
	s.connectionReporter = func(event ConnectionEvent) {
		if event.Action != "disconnect" || event.Reason != "server shutdown" {
			return
		}
		result := make(chan int, 1)
		go func() {
			result <- s.PreloginAdmissionMetricSnapshot().TrackedSessionBases
			close(snapshotDone)
		}()
		select {
		case count := <-result:
			reported <- reportResult{completed: true, count: count}
		case <-time.After(time.Second):
			reported <- reportResult{}
		}
	}
	go func() { s.Stop(); close(stopped) }()
	waitConfigurationTest(t, stopped)
	waitConfigurationTest(t, snapshotDone)
	select {
	case got := <-reported:
		if !got.completed || got.count != 1 {
			t.Fatalf("shutdown reporter snapshot completed=%t tracked bases=%d, want true and 1", got.completed, got.count)
		}
	default:
		t.Fatal("shutdown did not call the connection reporter")
	}
}

// A sampled station rejection must not wait on the registry after the full
// decision has already been made, even when the rejected peer does not read.
func TestStationDiagnosticSampledRejectionReleasesTicketWhileRegistryLocked(t *testing.T) {
	s := newBaseQuotaServer(t, 0, 1)
	s.maxPreloginSessions = 1
	s.rejectWriteDeadline = 500 * time.Millisecond
	s.admissionLogSample = 1
	s.admissionLogMaxLines = 1
	owner := baseQuotaRestore(t, s, "K1ABC-1")
	profile := filepath.Join(filter.UserDataDir, "K1ABC-2.yaml")
	profileBefore := oldLoginRecord(4)
	if err := os.WriteFile(profile, profileBefore, 0o600); err != nil {
		s.unregisterClient(owner)
		t.Fatal(err)
	}
	classified, allowClassification := make(chan struct{}), make(chan struct{})
	var classifiedOnce, releaseOnce sync.Once
	releaseClassification := func() { releaseOnce.Do(func() { close(allowClassification) }) }
	s.testLoginCapacityProbe = func(c *Client, checked, full bool) {
		if c.callsign == "K1ABC-2" && checked && full {
			classifiedOnce.Do(func() { close(classified) })
			<-allowClassification
		}
	}

	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	var restoreLogOnce sync.Once
	restoreLog := func() { restoreLogOnce.Do(func() { log.SetOutput(previous) }) }
	ticket, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.5", 1000))
	if ticket == nil || reason != "" {
		restoreLog()
		s.unregisterClient(owner)
		t.Fatalf("prelogin ticket: %v, %q", ticket, reason)
	}
	serverEnd, peer := net.Pipe()
	observed := &baseQuotaObservedConn{Conn: serverEnd, writeStarted: make(chan struct{}), closed: make(chan struct{})}
	handlerDone := make(chan struct{})
	locked := false
	t.Cleanup(func() {
		// Unblock both the capacity probe and the logger before waiting for the
		// handler; this also unwinds the original failure without leaking it.
		if locked {
			s.clientsMutex.Unlock()
			locked = false
		}
		releaseClassification()
		_ = peer.Close()
		_ = serverEnd.Close()
		select {
		case <-handlerDone:
		case <-time.After(3 * time.Second):
			t.Error("rejection handler did not drain during cleanup")
		}
		restoreLog()
		s.unregisterClient(owner)
	})
	go func() { defer close(handlerDone); s.handleClient(observed, ticket) }()
	ceilingReadThrough(t, peer, "login: ")
	if _, err := io.WriteString(peer, "K1ABC-2\r\n"); err != nil {
		t.Fatal(err)
	}
	waitConfigurationTest(t, classified)
	s.clientsMutex.Lock()
	locked = true
	started := time.Now()
	releaseClassification()

	// Keep the peer unread and the registry locked throughout both close and
	// ticket release. The deadline allows scheduling slack for race builds.
	deadline := time.NewTimer(s.rejectWriteDeadline + 2*time.Second)
	defer deadline.Stop()
	select {
	case <-observed.writeStarted:
	case <-deadline.C:
		t.Fatal("sampled rejection did not reach its notice while registry was locked")
	}
	if active := atomic.LoadInt64(&s.metrics.preloginActive); active != 1 {
		t.Fatalf("blocked notice retained %d prelogin tickets, want 1", active)
	}
	select {
	case <-observed.closed:
	case <-deadline.C:
		t.Fatal("rejected connection did not close while registry was locked")
	}
	select {
	case <-handlerDone:
	case <-deadline.C:
		t.Fatal("rejection handler did not finish while registry was locked")
	}
	if elapsed := time.Since(started); elapsed > s.rejectWriteDeadline+2*time.Second {
		t.Fatalf("rejection cleanup took %s with deadline %s", elapsed, s.rejectWriteDeadline)
	}
	if active := atomic.LoadInt64(&s.metrics.preloginActive); active != 0 {
		t.Fatalf("prelogin ticket still active after rejection: %d", active)
	}
	restoreLog()
	got := output.String()
	if !strings.Contains(got, "reason=max_sessions_per_base_call") || !strings.Contains(got, "tracked_session_bases=1") {
		t.Fatalf("sample lacks station reason or cardinality: %q", got)
	}
	if count := strings.Count(got, "Admission reject sample:"); count != 1 {
		t.Fatalf("sample count=%d, output=%q", count, got)
	}
	s.clientsMutex.Unlock()
	locked = false
	s.clientsMutex.RLock()
	ownerStillRegistered := s.clients[owner.callsign] == owner
	rejectedRegistered := s.clients["K1ABC-2"] != nil
	s.clientsMutex.RUnlock()
	if !ownerStillRegistered || rejectedRegistered {
		t.Fatalf("registry changed after rejection: owner=%t rejected=%t", ownerStillRegistered, rejectedRegistered)
	}
	profileAfter, err := os.ReadFile(profile)
	if err != nil || !bytes.Equal(profileBefore, profileAfter) {
		t.Fatalf("rejected login changed saved profile: %v", err)
	}
	if _, err := os.Stat(profile + ".pre-v5.bak"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected login created a profile backup: %v", err)
	}
}

func assertStationDiagnosticCardinality(t *testing.T, s *Server, expected int) {
	t.Helper()
	s.clientsMutex.RLock()
	actual := len(s.baseSessionCounts)
	s.clientsMutex.RUnlock()
	observed := s.PreloginAdmissionMetricSnapshot().TrackedSessionBases
	if actual != expected || observed != actual {
		t.Fatalf("station map entries=%d metric=%d, want %d", actual, observed, expected)
	}
}

func TestStationDiagnosticCardinalityOwnershipLifecycle(t *testing.T) {
	s := newBaseQuotaServer(t, 0, 2)
	assertStationDiagnosticCardinality(t, s, 0)
	pending := configurationTestClient(s, "K1ABC-1")
	entered, release := ceilingBlockedRestore(t, s)
	result, finished := ceilingRestoreAsync(s, pending, time.Now().Add(10*time.Second))
	t.Cleanup(func() {
		release()
		pending.interrupt()
		waitConfigurationTest(t, finished)
	})
	waitConfigurationTest(t, entered)
	assertStationDiagnosticCardinality(t, s, 1)
	other := baseQuotaRestore(t, s, "W1XYZ-1")
	assertStationDiagnosticCardinality(t, s, 2)
	release()
	if err := ceilingResult(t, result); err != nil {
		t.Fatalf("reserved login: %v", err)
	}
	assertStationDiagnosticCardinality(t, s, 2)
	replacement := baseQuotaRestore(t, s, pending.callsign)
	assertStationDiagnosticCardinality(t, s, 2)
	s.unregisterClient(pending)
	assertStationDiagnosticCardinality(t, s, 2)
	s.unregisterClient(replacement)
	assertStationDiagnosticCardinality(t, s, 1)
	s.unregisterClient(other)
	assertStationDiagnosticCardinality(t, s, 0)
	for i := 1; i <= 20; i++ {
		owner := baseQuotaRestore(t, s, fmt.Sprintf("K%dABC-1", i))
		assertStationDiagnosticCardinality(t, s, 1)
		s.unregisterClient(owner)
		assertStationDiagnosticCardinality(t, s, 0)
	}

	disabled := newBaseQuotaServer(t, 2, 0)
	one := baseQuotaRestore(t, disabled, "K1ABC-1")
	two := baseQuotaRestore(t, disabled, "W1XYZ-1")
	assertStationDiagnosticCardinality(t, disabled, 0)
	disabled.unregisterClient(one)
	disabled.unregisterClient(two)
	assertStationDiagnosticCardinality(t, disabled, 0)
}

func TestStationDiagnosticCardinalityPendingTransitions(t *testing.T) {
	t.Run("canceled_last_reservation", func(t *testing.T) {
		s := newBaseQuotaServer(t, 0, 2)
		pending := configurationTestClient(s, "K1ABC-1")
		entered, release := ceilingBlockedRestore(t, s)
		result, finished := ceilingRestoreAsync(s, pending, time.Now().Add(10*time.Second))
		t.Cleanup(func() {
			release()
			pending.interrupt()
			waitConfigurationTest(t, finished)
		})
		waitConfigurationTest(t, entered)
		baseQuotaCounts(t, s, "K1ABC", 0, 1)
		assertStationDiagnosticCardinality(t, s, 1)
		pending.interrupt()
		release()
		if err := ceilingResult(t, result); !errors.Is(err, errClientClosed) {
			t.Fatalf("canceled pending login: %v", err)
		}
		baseQuotaCounts(t, s, "K1ABC", 0, 0)
		assertStationDiagnosticCardinality(t, s, 0)
	})

	t.Run("two_pending_same_base", func(t *testing.T) {
		s := newBaseQuotaServer(t, 0, 2)
		first := configurationTestClient(s, "K1ABC-1")
		second := configurationTestClient(s, "K1ABC-2")
		if configurationStripe(first.callsign) == configurationStripe(second.callsign) {
			t.Fatal("the two identities must use different configuration stripes")
		}
		entered := make(chan struct{}, 2)
		unblock := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(unblock) }) }
		s.commitLoginRecordFn = func(prepared *filter.PreparedUserRecord) error {
			entered <- struct{}{}
			<-unblock
			return prepared.Commit()
		}
		firstResult, firstFinished := ceilingRestoreAsync(s, first, time.Now().Add(10*time.Second))
		secondResult, secondFinished := ceilingRestoreAsync(s, second, time.Now().Add(10*time.Second))
		t.Cleanup(func() {
			release()
			first.interrupt()
			second.interrupt()
			waitConfigurationTest(t, firstFinished)
			waitConfigurationTest(t, secondFinished)
		})
		waitConfigurationTest(t, entered)
		waitConfigurationTest(t, entered)
		baseQuotaCounts(t, s, "K1ABC", 0, 2)
		assertStationDiagnosticCardinality(t, s, 1)
		first.interrupt()
		release()
		if err := ceilingResult(t, firstResult); !errors.Is(err, errClientClosed) {
			t.Fatalf("canceled first login: %v", err)
		}
		if err := ceilingResult(t, secondResult); err != nil {
			t.Fatalf("second login: %v", err)
		}
		baseQuotaCounts(t, s, "K1ABC", 1, 0)
		assertStationDiagnosticCardinality(t, s, 1)
		s.unregisterClient(second)
		baseQuotaCounts(t, s, "K1ABC", 0, 0)
		assertStationDiagnosticCardinality(t, s, 0)
	})
}
