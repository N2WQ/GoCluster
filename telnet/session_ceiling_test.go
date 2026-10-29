package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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

func newSessionCeilingServer(t *testing.T, max int) *Server {
	t.Helper()
	return newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.MaxConnections = max
		opts.BroadcastWorkers = 1
		opts.Transport, opts.EchoMode, opts.LoginGreeting = "native", "off", "READY\n"
		opts.DuplicateLoginMsg = "Replaced by another login"
		opts.MaxPreloginSessions = 32
		opts.PreloginConcurrencyPerIP = 16
		opts.AcceptRatePerIP, opts.AcceptBurstPerIP = 100, 100
		opts.AcceptRatePerSubnet, opts.AcceptBurstPerSubnet = 100, 100
		opts.AcceptRateGlobal, opts.AcceptBurstGlobal = 100, 100
		opts.PreloginTimeout = 5 * time.Second
	})
}

func startSessionCeilingListener(t *testing.T, s *Server) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.startWithListener(listener)
	t.Cleanup(func() {
		s.Stop()
		waitSessionCeiling(t, func() bool {
			active, _, _, _, _, _, _ := s.PreloginMetricSnapshot()
			return s.GetClientCount() == 0 && active == 0
		})
	})
	return listener.Addr().String()
}

func waitSessionCeiling(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("session state did not reach the expected value")
		}
		time.Sleep(time.Millisecond)
	}
}

func ceilingReadThrough(t *testing.T, conn net.Conn, marker string) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	var b [1]byte
	for got.Len() < 65536 {
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			t.Fatalf("read waiting for %q: %v; received %q", marker, err, got.String())
		}
		got.WriteByte(b[0])
		if strings.Contains(got.String(), marker) {
			return
		}
	}
	t.Fatalf("response exceeded limit waiting for %q", marker)
}

func ceilingConnect(t *testing.T, address, call string) net.Conn {
	t.Helper()
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(t.Context(), "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ceilingReadThrough(t, conn, "login: ")
	if _, err := io.WriteString(conn, call+"\r\n"); err != nil {
		t.Fatal(err)
	}
	return conn
}

func ceilingWaitEOF(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err := io.Copy(io.Discard, conn)
	if err != nil {
		t.Fatalf("waiting for connection close: %v", err)
	}
}

func TestSessionCeilingListenerReplacementAndRejectedProfile(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	address := startSessionCeilingListener(t, s)
	old := ceilingConnect(t, address, "LZ3NY-1")
	ceilingReadThrough(t, old, "READY\r\n")

	// An old profile and shared library would both be upgraded by an admitted
	// login. A capacity rejection must not create backups or change either file.
	profile := filepath.Join(filter.UserDataDir, "LZ3NY-2.yaml")
	original := oldLoginRecord(4)
	if err := os.WriteFile(profile, original, 0o600); err != nil {
		t.Fatal(err)
	}
	library := presetLibraryPathFor("LZ3NY-2")
	if err := os.MkdirAll(filepath.Dir(library), 0o700); err != nil {
		t.Fatal(err)
	}
	oldPreset := strings.Split(string(original), "recent_ips:")[0]
	libraryBytes := []byte("presets:\n  OLD:\n    " + strings.ReplaceAll(strings.TrimSuffix(oldPreset, "\n"), "\n", "\n    ") + "\n")
	if err := os.WriteFile(library, libraryBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	rejected := ceilingConnect(t, address, "LZ3NY-2")
	ceilingReadThrough(t, rejected, "Server full")
	ceilingWaitEOF(t, rejected)
	for path, want := range map[string][]byte{profile: original, library: libraryBytes} {
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("capacity rejection changed %s: %v", path, err)
		}
		if _, err := os.Stat(path + ".pre-v6.bak"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("capacity rejection created backup for %s: %v", path, err)
		}
	}
	if got := s.GetClientCount(); got != 1 {
		t.Fatalf("registered count after rejection = %d, want 1", got)
	}

	// Existing normalization removes /P and folds case before exact comparison.
	replacement := ceilingConnect(t, address, "lz3ny-1/p")
	ceilingReadThrough(t, replacement, "READY\r\n")
	ceilingReadThrough(t, old, "Replaced by another login")
	ceilingWaitEOF(t, old)
	if got := s.GetClientCount(); got != 1 {
		t.Fatalf("registered count after replacement = %d, want 1", got)
	}
	_ = replacement.Close()
	waitSessionCeiling(t, func() bool { return s.GetClientCount() == 0 })

	// A departure frees the registered slot for a different identity.
	next := ceilingConnect(t, address, "K1ABC-4")
	ceilingReadThrough(t, next, "READY\r\n")
	if got := s.GetClientCount(); got != 1 {
		t.Fatalf("registered count after departure and login = %d, want 1", got)
	}
}

func TestSessionCeilingDisabledAPI(t *testing.T) {
	for _, limit := range []int{0, -1} {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
			s := newSessionCeilingServer(t, limit)
			address := startSessionCeilingListener(t, s)
			for _, call := range []string{"K1ABC-1", "K1ABC-2"} {
				conn := ceilingConnect(t, address, call)
				ceilingReadThrough(t, conn, "READY\r\n")
			}
			if got := s.GetClientCount(); got != 2 {
				t.Fatalf("disabled limit registered %d clients, want 2", got)
			}
		})
	}
}

func TestSessionCeilingContendedReservation(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_first_%t", cancelFirst), func(t *testing.T) {
			s := newSessionCeilingServer(t, 1)
			first := configurationTestClient(s, "K1ABC-1")
			second := configurationTestClient(s, "K1ABC-2")
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			s.commitLoginRecordFn = func(prepared *filter.PreparedUserRecord) error {
				once.Do(func() { close(entered); <-release })
				return prepared.Commit()
			}
			firstDone, secondDone := make(chan error, 1), make(chan error, 1)
			firstFinished, secondFinished := make(chan struct{}), make(chan struct{})
			deadline := time.Now().Add(5 * time.Second)
			go func() {
				defer close(firstFinished)
				_, err := s.restoreAndRegisterClient(first, time.Now().UTC(), deadline)
				firstDone <- err
			}()
			waitConfigurationTest(t, entered)
			s.clientsMutex.RLock()
			registered, reserved := len(s.clients), s.sessionReservations
			s.clientsMutex.RUnlock()
			if registered != 0 || reserved != 1 {
				t.Fatalf("blocked login: registered=%d reserved=%d, want 0 and 1", registered, reserved)
			}
			go func() {
				defer close(secondFinished)
				_, err := s.restoreAndRegisterClient(second, time.Now().UTC(), deadline)
				secondDone <- err
			}()
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				waitConfigurationTest(t, firstFinished)
				waitConfigurationTest(t, secondFinished)
			})
			waitSessionCeiling(t, func() bool {
				s.clientsMutex.RLock()
				defer s.clientsMutex.RUnlock()
				return s.sessionCapacityChanged != nil
			})
			select {
			case err := <-secondDone:
				t.Fatalf("competing login completed before reservation resolved: %v", err)
			default:
			}
			if _, err := os.Stat(filepath.Join(filter.UserDataDir, second.callsign+".yaml")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("waiting login wrote its profile: %v", err)
			}
			if cancelFirst {
				first.interrupt()
			}
			releaseOnce.Do(func() { close(release) })
			waitConfigurationTest(t, firstFinished)
			waitConfigurationTest(t, secondFinished)
			firstErr, secondErr := <-firstDone, <-secondDone
			if cancelFirst {
				if firstErr == nil || secondErr != nil {
					t.Fatalf("canceled first=%v, waiting second=%v", firstErr, secondErr)
				}
				if s.GetClientCount() != 1 || s.clients[second.callsign] != second {
					t.Fatal("waiting login did not take released capacity")
				}
				s.unregisterClient(second)
			} else {
				if firstErr != nil || !errors.Is(secondErr, errSessionFull) {
					t.Fatalf("first=%v, competing second=%v", firstErr, secondErr)
				}
				if s.GetClientCount() != 1 || s.clients[first.callsign] != first {
					t.Fatal("first login did not retain its slot")
				}
				if _, err := os.Stat(filepath.Join(filter.UserDataDir, second.callsign+".yaml")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("capacity-rejected login wrote its profile: %v", err)
				}
				s.unregisterClient(first)
			}
			s.clientsMutex.RLock()
			reserved = s.sessionReservations
			s.clientsMutex.RUnlock()
			if reserved != 0 || s.GetClientCount() != 0 {
				t.Fatalf("capacity did not recover: registered=%d reserved=%d", s.GetClientCount(), reserved)
			}
		})
	}
}

func TestSessionCeilingManyContenders(t *testing.T) {
	const limit, attempts = 3, 16
	s := newSessionCeilingServer(t, limit)
	entered := make(chan struct{}, attempts)
	release, start := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
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
	var workers sync.WaitGroup
	for i := 0; i < attempts; i++ {
		client := configurationTestClient(s, fmt.Sprintf("K1ABC-%d", i))
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, err := s.restoreAndRegisterClient(client, time.Now().UTC(), time.Now().Add(5*time.Second))
			results <- result{client, err}
		}()
	}
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		waitConfigurationTest(t, finished)
		s.clientsMutex.RLock()
		owners := make([]*Client, 0, len(s.clients))
		for _, client := range s.clients {
			owners = append(owners, client)
		}
		s.clientsMutex.RUnlock()
		for _, client := range owners {
			s.unregisterClient(client)
		}
	})
	close(start)
	for i := 0; i < limit; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("admitted contenders did not reach blocked profile commit")
		}
	}
	waitSessionCeiling(t, func() bool {
		s.clientsMutex.RLock()
		defer s.clientsMutex.RUnlock()
		return s.sessionCapacityChanged != nil
	})
	s.clientsMutex.RLock()
	registered, reserved := len(s.clients), s.sessionReservations
	s.clientsMutex.RUnlock()
	if registered != 0 || reserved != limit {
		t.Fatalf("blocked contenders: registered=%d reserved=%d, want 0 and %d", registered, reserved, limit)
	}
	select {
	case got := <-results:
		t.Fatalf("contender completed before blocked commits were released: %v", got.err)
	default:
	}
	var exceeded atomic.Bool
	stopObserving, observerDone := make(chan struct{}), make(chan struct{})
	var stopObservingOnce sync.Once
	go func() {
		defer close(observerDone)
		for {
			select {
			case <-stopObserving:
				return
			default:
			}
			s.clientsMutex.RLock()
			if len(s.clients)+s.sessionReservations > limit {
				exceeded.Store(true)
			}
			s.clientsMutex.RUnlock()
		}
	}()
	t.Cleanup(func() {
		stopObservingOnce.Do(func() { close(stopObserving) })
		waitConfigurationTest(t, observerDone)
	})
	releaseOnce.Do(func() { close(release) })
	waitConfigurationTest(t, finished)
	stopObservingOnce.Do(func() { close(stopObserving) })
	waitConfigurationTest(t, observerDone)
	if exceeded.Load() {
		t.Fatal("registered sessions and reserved slots exceeded the ceiling")
	}
	var successes, full int
	for i := 0; i < attempts; i++ {
		got := <-results
		switch {
		case got.err == nil:
			successes++
		case errors.Is(got.err, errSessionFull):
			full++
			if _, err := os.Stat(filepath.Join(filter.UserDataDir, got.client.callsign+".yaml")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rejected %s wrote a profile: %v", got.client.callsign, err)
			}
		default:
			t.Fatalf("unexpected contender result for %s: %v", got.client.callsign, got.err)
		}
	}
	if successes != limit || full != attempts-limit || s.GetClientCount() != limit {
		t.Fatalf("successes=%d full=%d registered=%d, want %d, %d, %d", successes, full, s.GetClientCount(), limit, attempts-limit, limit)
	}
	for i := 0; i < attempts; i++ {
		// Each identity is unique. Removal is checked via the registry rather
		// than assuming a particular set of scheduler-selected winners.
		call := fmt.Sprintf("K1ABC-%d", i)
		s.clientsMutex.RLock()
		client := s.clients[call]
		s.clientsMutex.RUnlock()
		if client != nil {
			s.unregisterClient(client)
		}
	}
	s.clientsMutex.RLock()
	registered, reserved = len(s.clients), s.sessionReservations
	s.clientsMutex.RUnlock()
	if registered != 0 || reserved != 0 {
		t.Fatalf("capacity after departures: registered=%d reserved=%d", registered, reserved)
	}
}

func TestSessionCeilingConcurrentSameIdentityHandoff(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	old := configurationTestClient(s, "K1ABC-1")
	old.configuredSettings = filter.SettingsConfiguration{NoiseClass: "URBAN"}
	old.configurationInitialized = true
	s.registerClient(old)
	if err := old.saveFilter(); err != nil {
		t.Fatal(err)
	}
	first := configurationTestClient(s, old.callsign)
	second := configurationTestClient(s, old.callsign)
	different := configurationTestClient(s, "K1ABC-2")
	entered, release := make(chan struct{}), make(chan struct{})
	var commitOnce sync.Once
	s.commitLoginRecordFn = func(prepared *filter.PreparedUserRecord) error {
		commitOnce.Do(func() { close(entered); <-release })
		return prepared.Commit()
	}
	firstStart, otherStart := make(chan struct{}), make(chan struct{})
	var firstStartOnce, otherStartOnce, releaseOnce sync.Once
	firstResult, secondResult, differentResult := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	var workers sync.WaitGroup
	workers.Add(3)
	for _, task := range []struct {
		start  <-chan struct{}
		client *Client
		result chan<- error
	}{{firstStart, first, firstResult}, {otherStart, second, secondResult}, {otherStart, different, differentResult}} {
		go func() {
			defer workers.Done()
			<-task.start
			_, err := s.restoreAndRegisterClient(task.client, time.Now().UTC(), time.Now().Add(5*time.Second))
			task.result <- err
		}()
	}
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	t.Cleanup(func() {
		firstStartOnce.Do(func() { close(firstStart) })
		otherStartOnce.Do(func() { close(otherStart) })
		releaseOnce.Do(func() { close(release) })
		waitConfigurationTest(t, finished)
		s.clientsMutex.RLock()
		owner := s.clients[old.callsign]
		s.clientsMutex.RUnlock()
		if owner != nil {
			s.unregisterClient(owner)
		}
	})
	firstStartOnce.Do(func() { close(firstStart) })
	waitConfigurationTest(t, entered)
	otherStartOnce.Do(func() { close(otherStart) })
	select {
	case err := <-differentResult:
		if !errors.Is(err, errSessionFull) {
			t.Fatalf("different identity while full: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("different identity could not be rejected while replacement was restoring")
	}
	select {
	case err := <-secondResult:
		t.Fatalf("second same-identity login bypassed handoff ownership: %v", err)
	default:
	}
	if _, err := os.Stat(filepath.Join(filter.UserDataDir, different.callsign+".yaml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected different identity wrote a profile: %v", err)
	}
	var exceeded atomic.Bool
	stopObserving, observerDone := make(chan struct{}), make(chan struct{})
	var stopObservingOnce sync.Once
	go func() {
		defer close(observerDone)
		for {
			select {
			case <-stopObserving:
				return
			default:
			}
			s.clientsMutex.RLock()
			if len(s.clients)+s.sessionReservations > 1 {
				exceeded.Store(true)
			}
			s.clientsMutex.RUnlock()
		}
	}()
	t.Cleanup(func() {
		stopObservingOnce.Do(func() { close(stopObserving) })
		waitConfigurationTest(t, observerDone)
	})
	releaseOnce.Do(func() { close(release) })
	waitConfigurationTest(t, finished)
	stopObservingOnce.Do(func() { close(stopObserving) })
	waitConfigurationTest(t, observerDone)
	if err := <-firstResult; err != nil {
		t.Fatalf("first replacement: %v", err)
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("second replacement: %v", err)
	}
	s.clientsMutex.RLock()
	current, count, reserved := s.clients[old.callsign], len(s.clients), s.sessionReservations
	s.clientsMutex.RUnlock()
	if exceeded.Load() || count != 1 || reserved != 0 || current != second {
		t.Fatalf("replacement ownership: exceeded=%t count=%d reserved=%d successor=%t", exceeded.Load(), count, reserved, current == second)
	}
	if second.configuredSettings.NoiseClass != "URBAN" {
		t.Fatalf("second replacement did not restore saved preferences: %+v", second.configuredSettings)
	}
	second.configuredSettings.NoiseClass = "INDUSTRIAL"
	if err := second.saveFilter(); err != nil {
		t.Fatalf("current owner could not save: %v", err)
	}
	path := filepath.Join(filter.UserDataDir, old.callsign+".yaml")
	before, err := os.ReadFile(path)
	if err != nil || !bytes.Contains(before, []byte("noise_class: INDUSTRIAL")) {
		t.Fatalf("current owner's saved preferences missing: %v", err)
	}
	for _, retired := range []*Client{old, first} {
		retired.configuredSettings.NoiseClass = "QUIET"
		if err := retired.saveFilter(); !errors.Is(err, errRetiredConfiguration) {
			t.Fatalf("retired save = %v", err)
		}
		s.unregisterClient(retired)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("retired sessions changed saved profile: %v", err)
	}
	s.clientsMutex.RLock()
	current, count, reserved = s.clients[old.callsign], len(s.clients), s.sessionReservations
	s.clientsMutex.RUnlock()
	if current != second || count != 1 || reserved != 0 {
		t.Fatalf("stale teardown changed successor: count=%d reserved=%d successor=%t", count, reserved, current == second)
	}
	s.unregisterClient(second)
	if s.GetClientCount() != 0 {
		t.Fatal("successor departure did not restore capacity")
	}
}

type ceilingObservedConn struct {
	net.Conn
	writeStarted chan struct{}
	closed       chan struct{}
	writeOnce    sync.Once
	closeOnce    sync.Once
}

func (c *ceilingObservedConn) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("Server full")) {
		c.writeOnce.Do(func() { close(c.writeStarted) })
	}
	return c.Conn.Write(p)
}

func (c *ceilingObservedConn) Close() error {
	err := c.Conn.Close()
	c.closeOnce.Do(func() { close(c.closed) })
	return err
}

func TestSessionCeilingNonreadingRejectedPeer(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	s.maxPreloginSessions = 1
	s.rejectWriteDeadline = 150 * time.Millisecond
	owner := configurationTestClient(s, "K1ABC-1")
	s.registerClient(owner)
	t.Cleanup(func() { s.unregisterClient(owner) })
	ticket, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.5", 1000))
	if ticket == nil || reason != "" {
		t.Fatalf("prelogin ticket: %v, %q", ticket, reason)
	}
	serverEnd, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close(); _ = serverEnd.Close() })
	observed := &ceilingObservedConn{Conn: serverEnd, writeStarted: make(chan struct{}), closed: make(chan struct{})}
	handlerDone := make(chan struct{})
	go func() { defer close(handlerDone); s.handleClient(observed, ticket) }()
	ceilingReadThrough(t, peer, "login: ")
	if _, err := io.WriteString(peer, "K1ABC-2\r\n"); err != nil {
		t.Fatal(err)
	}
	// Stop reading before the rejection. net.Pipe has no kernel send buffer,
	// so the server's final write cannot complete without the peer reading it.
	waitConfigurationTest(t, observed.writeStarted)
	started := time.Now()
	active, _, _, _, _, _, _ := s.PreloginMetricSnapshot()
	if active != 1 {
		t.Fatalf("blocked rejection retained %d prelogin tickets, want 1", active)
	}
	select {
	case <-observed.closed:
		t.Fatal("connection closed before the blocked write was checked")
	case <-handlerDone:
		t.Fatal("handler released ownership while the rejection writer was blocked")
	default:
	}
	if next, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.6", 1001)); next != nil || reason != preloginRejectGlobalCap {
		if next != nil {
			next.Release()
		}
		t.Fatalf("blocked rejection failed to retain admission capacity: ticket=%v reason=%q", next, reason)
	}
	waitConfigurationTest(t, observed.closed)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("stalled rejection took %s; configured rejection deadline was %s", elapsed, s.rejectWriteDeadline)
	}
	waitConfigurationTest(t, handlerDone)
	active, _, _, _, _, _, _ = s.PreloginMetricSnapshot()
	if active != 0 || s.GetClientCount() != 1 {
		t.Fatalf("after stalled rejection: prelogin=%d registered=%d", active, s.GetClientCount())
	}
	next, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.6", 1001))
	if next == nil || reason != "" {
		t.Fatalf("prelogin capacity did not recover: ticket=%v reason=%q", next, reason)
	}
	next.Release()
}

func TestSessionCeilingFullStillUsesPreloginGate(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	s.maxPreloginSessions = 1
	address := startSessionCeilingListener(t, s)
	old := ceilingConnect(t, address, "K1ABC-1")
	ceilingReadThrough(t, old, "READY\r\n")
	waitSessionCeiling(t, func() bool {
		active, _, _, _, _, _, _ := s.PreloginMetricSnapshot()
		return active == 0
	})
	occupied, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(t.Context(), "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })
	ceilingReadThrough(t, occupied, "login: ")
	waitSessionCeiling(t, func() bool {
		active, _, _, _, _, _, _ := s.PreloginMetricSnapshot()
		return active == 1
	})
	rejected, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(t.Context(), "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rejected.Close() })
	ceilingReadThrough(t, rejected, "Server busy")
	ceilingWaitEOF(t, rejected)
	if s.GetClientCount() != 1 {
		t.Fatal("prelogin gate displaced the existing registered owner")
	}
	_ = occupied.Close()
	waitSessionCeiling(t, func() bool {
		active, _, _, _, _, _, _ := s.PreloginMetricSnapshot()
		return active == 0
	})
	replacement := ceilingConnect(t, address, "K1ABC-1")
	ceilingReadThrough(t, replacement, "READY\r\n")
	ceilingReadThrough(t, old, "Replaced by another login")
	ceilingWaitEOF(t, old)
}
