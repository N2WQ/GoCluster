package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type shutdownBlockedWriteConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *shutdownBlockedWriteConn) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("SHUTDOWN-WRITE")) {
		c.once.Do(func() { close(c.entered) })
	}
	return c.Conn.Write(p)
}

// The timeout worker is deliberately absent: correctness must not depend on
// the shutdown observer winning a scheduling race with registration.
func TestShutdownBeforeReplacementDecisionPreservesOwner(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	old := configurationTestClient(s, "K1ABC")
	s.registerClient(old)
	replacement := configurationTestClient(s, old.callsign)
	deadline := time.Now().Add(time.Minute)
	workerDone := make(chan struct{})
	close(workerDone)
	replacement.preloginBudget = &preloginBudget{
		deadline: deadline,
		shutdown: s.shutdown,
		stopCh:   make(chan struct{}),
		done:     workerDone,
	}

	old.readPauseMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(old.readPauseMu.Unlock) }
	t.Cleanup(unlock)
	result := make(chan error, 1)
	go func() {
		_, _, err := s.registerClientOwned(replacement, loginReservation{}, deadline)
		result <- err
	}()
	// The registry is held while replacement waits for the old readback lock.
	waitSessionCeiling(t, func() bool {
		if s.clientsMutex.TryRLock() {
			s.clientsMutex.RUnlock()
			return false
		}
		return true
	})
	stopped := make(chan struct{})
	go func() { s.Stop(); close(stopped) }()
	retirementSignal(t, s.shutdown, "shutdown signal")
	unlock()
	select {
	case err := <-result:
		if !errors.Is(err, errClientClosed) {
			t.Fatalf("registration after shutdown = %v, want client closed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("registration did not return after shutdown")
	}
	retirementSignal(t, stopped, "server shutdown")
	s.clientsMutex.RLock()
	current := s.clients[old.callsign]
	s.clientsMutex.RUnlock()
	if current != old {
		t.Fatal("replacement displaced the owner after shutdown was signaled")
	}
	retirementSignal(t, old.done, "old owner shutdown")
}

func TestShutdownWaitsForInFlightRegistrationDecision(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	old := configurationTestClient(s, "K1ABC")
	s.registerClient(old)
	replacement := configurationTestClient(s, old.callsign)
	deadline := time.Now().Add(time.Minute)
	workerDone := make(chan struct{})
	close(workerDone)
	budget := &preloginBudget{deadline: deadline, shutdown: s.shutdown, stopCh: make(chan struct{}), done: workerDone}
	replacement.preloginBudget = budget
	budget.mu.Lock()
	var once sync.Once
	unlock := func() { once.Do(budget.mu.Unlock) }
	t.Cleanup(unlock)
	result := make(chan error, 1)
	go func() {
		_, _, err := s.registerClientOwned(replacement, loginReservation{}, deadline)
		result <- err
	}()
	// The held budget lock stops registration immediately after its final
	// shutdown check. The registration gate must remain held until publication.
	waitSessionCeiling(t, func() bool {
		if s.shutdownRegistrationMu.TryLock() {
			s.shutdownRegistrationMu.Unlock()
			return false
		}
		return true
	})
	stopStarted := make(chan struct{})
	s.nowFn = func() time.Time { close(stopStarted); return time.Now() }
	stopped := make(chan struct{})
	go func() { s.Stop(); close(stopped) }()
	retirementSignal(t, stopStarted, "shutdown started")
	select {
	case <-s.shutdown:
		t.Fatal("shutdown was signaled between the registration check and publication")
	case <-time.After(100 * time.Millisecond):
	}
	unlock()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("registration that won the ordering gate failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("registration did not finish")
	}
	retirementSignal(t, s.shutdown, "shutdown after publication")
	retirementSignal(t, stopped, "server shutdown")
	s.clientsMutex.RLock()
	current := s.clients[old.callsign]
	s.clientsMutex.RUnlock()
	if current != replacement {
		t.Fatal("registration did not publish before shutdown")
	}
}

func TestPreloginBudgetFinishObservesShutdownWithoutWorker(t *testing.T) {
	shutdown, done := make(chan struct{}), make(chan struct{})
	close(shutdown)
	close(done)
	budget := &preloginBudget{deadline: time.Now().Add(time.Minute), shutdown: shutdown, stopCh: make(chan struct{}), done: done}
	if err := budget.finish(); !errors.Is(err, errClientClosed) {
		t.Fatalf("finish after shutdown = %v, want client closed", err)
	}
	if expired := budget.stop(); expired {
		t.Fatal("shutdown was counted as timeout")
	}
}

func TestShutdownClosesDisplacedIdleSessionBeforeCallbackReturns(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		for _, callbackKind := range []string{"membership", "client_list"} {
			for _, pacing := range []bool{false, true} {
				name := transport + "/" + callbackKind + "/pacing_off"
				if pacing {
					name = transport + "/" + callbackKind + "/pacing_on"
				}
				t.Run(name, func(t *testing.T) {
					s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
						opts.Transport, opts.EchoMode = transport, "off"
						opts.LoginGreeting, opts.DuplicateLoginMsg = "ready\n", "duplicate notice"
						opts.ReadIdleTimeout = time.Hour
						if pacing {
							opts.CommandRatePerSecond, opts.CommandBurst = 1, 1
						}
					})
					_, oldPeer, oldDone := startHandshakeTranscriptSession(t, s)
					var newPeer net.Conn
					var newDone <-chan struct{}
					release := make(chan struct{})
					var releaseOnce sync.Once
					t.Cleanup(func() {
						releaseOnce.Do(func() { close(release) })
						_ = oldPeer.Close()
						if newPeer != nil {
							_ = newPeer.Close()
						}
						waitConfigurationTest(t, oldDone)
						if newDone != nil {
							waitConfigurationTest(t, newDone)
						}
					})
					ceilingReadThrough(t, oldPeer, "login: ")
					if _, err := io.WriteString(oldPeer, "K1ABC\r\n"); err != nil {
						t.Fatal(err)
					}
					ceilingReadThrough(t, oldPeer, "ready\r\n")
					s.clientsMutex.RLock()
					old := s.clients["K1ABC"]
					s.clientsMutex.RUnlock()
					if old == nil {
						t.Fatal("initial session did not register")
					}

					entered := make(chan struct{})
					var first atomic.Bool
					callback := func() {
						if first.CompareAndSwap(false, true) {
							close(entered)
							<-release
						}
					}
					if callbackKind == "membership" {
						s.SetPeerMembershipListener(callback)
					} else {
						s.SetClientListListener(callback)
					}
					_, newPeer, newDone = startHandshakeTranscriptSession(t, s)
					ceilingReadThrough(t, newPeer, "login: ")
					if _, err := io.WriteString(newPeer, "K1ABC\r\n"); err != nil {
						t.Fatal(err)
					}
					retirementSignal(t, entered, "blocked replacement callback")
					s.clientsMutex.RLock()
					current := s.clients["K1ABC"]
					s.clientsMutex.RUnlock()
					if current == nil || current == old {
						t.Fatal("replacement was not registered before shutdown")
					}
					select {
					case <-old.done:
						t.Fatal("old session closed before shutdown and before its duplicate notice")
					default:
					}
					if err := oldPeer.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
						t.Fatal(err)
					}
					if err := newPeer.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
						t.Fatal(err)
					}
					stopped := make(chan struct{})
					go func() { s.Stop(); close(stopped) }()
					retirementSignal(t, s.shutdown, "shutdown signal")
					retirementSignal(t, old.done, "displaced idle session interruption")
					retirementSignal(t, current.done, "replacement session interruption")
					if _, err := io.Copy(io.Discard, oldPeer); err != nil {
						t.Fatalf("old peer did not observe closure: %v", err)
					}
					if _, err := io.Copy(io.Discard, newPeer); err != nil {
						t.Fatalf("replacement peer did not observe closure: %v", err)
					}
					retirementSignal(t, stopped, "server stop")
					select {
					case <-release:
						t.Fatal("callback was released before both sockets closed")
					default:
					}
					releaseOnce.Do(func() { close(release) })
				})
			}
		}
	}
}

func TestShutdownRetiresDisplacedSessionWithBlockedWrite(t *testing.T) {
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.Transport, opts.EchoMode = "ziutek", "off"
		opts.LoginGreeting, opts.DuplicateLoginMsg = "ready\n", "duplicate notice"
		opts.ReadIdleTimeout = time.Hour
	})
	server, oldPeer := net.Pipe()
	observed := &shutdownBlockedWriteConn{Conn: server, entered: make(chan struct{})}
	oldDone := make(chan struct{})
	go func() { s.handleClient(observed, nil); close(oldDone) }()
	var newPeer net.Conn
	var newDone <-chan struct{}
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		_ = oldPeer.Close()
		if newPeer != nil {
			_ = newPeer.Close()
		}
		waitConfigurationTest(t, oldDone)
		if newDone != nil {
			waitConfigurationTest(t, newDone)
		}
	})
	ceilingReadThrough(t, oldPeer, "login: ")
	if _, err := io.WriteString(oldPeer, "K1ABC\r\n"); err != nil {
		t.Fatal(err)
	}
	ceilingReadThrough(t, oldPeer, "ready\r\n")
	s.clientsMutex.RLock()
	old := s.clients["K1ABC"]
	s.clientsMutex.RUnlock()
	if err := old.Send("SHUTDOWN-WRITE\n"); err != nil {
		t.Fatal(err)
	}
	retirementSignal(t, observed.entered, "blocked old socket write")
	entered := make(chan struct{})
	var first atomic.Bool
	s.SetPeerMembershipListener(func() {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	})
	_, newPeer, newDone = startHandshakeTranscriptSession(t, s)
	ceilingReadThrough(t, newPeer, "login: ")
	if _, err := io.WriteString(newPeer, "K1ABC\r\n"); err != nil {
		t.Fatal(err)
	}
	retirementSignal(t, entered, "blocked replacement callback")
	stopped := make(chan struct{})
	go func() { s.Stop(); close(stopped) }()
	retirementSignal(t, s.shutdown, "shutdown signal")
	// The writer is inside a real net.Pipe write and cannot select shutdown.
	// Its existing write deadline must let it retire without the callback.
	retirementSignal(t, old.done, "displaced blocked writer interruption")
	retirementSignal(t, stopped, "server shutdown")
	select {
	case <-release:
		t.Fatal("callback returned before displaced socket was interrupted")
	default:
	}
	once.Do(func() { close(release) })
}

func TestShutdownWriterReleasesQueuedBulletinReservations(t *testing.T) {
	server, peer := net.Pipe()
	defer peer.Close()
	s := &Server{shutdown: make(chan struct{})}
	c := &Client{server: s, conn: server, writer: bufio.NewWriter(server), done: make(chan struct{}), controlChan: make(chan controlMessage, 2)}
	for range 2 {
		if !s.reserveBulletinResponse() {
			t.Fatal("could not reserve bulletin response")
		}
		c.controlChan <- controlMessage{line: "reserved bulletin\n", bulletinReservation: bulletinResponseReservation}
	}
	close(s.shutdown)
	finished := make(chan struct{})
	go func() { c.writerLoop(); close(finished) }()
	retirementSignal(t, finished, "writer shutdown")
	retirementSignal(t, c.done, "writer socket interruption")
	if got := s.bulletinResponseBytes.Load(); got != 0 {
		t.Fatalf("shutdown retained %d bulletin response bytes", got)
	}
}
