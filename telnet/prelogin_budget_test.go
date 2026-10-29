package telnet

import (
	"bytes"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"dxcluster/filter"
)

type preloginObservedWrite struct {
	net.Conn
	once    sync.Once
	entered chan struct{}
}

type preloginBlockingClose struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type preloginBlockingLog struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *preloginBlockingLog) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("Error reading callsign")) {
		w.once.Do(func() { close(w.entered) })
		<-w.release
	}
	return len(p), nil
}

func (c *preloginBlockingClose) Close() error {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Conn.Close()
}

func (c *preloginObservedWrite) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(p)
}

func preloginTestTicket(t *testing.T, s *Server) *preloginTicket {
	t.Helper()
	ticket, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.8", 1234))
	if ticket == nil {
		t.Fatalf("prelogin admission failed: %s", reason)
	}
	return ticket
}

func TestPreloginBudgetIncludesAdmissionAfterAccept(t *testing.T) {
	s := newSessionCeilingServer(t, 10)
	s.preloginTimeout = 200 * time.Millisecond
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	s.admissionGeoLookupFn = func(string, time.Time) (string, string) {
		close(entered)
		<-release
		return "", ""
	}
	address := startSessionCeilingListener(t, s)
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitConfigurationTest(t, entered)
	// The lookup starts only after Accept returns. Keep it blocked for the
	// entire budget, then verify that login does not receive a new budget.
	<-time.After(350 * time.Millisecond)
	once.Do(func() { close(release) })
	ceilingWaitEOF(t, conn)
	waitSessionCeiling(t, func() bool {
		active, _, _, _, timeouts, _, _ := s.PreloginMetricSnapshot()
		return active == 0 && timeouts == 1
	})
	if s.GetClientCount() != 0 {
		t.Fatal("expired admission registered a client")
	}
}

func TestPreloginBudgetInterruptsNegotiationAndShutdown(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		for _, shutdown := range []bool{false, true} {
			t.Run(transport+map[bool]string{false: "/timeout", true: "/shutdown"}[shutdown], func(t *testing.T) {
				s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
					opts.Transport, opts.HandshakeMode = transport, telnetHandshakeMinimal
					opts.PreloginTimeout = 250 * time.Millisecond
					if shutdown {
						opts.PreloginTimeout = 5 * time.Second
					}
				})
				ticket := preloginTestTicket(t, s)
				server, peer := net.Pipe()
				defer peer.Close()
				observed := &preloginObservedWrite{Conn: server, entered: make(chan struct{})}
				done := make(chan struct{})
				go func() { s.handleClient(observed, ticket); close(done) }()
				waitConfigurationTest(t, observed.entered)
				if shutdown {
					s.Stop()
				}
				waitConfigurationTest(t, done)
				active, _, _, _, timeouts, _, _ := s.PreloginMetricSnapshot()
				wantTimeouts := uint64(1)
				if shutdown {
					wantTimeouts = 0
				}
				if active != 0 || timeouts != wantTimeouts {
					t.Fatalf("active=%d timeouts=%d, want 0 and %d", active, timeouts, wantTimeouts)
				}
			})
		}
	}
}

func TestPreloginBudgetInterruptsZiutekReadNegotiationWrite(t *testing.T) {
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.Transport, opts.HandshakeMode = "ziutek", telnetHandshakeNone
		opts.LoginPrompt, opts.PreloginTimeout = "", 300*time.Millisecond
	})
	ticket := preloginTestTicket(t, s)
	server, peer := net.Pipe()
	defer peer.Close()
	observed := &preloginObservedWrite{Conn: server, entered: make(chan struct{})}
	done := make(chan struct{})
	go func() { s.handleClient(observed, ticket); close(done) }()
	go func() { _, _ = peer.Write([]byte{IAC, DO, 38}) }()
	waitConfigurationTest(t, observed.entered)
	waitConfigurationTest(t, done)
	active, _, _, _, timeouts, _, _ := s.PreloginMetricSnapshot()
	if active != 0 || timeouts != 1 {
		t.Fatalf("active=%d timeouts=%d, want 0 and 1", active, timeouts)
	}
}

func TestPreloginBudgetPeerDisconnectIsNotTimeout(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
				opts.Transport, opts.PreloginTimeout = transport, 5*time.Second
			})
			ticket := preloginTestTicket(t, s)
			server, peer := net.Pipe()
			done := make(chan struct{})
			go func() { s.handleClient(server, ticket); close(done) }()
			ceilingReadThrough(t, peer, "login: ")
			_ = peer.Close()
			waitConfigurationTest(t, done)
			if got := s.PreloginAdmissionMetricSnapshot(); got.Active != 0 || got.Timeouts != 0 {
				t.Fatalf("peer disconnect counted as timeout or retained ticket: %+v", got)
			}
		})
	}
}

func TestPreloginBudgetDisconnectCleanupPastDeadlineIsNotTimeout(t *testing.T) {
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.PreloginTimeout = 300 * time.Millisecond
	})
	ticket := preloginTestTicket(t, s)
	server, peer := net.Pipe()
	observed := &preloginBlockingClose{Conn: server, entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(observed.release) }); _ = peer.Close() })
	done := make(chan struct{})
	go func() { s.handleClient(observed, ticket); close(done) }()
	ceilingReadThrough(t, peer, "login: ")
	_ = peer.Close()
	waitConfigurationTest(t, observed.entered)
	<-time.After(450 * time.Millisecond)
	once.Do(func() { close(observed.release) })
	waitConfigurationTest(t, done)
	if got := s.PreloginAdmissionMetricSnapshot(); got.Active != 0 || got.Timeouts != 0 {
		t.Fatalf("slow disconnect cleanup counted as timeout or retained ticket: %+v", got)
	}
}

func TestPreloginBudgetDisconnectLoggingPastDeadlineIsNotTimeout(t *testing.T) {
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.PreloginTimeout = 300 * time.Millisecond
	})
	ticket := preloginTestTicket(t, s)
	server, peer := net.Pipe()
	blocked := &preloginBlockingLog{entered: make(chan struct{}), release: make(chan struct{})}
	original := log.Writer()
	log.SetOutput(blocked)
	var once sync.Once
	t.Cleanup(func() {
		once.Do(func() { close(blocked.release) })
		_ = peer.Close()
		log.SetOutput(original)
	})
	done := make(chan struct{})
	go func() { s.handleClient(server, ticket); close(done) }()
	ceilingReadThrough(t, peer, "login: ")
	_ = peer.Close()
	waitConfigurationTest(t, blocked.entered)
	<-time.After(450 * time.Millisecond)
	once.Do(func() { close(blocked.release) })
	waitConfigurationTest(t, done)
	if got := s.PreloginAdmissionMetricSnapshot(); got.Active != 0 || got.Timeouts != 0 {
		t.Fatalf("slow disconnect logging counted as timeout or retained ticket: %+v", got)
	}
}

func TestPreloginBudgetRegistrationRetiresTimeout(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s := newSessionCeilingServer(t, 10)
			s.useZiutek = transport == "ziutek"
			s.preloginTimeout = 350 * time.Millisecond
			address := startSessionCeilingListener(t, s)
			conn := ceilingConnect(t, address, "K1ABC")
			ceilingReadThrough(t, conn, "READY\r\n")
			<-time.After(500 * time.Millisecond)
			if _, err := io.WriteString(conn, "SHOW USERS\r\n"); err != nil {
				t.Fatalf("registered connection closed at former deadline: %v", err)
			}
			ceilingReadThrough(t, conn, "Unknown SHOW subcommand: USERS")
			_, _, _, _, timeouts, _, _ := s.PreloginMetricSnapshot()
			if timeouts != 0 {
				t.Fatalf("registered session counted as timeout: %d", timeouts)
			}
		})
	}
}

func TestPreloginBudgetWaitsForProfileWorkBeforeCleanup(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "shutdown"}[shutdown], func(t *testing.T) {
			s := newSessionCeilingServer(t, 2)
			s.preloginTimeout = 300 * time.Millisecond
			if shutdown {
				s.preloginTimeout = 5 * time.Second
			}
			entered, release := ceilingBlockedRestore(t, s)
			address := startSessionCeilingListener(t, s)
			conn := ceilingConnect(t, address, "K1ABC")
			waitConfigurationTest(t, entered)
			if shutdown {
				s.Stop()
			} else {
				ceilingWaitEOF(t, conn)
			}
			ceilingOccupancy(t, s, 0, 1)
			if active := s.PreloginAdmissionMetricSnapshot().Active; active != 1 {
				t.Fatalf("profile work lost its prelogin ticket: %d", active)
			}
			release()
			waitSessionCeiling(t, func() bool {
				got := s.PreloginAdmissionMetricSnapshot()
				return got.Active == 0 && (shutdown || got.Timeouts == 1)
			})
			ceilingOccupancy(t, s, 0, 0)
			if _, err := os.Stat(filepath.Join(filter.UserDataDir, "K1ABC.yaml")); err != nil {
				t.Fatalf("completed profile commit was not retained: %v", err)
			}
			_, _, _, _, timeouts, _, _ := s.PreloginMetricSnapshot()
			want := uint64(1)
			if shutdown {
				want = 0
			}
			if timeouts != want {
				t.Fatalf("timeouts=%d, want %d", timeouts, want)
			}
		})
	}
}

func TestPreloginBudgetExpiredReconnectKeepsExistingOwner(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	s.preloginTimeout = 400 * time.Millisecond
	address := startSessionCeilingListener(t, s)
	owner := ceilingConnect(t, address, "K1ABC")
	ceilingReadThrough(t, owner, "READY\r\n")
	entered, release := ceilingBlockedRestore(t, s)
	reconnect := ceilingConnect(t, address, "K1ABC")
	waitConfigurationTest(t, entered)
	ceilingWaitEOF(t, reconnect)
	if s.GetClientCount() != 1 || s.PreloginAdmissionMetricSnapshot().Active != 1 {
		t.Fatal("expired reconnect displaced the owner or released its ticket during profile work")
	}
	release()
	waitSessionCeiling(t, func() bool {
		return s.PreloginAdmissionMetricSnapshot().Active == 0 && s.PreloginAdmissionMetricSnapshot().Timeouts == 1
	})
	if _, err := io.WriteString(owner, "SHOW USERS\r\n"); err != nil {
		t.Fatalf("existing owner was closed: %v", err)
	}
	ceilingReadThrough(t, owner, "Unknown SHOW subcommand: USERS")
}

func TestPreloginBudgetRejectionNoticeHasSeparateDeadline(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	s.preloginTimeout = 300 * time.Millisecond
	s.rejectWriteDeadline = 2 * time.Second
	owner := configurationTestClient(s, "K1ABC-1")
	s.registerClient(owner)
	t.Cleanup(func() { s.unregisterClient(owner) })
	ticket := preloginTestTicket(t, s)
	server, peer := net.Pipe()
	defer peer.Close()
	observed := &ceilingObservedConn{Conn: server, writeStarted: make(chan struct{}), closed: make(chan struct{})}
	done := make(chan struct{})
	go func() { s.handleClient(observed, ticket); close(done) }()
	ceilingReadThrough(t, peer, "login: ")
	if _, err := io.WriteString(peer, "K1ABC-2\r\n"); err != nil {
		t.Fatal(err)
	}
	waitConfigurationTest(t, observed.writeStarted)
	<-time.After(450 * time.Millisecond)
	ceilingReadThrough(t, peer, "Server full. Try again later.")
	waitConfigurationTest(t, done)
	if got := s.PreloginAdmissionMetricSnapshot(); got.Active != 0 || got.Timeouts != 0 {
		t.Fatalf("rejection misclassified or ticket retained: %+v", got)
	}
}

func TestPreloginBudgetExpiredBehindReplacementLockKeepsOwner(t *testing.T) {
	s := newSessionCeilingServer(t, 1)
	s.preloginTimeout = 400 * time.Millisecond
	address := startSessionCeilingListener(t, s)
	ownerConn := ceilingConnect(t, address, "K1ABC")
	ceilingReadThrough(t, ownerConn, "READY\r\n")
	s.clientsMutex.RLock()
	owner := s.clients["K1ABC"]
	s.clientsMutex.RUnlock()
	owner.readPauseMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(owner.readPauseMu.Unlock) }
	t.Cleanup(unlock)
	reconnect := ceilingConnect(t, address, "K1ABC")
	waitSessionCeiling(t, func() bool {
		if s.clientsMutex.TryRLock() {
			s.clientsMutex.RUnlock()
			return false
		}
		return true
	})
	ceilingWaitEOF(t, reconnect)
	unlock()
	waitSessionCeiling(t, func() bool { return s.PreloginAdmissionMetricSnapshot().Timeouts == 1 })
	s.clientsMutex.RLock()
	current := s.clients["K1ABC"]
	s.clientsMutex.RUnlock()
	if current != owner {
		t.Fatal("expired replacement changed registry ownership")
	}
	owner.readPauseMu.Lock()
	closed := owner.readPauseClosed
	owner.readPauseMu.Unlock()
	if closed {
		t.Fatal("expired replacement invalidated the existing owner's readback")
	}
}
