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

// The wrapper presents the same observed peer to both the accept-time gate
// and login, while the transport remains a real loopback TCP connection.
type ipQuotaListener struct {
	net.Listener
	accepted int
}

func (l *ipQuotaListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.accepted++
	ip := "192.0.2.1"
	if l.accepted == 13 {
		ip = "192.0.2.2"
	}
	port := conn.RemoteAddr().(*net.TCPAddr).Port
	return &ipQuotaPeerConn{Conn: conn, peer: tcpAddr(ip, port)}, nil
}

type ipQuotaPeerConn struct {
	net.Conn
	peer net.Addr
}

func (c *ipQuotaPeerConn) RemoteAddr() net.Addr { return c.peer }

func startIPQuotaListener(t *testing.T, s *Server) string {
	t.Helper()
	base, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := &ipQuotaListener{Listener: base}
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

func TestIPSessionQuotaListenerTenAndRejectedFiles(t *testing.T) {
	s := newIPQuotaServer(t, 20, 0, 10)
	address := startIPQuotaListener(t, s)
	sessions := make([]net.Conn, 10)
	for i := range sessions {
		sessions[i] = ceilingConnect(t, address, fmt.Sprintf("LZ3NY-%d", i))
		ceilingReadThrough(t, sessions[i], "READY\r\n")
	}
	ipQuotaCounts(t, s, "192.0.2.1", 10, 0)

	profile := filepath.Join(filter.UserDataDir, "LZ3NY-10.yaml")
	original := oldLoginRecord(4)
	if err := os.WriteFile(profile, original, 0o600); err != nil {
		t.Fatal(err)
	}
	library := presetLibraryPathFor("LZ3NY-10")
	if err := os.MkdirAll(filepath.Dir(library), 0o700); err != nil {
		t.Fatal(err)
	}
	oldPreset := strings.Split(string(original), "recent_ips:")[0]
	libraryBytes := []byte("presets:\n  OLD:\n    " + strings.ReplaceAll(strings.TrimSuffix(oldPreset, "\n"), "\n", "\n    ") + "\n")
	if err := os.WriteFile(library, libraryBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{profile + ".pre-v6.bak", library + ".pre-v6.bak"} {
		if err := os.WriteFile(path, []byte("existing backup "+filepath.Base(path)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	before := make(map[string][]byte)
	for _, path := range []string{profile, library, profile + ".pre-v6.bak", library + ".pre-v6.bak"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = data
	}

	rejected := ceilingConnect(t, address, "LZ3NY-10")
	ceilingReadThrough(t, rejected, "IP session limit reached")
	ceilingWaitEOF(t, rejected)
	waitSessionCeiling(t, func() bool { return s.PreloginAdmissionMetricSnapshot().Active == 0 })
	for path, want := range before {
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("IP rejection changed %s: %v", path, err)
		}
	}
	if got := s.PreloginAdmissionMetricSnapshot(); got.IPSessionRejects != 1 || got.TrackedSessionIPs != 1 || s.GetClientCount() != 10 {
		t.Fatalf("after IP rejection: %+v, registered=%d", got, s.GetClientCount())
	}

	replacement := ceilingConnect(t, address, "lz3ny-0/p")
	ceilingReadThrough(t, replacement, "READY\r\n")
	ceilingReadThrough(t, sessions[0], "Replaced by another login")
	ceilingWaitEOF(t, sessions[0])
	if s.GetClientCount() != 10 {
		t.Fatalf("same-IP replacement changed registered count: %d", s.GetClientCount())
	}
	independent := ceilingConnect(t, address, "K1ABC-1")
	ceilingReadThrough(t, independent, "READY\r\n")
	ipQuotaCounts(t, s, "192.0.2.2", 1, 0)
	_ = sessions[1].Close()
	waitSessionCeiling(t, func() bool { return s.GetClientCount() == 10 })
	freed := ceilingConnect(t, address, "LZ3NY-10")
	ceilingReadThrough(t, freed, "READY\r\n")
	ipQuotaCounts(t, s, "192.0.2.1", 10, 0)
}

func TestIPSessionQuotaAddressNormalization(t *testing.T) {
	for _, tc := range []struct {
		name, address, want string
	}{
		{"ipv4_port_one", "192.0.2.1:1234", "192.0.2.1"},
		{"ipv4_port_two", "192.0.2.1:5678", "192.0.2.1"},
		{"mapped_ipv4", "[::ffff:192.0.2.1]:4567", "192.0.2.1"},
		{"ipv6_expanded", "[2001:0db8:0000:0000:0000:0000:0000:0001]:123", "2001:db8::1"},
		{"ipv6_short", "[2001:db8::1]:456", "2001:db8::1"},
		{"scope_one", "[fe80::1%zone1]:234", "fe80::1%zone1"},
		{"scope_two", "[fe80::1%zone2]:234", "fe80::1%zone2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := sessionIP(tc.address)
			if err != nil || got != tc.want {
				t.Fatalf("sessionIP(%q)=%q, %v; want %q", tc.address, got, err, tc.want)
			}
		})
	}
	for _, address := range []string{"", "pipe", "not-an-ip:1234", "[not-an-ip]:1234"} {
		if _, err := sessionIP(address); !errors.Is(err, errInvalidSessionIP) {
			t.Fatalf("sessionIP(%q): %v, want invalid IP", address, err)
		}
	}
}

// These checks exercise quota decisions, not just the address parser, so an
// admission path that accidentally uses the raw address cannot pass them.
func TestIPSessionQuotaAddressGrouping(t *testing.T) {
	for _, tc := range []struct {
		name, owner, contender string
		want                   error
	}{
		{"ipv4_different_port", "192.0.2.1:1001", "192.0.2.1:1002", errIPSessionFull},
		{"ipv4_mapped_ipv6", "192.0.2.1:1001", "[::ffff:192.0.2.1]:1002", errIPSessionFull},
		{"ipv6_equivalent", "[2001:db8::1]:1001", "[2001:0db8:0:0:0:0:0:1]:1002", errIPSessionFull},
		{"same_ipv6_zone", "[fe80::1%zone1]:1001", "[fe80:0:0:0:0:0:0:1%zone1]:1002", errIPSessionFull},
		{"different_ipv6_zones", "[fe80::1%zone1]:1001", "[fe80::1%zone2]:1002", nil},
		{"different_ips", "192.0.2.1:1001", "192.0.2.2:1002", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newIPQuotaServer(t, 0, 0, 1)
			owner := ipQuotaLogin(t, s, "K1ABC-1", tc.owner)
			defer s.unregisterClient(owner)
			contender := ipQuotaClient(s, "W1XYZ-1", tc.contender)
			if err := ipQuotaTry(s, contender); !errors.Is(err, tc.want) {
				t.Fatalf("owner %s, contender %s: got %v, want %v", tc.owner, tc.contender, err, tc.want)
			} else if err == nil {
				defer s.unregisterClient(contender)
			} else {
				recheckNoProfile(t, contender.callsign)
			}
		})
	}
}

type ipQuotaStringAddr string

func (a ipQuotaStringAddr) Network() string { return "tcp" }
func (a ipQuotaStringAddr) String() string  { return string(a) }

func TestIPSessionQuotaInvalidPeerEnabledAndDisabled(t *testing.T) {
	for _, limit := range []int{1, 0} {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
			s := newIPQuotaServer(t, 0, 0, limit)
			profile := filepath.Join(filter.UserDataDir, "K1ABC-1.yaml")
			before := oldLoginRecord(4)
			if err := os.WriteFile(profile, before, 0o600); err != nil {
				t.Fatal(err)
			}
			serverEnd, peer := net.Pipe()
			observed := &ipQuotaPeerConn{Conn: serverEnd, peer: ipQuotaStringAddr("invalid-address:1000")}
			ticket, reason := s.tryAcquirePrelogin(observed.RemoteAddr())
			if ticket == nil || reason != "" {
				t.Fatalf("prelogin ticket: %v, %q", ticket, reason)
			}
			done := make(chan struct{})
			go func() { defer close(done); s.handleClient(observed, ticket) }()
			t.Cleanup(func() { _ = peer.Close(); _ = serverEnd.Close(); waitConfigurationTest(t, done) })
			ceilingReadThrough(t, peer, "login: ")
			if _, err := io.WriteString(peer, "K1ABC-1\r\n"); err != nil {
				t.Fatal(err)
			}
			if limit > 0 {
				ceilingReadThrough(t, peer, "Unable to verify connection address")
				ceilingWaitEOF(t, peer)
				waitConfigurationTest(t, done)
				after, err := os.ReadFile(profile)
				if err != nil || !bytes.Equal(after, before) {
					t.Fatalf("invalid peer changed profile: %v", err)
				}
				if _, err := os.Stat(profile + ".pre-v6.bak"); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("invalid peer created profile backup: %v", err)
				}
				if s.GetClientCount() != 0 || s.PreloginAdmissionMetricSnapshot().Active != 0 {
					t.Fatal("invalid peer retained registration or prelogin ticket")
				}
			} else {
				ceilingReadThrough(t, peer, "READY\r\n")
				if s.GetClientCount() != 1 {
					t.Fatal("disabled IP limit did not preserve existing address behavior")
				}
				_ = peer.Close()
				waitConfigurationTest(t, done)
				if s.GetClientCount() != 0 {
					t.Fatal("disabled-IP session did not unregister")
				}
			}
		})
	}
}

func TestIPSessionQuotaNilPeerEnabledAndDisabled(t *testing.T) {
	for _, limit := range []int{1, 0} {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
			s := newIPQuotaServer(t, 0, 0, limit)
			serverEnd, peer := net.Pipe()
			// The exact same nil peer is supplied to the prelogin gate and handler.
			observed := &ipQuotaPeerConn{Conn: serverEnd}
			ticket, reason := s.tryAcquirePrelogin(observed.RemoteAddr())
			if ticket == nil || reason != "" {
				_ = peer.Close()
				_ = serverEnd.Close()
				t.Fatalf("nil-peer prelogin ticket: %v, %q", ticket, reason)
			}
			done := make(chan struct{})
			go func() { defer close(done); s.handleClient(observed, ticket) }()
			t.Cleanup(func() { _ = peer.Close(); _ = serverEnd.Close(); waitConfigurationTest(t, done) })
			ceilingReadThrough(t, peer, "login: ")
			if _, err := io.WriteString(peer, "K1ABC-1\r\n"); err != nil {
				t.Fatal(err)
			}
			if limit > 0 {
				ceilingReadThrough(t, peer, "Unable to verify connection address")
				ceilingWaitEOF(t, peer)
				waitConfigurationTest(t, done)
				recheckNoProfile(t, "K1ABC-1")
				if s.GetClientCount() != 0 || s.PreloginAdmissionMetricSnapshot().Active != 0 {
					t.Fatal("nil peer retained registration or prelogin ticket")
				}
			} else {
				// Nil was not supported safely before; this checks the new defensive
				// handling, not compatibility with the earlier panic.
				ceilingReadThrough(t, peer, "READY\r\n")
				if s.GetClientCount() != 1 {
					t.Fatal("disabled IP limit did not admit nil peer")
				}
				_ = peer.Close()
				waitConfigurationTest(t, done)
				if s.GetClientCount() != 0 || s.PreloginAdmissionMetricSnapshot().Active != 0 {
					t.Fatal("nil-peer session or prelogin ticket did not clean up")
				}
			}
		})
	}
}

type ipQuotaObservedConn struct {
	*ipQuotaPeerConn
	writeStarted chan struct{}
	closed       chan struct{}
	writeOnce    sync.Once
	closeOnce    sync.Once
}

func (c *ipQuotaObservedConn) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("IP session limit reached")) {
		c.writeOnce.Do(func() { close(c.writeStarted) })
	}
	return c.Conn.Write(p)
}

func (c *ipQuotaObservedConn) Close() error {
	err := c.Conn.Close()
	c.closeOnce.Do(func() { close(c.closed) })
	return err
}

func TestIPSessionQuotaSampledRejectionWhileRegistryLocked(t *testing.T) {
	s := newIPQuotaServer(t, 0, 0, 1)
	s.maxPreloginSessions = 1
	s.rejectWriteDeadline = 500 * time.Millisecond
	s.admissionLogSample, s.admissionLogMaxLines = 1, 1
	owner := ipQuotaLogin(t, s, "K1ABC-1", "192.0.2.1:1001")
	classified, proceed := make(chan struct{}), make(chan struct{})
	var classifiedOnce, proceedOnce sync.Once
	releaseProbe := func() { proceedOnce.Do(func() { close(proceed) }) }
	s.testLoginCapacityProbe = func(c *Client, checked, full bool) {
		if c.callsign == "K2XYZ-1" && checked && full {
			classifiedOnce.Do(func() { close(classified) })
			<-proceed
		}
	}
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	var restoreOnce sync.Once
	restoreLog := func() { restoreOnce.Do(func() { log.SetOutput(previous) }) }
	serverEnd, peer := net.Pipe()
	remote := tcpAddr("192.0.2.1", 1002)
	observed := &ipQuotaObservedConn{
		ipQuotaPeerConn: &ipQuotaPeerConn{Conn: serverEnd, peer: remote},
		writeStarted:    make(chan struct{}), closed: make(chan struct{}),
	}
	ticket, reason := s.tryAcquirePrelogin(observed.RemoteAddr())
	if ticket == nil || reason != "" {
		restoreLog()
		_ = peer.Close()
		_ = serverEnd.Close()
		s.unregisterClient(owner)
		t.Fatalf("prelogin ticket: %v, %q", ticket, reason)
	}
	done := make(chan struct{})
	locked := false
	t.Cleanup(func() {
		if locked {
			s.clientsMutex.Unlock()
		}
		releaseProbe()
		_ = peer.Close()
		_ = serverEnd.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("rejection handler did not drain during cleanup")
		}
		restoreLog()
		s.unregisterClient(owner)
	})
	go func() { defer close(done); s.handleClient(observed, ticket) }()
	ceilingReadThrough(t, peer, "login: ")
	if _, err := io.WriteString(peer, "K2XYZ-1\r\n"); err != nil {
		t.Fatal(err)
	}
	waitConfigurationTest(t, classified)
	s.clientsMutex.Lock()
	locked = true
	releaseProbe()
	deadline := time.NewTimer(s.rejectWriteDeadline + 2*time.Second)
	defer deadline.Stop()
	select {
	case <-observed.writeStarted:
	case <-deadline.C:
		t.Fatal("sampled IP rejection did not reach notice while registry was locked")
	}
	if active := atomic.LoadInt64(&s.metrics.preloginActive); active != 1 {
		t.Fatalf("blocked notice retained %d tickets, want 1", active)
	}
	select {
	case <-observed.closed:
	case <-deadline.C:
		t.Fatal("rejected socket did not close while registry was locked")
	}
	select {
	case <-done:
	case <-deadline.C:
		t.Fatal("rejection handler did not finish while registry was locked")
	}
	if active := atomic.LoadInt64(&s.metrics.preloginActive); active != 0 {
		t.Fatalf("rejected IP retained %d prelogin tickets", active)
	}
	restoreLog()
	got := output.String()
	if !strings.Contains(got, "reason=max_sessions_per_ip") || !strings.Contains(got, "tracked_session_ips=1") {
		t.Fatalf("sample lacks IP reason or tracked count: %q", got)
	}
	if n := strings.Count(got, "Admission reject sample:"); n != 1 {
		t.Fatalf("sample count=%d, output=%q", n, got)
	}
	s.clientsMutex.Unlock()
	locked = false
	ipQuotaCurrent(t, s, owner.callsign, owner)
	recheckNoProfile(t, "K2XYZ-1")
}

func TestIPSessionQuotaSnapshotInShutdownReporter(t *testing.T) {
	s := newIPQuotaServer(t, 0, 0, 1)
	owner := ipQuotaLogin(t, s, "K1ABC-1", "192.0.2.1:1001")
	stopped, snapshotDone := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-stopped:
			s.unregisterClient(owner)
		default:
		}
	})
	type reportResult struct {
		completed bool
		count     int
	}
	reported := make(chan reportResult, 1)
	s.connectionReporter = func(event ConnectionEvent) {
		if event.Action != "disconnect" || event.Reason != "server shutdown" {
			return
		}
		result := make(chan int, 1)
		go func() {
			result <- s.PreloginAdmissionMetricSnapshot().TrackedSessionIPs
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
			t.Fatalf("shutdown reporter completed=%t tracked IPs=%d, want true and 1", got.completed, got.count)
		}
	default:
		t.Fatal("shutdown did not call connection reporter")
	}
}
