package telnet

import (
	"net"
	"testing"
	"time"
)

func newPreloginTestServer(now *time.Time) *Server {
	s := &Server{
		maxPreloginSessions:  8,
		acceptRatePerIP:      10,
		acceptBurstPerIP:     10,
		preloginConcPerIP:    4,
		preloginByIP:         make(map[string]preloginIPState),
		preloginTrackedMax:   64,
		preloginStateIdleTTL: time.Minute,
	}
	if now != nil {
		s.nowFn = func() time.Time { return *now }
	}
	return s
}

func tcpAddr(ip string, port int) net.Addr {
	return &net.TCPAddr{IP: net.ParseIP(ip), Port: port}
}

func TestTryAcquirePreloginHonorsGlobalCap(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	s := newPreloginTestServer(&now)
	s.maxPreloginSessions = 1

	t1, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.10", 1000))
	if t1 == nil || reason != "" {
		t.Fatalf("expected first admission success, got ticket=%v reason=%q", t1, reason)
	}
	t2, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.11", 1001))
	if t2 != nil || reason != preloginRejectGlobalCap {
		t.Fatalf("expected global cap reject, got ticket=%v reason=%q", t2, reason)
	}
	t1.Release()
	t3, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.11", 1002))
	if t3 == nil || reason != "" {
		t.Fatalf("expected admission after release, got ticket=%v reason=%q", t3, reason)
	}
	t3.Release()
}

func TestTryAcquirePreloginHonorsPerIPConcurrency(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	s := newPreloginTestServer(&now)
	s.preloginConcPerIP = 2

	t1, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.20", 2000))
	if t1 == nil || reason != "" {
		t.Fatalf("expected first admission success, got ticket=%v reason=%q", t1, reason)
	}
	t2, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.20", 2001))
	if t2 == nil || reason != "" {
		t.Fatalf("expected second admission success, got ticket=%v reason=%q", t2, reason)
	}
	t3, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.20", 2002))
	if t3 != nil || reason != preloginRejectIPConcurrency {
		t.Fatalf("expected per-ip concurrency reject, got ticket=%v reason=%q", t3, reason)
	}
	t1.Release()
	t4, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.20", 2003))
	if t4 == nil || reason != "" {
		t.Fatalf("expected admission after concurrency release, got ticket=%v reason=%q", t4, reason)
	}
	t2.Release()
	t4.Release()
}

func TestTryAcquirePreloginHonorsPerIPRate(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	s := newPreloginTestServer(&now)
	s.acceptRatePerIP = 1
	s.acceptBurstPerIP = 1
	s.preloginConcPerIP = 8

	t1, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.30", 3000))
	if t1 == nil || reason != "" {
		t.Fatalf("expected first admission success, got ticket=%v reason=%q", t1, reason)
	}
	t1.Release()

	t2, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.30", 3001))
	if t2 != nil || reason != preloginRejectIPRate {
		t.Fatalf("expected rate reject, got ticket=%v reason=%q", t2, reason)
	}

	now = now.Add(1100 * time.Millisecond)
	t3, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.30", 3002))
	if t3 == nil || reason != "" {
		t.Fatalf("expected admission after refill, got ticket=%v reason=%q", t3, reason)
	}
	t3.Release()
}

func TestTryAcquirePreloginEvictsIdleStateWhenTrackerFull(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	s := newPreloginTestServer(&now)
	s.preloginTrackedMax = 2
	s.preloginStateIdleTTL = 24 * time.Hour

	t1, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.40", 4000))
	if t1 == nil || reason != "" {
		t.Fatalf("expected first admission success, got ticket=%v reason=%q", t1, reason)
	}
	t1.Release()

	now = now.Add(10 * time.Millisecond)
	t2, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.41", 4001))
	if t2 == nil || reason != "" {
		t.Fatalf("expected second admission success, got ticket=%v reason=%q", t2, reason)
	}
	t2.Release()

	now = now.Add(10 * time.Millisecond)
	t3, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.42", 4002))
	if t3 == nil || reason != "" {
		t.Fatalf("expected third admission success with eviction, got ticket=%v reason=%q", t3, reason)
	}
	t3.Release()

	if got := len(s.preloginByIP); got > 2 {
		t.Fatalf("expected tracked prelogin states <=2, got %d", got)
	}
	_, _, _, _, _, evictions, _ := s.PreloginMetricSnapshot()
	if evictions == 0 {
		t.Fatal("expected at least one prelogin state eviction")
	}
}

func TestHandleClientPreloginTimeoutReleasesTicket(t *testing.T) {
	s := NewServer(ServerOptions{
		SkipHandshake:            true,
		WelcomeMessage:           "",
		LoginPrompt:              "",
		PreloginTimeout:          20 * time.Millisecond,
		MaxPreloginSessions:      32,
		AcceptRatePerIP:          10,
		AcceptBurstPerIP:         10,
		PreloginConcurrencyPerIP: 8,
		LoginLineLimit:           32,
		CommandLineLimit:         128,
	}, nil)

	ticket, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.50", 5000))
	if ticket == nil || reason != "" {
		t.Fatalf("expected prelogin ticket acquisition, got ticket=%v reason=%q", ticket, reason)
	}

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		s.handleClient(serverConn, ticket)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleClient did not exit after prelogin timeout")
	}

	active, _, _, _, timeouts, _, _ := s.PreloginMetricSnapshot()
	if active != 0 {
		t.Fatalf("expected prelogin active gauge 0 after timeout, got %d", active)
	}
	if timeouts == 0 {
		t.Fatal("expected prelogin timeout counter increment")
	}
}
