package peer

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/peerdiag"
)

func TestSessionOwnerReservationSurvivesDiagnosticOverload(t *testing.T) {
	m, _ := newInboundHarnessManager(t, inboundScenario{name: t.Name()})
	m.diagnostics = peerdiag.New(peerdiag.Options{Enabled: true})
	for range peerdiag.QueueSize {
		m.reportDiagnostic("fill", "", "diagnostic pressure")
	}
	endpoint := PeerEndpoint{host: "pipe", remoteCall: "N1REM", family: config.PeeringPeerFamilyDXSpider}
	for i := 0; i < 192; i++ {
		local, remote := net.Pipe()
		settings := m.sessionSettings(endpoint)
		settings.loginTimeout, settings.initTimeout = time.Second, time.Second
		settings.idleTimeout, settings.keepalive, settings.configEvery = 0, 0, 0
		s := newSession(local, dirOutbound, m, endpoint, settings)
		done := make(chan error, 1)
		go func() { done <- s.Run() }()
		reader := bufio.NewReader(remote)
		if got := readSessionWire(t, reader, remote); got != "N0CALL" {
			t.Fatalf("login=%q", got)
		}
		writeSessionWire(t, remote, "PC18^DXSpider Version: 1.57^5457^")
		if got := readSessionWire(t, reader, remote); !strings.HasPrefix(got, "PC19^") {
			t.Fatalf("initial=%q", got)
		}
		if got := readSessionWire(t, reader, remote); got != "PC20^" {
			t.Fatalf("completion=%q", got)
		}
		writeSessionWire(t, remote, "PC22^")
		deadline := time.Now().Add(time.Second)
		for m.ActiveSessionCount() != 1 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if m.ActiveSessionCount() != 1 {
			t.Fatal("full diagnostic mailbox prevented establishment")
		}
		_ = remote.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("full diagnostic mailbox prevented terminal Run")
		}
		if m.ActiveSessionCount() != 0 || len(m.pendingSlots) != 0 || len(m.ownerSlots) != 0 {
			t.Fatalf("cycle %d retained session credits", i)
		}
	}
	if stats := m.DiagnosticStats(); stats.Queued != peerdiag.QueueSize || stats.Dropped < 384 {
		t.Fatalf("diagnostic overflow accounting=%+v", stats)
	}
	m.Stop()
	if m.reserveCandidateSlots() {
		t.Fatal("Stop admitted new transport")
	}
}

func TestSessionOwnerReservationCancelledBeforeRun(t *testing.T) {
	m, _ := newInboundHarnessManager(t, inboundScenario{name: t.Name()})
	if !m.reserveCandidateSlots() {
		t.Fatal("initial reservation refused")
	}
	local, remote := net.Pipe()
	defer remote.Close()
	s := newSession(local, dirOutbound, m, PeerEndpoint{}, m.sessionSettings(PeerEndpoint{}))
	s.pendingReserved, s.ownerReserved = true, true
	m.mu.Lock()
	m.stopping = true
	m.mu.Unlock()
	if err := s.Run(); err == nil {
		t.Fatal("stopped manager accepted a pre-reserved candidate")
	}
	if len(m.pendingSlots) != 0 || len(m.ownerSlots) != 0 || s.pendingReserved || s.ownerReserved {
		t.Fatal("pre-Run rejection retained reservations")
	}
}
