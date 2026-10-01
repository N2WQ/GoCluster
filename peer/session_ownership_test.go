package peer

import (
	"bufio"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
)

func TestSessionOwnerReservationSurvivesTerminalReporter(t *testing.T) {
	m, _ := newInboundHarnessManager(t, inboundScenario{name: t.Name()})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	established := make(chan struct{}, 1)
	retired := make(chan struct{}, 1)
	m.SetConnectionReporter(func(event ConnectionEvent) {
		switch event.Action {
		case "established":
			established <- struct{}{}
		case "disconnected":
			retired <- struct{}{}
			<-release
		}
	})
	done := make(chan error, 192)
	endpoint := PeerEndpoint{host: "pipe", remoteCall: "N1REM", family: config.PeeringPeerFamilyDXSpider}
	for i := 0; i < 192; i++ {
		local, remote := net.Pipe()
		settings := m.sessionSettings(endpoint)
		settings.loginTimeout, settings.initTimeout = time.Second, time.Second
		settings.idleTimeout, settings.keepalive, settings.configEvery = 0, 0, 0
		s := newSession(local, dirOutbound, m, endpoint, settings)
		go func() { done <- s.Run(m.ctx) }()
		reader := bufio.NewReader(remote)
		if got := readSessionWire(t, reader, remote); got != "N0CALL" {
			t.Fatalf("replacement %d login=%q", i, got)
		}
		// Legacy completion avoids the unrelated PC92 timestamp quota. It still
		// exercises the same real registry, terminal callback and Run ownership.
		writeSessionWire(t, remote, "PC18^DXSpider Version: 1.57^5457^")
		if got := readSessionWire(t, reader, remote); !strings.HasPrefix(got, "PC19^") {
			t.Fatalf("replacement %d initial=%q", i, got)
		}
		if got := readSessionWire(t, reader, remote); got != "PC20^" {
			t.Fatalf("replacement %d completion=%q", i, got)
		}
		writeSessionWire(t, remote, "PC22^")
		awaitOwnerEvent(t, established, "establishment")
		_ = remote.Close()
		awaitOwnerEvent(t, retired, "terminal reporter")
		if m.ActiveSessionCount() != 0 || len(m.pendingSlots) != 0 || len(m.ownerSlots) != i+1 {
			t.Fatalf("replacement %d: registry=%d pending=%d owners=%d", i, m.ActiveSessionCount(), len(m.pendingSlots), len(m.ownerSlots))
		}
	}
	// Registry and handshake slots are empty, but every transport lifetime is
	// still owned. Both production pre-construction admission and Run's guard
	// must reject a further replacement until terminal ownership is released.
	if m.reserveCandidateSlots() {
		m.releaseUnstartedCandidateSlots()
		t.Fatal("retired owners allowed a 193rd transport reservation")
	}
	local, remote := net.Pipe()
	s := newSession(local, dirOutbound, m, endpoint, m.sessionSettings(endpoint))
	err := s.Run(m.ctx)
	_ = remote.Close()
	if err == nil || !strings.Contains(err.Error(), "capacity") {
		t.Fatalf("Run owner-capacity refusal=%v", err)
	}
	releaseOnce.Do(func() { close(release) })
	for range 192 {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("terminal reporter release did not join Run")
		}
	}
	if len(m.ownerSlots) != 0 || len(m.pendingSlots) != 0 {
		t.Fatalf("terminal ownership survived Run: owners=%d pending=%d", len(m.ownerSlots), len(m.pendingSlots))
	}
	// Refill proves released credits are usable and pending admission remains
	// independently bounded even when the combined owner pool has headroom.
	for i := 0; i < 128; i++ {
		if !m.reserveCandidateSlots() {
			t.Fatalf("refill refused pending reservation %d", i)
		}
	}
	if m.reserveCandidateSlots() {
		t.Fatal("pending admission exceeded128")
	}
	for range 128 {
		m.releaseUnstartedCandidateSlots()
	}
	m.Stop()
	if m.reserveCandidateSlots() || len(m.ownerSlots) != 0 || len(m.pendingSlots) != 0 {
		t.Fatal("Stop admitted or retained transport ownership")
	}
}

func awaitOwnerEvent(t *testing.T, event <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out awaiting %s", label)
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
	if err := s.Run(m.ctx); err == nil {
		t.Fatal("stopped manager accepted a pre-reserved candidate")
	}
	if len(m.pendingSlots) != 0 || len(m.ownerSlots) != 0 || s.pendingReserved || s.ownerReserved {
		t.Fatal("pre-Run rejection retained reservations")
	}
}
