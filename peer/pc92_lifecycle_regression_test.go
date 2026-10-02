package peer

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"dxcluster/config"
)

func TestPC92FrozenQuietClockClosesAndGates(t *testing.T) {
	p, source, _, wall := controllerTestOwner(t)
	elapsed := wall
	p.wallNow = func() time.Time { return wall }
	p.elapsedNow = func() time.Time { return elapsed }
	p.tick(elapsed)
	elapsed = elapsed.Add(3999 * time.Millisecond)
	p.tick(elapsed)
	if p.clockGate {
		t.Fatal("frozen clock falsely diagnosed before its detection interval")
	}
	elapsed = elapsed.Add(time.Millisecond)
	p.tick(elapsed)
	if !p.clockGate || source.ctx.Err() == nil {
		t.Fatal("quiet frozen clock escaped gating because timestamps were not exhausted")
	}
}

func TestPC92PeriodicKCoalescesAtTimestampLimit(t *testing.T) {
	p, source, _, wall := controllerTestOwner(t)
	p.wallNow = func() time.Time { return wall }
	p.elapsedNow = func() time.Time { return wall }
	for i := 0; i < 100; i++ {
		if _, err := p.timestamps.NextAt(wall); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := p.request(protocolRequest{kind: "K", source: source}); err != nil {
			t.Fatal(err)
		}
	}
	if source.ctx.Err() != nil || len(source.priorityLineCh) != 0 || p.pendingK.Len() != 1 {
		t.Fatal("periodic K did not coalesce without closing a healthy session")
	}
	wall = wall.Add(time.Second)
	p.tick(wall)
	var keepalives int
	for _, r := range publicationRecords(t, source) {
		if r.Action == "K" {
			keepalives++
		}
	}
	if keepalives != 1 || p.pendingK.Len() != 0 {
		t.Fatal("coalesced K was not published once after UTC advanced")
	}
}

func TestPC92StaleCloseCannotInvalidateReplacement(t *testing.T) {
	p, old, _, now := controllerTestOwner(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	replacement := &session{id: old.id, remoteCall: old.remoteCall, pc9x: true, ctx: ctx, cancel: cancel, manager: p.manager}
	p.manager.sessions.Set(old.id, replacement)
	receiveControllerWire(t, p, replacement, "PC92^N2AAA^43200^C^5N2AAA^1K1USER^H1^", now)
	if err := p.request(protocolRequest{kind: "closed", source: old}); err != nil {
		t.Fatal(err)
	}
	if p.manager.sessions.Value(old.id) != replacement || !p.graph.nodes.Value("N2AAA").Complete || p.graph.ingress.Len() != 1 {
		t.Fatal("stale close removed replacement authority")
	}
	if err := p.request(protocolRequest{kind: "closed", source: replacement}); err != nil {
		t.Fatal(err)
	}
	if p.manager.sessions.Value(old.id) != nil || p.graph.nodes.Value("N2AAA").Complete || p.graph.ingress.Len() != 0 {
		t.Fatal("current close failed to invalidate its own ingress")
	}
}

func TestPC92CanceledSourceCannotRestoreAuthority(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^5N2AAA^1K1OLD^H1^", now)
	p.graph.loseIngress(source.remoteCall)
	source.close()
	receiveControllerWire(t, p, source, "PC92^N2AAA^43201^C^5N2AAA^1K1NEW^H1^", now)
	if p.graph.nodes.Value("N2AAA").Complete || p.graph.freshness.Value("N2AAA").Value != 43200 || p.graph.users.Value("K1NEW") != 0 {
		t.Fatal("already queued record restored authority after terminal source failure")
	}
}

func TestPC92OutboundGatePreventsTCPDial(t *testing.T) {
	for _, gate := range []string{"clock", "authority"} {
		t.Run(gate, func(t *testing.T) {
			var lc net.ListenConfig
			listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			host, portText, err := net.SplitHostPort(listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			port, err := strconv.Atoi(portText)
			if err != nil {
				t.Fatal(err)
			}
			m, err := NewManager(completeProtocolTestConfig(config.PeeringConfig{}, "N0LOCAL"), "N0LOCAL", nil, 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			m.ctx, m.cancel = context.WithCancel(context.Background())
			defer m.cancel()
			if gate == "clock" {
				m.pc9xGated.Store(true)
			} else {
				old := retryV14Candidate(t, m, "N1PEER")
				m.sessions.Set(old.id, old)
				m.retryRefused(old, admissionAuthority, time.Now())
				m.mu.Lock()
				m.retryRetireLocked(old, time.Now())
				m.sessions.Delete(old.id)
				m.mu.Unlock()
				// Neither elapsed cooldown nor TCP availability may bypass
				// the still-unacknowledged controller invalidation barrier.
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				m.runOutbound(PeerEndpoint{host: host, port: port, remoteCall: "N1PEER", preferPC9x: true})
			}()
			if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			conn, err := listener.Accept()
			if err == nil {
				_ = conn.Close()
				t.Error("gated peer opened a TCP connection")
			}
			m.cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("gate wait did not join after cancellation")
			}
		})
	}
}
