package peer

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

func TestManagerStartDialsOutboundAndBothPeersOnly(t *testing.T) {
	ctx := context.Background()

	outboundLn, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen outbound: %v", err)
	}
	defer outboundLn.Close()

	bothLn, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen both: %v", err)
	}
	defer bothLn.Close()

	outboundHost, outboundPort := splitHostPort(t, outboundLn.Addr().String())
	bothHost, bothPort := splitHostPort(t, bothLn.Addr().String())

	manager, err := NewManager(config.PeeringConfig{
		LocalCallsign:  "N0CALL",
		ListenPort:     0,
		NodeVersion:    "5457",
		LegacyVersion:  "1.57",
		PC92Bitmap:     5,
		HopCount:       99,
		WriteQueueSize: 8,
		MaxLineLength:  4096,
		PC92MaxBytes:   4096,
		Timeouts: config.PeeringTimeouts{
			LoginSeconds: 1,
			InitSeconds:  1,
			IdleSeconds:  1,
		},
		Backoff: config.PeeringBackoff{
			BaseMS: 25,
			MaxMS:  50,
		},
		Peers: []config.PeeringPeer{
			{
				Enabled:        true,
				Direction:      config.PeeringPeerDirectionOutbound,
				Family:         config.PeeringPeerFamilyDXSpider,
				Host:           outboundHost,
				Port:           outboundPort,
				RemoteCallsign: "OUTBOUND-1",
			},
			{
				Enabled:        true,
				Direction:      config.PeeringPeerDirectionBoth,
				Family:         config.PeeringPeerFamilyCCluster,
				Host:           bothHost,
				Port:           bothPort,
				RemoteCallsign: "BOTH-1",
			},
			{
				Enabled:        true,
				Direction:      config.PeeringPeerDirectionInbound,
				Family:         config.PeeringPeerFamilyCCluster,
				RemoteCallsign: "INBOUND-1",
			},
			{
				Enabled:        false,
				Direction:      config.PeeringPeerDirectionOutbound,
				Family:         config.PeeringPeerFamilyDXSpider,
				Host:           outboundHost,
				Port:           outboundPort,
				RemoteCallsign: "DISABLED-1",
			},
		},
	}, "N0CALL", nil, 0, nil)
	if err != nil {
		t.Fatalf("NewManager() error: %v", err)
	}

	if got := len(manager.outboundPeers); got != 2 {
		t.Fatalf("expected 2 outbound peers, got %d", got)
	}
	if got := len(manager.inboundPeers); got != 2 {
		t.Fatalf("expected 2 inbound peers, got %d", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	defer manager.Stop()

	outboundConn := acceptOneConn(t, outboundLn)
	defer outboundConn.Close()
	bothConn := acceptOneConn(t, bothLn)
	defer bothConn.Close()
}

func TestAuthorizeInboundRejectsDuplicateActivePeer(t *testing.T) {
	manager, err := NewManager(config.PeeringConfig{
		NodeVersion:   "5457",
		LegacyVersion: "1.57",
		Peers: []config.PeeringPeer{{
			Enabled:        true,
			Direction:      config.PeeringPeerDirectionBoth,
			Family:         config.PeeringPeerFamilyCCluster,
			Host:           "example.net",
			Port:           7300,
			RemoteCallsign: "REMOTE",
		}},
	}, "N0CALL", nil, 0, nil)
	if err != nil {
		t.Fatalf("NewManager() error: %v", err)
	}

	existing := &session{id: "REMOTE", remoteCall: "REMOTE"}
	manager.sessions.Set("REMOTE", existing)

	_, err = manager.authorizeInbound("REMOTE", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 7000})
	if err == nil || !strings.Contains(err.Error(), "duplicate peer session") {
		t.Fatalf("expected duplicate peer session error, got %v", err)
	}
	if got := manager.sessions.Value("REMOTE"); got != existing {
		t.Fatalf("expected original session to remain active")
	}
}

func TestPeerRegistryDistinguishesBlankRemoteCallByHostPort(t *testing.T) {
	manager, err := NewManager(config.PeeringConfig{
		NodeVersion:   "5457",
		LegacyVersion: "1.57",
		Peers: []config.PeeringPeer{
			{
				Enabled:   true,
				Direction: config.PeeringPeerDirectionOutbound,
				Family:    config.PeeringPeerFamilyDXSpider,
				Host:      "127.0.0.1",
				Port:      7300,
			},
			{
				Enabled:   true,
				Direction: config.PeeringPeerDirectionOutbound,
				Family:    config.PeeringPeerFamilyDXSpider,
				Host:      "127.0.0.1",
				Port:      7301,
			},
		},
	}, "N0CALL", nil, 0, nil)
	if err != nil {
		t.Fatalf("NewManager() error: %v", err)
	}

	if got := len(manager.outboundPeers); got != 2 {
		t.Fatalf("expected 2 outbound peers, got %d", got)
	}
	if manager.outboundPeers[0].ID() == manager.outboundPeers[1].ID() {
		t.Fatalf("expected distinct host:port fallback identities, got %q", manager.outboundPeers[0].ID())
	}
}

func acceptOneConn(t *testing.T, ln net.Listener) net.Conn {
	t.Helper()

	type acceptedConn struct {
		conn net.Conn
		err  error
	}
	ch := make(chan acceptedConn, 1)
	go func() {
		conn, err := ln.Accept()
		ch <- acceptedConn{conn: conn, err: err}
	}()

	select {
	case result := <-ch:
		if result.err != nil {
			t.Fatalf("accept on %s: %v", ln.Addr(), result.err)
		}
		return result.conn
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for outbound connection on %s", ln.Addr())
		return nil
	}
}

func splitHostPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host port %q: %v", addr, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("atoi port %q: %v", portText, err)
	}
	return host, port
}
