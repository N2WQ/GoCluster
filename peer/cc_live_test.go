package peer

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// TestCCLiveProductionSession is explicitly opt-in, using an owner-authorized
// endpoint and node identities. It runs the actual outbound manager, parser,
// controller and writer; it publishes no test spots and changes no config file.
func TestCCLiveProductionSession(t *testing.T) {
	endpoint := os.Getenv("GOCLUSTER_CC_LIVE_ENDPOINT")
	if endpoint == "" {
		t.Skip("live CC evidence requires GOCLUSTER_CC_LIVE_ENDPOINT, GOCLUSTER_CC_LIVE_LOCAL and GOCLUSTER_CC_LIVE_REMOTE")
	}
	local, remote := os.Getenv("GOCLUSTER_CC_LIVE_LOCAL"), os.Getenv("GOCLUSTER_CC_LIVE_REMOTE")
	if local == "" || remote == "" {
		t.Fatal("explicit authorized local and remote identities required")
	}
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		t.Fatal("invalid live endpoint port")
	}
	cfg := completeProtocolTestConfig(config.PeeringConfig{
		Enabled: true, NodeBuild: "633", KeepaliveSeconds: 10,
		Timeouts: config.PeeringTimeouts{LoginSeconds: 15, InitSeconds: 20, IdleSeconds: 30},
		Peers: []config.PeeringPeer{{Enabled: true, Family: config.PeeringPeerFamilyCCluster,
			Direction: "outbound", Host: host, Port: port, LoginCallsign: local,
			RemoteCallsign: remote, PreferPC9x: true}},
	}, local)
	ingest := make(chan *spot.Spot, 128)
	m, err := NewManager(cfg, local, ingest, 300, nil)
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan ConnectionEvent, 32)
	observeConnectionEvents(t, m, func(event ConnectionEvent) {
		select {
		case events <- event:
		default:
			m.cancel()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 130*time.Second)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	established := false
	spots := 0
	var stable <-chan time.Time
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case event := <-events:
			t.Logf("production peer event: action=%s reason=%s", event.Action, event.Reason)
			switch event.Action {
			case "established":
				if established {
					t.Fatal("unexpected reconnection")
				}
				established = true
				timer = time.NewTimer(90 * time.Second)
				stable = timer.C
			case "rejected", "dial_failed", "disconnected":
				t.Fatalf("live session failed: %+v", event)
			}
		case <-ingest:
			spots++
		case <-stable:
			if m.ActiveSessionCount() != 1 || spots == 0 {
				t.Fatalf("live session lacks streaming evidence: active=%d spots=%d", m.ActiveSessionCount(), spots)
			}
			m.Stop()
			m.mu.RLock()
			candidates, records, bytes := m.candidates.Len(), m.stagedRecords, m.stagedBytes
			m.mu.RUnlock()
			if m.ActiveSessionCount() != 0 || candidates != 0 || records != 0 || bytes != 0 {
				t.Fatal("live shutdown retained session or candidate resources")
			}
			t.Logf("production CC session remained established for 90s; ingested %d spots; shutdown released session and staging", spots)
			return
		case <-ctx.Done():
			t.Fatalf("live session timed out: established=%v spots=%d", established, spots)
		}
	}
}
