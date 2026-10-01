package peer

import (
	"dxcluster/config"
	"fmt"
	"testing"
	"time"
)

func inboundPeer(family string) config.PeeringPeer {
	return config.PeeringPeer{Enabled: true, Direction: config.PeeringPeerDirectionInbound, Family: family, RemoteCallsign: "N1REM", PreferPC9x: true}
}

func currentStartupPC92() string {
	now := time.Now().UTC()
	seconds := now.Hour()*3600 + now.Minute()*60 + now.Second()
	return fmt.Sprintf("PC92^N1REM^%d^C^5N1REM:5457^H99^", seconds)
}

func inboundLoginSteps(pc9x bool) []handshakeStep {
	banner := "PC18^GoCluster Version: test^5457^"
	if pc9x {
		banner = "PC18^GoCluster Version: test pc9x^5457^"
	}
	return []handshakeStep{
		{kind: handshakeExpectTx, matcher: exactLine("login:")},
		{kind: handshakeSendRx, line: "N1REM"},
		{kind: handshakeExpectTx, matcher: exactLine(banner)},
	}
}

func inboundEndSteps() []handshakeStep {
	return []handshakeStep{
		{kind: handshakeAwaitRegistered, timeout: time.Second},
		{kind: handshakeCloseRemote},
		{kind: handshakeAwaitResult, timeout: time.Second, errCheck: errIsEOFOrClosedPipe},
	}
}

func TestInboundHandshakeConfiguredPeerBehavior(t *testing.T) {
	t.Run("unknown peer rejected before banner", func(t *testing.T) {
		runInboundScenario(t, inboundScenario{name: t.Name(), steps: []handshakeStep{
			{kind: handshakeExpectTx, matcher: exactLine("login:")},
			{kind: handshakeSendRx, line: "N1REM"},
			{kind: handshakeAwaitResult, timeout: time.Second, errCheck: errContains("unauthorized inbound peer")},
		}})
	})
	t.Run("disabled peer rejected before banner", func(t *testing.T) {
		peer := inboundPeer(config.PeeringPeerFamilyDXSpider)
		peer.Enabled = false
		runInboundScenario(t, inboundScenario{name: t.Name(), peers: []config.PeeringPeer{peer}, steps: []handshakeStep{
			{kind: handshakeExpectTx, matcher: exactLine("login:")},
			{kind: handshakeSendRx, line: "N1REM"},
			{kind: handshakeAwaitResult, timeout: time.Second, errCheck: errContains("unauthorized inbound peer")},
		}})
	})
	t.Run("password rejected before banner", func(t *testing.T) {
		peer := inboundPeer(config.PeeringPeerFamilyDXSpider)
		peer.Password = "expected-test-password"
		runInboundScenario(t, inboundScenario{name: t.Name(), peers: []config.PeeringPeer{peer}, wantRemoteCall: "N1REM", steps: []handshakeStep{
			{kind: handshakeExpectTx, matcher: exactLine("login:")},
			{kind: handshakeSendRx, line: "N1REM"},
			{kind: handshakeExpectTx, matcher: exactLine("password:")},
			{kind: handshakeSendRx, line: "wrong-test-password"},
			{kind: handshakeAwaitResult, timeout: time.Second, errCheck: errContains("unauthorized password")},
		}})
	})
	t.Run("startup PC92 timeout remains unestablished", func(t *testing.T) {
		steps := inboundLoginSteps(true)
		steps = append(steps, handshakeStep{kind: handshakeSendRx, line: currentStartupPC92()}, handshakeStep{kind: handshakeAwaitResult, timeout: time.Second, errCheck: errIsTimeout})
		runInboundScenario(t, inboundScenario{name: t.Name(), peers: []config.PeeringPeer{inboundPeer(config.PeeringPeerFamilyDXSpider)}, wantRemoteCall: "N1REM", wantPC9x: true, steps: steps})
	})
	t.Run("DXSpider completes without extra PC20", func(t *testing.T) {
		steps := inboundLoginSteps(true)
		steps = append(steps,
			handshakeStep{kind: handshakeSendRx, line: "PC18^DXSpider Version: 1.57 Build: 633 [pc9x 91]^5457^"},
			handshakeStep{kind: handshakeSendRx, line: "PC20^"},
			handshakeStep{kind: handshakeExpectTx, matcher: pc92TypeLine("A")},
			handshakeStep{kind: handshakeExpectTx, matcher: pc92TypeLine("K")},
			handshakeStep{kind: handshakeExpectTx, matcher: exactLine("PC22^")},
		)
		steps = append(steps, inboundEndSteps()...)
		runInboundScenario(t, inboundScenario{name: t.Name(), peers: []config.PeeringPeer{inboundPeer(config.PeeringPeerFamilyDXSpider)}, wantRemoteCall: "N1REM", wantPC9x: true, wantRegistered: true, steps: steps})
	})
	for _, tc := range []struct {
		name       string
		preference bool
		banner     string
	}{
		{"capability absent", true, "PC18^DXSpider Version: 1.57^5457^"},
		{"preference disabled", false, "PC18^DXSpider Version: 1.57 [pc9x 91]^5457^"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := inboundPeer(config.PeeringPeerFamilyDXSpider)
			peer.PreferPC9x = tc.preference
			steps := inboundLoginSteps(tc.preference)
			steps = append(steps,
				handshakeStep{kind: handshakeSendRx, line: tc.banner},
				handshakeStep{kind: handshakeSendRx, line: "PC20^"},
				handshakeStep{kind: handshakeExpectTx, matcher: exactLine("PC19^1^N0CALL^0^1.57^H99^")},
				handshakeStep{kind: handshakeExpectTx, matcher: exactLine("PC22^")},
			)
			steps = append(steps, inboundEndSteps()...)
			runInboundScenario(t, inboundScenario{name: t.Name(), peers: []config.PeeringPeer{peer}, wantRemoteCall: "N1REM", wantRegistered: true, steps: steps})
		})
	}
	for _, first := range []string{"PC18^CC Cluster Version: 6.0^6.0^", currentStartupPC92()} {
		t.Run(first, func(t *testing.T) {
			steps := inboundLoginSteps(true)
			steps = append(steps, handshakeStep{kind: handshakeSendRx, line: first},
				handshakeStep{kind: handshakeExpectTx, matcher: pc92TypeLine("A")},
				handshakeStep{kind: handshakeExpectTx, matcher: pc92TypeLine("K")},
				handshakeStep{kind: handshakeExpectTx, matcher: exactLine("PC20^")},
				handshakeStep{kind: handshakeExpectTx, matcher: pc92TypeLine("C")},
				handshakeStep{kind: handshakeExpectTx, matcher: pc92TypeLine("A")},
				handshakeStep{kind: handshakeSendRx, line: "PC20^"},
				handshakeStep{kind: handshakeExpectTx, matcher: exactLine("PC22^")},
			)
			steps = append(steps, inboundEndSteps()...)
			runInboundScenario(t, inboundScenario{name: t.Name(), peers: []config.PeeringPeer{inboundPeer(config.PeeringPeerFamilyCCluster)}, wantRemoteCall: "N1REM", wantPC9x: true, wantRegistered: true, steps: steps})
		})
	}
	for _, tc := range []struct{ family, banner string }{
		{config.PeeringPeerFamilyDXSpider, "PC18^CC Cluster Version: 6.0^6.0^"},
		{config.PeeringPeerFamilyCCluster, "PC18^DXSpider Version: 1.57 [pc9x 91]^5457^"},
	} {
		t.Run("family mismatch "+tc.family, func(t *testing.T) {
			steps := inboundLoginSteps(true)
			steps = append(steps, handshakeStep{kind: handshakeSendRx, line: tc.banner}, handshakeStep{kind: handshakeAwaitResult, timeout: time.Second, errCheck: errContains("family mismatch")})
			runInboundScenario(t, inboundScenario{name: t.Name(), peers: []config.PeeringPeer{inboundPeer(tc.family)}, wantRemoteCall: "N1REM", steps: steps})
		})
	}
}
