package peer

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// Exercise Run, including the writer and controller, rather than granting
// establishment directly through an internal frame handler.
func ccOutboundHarness(t *testing.T, family string, modern bool) (*session, *Manager, net.Conn, chan error, context.CancelFunc, <-chan *spot.Spot) {
	t.Helper()
	m, ingest := newInboundHarnessManager(t, inboundScenario{name: t.Name()})
	local, remote := net.Pipe()
	ep := PeerEndpoint{host: "pipe", remoteCall: "N1REM", family: family, preferPC9x: modern}
	settings := m.sessionSettings(ep)
	settings.loginTimeout, settings.initTimeout = 100*time.Millisecond, 250*time.Millisecond
	settings.idleTimeout = 0
	s := newSession(local, dirOutbound, m, ep, settings)
	done := make(chan error, 1)
	go func() { done <- s.Run() }()
	cancel := func() { m.cancel() }
	t.Cleanup(func() { cancel(); _ = remote.Close() })
	return s, m, remote, done, cancel, ingest
}

func ccReadConfiguration(t *testing.T, r *bufio.Reader, conn net.Conn, modern bool, end string) {
	t.Helper()
	if modern {
		for _, action := range []string{"A", "K"} {
			if got := readSessionWire(t, r, conn); !pc92TypeLine(action).match(got) {
				t.Fatalf("want configuration %s, got %q", action, got)
			}
		}
	} else if got := readSessionWire(t, r, conn); !strings.HasPrefix(got, "PC19^") {
		t.Fatalf("want legacy configuration PC19, got %q", got)
	}
	if got := readSessionWire(t, r, conn); got != end {
		t.Fatalf("want %s, got %q", end, got)
	}
}

func ccJoin(t *testing.T, done <-chan error, cancel context.CancelFunc) {
	t.Helper()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not join session")
	}
}

func TestCCOutboundCompletionExchange(t *testing.T) {
	for _, modern := range []bool{true, false} {
		t.Run(fmt.Sprintf("modern=%v", modern), func(t *testing.T) {
			s, m, conn, done, cancel, ingest := ccOutboundHarness(t, config.PeeringPeerFamilyCCluster, modern)
			r := bufio.NewReader(conn)
			if got := readSessionWire(t, r, conn); got != "N0CALL" {
				t.Fatalf("login=%q", got)
			}
			writeSessionWire(t, conn, "PC18^CC Cluster Version: 3.397^5457^")
			ccReadConfiguration(t, r, conn, modern, "PC20^")
			// A spot during initialization must not enter live ingestion.
			now := time.Now().UTC()
			spotWire := fmt.Sprintf("PC61^14074.0^K1ABC^%s^%s^CQ^W1ABC^N1REM^127.0.0.1^H9^", now.Format("02-Jan-2006"), now.Format("1504Z"))
			writeSessionWire(t, conn, spotWire)
			writeSessionWire(t, conn, "PC51^N0CALL^N1REM^1^")
			if got := readSessionWire(t, r, conn); got != "PC51^N1REM^N0CALL^0^" {
				t.Fatalf("ping=%q", got)
			}
			if len(m.ingest) != 0 || m.ActiveSessionCount() != 0 {
				t.Fatal("startup spot or ping granted establishment")
			}
			writeSessionWire(t, conn, "PC20^")
			ccReadConfiguration(t, r, conn, modern, "PC22^")
			if modern {
				for _, action := range []string{"C", "A"} {
					if got := readSessionWire(t, r, conn); !pc92TypeLine(action).match(got) {
						t.Fatalf("want recovery %s, got %q", action, got)
					}
				}
			}
			// A repeated completion marker may acknowledge, but cannot send
			// configuration again or restart membership recovery.
			writeSessionWire(t, conn, "PC20^")
			if got := readSessionWire(t, r, conn); got != "PC22^" {
				t.Fatalf("duplicate completion=%q", got)
			}
			writeSessionWire(t, conn, "PC51^N0CALL^N1REM^1^")
			if got := readSessionWire(t, r, conn); got != "PC51^N1REM^N0CALL^0^" {
				t.Fatalf("post-completion ping=%q", got)
			}
			if m.ActiveSessionCount() != 1 {
				t.Fatalf("active sessions=%d", m.ActiveSessionCount())
			}
			writeSessionWire(t, conn, spotWire)
			select {
			case <-ingest:
			case <-time.After(time.Second):
				t.Fatal("same valid spot was not admitted after completion; startup must not ingest or seed dedupe")
			}
			ccJoin(t, done, cancel)
			if !s.established {
				t.Fatal("CC PC20 did not establish")
			}
			assertCancellationPublicationRetired(t, m, s)
		})
	}
}

func TestCCOutboundBannerlessStartup(t *testing.T) {
	for _, origin := range []string{"N1REM", "N2OTHER"} {
		t.Run(origin, func(t *testing.T) {
			s, m, conn, done, cancel, _ := ccOutboundHarness(t, config.PeeringPeerFamilyCCluster, true)
			r := bufio.NewReader(conn)
			readSessionWire(t, r, conn)
			// Valid startup topology retains its existing eligibility even when
			// the origin differs from the configured transport peer.
			wire := strings.ReplaceAll(currentStartupPC92(), "N1REM", origin)
			writeSessionWire(t, conn, wire)
			ccReadConfiguration(t, r, conn, true, "PC20^")
			writeSessionWire(t, conn, "PC20^")
			ccReadConfiguration(t, r, conn, true, "PC22^")
			for _, action := range []string{"C", "A"} {
				if got := readSessionWire(t, r, conn); !pc92TypeLine(action).match(got) {
					t.Fatalf("recovery=%q", got)
				}
			}
			ccJoin(t, done, cancel)
			if !s.established {
				t.Fatal("bannerless CC did not establish")
			}
			assertCancellationPublicationRetired(t, m, s)
		})
	}
}

func TestCCOutboundPrematureCompletionAndPingDeadline(t *testing.T) {
	s, m, conn, done, _, _ := ccOutboundHarness(t, config.PeeringPeerFamilyCCluster, true)
	r := bufio.NewReader(conn)
	readSessionWire(t, r, conn)
	start := time.Now()
	writeSessionWire(t, conn, "PC20^")
	writeSessionWire(t, conn, "PC92^N1REM^invalid^K^5N1REM^H99^")
	for _, ignored := range []string{"PC51^OTHER^N1REM^1^", "PC51^N0CALL^N1REM^0^"} {
		writeSessionWire(t, conn, ignored)
	}
	// The next response proves the ignored records did not queue replies.
	for _, destination := range []string{"N0CALL", "*", ""} {
		writeSessionWire(t, conn, fmt.Sprintf("PC51^%s^N1REM^1^", destination))
		if got := readSessionWire(t, r, conn); got != fmt.Sprintf("PC51^N1REM^%s^0^", destination) {
			t.Fatalf("pre-init ping=%q", got)
		}
		time.Sleep(60 * time.Millisecond)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("premature PC20 established")
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("pings extended fixed handshake deadline")
	}
	if time.Since(start) > time.Second || s.established || m.ActiveSessionCount() != 0 {
		t.Fatal("premature completion or ping changed handshake authority/deadline")
	}
	assertCancellationPublicationRetired(t, m, s)
}

func TestCCOutboundDXSpiderStillRequiresPC22(t *testing.T) {
	s, m, conn, done, _, _ := ccOutboundHarness(t, config.PeeringPeerFamilyDXSpider, true)
	r := bufio.NewReader(conn)
	readSessionWire(t, r, conn)
	writeSessionWire(t, conn, "PC18^DXSpider Version: 1.57 Build: 633 [pc9x 91]^5457^")
	ccReadConfiguration(t, r, conn, true, "PC20^")
	writeSessionWire(t, conn, "PC20^")
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("DXSpider PC20 established")
		}
	case <-time.After(time.Second):
		t.Fatal("DXSpider handshake did not time out")
	}
	if s.established {
		t.Fatal("CC completion leaked into DXSpider")
	}
	assertCancellationPublicationRetired(t, m, s)
}

func TestCCOutboundFamilyMismatch(t *testing.T) {
	s, m, conn, done, _, _ := ccOutboundHarness(t, config.PeeringPeerFamilyCCluster, true)
	r := bufio.NewReader(conn)
	readSessionWire(t, r, conn)
	writeSessionWire(t, conn, "PC18^DXSpider Version: 1.57 Build: 633 [pc9x 91]^5457^")
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("mismatched family accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("family mismatch did not close")
	}
	if s.established {
		t.Fatal("mismatched family established")
	}
	assertCancellationPublicationRetired(t, m, s)
}
