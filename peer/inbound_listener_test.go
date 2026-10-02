package peer

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

func TestInboundListenerRejectsPeerIPBeforePC18(t *testing.T) {
	manager, err := NewManager(completeProtocolTestConfig(config.PeeringConfig{
		NodeVersion:    "5457",
		LegacyVersion:  "5457",
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
		Peers: []config.PeeringPeer{{
			Enabled:        true,
			Direction:      config.PeeringPeerDirectionInbound,
			Family:         config.PeeringPeerFamilyCCluster,
			RemoteCallsign: "N1REM",
			AllowIPs:       []string{"203.0.113.0/24"},
		}},
	}, "N0CALL"), "N0CALL", nil, 0, nil)
	if err != nil {
		t.Fatalf("NewManager() error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	manager.ctx = ctx
	manager.listener = ln

	done := make(chan struct{})
	go func() {
		manager.acceptLoop()
		close(done)
	}()
	defer func() {
		cancel()
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for accept loop shutdown")
		}
	}()

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	reader := NewLineReader(conn, 4096, 4096, nil)
	writer := bufio.NewWriter(conn)

	line, err := reader.ReadLine(time.Now().UTC().Add(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("read login prompt: %v", err)
	}
	if line != "login:" {
		t.Fatalf("expected login prompt, got %q", line)
	}

	if _, err := writer.WriteString("N1REM\r\n"); err != nil {
		t.Fatalf("write callsign: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush callsign: %v", err)
	}

	line, err = reader.ReadLine(time.Now().UTC().Add(500 * time.Millisecond))
	if err == nil {
		t.Fatalf("expected connection close before PC18, got %q", line)
	}
	var netErr net.Error
	if !errors.Is(err, io.EOF) && (!errors.As(err, &netErr) || !netErr.Timeout()) && !strings.Contains(strings.ToLower(err.Error()), "closed") && !strings.Contains(strings.ToLower(err.Error()), "reset") {
		t.Fatalf("expected connection close/timeout after blocked peer IP, got %v", err)
	}

	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if manager.sessions.Len() != 0 {
		t.Fatalf("expected no registered sessions, got %d", manager.sessions.Len())
	}
}
