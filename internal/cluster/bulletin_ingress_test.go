package cluster

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/bulletinhistory"
	"dxcluster/config"
	"dxcluster/filter"
	"dxcluster/peer"
	"dxcluster/rbn"
)

// This exercises both runtime ingress adapters without an eligible telnet
// client. A direct history insert would not catch a disconnected callback or
// a return before archive admission in the live fanout path.
func TestBulletinRuntimeIngressWithoutClients(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	host, port := splitRuntimeTestAddress(t, listener.Addr().String())
	// Hold the selected client port until just before production Server.Start.
	// The tiny close/rebind interval is inherent in the public startup API.
	clientPort, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clientPort.Close() })
	cfg := peerRuntimeTestConfig()
	cfg.Telnet.Port = clientPort.Addr().(*net.TCPAddr).Port
	cfg.Telnet.LoginPrompt = "login:\n"
	cfg.Telnet.LoginGreeting = "ready\n"
	cfg.Telnet.SkipHandshake = config.TelnetHandshakeNone
	cfg.Telnet.EchoMode = "local"
	cfg.HumanTelnet = config.HumanTelnetRegistry{{
		Enabled: true, Name: "TEST", Host: host, Port: port, Callsign: "N0CALL-1",
		TelnetTransport: config.TelnetTransportNative, SlotBuffer: 4,
	}}
	r := newClusterRuntime(peerRuntimeTestBuild(), cfg, "", config.LoadDiagnostics{})
	r.ctx, r.cancel = context.WithCancel(t.Context())
	r.bulletinHistory = bulletinhistory.Open(filepath.Join(t.TempDir(), "bulletins.json"), 10*time.Minute, 64)
	originalUserDir := filter.UserDataDir
	filter.UserDataDir = t.TempDir()
	t.Cleanup(func() { filter.UserDataDir = originalUserDir })
	t.Cleanup(func() {
		r.stopHumanTelnetFeeds()
		if r.peerManager != nil {
			r.peerManager.Stop()
		}
		if r.telnetServer != nil {
			r.telnetServer.Stop()
		}
		if err := r.bulletinHistory.Close(); err != nil {
			t.Errorf("close history: %v", err)
		}
		r.cancel()
	})
	if !r.initializePeerManager() {
		t.Fatalf("peer manager: %v", r.startupErr)
	}
	if !r.initializeTelnetServer() {
		t.Fatal("telnet initialization failed")
	}
	if got := r.telnetServer.ListClientCallsigns(); len(got) != 0 {
		t.Fatalf("fixture unexpectedly has clients: %v", got)
	}
	r.connectHumanTelnetFeeds()
	upstream := acceptRuntimeTestConnection(t, listener, 0)
	t.Cleanup(func() { _ = upstream.Close() })
	beforeHuman := time.Now().UTC()
	if _, err := upstream.Write([]byte("WWV de HUMAN <1200> : SFI=123 A=4 K=2\r\n")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for r.bulletinHistory.Stats().WWVRecords != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := r.bulletinHistory.Render("WWV", true, time.Now().UTC()); !strings.Contains(got, "WWV de HUMAN <1200> : SFI=123 A=4 K=2") {
		t.Fatalf("human source did not reach history: %q", got)
	} else {
		assertBulletinReceiptWindow(t, got, beforeHuman, time.Now().UTC())
	}
	frame, err := peer.ParseFrame("PC73^19-Apr-2026^1200Z^124^3^2^4^5^SA^GMF^AUR^W1AW^NODE^H95^")
	if err != nil {
		t.Fatal(err)
	}
	beforePeer := time.Now().UTC()
	r.peerManager.HandleFrame(frame, nil)
	afterPeer := time.Now().UTC()
	got := r.bulletinHistory.Render("WCY", true, afterPeer)
	if !strings.Contains(got, "WCY de W1AW <1200Z>") || !strings.Contains(got, "expK=4") {
		t.Fatalf("peer source did not reach history: %q", got)
	}
	assertBulletinReceiptWindow(t, got, beforePeer, afterPeer)

	if err := clientPort.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.telnetServer.Start(); err != nil {
		t.Fatalf("start real telnet server on reserved port: %v", err)
	}
	client, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", cfg.Telnet.Port))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		deadline := time.Now().Add(5 * time.Second)
		for len(r.telnetServer.ListClientCallsigns()) != 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if clients := r.telnetServer.ListClientCallsigns(); len(clients) != 0 {
			t.Errorf("telnet client did not retire: %v", clients)
		}
	})
	if err := client.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(client)
	if _, err := fmt.Fprint(client, "DL1TEST\r\n"); err != nil {
		t.Fatal(err)
	}
	readRuntimeBulletinLines(t, reader, "WWV de HUMAN <1200>", "WCY de W1AW <1200Z>")
	for _, item := range []struct{ command, want string }{
		{"SHOW WWV", "WWV de HUMAN <1200>"},
		{"SHOW WCY", "WCY de W1AW <1200Z>"},
		{"SHOW WWV FULL", "WWV de HUMAN <1200>"},
		{"SHOW WCY FULL", "WCY de W1AW <1200Z>"},
	} {
		if _, err := fmt.Fprintf(client, "%s\r\n", item.command); err != nil {
			t.Fatal(err)
		}
		readRuntimeBulletinLines(t, reader, item.want)
	}
}

// Each lookup reads actual client wire lines and has an upper byte limit; it
// does not mistake a buffered pre-login response for a later command result.
func readRuntimeBulletinLines(t *testing.T, reader *bufio.Reader, tokens ...string) {
	t.Helper()
	remaining := make(map[string]bool, len(tokens))
	for _, token := range tokens {
		remaining[token] = true
	}
	var transcript strings.Builder
	for len(remaining) != 0 && transcript.Len() < 65536 {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read bulletin %v: %v, transcript=%q", remaining, err, transcript.String())
		}
		transcript.WriteString(line)
		for token := range remaining {
			if strings.Contains(line, token) {
				delete(remaining, token)
			}
		}
	}
	if len(remaining) != 0 {
		t.Fatalf("missing bulletin %v, transcript=%q", remaining, transcript.String())
	}
}

// A queued human line must retain the reader's earlier receipt time even if
// the shared passthrough worker starts later.
func TestBulletinHumanPassthroughPreservesQueuedReceipt(t *testing.T) {
	r := &clusterRuntime{cfg: &config.Config{}}
	r.bulletinHistory = bulletinhistory.Open(filepath.Join(t.TempDir(), "bulletins.json"), 0, 0)
	if !r.initializeTelnetServer() {
		t.Fatal("telnet initialization failed")
	}
	t.Cleanup(func() {
		r.telnetServer.Stop()
		if err := r.bulletinHistory.Close(); err != nil {
			t.Errorf("close history: %v", err)
		}
	})
	received := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Second)
	queued := make(chan rbn.RawLine, 1)
	queued <- rbn.RawLine{Line: "WCY de DELAYED : K=3", ReceivedAt: received}
	close(queued)
	r.forwardHumanPassthrough(queued)
	got := r.bulletinHistory.Render("WCY", true, time.Now().UTC())
	if !strings.Contains(got, "WCY de DELAYED : K=3") {
		t.Fatalf("queued bulletin missing: %q", got)
	}
	assertBulletinReceiptWindow(t, got, received, received)
}

func assertBulletinReceiptWindow(t *testing.T, rendered string, start, end time.Time) {
	t.Helper()
	const prefix = "received "
	i := strings.Index(rendered, prefix)
	if i < 0 || len(rendered) < i+len(prefix)+len("2006-01-02 15:04:05") {
		t.Fatalf("missing receipt timestamp: %q", rendered)
	}
	text := rendered[i+len(prefix) : i+len(prefix)+len("2006-01-02 15:04:05")]
	at, err := time.ParseInLocation("2006-01-02 15:04:05", text, time.UTC)
	if err != nil {
		t.Fatalf("invalid receipt timestamp %q: %v", text, err)
	}
	if at.Before(start.Truncate(time.Second)) || at.After(end.Truncate(time.Second)) {
		t.Fatalf("receipt %v outside source window [%v,%v]; output %q", at, start, end, rendered)
	}
}
