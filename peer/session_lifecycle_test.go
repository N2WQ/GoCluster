package peer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

func outboundHarness(t *testing.T) (*session, *Manager, net.Conn, chan error, context.CancelFunc) {
	t.Helper()
	manager, _ := newInboundHarnessManager(t, inboundScenario{name: t.Name()})
	local, remote := net.Pipe()
	endpoint := PeerEndpoint{host: "pipe", remoteCall: "N1REM", family: config.PeeringPeerFamilyDXSpider, preferPC9x: true}
	settings := manager.sessionSettings(endpoint)
	settings.loginTimeout = 100 * time.Millisecond
	settings.initTimeout = 150 * time.Millisecond
	settings.idleTimeout = 0
	s := newSession(local, dirOutbound, manager, endpoint, settings)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() { cancel(); _ = remote.Close() })
	return s, manager, remote, done, cancel
}

func writeSessionWire(t *testing.T, conn net.Conn, line string) {
	t.Helper()
	if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "%s\r\n", line); err != nil {
		t.Fatal(err)
	}
}

func readOutboundInit(t *testing.T, remote net.Conn, reader *bufio.Reader) {
	t.Helper()
	if got := readSessionWire(t, reader, remote); got != "N0CALL" {
		t.Fatalf("login=%q", got)
	}
	writeSessionWire(t, remote, "PC18^DXSpider Version: 1.57 Build: 633 [pc9x 91]^5457^")
	for _, action := range []string{"A", "K"} {
		line := readSessionWire(t, reader, remote)
		if !pc92TypeLine(action).match(line) {
			t.Fatalf("want initial %s, got %q", action, line)
		}
	}
	if got := readSessionWire(t, reader, remote); got != "PC20^" {
		t.Fatalf("completion request=%q", got)
	}
}

func TestOutboundHandshakeRequiresPC22(t *testing.T) {
	s, manager, remote, done, _ := outboundHarness(t)
	reader := bufio.NewReader(remote)
	readOutboundInit(t, remote, reader)
	writeSessionWire(t, remote, currentStartupPC92())
	writeSessionWire(t, remote, "PC61^14074.0^K1ABC^01-Oct-2026^1200Z^CQ^W1ABC^N1REM^127.0.0.1^H9^")
	select {
	case err := <-done:
		if msg := errIsTimeout(err); msg != "" {
			t.Fatal(msg)
		}
	case <-time.After(time.Second):
		t.Fatal("startup traffic extended handshake deadline")
	}
	if s.established || manager.ActiveSessionCount() != 0 {
		t.Fatal("spot granted establishment authority")
	}
	manager.mu.RLock()
	records, bytes, candidates := manager.stagedRecords, manager.stagedBytes, manager.candidates.Len()
	manager.mu.RUnlock()
	if records != 0 || bytes != 0 || candidates != 0 {
		t.Fatalf("failed candidate retained staging: records=%d bytes=%d candidates=%d", records, bytes, candidates)
	}
}

func TestOutboundHandshakePC22CompletesAndRecovers(t *testing.T) {
	s, _, remote, done, cancel := outboundHarness(t)
	reader := bufio.NewReader(remote)
	readOutboundInit(t, remote, reader)
	// Repeated PC18 cannot emit another initial exchange or renegotiate capability.
	writeSessionWire(t, remote, "PC18^DXSpider Version: 1.57^5457^")
	writeSessionWire(t, remote, currentStartupPC92())
	writeSessionWire(t, remote, "PC22^")
	for _, action := range []string{"C", "A"} {
		got := readSessionWire(t, reader, remote)
		if !pc92TypeLine(action).match(got) {
			t.Fatalf("want recovery %s, got %q", action, got)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not join session workers")
	}
	if !s.established {
		t.Fatal("PC22 did not establish")
	}
}

func TestSessionCancellationInterruptsReadAndJoinsWorkers(t *testing.T) {
	for i := 0; i < 8; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			_, manager, remote, done, cancel := outboundHarness(t)
			if got := readSessionWire(t, bufio.NewReader(remote), remote); got != "N0CALL" {
				t.Fatalf("login=%q", got)
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("canceled read did not return with workers joined")
			}
			manager.mu.RLock()
			count := manager.candidates.Len()
			manager.mu.RUnlock()
			if count != 0 {
				t.Fatalf("candidate survived cancellation: %d", count)
			}
		})
	}
}

func TestSessionTerminalRunReleasesQueuedPayloads(t *testing.T) {
	s, manager, remote, done, cancel := outboundHarness(t)
	reader := bufio.NewReader(remote)
	readOutboundInit(t, remote, reader)
	writeSessionWire(t, remote, "PC22^")
	for _, action := range []string{"C", "A"} {
		if wire := readSessionWire(t, reader, remote); !pc92TypeLine(action).match(wire) {
			t.Fatalf("want recovery %s, got %q", action, wire)
		}
	}
	// No further reads: the writer owns one blocked control record while each
	// queued lane contains payloads that older controller work could retain.
	if err := s.sendControlLine(strings.Repeat("x", MaxPeerFrameBytes)); err != nil {
		t.Fatal(err)
	}
	waitSessionActive(t, s, true)
	if err := s.sendControlLine("queued control"); err != nil {
		t.Fatal(err)
	}
	if err := s.sendLine(strings.Repeat("y", MaxPeerFrameBytes)); err != nil {
		t.Fatal(err)
	}
	if !s.sendPriorityRaw([]byte{255, 252, 1}) {
		t.Fatal("Telnet reply enqueue failed")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("terminal Run did not join its blocked writer")
	}
	s.queueMu.Lock()
	clean := s.dataBytes == 0 && s.controlBytes == 0 && s.controlCount == 0 && s.activeBytes == 0 && !s.activeControl && s.lineTimes.count == 0 && s.rawTimes.count == 0
	s.queueMu.Unlock()
	if !clean || len(s.writeCh)+len(s.priorityLineCh)+len(s.priorityRawCh) != 0 {
		t.Fatal("terminal Run retained queued payload references or active/queued charges")
	}
	if s.writeCh != nil || s.priorityLineCh != nil || s.priorityRawCh != nil {
		t.Fatal("terminal Run retained empty channel backing through its stale identity")
	}
	if s.reader.buf != nil || s.reader.readBuf != nil || s.reader.readFn != nil || s.reader.replyFn != nil || s.writer != nil {
		t.Fatal("terminal Run retained reader/writer backing through its stale identity")
	}
	if s.remoteVersion != "" || s.remoteBuild != "" {
		t.Fatal("terminal Run retained remote metadata through its stale identity")
	}
	if !errors.Is(s.sendLine("late data"), context.Canceled) || !errors.Is(s.sendControlLine("late control"), context.Canceled) || s.sendPriorityRaw([]byte{255, 252, 1}) {
		t.Fatal("terminal session admitted output after its final drain")
	}
	manager.mu.RLock()
	owned := manager.ownedRuns.Value(s)
	manager.mu.RUnlock()
	if owned {
		t.Fatal("terminal Run retained manager ownership")
	}
}
