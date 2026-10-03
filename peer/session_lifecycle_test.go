package peer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
)

// Close calls are counted at the socket boundary, including cancellation workers.
type cancellationPublicationConn struct {
	net.Conn
	closes      atomic.Int32
	closeSignal chan struct{}
	closeOnce   sync.Once
}

func (c *cancellationPublicationConn) Close() error {
	c.closes.Add(1)
	if c.closeSignal != nil {
		c.closeOnce.Do(func() { close(c.closeSignal) })
	}
	return c.Conn.Close()
}

func TestSessionCancellationPublicationInstallOrdering(t *testing.T) {
	for _, ordering := range []string{"close first", "install first", "overlap"} {
		t.Run(ordering, func(t *testing.T) {
			local, remote := net.Pipe()
			t.Cleanup(func() { _ = remote.Close() })
			conn := &cancellationPublicationConn{Conn: local}
			s := &session{conn: conn}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			var err error
			switch ordering {
			case "close first":
				s.close()
				err = s.installContext(ctx, cancel)
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("closed installation returned %v", err)
				}
			case "install first":
				err = s.installContext(ctx, cancel)
				if err != nil {
					t.Fatal(err)
				}
				s.close()
			case "overlap":
				start := make(chan struct{})
				var pending sync.WaitGroup
				pending.Add(2)
				go func() { defer pending.Done(); <-start; err = s.installContext(ctx, cancel) }()
				go func() { defer pending.Done(); <-start; s.close() }()
				close(start)
				pending.Wait()
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
			// Repeated callers must neither reopen cancellation nor close twice.
			var closers sync.WaitGroup
			for range 8 {
				closers.Add(1)
				go func() { defer closers.Done(); s.close() }()
			}
			closers.Wait()
			if ctx.Err() == nil || conn.closes.Load() != 1 {
				t.Fatalf("canceled=%v socket close calls=%d", ctx.Err(), conn.closes.Load())
			}
		})
	}
}

// Err gates the return from installation while the real operation's Done and
// cancellation remain intact. Manager.Stop must close this registered session
// before the reader is permitted to start any workers or handshake I/O.
type cancellationPublicationContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *cancellationPublicationContext) Err() error {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Context.Err()
}

func TestSessionCancellationPublicationManagerStop(t *testing.T) {
	for _, dir := range []direction{dirInbound, dirOutbound} {
		t.Run(fmt.Sprintf("direction=%d", dir), func(t *testing.T) {
			m, _ := newInboundHarnessManager(t, inboundScenario{name: t.Name()})
			local, remote := net.Pipe()
			t.Cleanup(func() { _ = remote.Close() })
			conn := &cancellationPublicationConn{Conn: local, closeSignal: make(chan struct{})}
			s := newSession(conn, dir, m, PeerEndpoint{}, m.sessionSettings(PeerEndpoint{}))
			s.operation = m.beginContextOperation()
			if s.operation == nil {
				t.Fatal("operation admission failed")
			}
			gate := &cancellationPublicationContext{Context: s.operation.ctx, entered: make(chan struct{}), release: make(chan struct{})}
			s.operation.ctx = gate
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate.release) }) }
			t.Cleanup(release)
			runDone, stopDone := make(chan error, 1), make(chan struct{})
			go func() { runDone <- s.Run() }()
			select {
			case <-gate.entered:
			case <-time.After(time.Second):
				t.Fatal("startup did not reach cancellation installation")
			}
			go func() { m.Stop(); close(stopDone) }()
			select {
			case <-conn.closeSignal:
			case <-time.After(3 * time.Second):
				t.Fatal("Stop did not close the published startup session")
			}
			release()
			select {
			case err := <-runDone:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled startup returned %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Run did not retire after shutdown")
			}
			select {
			case <-stopDone:
			case <-time.After(time.Second):
				t.Fatal("Stop did not join startup ownership")
			}
			if conn.closes.Load() != 1 {
				t.Fatalf("socket close calls=%d", conn.closes.Load())
			}
			assertCancellationPublicationRetired(t, m, s)
		})
	}
}

func assertCancellationPublicationRetired(t *testing.T, m *Manager, s *session) {
	t.Helper()
	m.mu.RLock()
	clean := m.candidates.Len() == 0 && m.ownedRuns.Len() == 0 && m.sessions.Len() == 0 &&
		len(m.pendingSlots) == 0 && len(m.ownerSlots) == 0 && !s.pendingReserved && !s.ownerReserved
	m.mu.RUnlock()
	if !clean || m.ContextOwnership().Active != 0 {
		t.Fatal("terminal session retained registry, context or transport ownership")
	}
}

// This reproducer uses only baseline Run/close entry points. The concurrent
// close must cancel the operation even when it wins before Run installs it.
func TestSessionCancellationPublicationRunCloseOverlap(t *testing.T) {
	for _, dir := range []direction{dirInbound, dirOutbound} {
		for i := 0; i < 4; i++ {
			t.Run(fmt.Sprintf("direction=%d/cycle=%d", dir, i), func(t *testing.T) {
				m, _ := newInboundHarnessManager(t, inboundScenario{name: t.Name()})
				local, remote := net.Pipe()
				t.Cleanup(func() { _ = remote.Close() })
				conn := &cancellationPublicationConn{Conn: local}
				s := newSession(conn, dir, m, PeerEndpoint{}, m.sessionSettings(PeerEndpoint{}))
				s.operation = m.beginContextOperation()
				if s.operation == nil {
					t.Fatal("operation admission failed")
				}
				start, closed := make(chan struct{}), make(chan struct{})
				done := make(chan error, 1)
				go func() { <-start; done <- s.Run() }()
				go func() { <-start; s.close(); close(closed) }()
				close(start)
				<-closed
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("closed startup succeeded")
					}
				case <-time.After(time.Second):
					// Retire the baseline's stranded cancellation watcher before failing.
					m.cancel()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Fatal("parent cancellation did not retire startup")
					}
					t.Fatal("concurrent close lost operation cancellation")
				}
				if s.operation.ctx.Err() == nil || conn.closes.Load() != 1 {
					t.Fatalf("canceled=%v socket close calls=%d", s.operation.ctx.Err(), conn.closes.Load())
				}
				assertCancellationPublicationRetired(t, m, s)
			})
		}
	}
}

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
	cancel := func() { manager.cancel() }
	done := make(chan error, 1)
	go func() { done <- s.Run() }()
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
