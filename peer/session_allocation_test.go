package peer

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"unsafe"
)

func TestSessionInitialPasswordBorrowsOnlyConfiguredStorage(t *testing.T) {
	s, _ := allocationSession(t)
	s.password = strings.Repeat("p", MaxPeerFrameBytes)
	if err := s.sendInitialPassword(); err != nil {
		t.Fatal(err)
	}
	wire := <-s.priorityLineCh
	if unsafe.StringData(wire) != unsafe.StringData(s.password) {
		t.Fatal("initial password duplicated immutable configuration backing")
	}
	if s.controlCount != 1 || s.controlBytes != 6016+65536+2 {
		t.Fatal("borrowed initial password bypassed logical control queue admission")
	}
	if err := s.sendControlLine(s.password); err != nil {
		t.Fatal(err)
	}
	untrusted := <-s.priorityLineCh
	if unsafe.StringData(untrusted) == unsafe.StringData(s.password) {
		t.Fatal("ordinary control publication borrowed potentially unowned input")
	}
}

func allocationSession(t *testing.T) (*session, net.Conn) {
	t.Helper()
	local, remote := net.Pipe()
	s := newSession(local, dirInbound, nil, PeerEndpoint{remoteCall: "W1AAA"}, sessionSettings{writeQueue: int(^uint(0) >> 1)})
	s.ctx, s.cancel = context.WithCancel(context.Background())
	t.Cleanup(func() { s.close(); _ = remote.Close(); s.workers.Wait() })
	return s, remote
}

func TestSessionNormalBackingRequiresRegistryWinner(t *testing.T) {
	m := &Manager{sessions: newFixedIndex[string, *session](64)}
	winner, _ := allocationSession(t)
	loser, _ := allocationSession(t)
	if !errors.Is(winner.sendLine("before establishment"), errSessionWriteQueueFull) || winner.writeCh != nil || winner.dataBytes != 0 {
		t.Fatal("candidate admitted normal output or allocated its backing")
	}
	if err := m.registerSession(winner); err != nil {
		t.Fatal(err)
	}
	if err := m.registerSession(loser); err == nil || loser.writeCh != nil || loser.dataBytes != 0 {
		t.Fatal("duplicate candidate acquired normal-lane backing")
	}
	if winner.writeCh == nil || winner.dataBytes > peerQueueBytes {
		t.Fatal("registry winner lacks its bounded normal lane")
	}
	winner.close()
	winner.discardQueuedOutput()
	m.unregisterSession(winner)
	if err := m.registerSession(loser); err != nil {
		t.Fatal(err)
	}
	if winner.writeCh != nil || winner.dataBytes != 0 || loser.writeCh == nil {
		t.Fatal("replacement retained normal backing through the stale owner")
	}
}

func TestSessionNormalActivationPublishesToWriterAndJoins(t *testing.T) {
	s, remote := allocationSession(t)
	// Use an ordinary capacity to leave payload headroom for the wire check.
	s.normalCapacity = 128
	s.startWorker(s.writerLoop)
	reader := bufio.NewReader(remote)
	if err := s.sendControlLine("startup"); err != nil {
		t.Fatal(err)
	}
	if got := readSessionWire(t, reader, remote); got != "startup" {
		t.Fatalf("pre-establishment control=%q", got)
	}
	if err := s.activateNormalQueue(); err != nil {
		t.Fatal(err)
	}
	if err := s.sendLine("normal"); err != nil {
		t.Fatal(err)
	}
	if got := readSessionWire(t, reader, remote); got != "normal" {
		t.Fatalf("post-activation output=%q", got)
	}
	s.close()
	s.workers.Wait()
	s.discardQueuedOutput()
	if s.writeCh != nil || s.dataBytes != 0 {
		t.Fatal("joined writer retained activated channel backing")
	}
}

func TestSessionNormalActivationCancellationRace(t *testing.T) {
	for range 32 {
		s, _ := allocationSession(t)
		s.startWorker(s.writerLoop)
		var pending sync.WaitGroup
		pending.Add(2)
		go func() { defer pending.Done(); _ = s.activateNormalQueue() }()
		go func() { defer pending.Done(); s.close() }()
		pending.Wait()
		s.workers.Wait()
		s.discardQueuedOutput()
		if s.writeCh != nil || s.dataBytes != 0 || s.controlBytes != 0 {
			t.Fatal("activation/cancellation race retained output backing")
		}
	}
}
