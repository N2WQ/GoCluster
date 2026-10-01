package peer

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func newTransportTestSession(t *testing.T) (*session, net.Conn) {
	t.Helper()
	local, remote := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	s := &session{conn: local, writer: bufio.NewWriter(local), ctx: ctx, cancel: cancel, localCall: "N0CALL", remoteCall: "N1REM", nodeVersion: "5457", remoteVersion: "5457", pc92Bitmap: 5, hopCount: 99,
		writeCh: make(chan string, 8), priorityLineCh: make(chan string, defaultPriorityQueue), priorityRawCh: make(chan []byte, defaultPriorityQueue)}
	t.Cleanup(func() { s.close(); remote.Close(); s.workers.Wait() })
	return s, remote
}

func readSessionWire(t *testing.T, r *bufio.Reader, conn net.Conn) string {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimRight(line, "\r\n")
}

func TestPC92WireOrderUsesSingleControlLane(t *testing.T) {
	s, remote := newTransportTestSession(t)
	if err := s.sendLine("spot backlog"); err != nil {
		t.Fatal(err)
	}
	frames := []string{"PC92^N0CALL^100^A^^5N1REM:5457^H99^", "PC92^N0CALL^100.01^C^5N0CALL:5457^H99^", "PC92^N0CALL^100.02^K^5N0CALL:5457^1^0^H99^"}
	for _, line := range frames {
		if err := s.sendLine(line); err != nil {
			t.Fatal(err)
		}
	}
	s.startWorker(s.writerLoop)
	reader := bufio.NewReader(remote)
	for _, want := range append(frames, "spot backlog") {
		if got := readSessionWire(t, reader, remote); got != want {
			t.Fatalf("wire=%q want=%q", got, want)
		}
	}
}

func TestSessionQueueByteBoundsAndActiveCharge(t *testing.T) {
	t.Run("data refuses without closure", func(t *testing.T) {
		s, _ := newTransportTestSession(t)
		s.writeCh = make(chan string, 128)
		fillSessionQueueBytes(t, s, false)
		if err := s.sendLine("x"); !errors.Is(err, errSessionWriteQueueFull) {
			t.Fatalf("overflow=%v", err)
		}
		if s.ctx.Err() != nil {
			t.Fatal("data overflow closed session")
		}
	})
	t.Run("control overflow closes", func(t *testing.T) {
		s, _ := newTransportTestSession(t)
		fillSessionQueueBytes(t, s, true)
		if err := s.sendControlLine("x"); !errors.Is(err, errSessionPriorityQueueFull) {
			t.Fatalf("overflow=%v", err)
		}
		if s.ctx.Err() == nil {
			t.Fatal("control overflow did not close")
		}
	})
	t.Run("control active write has separate bounded charge", func(t *testing.T) {
		s, _ := newTransportTestSession(t)
		line := strings.Repeat("x", MaxPeerFrameBytes)
		if err := s.sendControlLine(line); err != nil {
			t.Fatal(err)
		}
		s.startWorker(s.writerLoop)
		waitSessionActive(t, s, true)
		fillSessionQueueBytes(t, s, true)
		s.queueMu.Lock()
		queued, active := s.controlBytes, s.activeBytes
		s.queueMu.Unlock()
		if queued != peerQueueBytes || active != MaxPeerFrameBytes+2 {
			t.Fatalf("queued=%d active=%d", queued, active)
		}
		if err := s.sendControlLine("next"); !errors.Is(err, errSessionPriorityQueueFull) {
			t.Fatalf("queued byte limit bypassed: %v", err)
		}
	})
	t.Run("data active write has separate bounded charge", func(t *testing.T) {
		s, _ := newTransportTestSession(t)
		s.writeCh = make(chan string, 128)
		if err := s.sendLine(strings.Repeat("x", MaxPeerFrameBytes)); err != nil {
			t.Fatal(err)
		}
		s.startWorker(s.writerLoop)
		waitSessionActive(t, s, false)
		fillSessionQueueBytes(t, s, false)
		fillSessionQueueBytes(t, s, true)
		s.queueMu.Lock()
		data, control, active := s.dataBytes, s.controlBytes, s.activeBytes
		s.queueMu.Unlock()
		if data != peerQueueBytes || control != peerQueueBytes || active != MaxPeerFrameBytes+2 {
			t.Fatalf("data=%d control=%d shared active=%d", data, control, active)
		}
		if err := s.sendLine("next"); !errors.Is(err, errSessionWriteQueueFull) || s.ctx.Err() != nil {
			t.Fatalf("data overflow behavior=%v context=%v", err, s.ctx.Err())
		}
	})
	t.Run("128 controls plus one active", func(t *testing.T) {
		s, _ := newTransportTestSession(t)
		if err := s.sendControlLine("active"); err != nil {
			t.Fatal(err)
		}
		s.startWorker(s.writerLoop)
		waitSessionActive(t, s, true)
		for i := 0; i < defaultPriorityQueue; i++ {
			if err := s.sendControlLine("queued"); err != nil {
				t.Fatalf("queued record %d refused: %v", i+1, err)
			}
		}
		if err := s.sendControlLine("overflow"); !errors.Is(err, errSessionPriorityQueueFull) {
			t.Fatalf("queued count overflow=%v", err)
		}
	})
}

func fillSessionQueueBytes(t *testing.T, s *session, control bool) {
	t.Helper()
	// Independent Go allocation boundaries. Include the fixed channel backing:
	// 128 string slots reserve 2,560 bytes; the two control channels reserve
	// 6,016 bytes. Fill remaining payload space exactly, including CRLF charges.
	fixed := 2560
	if control {
		fixed = 6016
	}
	sizes := []int{65536, 57344, 49152, 40960, 32768, 16384, 8192, 4096, 2048, 1024, 512, 256, 128, 64, 32, 16, 8, 0}
	remaining := peerQueueBytes - fixed
	for remaining > 0 {
		size := 0
		for _, candidate := range sizes {
			if candidate+2 <= remaining {
				size = candidate
				break
			}
		}
		line := strings.Repeat("x", size)
		var err error
		if control {
			err = s.sendControlLine(line)
		} else {
			err = s.sendLine(line)
		}
		if err != nil {
			t.Fatalf("queue refused with %d bytes remaining: %v", remaining, err)
		}
		remaining -= size + 2
	}
	if remaining != 0 {
		t.Fatalf("invalid exact-byte fixture: %d unfilled", remaining)
	}
}

func waitSessionActive(t *testing.T, s *session, control bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.queueMu.Lock()
		active, activeControl := s.activeBytes, s.activeControl
		s.queueMu.Unlock()
		if active > 0 && activeControl == control {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("writer did not own the expected active write")
}

func TestSessionOutputFrameBounds(t *testing.T) {
	for _, control := range []bool{false, true} {
		s, _ := newTransportTestSession(t)
		line := strings.Repeat("x", MaxPeerFrameBytes+1)
		var err error
		if control {
			err = s.sendControlLine(line)
		} else {
			err = s.sendLine(line)
		}
		if err == nil || len(s.writeCh)+len(s.priorityLineCh) != 0 || (s.ctx.Err() != nil) != control {
			t.Fatalf("oversized output control=%v error=%v context=%v", control, err, s.ctx.Err())
		}
	}
	s, _ := newTransportTestSession(t)
	if s.sendPriorityRaw([]byte{255, 252, 1, 0}) || s.ctx.Err() == nil || len(s.priorityRawCh) != 0 {
		t.Fatal("oversized Telnet reply was retained")
	}
}

func TestSessionControlQueueAgeCloses(t *testing.T) {
	s, _ := newTransportTestSession(t)
	if err := s.sendControlLine("PC51^N1REM^N0CALL^1^"); err != nil {
		t.Fatal(err)
	}
	s.queueMu.Lock()
	s.lineTimes.epoch = time.Now().Add(-peerControlMaxAge - time.Millisecond)
	s.queueMu.Unlock()
	s.startWorker(s.controlAgeLoop)
	select {
	case <-s.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("expired control did not close session")
	}
}

func TestSessionRawQueueOwnsBytes(t *testing.T) {
	s, remote := newTransportTestSession(t)
	raw := []byte{255, 252, 1}
	if !s.sendPriorityRaw(raw) {
		t.Fatal("enqueue failed")
	}
	raw[2] = 42
	s.startWorker(s.writerLoop)
	if err := remote.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 3)
	if _, err := remote.Read(got); err != nil {
		t.Fatal(err)
	}
	if got[2] != 1 {
		t.Fatalf("writer retained caller buffer: %v", got)
	}
}

func TestStartupPC92CannotEstablishFromDroppedRecord(t *testing.T) {
	s := &session{pc9x: true, localCall: "N0CALL"}
	for _, wire := range []string{
		"PC92^N1REM^100^C^5N1REM:5457^H0^",
		"PC92^N0CALL^100^C^5N0CALL:5457^H99^",
		"PC92^N1REM^100^F^5N1REM:5457^H99^",
	} {
		frame, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		accepted, err := s.stageStartupPC92(frame)
		if accepted || err != nil {
			t.Fatalf("dropped record became startup authority: %q accepted=%v err=%v", wire, accepted, err)
		}
	}
}

func TestPC18CapabilityWordBoundaries(t *testing.T) {
	for _, tc := range []struct {
		banner string
		want   bool
	}{
		{"DXSpider Version: 1.57 [pc9x 91]", true},
		{"DXSpider Version: 1.57 (PC9X)", true},
		{"DXSpider Version: 1.57 pc9x,", true},
		{"GoCluster Version: mypc9xbuild", false},
		{"DXSpider Version: 1.57 pc9x_unsupported", true},
	} {
		if got := bannerHasPC9x(tc.banner); got != tc.want {
			t.Fatalf("capability %q=%v want%v", tc.banner, got, tc.want)
		}
	}
}
