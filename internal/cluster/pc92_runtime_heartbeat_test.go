//go:build qualification

package cluster

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

// The connection is driven synchronously by the fixture's existing reader.
// Tests replace only I/O outcomes; no production clock or liveness is changed.
type qualificationHeartbeatConn struct {
	net.Conn
	read  func([]byte) (int, error)
	write func([]byte) (int, error)
}

func (c *qualificationHeartbeatConn) Read(p []byte) (int, error)     { return c.read(p) }
func (c *qualificationHeartbeatConn) Write(p []byte) (int, error)    { return c.write(p) }
func (*qualificationHeartbeatConn) SetReadDeadline(time.Time) error  { return nil }
func (*qualificationHeartbeatConn) SetWriteDeadline(time.Time) error { return nil }

func qualificationHeartbeatSocket(t *testing.T) (*qualificationSocket, *bytes.Buffer, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	var wire bytes.Buffer
	conn := &qualificationHeartbeatConn{
		read:  func([]byte) (int, error) { return 0, io.EOF },
		write: wire.Write,
	}
	cfg := &config.Config{}
	cfg.Peering.LocalCallsign = "N0CALL-1"
	cfg.Peering.Peers = []config.PeeringPeer{{RemoteCallsign: "GB7DXC"}}
	d := &qualificationDriver{ctx: ctx, cfg: cfg, oracle: newQualificationOracle(1, 0, 1, 1)}
	return newQualificationSocket(d, conn, bufio.NewReader(conn), d.oracle.peers[0]), &wire, cancel
}

func TestQualificationHeartbeatBoundariesAndWire(t *testing.T) {
	before := time.Now()
	s, wire, _ := qualificationHeartbeatSocket(t)
	after := time.Now()
	if s.nextPing.Before(before.Add(300*time.Second)) || s.nextPing.After(after.Add(300*time.Second)) {
		t.Fatal("initial ping must wait the pinned 300-second interval")
	}
	due := s.nextPing
	if err := s.pingIfDue(due.Add(-time.Nanosecond)); err != nil || wire.Len() != 0 {
		t.Fatalf("early ping: %q, %v", wire.String(), err)
	}
	if err := s.pingIfDue(due); err != nil || wire.String() != "PC51^N0CALL-1^GB7DXC^1^\r\n" {
		t.Fatalf("boundary wire: %q, %v", wire.String(), err)
	}
	wire.Reset()
	late := due.Add(1500 * time.Second)
	for range 3 {
		if err := s.pingIfDue(late); err != nil {
			t.Fatal(err)
		}
	}
	if wire.String() != "PC51^N0CALL-1^GB7DXC^1^\r\n" || !s.nextPing.Equal(late.Add(300*time.Second)) {
		t.Fatalf("delayed service must send once and rearm from now: %q next=%v", wire.String(), s.nextPing)
	}
	wire.Reset()
	s.observe([]byte("PC51^GB7DXC^N0CALL-1^1^"), due)
	if wire.String() != "PC51^N0CALL-1^GB7DXC^0^\r\n" {
		t.Fatalf("existing ping response changed: %q", wire.String())
	}
	wire.Reset()
	s.observe([]byte("PC51^GB7DXC^N0CALL-1^0^"), due)
	if wire.Len() != 0 {
		t.Fatalf("ping response generated a response loop: %q", wire.String())
	}
}

func TestQualificationHeartbeatSuppressedAtLifecycleBoundaries(t *testing.T) {
	for _, state := range []string{"paused", "expected-close", "canceled", "closing", "client"} {
		t.Run(state, func(t *testing.T) {
			s, wire, cancel := qualificationHeartbeatSocket(t)
			switch state {
			case "paused":
				s.paused.Store(true)
			case "expected-close":
				s.expectedClose.Store(true)
			case "canceled":
				cancel()
			case "closing":
				s.driver.oracle.closing.Store(true)
			case "client":
				s.row.peer = false
			}
			due := s.nextPing
			if err := s.pingIfDue(due.Add(time.Hour)); err != nil || wire.Len() != 0 || !s.nextPing.Equal(due) {
				t.Fatalf("suppressed heartbeat changed wire or schedule: wire=%q err=%v", wire.String(), err)
			}
		})
	}
}

func TestQualificationHeartbeatBusyReaderServicesDuePing(t *testing.T) {
	s, wire, cancel := qualificationHeartbeatSocket(t)
	conn := s.conn.(*qualificationHeartbeatConn)
	start := time.Now()
	s.nextPing = start.Add(20 * time.Millisecond)
	reads := 0
	conn.read = func(p []byte) (int, error) {
		reads++
		if time.Since(start) > time.Second {
			return 0, errors.New("busy reader never serviced heartbeat")
		}
		time.Sleep(time.Millisecond)
		return copy(p, "PC50^GB7DXC^0^H99^\r\n"), nil
	}
	conn.write = func(p []byte) (int, error) {
		if time.Now().Before(s.nextPing) {
			t.Error("busy reader sent an early heartbeat")
		}
		cancel()
		return wire.Write(p)
	}
	s.read()
	if reads == 0 || wire.String() != "PC51^N0CALL-1^GB7DXC^1^\r\n" || s.driver.oracle.failures.Load() != 0 {
		t.Fatalf("busy-read heartbeat: reads=%d wire=%q failures=%v", reads, wire.String(), s.driver.oracle.examples)
	}
	select {
	case <-s.done:
	default:
		t.Fatal("canceled reader did not close done")
	}
}

func TestQualificationHeartbeatWriteFailureAndPausedCancellation(t *testing.T) {
	t.Run("cancel-during-write", func(t *testing.T) {
		s, _, cancel := qualificationHeartbeatSocket(t)
		s.nextPing = time.Now().Add(-time.Second)
		s.conn.(*qualificationHeartbeatConn).write = func([]byte) (int, error) {
			cancel()
			return 0, net.ErrClosed
		}
		s.read()
		if s.driver.oracle.failures.Load() != 0 {
			t.Fatalf("canceled write became an unexpected failure: %v", s.driver.oracle.examples)
		}
		select {
		case <-s.done:
		default:
			t.Fatal("canceled write did not close done")
		}
	})
	t.Run("write-failure", func(t *testing.T) {
		s, _, _ := qualificationHeartbeatSocket(t)
		s.nextPing = time.Now().Add(-time.Second)
		s.conn.(*qualificationHeartbeatConn).write = func([]byte) (int, error) { return 0, errors.New("heartbeat write rejected") }
		s.read()
		if s.driver.oracle.failures.Load() != 1 || !strings.Contains(s.driver.oracle.examples[0], "heartbeat write rejected") {
			t.Fatalf("heartbeat write failure was hidden: %v", s.driver.oracle.examples)
		}
		select {
		case <-s.done:
		default:
			t.Fatal("failed reader did not close done")
		}
	})
	t.Run("paused-cancellation", func(t *testing.T) {
		s, wire, cancel := qualificationHeartbeatSocket(t)
		s.nextPing = time.Now().Add(-time.Second)
		s.paused.Store(true)
		go s.read()
		cancel()
		select {
		case <-s.done:
		case <-time.After(time.Second):
			t.Fatal("paused reader did not join on cancellation")
		}
		if wire.Len() != 0 || s.driver.oracle.failures.Load() != 0 {
			t.Fatalf("paused cancellation wrote or failed: %q %v", wire.String(), s.driver.oracle.examples)
		}
	})
}
