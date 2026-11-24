package telnet

import (
	"bufio"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"dxcluster/spot"
)

type errConn struct {
	writeErr error
	closed   bool
}

func (c *errConn) Read(b []byte) (int, error) {
	return 0, io.EOF
}

func (c *errConn) Write(b []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(b), nil
}

func (c *errConn) Close() error {
	c.closed = true
	return nil
}

func (c *errConn) LocalAddr() net.Addr {
	return stubAddr("local")
}

func (c *errConn) RemoteAddr() net.Addr {
	return stubAddr("remote")
}

func (c *errConn) SetDeadline(time.Time) error {
	return nil
}

func (c *errConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *errConn) SetWriteDeadline(time.Time) error {
	return nil
}

type stubAddr string

func (a stubAddr) Network() string {
	return string(a)
}

func (a stubAddr) String() string {
	return string(a)
}

func TestWriterLoopDisconnectsOnSpotSendFailure(t *testing.T) {
	srv := &Server{}
	conn := &errConn{writeErr: errors.New("write failed")}
	client := &Client{sessionProfile: &sessionProfile{},
		conn:        conn,
		writer:      bufio.NewWriter(conn),
		server:      srv,
		callsign:    "N0CALL",
		spotChan:    make(chan *spotEnvelope, 1),
		controlChan: make(chan controlMessage, 1),
		done:        make(chan struct{}),
	}
	loopDone := make(chan struct{})
	go func() {
		client.writerLoop()
		close(loopDone)
	}()

	client.spotChan <- &spotEnvelope{spot: &spot.Spot{
		DECall:    "N0CALL",
		DXCall:    "K1ABC",
		Frequency: 14070.0,
		Time:      time.Now().UTC(),
	}}

	select {
	case <-loopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("writerLoop did not exit after send failure")
	}

	if !conn.closed {
		t.Fatal("expected connection to be closed on send failure")
	}

	_, _, senderFailures := srv.BroadcastMetricSnapshot()
	if senderFailures != 1 {
		t.Fatalf("expected senderFailures=1, got %d", senderFailures)
	}
}

func TestWriterLoopDisconnectsOnControlSendFailure(t *testing.T) {
	srv := &Server{}
	conn := &errConn{writeErr: errors.New("write failed")}
	client := &Client{sessionProfile: &sessionProfile{},
		conn:        conn,
		writer:      bufio.NewWriter(conn),
		server:      srv,
		callsign:    "N0CALL",
		spotChan:    make(chan *spotEnvelope, 1),
		controlChan: make(chan controlMessage, 1),
		done:        make(chan struct{}),
	}
	loopDone := make(chan struct{})
	go func() {
		client.writerLoop()
		close(loopDone)
	}()

	client.controlChan <- controlMessage{line: "WWV test\n"}

	select {
	case <-loopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("writerLoop did not exit after send failure")
	}

	if !conn.closed {
		t.Fatal("expected connection to be closed on send failure")
	}

	_, _, senderFailures := srv.BroadcastMetricSnapshot()
	if senderFailures != 1 {
		t.Fatalf("expected senderFailures=1, got %d", senderFailures)
	}
}
