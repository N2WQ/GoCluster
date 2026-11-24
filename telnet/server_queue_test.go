package telnet

import (
	"bufio"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestEnqueueControlDisconnectsOnFullQueue(t *testing.T) {
	srv := &Server{}
	conn := &errConn{}
	client := &Client{sessionProfile: &sessionProfile{},
		conn:        conn,
		writer:      bufio.NewWriter(conn),
		server:      srv,
		callsign:    "N0CALL",
		controlChan: make(chan controlMessage, 1),
		spotChan:    make(chan *spotEnvelope, 1),
		done:        make(chan struct{}),
	}

	if err := client.enqueueControl(controlMessage{line: "first\n"}); err != nil {
		t.Fatalf("unexpected enqueue error: %v", err)
	}
	if err := client.enqueueControl(controlMessage{line: "second\n"}); err == nil {
		t.Fatal("expected control queue full error")
	}

	select {
	case <-client.done:
	default:
		t.Fatal("expected client to be closed on control queue overflow")
	}
	if !conn.closed {
		t.Fatal("expected connection closed on control queue overflow")
	}
}

func TestEnqueueSpotDisconnectsOnExtremeDrops(t *testing.T) {
	srv := &Server{
		dropExtremeRate:   0.8,
		dropExtremeWindow: 30 * time.Second,
		dropExtremeMinAtt: 10,
	}
	conn := &errConn{}
	client := &Client{sessionProfile: &sessionProfile{},
		conn:       conn,
		writer:     bufio.NewWriter(conn),
		server:     srv,
		callsign:   "N0CALL",
		spotChan:   make(chan *spotEnvelope),
		done:       make(chan struct{}),
		dropWindow: newDropWindow(srv.dropExtremeWindow),
	}

	testSpot := spot.NewSpot("K1ABC", "N0CALL", 14074.0, "FT8")
	for i := 0; i < srv.dropExtremeMinAtt; i++ {
		client.enqueueSpot(&spotEnvelope{spot: testSpot})
	}

	select {
	case <-client.done:
	default:
		t.Fatal("expected client to be closed on extreme drop rate")
	}
	if !conn.closed {
		t.Fatal("expected connection closed on extreme drop rate")
	}
}
