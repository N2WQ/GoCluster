package rbn

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

// This uses a real reader and a deliberately full queue: the dropped record's
// timestamp must come from the same source reader, and the reader must proceed.
func TestRawPassthroughReceiptAndDrop(t *testing.T) {
	clientConn, sourceConn := net.Pipe()
	t.Cleanup(func() { _ = sourceConn.Close() })
	c := newLifecycleTestClient()
	c.loginDelay = time.Nanosecond
	c.UseMinimalParser()
	c.dial = func(context.Context, string, string) (net.Conn, error) { return clientConn, nil }
	raw := make(chan RawLine, 1)
	dropped := make(chan RawLine, 1)
	c.SetRawPassthrough(raw)
	c.SetRawPassthroughDropReporter(func(line string, at time.Time) {
		dropped <- RawLine{Line: line, ReceivedAt: at}
	})
	login := make(chan error, 1)
	go func() {
		_, err := bufio.NewReader(sourceConn).ReadString('\n')
		login <- err
	}()
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	if err := awaitValue(t, login, "source login"); err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC()
	if _, err := sourceConn.Write([]byte("WWV first\nWCY second\n")); err != nil {
		t.Fatal(err)
	}
	lost := awaitValue(t, dropped, "raw queue drop")
	first := awaitValue(t, raw, "first raw bulletin")
	if first.Line != "WWV first" || lost.Line != "WCY second" {
		t.Fatalf("unexpected raw records: first=%q dropped=%q", first.Line, lost.Line)
	}
	if first.ReceivedAt.Before(start) || lost.ReceivedAt.Before(first.ReceivedAt) || lost.ReceivedAt.After(time.Now().UTC()) {
		t.Fatalf("receipt time does not match source order: start=%v first=%v dropped=%v", start, first.ReceivedAt, lost.ReceivedAt)
	}
	if _, err := sourceConn.Write([]byte("WWV third\n")); err != nil {
		t.Fatal(err)
	}
	if next := awaitValue(t, raw, "raw reader after drop"); next.Line != "WWV third" {
		t.Fatalf("reader failed to continue after drop: %q", next.Line)
	}
}
