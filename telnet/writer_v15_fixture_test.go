package telnet

import (
	"bufio"
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/spot"
)

// writerV15Oracle is the frozen, pre-change outbound transform. Literal byte
// vectors separately guard the oracle; this must not call the new append path.
func writerV15Oracle(message string) string {
	normalized := strings.ReplaceAll(message, "\r\n", "\n")
	return strings.ReplaceAll(normalized, "\n", "\r\n")
}

// writerV15Conn checks a repeated exact record without retaining output or
// allocating per write. Only writerLoop mutates it; callers read after join.
type writerV15Conn struct {
	discardConn
	expected []byte
	target   int
	count    int
	offset   int
	bad      bool
	bytes    int
	writes   int
	lengths  []int // optional, preallocated by consumer fixtures only
	reached  chan struct{}
	once     sync.Once
}

func (c *writerV15Conn) Write(p []byte) (int, error) {
	n := len(p)
	c.bytes += n
	c.writes++
	if c.lengths != nil {
		c.lengths = append(c.lengths, n)
	}
	for len(p) > 0 && len(c.expected) > 0 {
		part := min(len(p), len(c.expected)-c.offset)
		if !bytes.Equal(p[:part], c.expected[c.offset:c.offset+part]) {
			c.bad = true
		}
		c.offset += part
		p = p[part:]
		if c.offset == len(c.expected) {
			c.offset = 0
			c.count++
		}
	}
	if len(p) != 0 || c.count > c.target {
		c.bad = true
	}
	if c.bad || c.count >= c.target {
		c.once.Do(func() { close(c.reached) })
	}
	return n, nil
}

func writerV15Spot() *spot.Spot {
	sp := spot.NewSpot("K1ABC", "N0CALL", 14074.0, "FT8")
	sp.Time = time.Date(2026, time.October, 2, 12, 34, 0, 0, time.UTC)
	sp.Comment = "fixture"
	return sp
}

func writerV15Client(server *Server, conn *writerV15Conn) *Client {
	return &Client{
		conn: conn, writer: bufio.NewWriterSize(conn, 32768), server: server,
		callsign: "N0CALL", spotChan: make(chan *spotEnvelope, 256),
		controlChan: make(chan controlMessage, 16), done: make(chan struct{}),
	}
}

func writerV15Start(c *Client) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.writerLoop()
	}()
	return done
}

func writerV15Wait(tb testing.TB, signal <-chan struct{}, label string) {
	tb.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		tb.Fatalf("writer %s did not complete", label)
	}
}

func writerV15Check(tb testing.TB, conn *writerV15Conn) {
	tb.Helper()
	if conn.bad || conn.offset != 0 || conn.count != conn.target || conn.bytes != conn.target*len(conn.expected) {
		tb.Fatalf("writer output: bad=%v partial=%d records=%d/%d bytes=%d/%d", conn.bad, conn.offset,
			conn.count, conn.target, conn.bytes, conn.target*len(conn.expected))
	}
}
