package telnet

import (
	"bufio"
	"testing"
	"time"
)

// Activation is a separate verified write before timing/admission. All measured
// writes go through the existing repeated-record, verified-drain sink.
type ve7ccBenchmarkConn struct {
	writerV15Conn
	ready   chan struct{}
	started bool
}

func (c *ve7ccBenchmarkConn) Write(data []byte) (int, error) {
	if !c.started {
		c.started = true
		if string(data) != "VE7CC enabled for this session.\r\n" {
			c.bad = true
		}
		close(c.ready)
		return len(data), nil
	}
	return c.writerV15Conn.Write(data)
}

func BenchmarkVE7CCWriterSpotDrain(b *testing.B) {
	s := &Server{clusterCall: "N2WQ-2", writerBatchMaxBytes: 16384, writerBatchWait: 5 * time.Millisecond}
	conn := &ve7ccBenchmarkConn{
		writerV15Conn: writerV15Conn{expected: []byte(ve7ccWireSpot), target: b.N, reached: make(chan struct{})},
		ready:         make(chan struct{}),
	}
	c := &Client{server: s, callsign: "W1BENCH", conn: conn, writer: bufio.NewWriterSize(conn, 32768),
		done: make(chan struct{}), controlChan: make(chan controlMessage, 8), spotChan: make(chan *spotEnvelope, 256)}
	if handled, proceed := s.handleVE7CCCommand(c, "SET/VE7CC"); !handled || !proceed {
		b.Fatal("native benchmark selection failed")
	}
	done := writerV15Start(c)
	defer func() { c.close(""); writerV15Wait(b, done, "native benchmark cleanup") }()
	writerV15Wait(b, conn.ready, "native benchmark activation")
	if conn.bad {
		b.Fatal("incorrect native activation bytes")
	}
	env := &spotEnvelope{spot: ve7ccWireFixture()}
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	b.ReportAllocs()
	b.SetBytes(int64(len(conn.expected)))
	b.ResetTimer()
	for range b.N {
		select {
		case c.spotChan <- env:
		case <-done:
			b.Fatal("writer exited before all native spot admissions")
		case <-deadline.C:
			b.Fatal("native spot admission timed out")
		}
	}
	writerV15Wait(b, conn.reached, "all native record bytes")
	// The sink checks each byte before signaling reached. With all b.N inputs
	// consumed, it cannot write again; verify the exact count before stopping.
	writerV15Check(b, &conn.writerV15Conn)
	b.StopTimer()
	c.close("")
	writerV15Wait(b, done, "native benchmark shutdown")
	writerV15Check(b, &conn.writerV15Conn)
	b.ReportMetric(float64(conn.count), "verified-spots")
}
