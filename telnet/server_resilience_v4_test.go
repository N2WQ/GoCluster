package telnet

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/spot"
)

type countingListener struct {
	closeCount atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	return nil, errors.New("closed")
}

func (l *countingListener) Close() error {
	l.closeCount.Add(1)
	return nil
}

func (l *countingListener) Addr() net.Addr {
	return stubAddr("listener")
}

type recordingConn struct {
	mu     sync.Mutex
	writes [][]byte
	closed bool
}

func (c *recordingConn) Read(b []byte) (int, error) {
	return 0, errors.New("unsupported")
}

func (c *recordingConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	copyBuf := make([]byte, len(b))
	copy(copyBuf, b)
	c.writes = append(c.writes, copyBuf)
	return len(b), nil
}

func (c *recordingConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *recordingConn) LocalAddr() net.Addr {
	return stubAddr("local")
}

func (c *recordingConn) RemoteAddr() net.Addr {
	return stubAddr("remote")
}

func (c *recordingConn) SetDeadline(time.Time) error {
	return nil
}

func (c *recordingConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *recordingConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (c *recordingConn) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, part := range c.writes {
		total += len(part)
	}
	out := make([]byte, 0, total)
	for _, part := range c.writes {
		out = append(out, part...)
	}
	return out
}

func (c *recordingConn) WriteCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.writes)
}

type discardConn struct{}

func (c *discardConn) Read(b []byte) (int, error) {
	return 0, errors.New("unsupported")
}

func (c *discardConn) Write(b []byte) (int, error) {
	return len(b), nil
}

func (c *discardConn) Close() error {
	return nil
}

func (c *discardConn) LocalAddr() net.Addr {
	return stubAddr("local")
}

func (c *discardConn) RemoteAddr() net.Addr {
	return stubAddr("remote")
}

func (c *discardConn) SetDeadline(time.Time) error {
	return nil
}

func (c *discardConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *discardConn) SetWriteDeadline(time.Time) error {
	return nil
}

func TestServerStopIsIdempotent(t *testing.T) {
	s := NewServer(ServerOptions{}, nil)
	listener := &countingListener{}
	s.listener = listener

	s.Stop()
	s.Stop()

	if got := listener.closeCount.Load(); got != 1 {
		t.Fatalf("expected listener.Close called once, got %d", got)
	}
}

func TestEnqueueRejectQueueFullClosesImmediately(t *testing.T) {
	s := &Server{
		shutdown:            make(chan struct{}),
		rejectQueue:         make(chan rejectJob, 1),
		rejectWriteDeadline: 10 * time.Millisecond,
	}
	first := &errConn{}
	second := &errConn{}

	s.enqueueReject(first, "203.0.113.10:1111", "Server busy.\r\n", "test-first")
	s.enqueueReject(second, "203.0.113.11:2222", "Server busy.\r\n", "test-second")

	if !second.closed {
		t.Fatal("expected second reject to close immediately when reject queue is full")
	}

	_, queueDrops := s.RejectMetricSnapshot()
	if queueDrops == 0 {
		t.Fatal("expected reject queue drop metric to increment")
	}
}

func TestWriterLoopBatchesAndPrioritizesControl(t *testing.T) {
	srv := &Server{
		writerBatchMaxBytes: 4096,
		writerBatchWait:     25 * time.Millisecond,
		latency:             newLatencyMetrics(),
	}
	conn := &recordingConn{}
	client := &Client{
		conn:        conn,
		writer:      bufio.NewWriter(conn),
		server:      srv,
		callsign:    "N0CALL",
		spotChan:    make(chan *spotEnvelope, 2),
		controlChan: make(chan controlMessage, 2),
		done:        make(chan struct{}),
	}

	dxSpot := spot.NewSpot("K1ABC", "N0CALL", 14074.0, "FT8")
	client.spotChan <- &spotEnvelope{spot: dxSpot, enqueueAt: time.Now().UTC()}
	client.controlChan <- controlMessage{line: "WWV test\n"}

	loopDone := make(chan struct{})
	go func() {
		client.writerLoop()
		close(loopDone)
	}()

	deadline := time.After(2 * time.Second)
	for conn.WriteCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for writer loop flush")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	client.close("test shutdown")
	select {
	case <-loopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("writerLoop did not stop after client close")
	}

	output := string(conn.Bytes())
	controlLine := normalizeOutboundLine("WWV test\n")
	spotLine := normalizeOutboundLine(dxSpot.FormatDXCluster() + "\n")
	if !strings.Contains(output, controlLine) {
		t.Fatalf("expected control line in output, got %q", output)
	}
	if !strings.Contains(output, spotLine) {
		t.Fatalf("expected spot line in output, got %q", output)
	}
	if strings.Index(output, controlLine) > strings.Index(output, spotLine) {
		t.Fatalf("expected control to be written before spot, got %q", output)
	}
	if writes := conn.WriteCount(); writes != 1 {
		t.Fatalf("expected single flushed write for batched control+spot, got %d", writes)
	}
}

func TestCachedClientShardsConcurrentAccess(t *testing.T) {
	s := NewServer(ServerOptions{
		BroadcastWorkers: 4,
	}, nil)

	for i := 0; i < 8; i++ {
		call := fmt.Sprintf("INIT%02d", i)
		s.clients[call] = &Client{callsign: call}
	}
	s.shardsDirty.Store(true)

	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				_ = s.cachedClientShards()
			}
		}()
	}

	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			call := fmt.Sprintf("MUT%04d", i)
			s.clientsMutex.Lock()
			s.clients[call] = &Client{callsign: call}
			if i%2 == 0 {
				delete(s.clients, call)
			}
			s.shardsDirty.Store(true)
			s.clientsMutex.Unlock()
		}
	}()

	wg.Wait()
}

func BenchmarkWriterLoopBurst(b *testing.B) {
	prevOutput := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() {
		log.SetOutput(prevOutput)
	})

	cases := []struct {
		name     string
		maxBytes int
		wait     time.Duration
	}{
		{name: "batched", maxBytes: 4096, wait: 5 * time.Millisecond},
		{name: "immediate", maxBytes: 1, wait: 0},
	}
	dxSpot := spot.NewSpot("K1ABC", "N0CALL", 14074.0, "FT8")

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				conn := &discardConn{}
				srv := &Server{
					writerBatchMaxBytes: tc.maxBytes,
					writerBatchWait:     tc.wait,
					latency:             newLatencyMetrics(),
				}
				client := &Client{
					conn:        conn,
					writer:      bufio.NewWriter(conn),
					server:      srv,
					callsign:    "N0CALL",
					spotChan:    make(chan *spotEnvelope, 2),
					controlChan: make(chan controlMessage, 3),
					done:        make(chan struct{}),
				}
				client.controlChan <- controlMessage{line: "WWV test\n"}
				client.spotChan <- &spotEnvelope{spot: dxSpot, enqueueAt: time.Now().UTC()}
				client.controlChan <- controlMessage{line: "73\n", closeAfter: true}
				client.writerLoop()
			}
		})
	}
}
