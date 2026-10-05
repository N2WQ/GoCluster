package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/pathreliability"
)

func TestMachineEchoFullSessionNativeAndZiutek(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s, conn, reader := startMachineEchoCoverageSession(t, transport)
			join := startMachineEchoCoverageWrite(t, conn, "PAUSE 120\r\n")
			readMachineEchoCoverageLiteral(t, reader, "PAUSE 120\r\n")
			for range 2 {
				if _, err := reader.ReadString('\n'); err != nil {
					t.Fatal(err)
				}
			}
			join()
			s.clientsMutex.RLock()
			client := s.clients["W1ABC-1"]
			s.clientsMutex.RUnlock()
			if client == nil || !client.echoInput {
				t.Fatal("session did not establish server echo")
			}
			pause := machineEchoCoveragePause(client)
			if pause.until <= time.Now().UnixNano() || pause.pending {
				t.Fatal("fixture did not establish an active finite pause")
			}
			response := machineEchoCoverageRoundTrip(t, conn, reader,
				"get yaml settings id Noise-Ab1\r\n", "GET yaml settings id Noise-Ab1\r\n")
			if !strings.Contains(response, "request_id: Noise-Ab1\r\n") || machineEchoCoveragePause(client) != pause {
				t.Fatalf("GET lost case-preserved correlation or changed pause: %q", response)
			}
			revision := machineTestRevision(t, response)
			body := fmt.Sprintf("patch yaml settings\r\n---\r\nschema_version: 1\r\nrequest_id: Echo-Edit1\r\nif_revision: %s\r\nconfiguration:\r\n  noise_class: URBAN\r\n...\r\n", revision)
			response = machineEchoCoverageRoundTrip(t, conn, reader, body, "PATCH yaml settings\r\n")
			// An echoed request is itself a framed document. These literal reply
			// fields distinguish the acknowledgement from that false-green frame.
			if !strings.Contains(response, "request_id: Echo-Edit1\r\n") || !strings.Contains(response, "operation: PATCH\r\n") || !strings.Contains(response, "persisted: true\r\n") || strings.Contains(response, "configuration:") {
				t.Fatalf("PATCH response included input body or lacked its acknowledgement: %q", response)
			}
			newRevision := machineTestRevision(t, response)
			if newRevision == revision || machineEchoCoveragePause(client) != pause {
				t.Fatal("successful YAML edit failed its revision or pause contract")
			}
			invalid := fmt.Sprintf("patch yaml settings\r\n---\r\nschema_version: 1\r\nrequest_id: Echo-Bad1\r\nif_revision: %s\r\nconfiguration: {noise_class: true}\r\n...\r\n", newRevision)
			response = machineEchoCoverageRoundTrip(t, conn, reader, invalid, "PATCH yaml settings\r\n")
			if !strings.Contains(response, "request_id: Echo-Bad1\r\n") || !strings.Contains(response, "code: invalid_document\r\n") || strings.Contains(response, "noise_class: true") || machineEchoCoveragePause(client) != pause {
				t.Fatalf("received invalid body was echoed or changed pause: %q", response)
			}
			response = machineEchoCoverageRoundTrip(t, conn, reader,
				"get yaml settings id Echo-After1\r\n", "GET yaml settings id Echo-After1\r\n")
			if !strings.Contains(response, "request_id: Echo-After1\r\n") || !strings.Contains(response, "noise_class: URBAN\r\n") || machineTestRevision(t, response) != newRevision || machineEchoCoveragePause(client) != pause {
				t.Fatalf("echo-enabled session did not recover unchanged after invalid YAML: %q", response)
			}
		})
	}
}

func startMachineEchoCoverageSession(t *testing.T, transport string) (*Server, net.Conn, *bufio.Reader) {
	t.Helper()
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.Transport, opts.EchoMode = transport, "server"
		opts.LoginGreeting = "ready\n"
		opts.NoiseModel = pathreliability.DefaultConfig().NoiseModel()
		opts.DefaultDedupePolicy = "FAST"
		opts.DedupeFastEnabled, opts.DedupeMedEnabled, opts.DedupeSlowEnabled = true, true, true
	})
	_, conn, done := startHandshakeTranscriptSession(t, s)
	t.Cleanup(func() { closeHandshakeTranscriptSession(t, conn, done) })
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	readMachineEchoCoverageLiteral(t, reader, "login: ")
	join := startMachineEchoCoverageWrite(t, conn, "w1abc-1\r\n")
	readMachineEchoCoverageLiteral(t, reader, "W1ABC-1\r\nready\r\n")
	join()
	return s, conn, reader
}

// A net.Pipe sender must run while the peer consumes per-character echo.
// Cleanup closes the peer before joining, including when an assertion fails.
func startMachineEchoCoverageWrite(t *testing.T, conn net.Conn, input string) func() {
	t.Helper()
	done := make(chan struct{})
	var writeErr error
	go func() {
		_, writeErr = io.WriteString(conn, input)
		close(done)
	}()
	t.Cleanup(func() {
		_ = conn.Close()
		awaitYAMLTestSignal(t, done, "echo-session sender cleanup")
	})
	return func() {
		t.Helper()
		awaitYAMLTestSignal(t, done, "echo-session sender")
		if writeErr != nil {
			t.Fatalf("echo-session write: %v", writeErr)
		}
	}
}

func readMachineEchoCoverageLiteral(t *testing.T, reader *bufio.Reader, want string) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(reader, got); err != nil || string(got) != want {
		t.Fatalf("echo transcript=%q err=%v; want %q", got, err, want)
	}
}

func machineEchoCoverageRoundTrip(t *testing.T, conn net.Conn, reader *bufio.Reader, input, echo string) string {
	t.Helper()
	join := startMachineEchoCoverageWrite(t, conn, input)
	readMachineEchoCoverageLiteral(t, reader, echo)
	response := readMachineTestFrame(t, reader)
	join()
	return response
}

type machineEchoPauseSnapshot struct {
	until, cutoff     int64
	epoch, suppressed uint64
	pending, closed   bool
}

func machineEchoCoveragePause(c *Client) machineEchoPauseSnapshot {
	c.readPauseMu.Lock()
	defer c.readPauseMu.Unlock()
	return machineEchoPauseSnapshot{
		until: c.readPauseUntilUnixNano.Load(), cutoff: c.readPauseDiscardBefore.Load(),
		epoch: c.readPauseEpoch, suppressed: c.readPauseSuppressed.Load(),
		pending: c.readPausePending.Load(), closed: c.readPauseClosed,
	}
}

func TestYAMLTrickleUsesFixedAbsoluteDeadline(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		name := "native"
		if ziutek {
			name = "ziutek"
		}
		t.Run(name, func(t *testing.T) {
			c, conn := newMachineTransportCoverageClient(t, ziutek, false)
			hooks, timer := fixedYAMLTestHooks()
			start := hooks.now()
			var now, creations atomic.Int64
			now.Store(start.UnixNano())
			hooks.now = func() time.Time { return time.Unix(0, now.Load()) }
			originalAfter := hooks.afterFunc
			hooks.afterFunc = func(duration time.Duration, callback func()) yamlWatchdog {
				creations.Add(1)
				return originalAfter(duration, callback)
			}
			deadline := start.Add(30 * time.Second)
			result := startMachineTransportCoverageReception(t, c, deadline, hooks)
			chunks := []struct {
				elapsed time.Duration
				data    string
			}{
				{0, "---\n"}, {10 * time.Second, "value: "}, {20 * time.Second, "U"},
				{29 * time.Second, "R"}, {30 * time.Second, "BAN\n...\nRESUME\n"},
			}
			for _, chunk := range chunks {
				// A new raw read proves the previous chunk was drained before the
				// next clock advance. No watchdog is fired to make this test pass.
				awaitYAMLTestSignal(t, conn.readEntered, "trickle read barrier")
				now.Store(start.Add(chunk.elapsed).UnixNano())
				conn.supply(t, []byte(chunk.data))
			}
			received := awaitMachineTransportCoverageReception(t, result)
			if received.body != nil || !errors.Is(received.err, errYAMLUploadDeadline) || timer.hasFired() || !timer.isStopped() || creations.Load() != 1 {
				t.Fatalf("trickle escaped its fixed deadline: body=%q err=%v fired=%t stopped=%t watchdogs=%d", received.body, received.err, timer.hasFired(), timer.isStopped(), creations.Load())
			}
			assertMachineTransportCoverageClosed(t, c, conn, deadline)
		})
	}
}

func TestYAMLZiutekWatchdogInterruptsBlockedNegotiationWrite(t *testing.T) {
	c, conn := newMachineTransportCoverageClient(t, true, true)
	hooks, timer := fixedYAMLTestHooks()
	start := hooks.now()
	var now atomic.Int64
	now.Store(start.UnixNano())
	hooks.now = func() time.Time { return time.Unix(0, now.Load()) }
	deadline := start.Add(30 * time.Second)
	result := startMachineTransportCoverageReception(t, c, deadline, hooks)
	awaitYAMLTestSignal(t, conn.readEntered, "ziutek upload read")
	wire := append([]byte("---\n"), IAC, DO, byte(1))
	wire = append(wire, []byte("value: URBAN\n...\nRESUME\n")...)
	conn.supply(t, wire)
	select {
	case reply := <-conn.writeEntered:
		if !bytes.Equal(reply, []byte{IAC, WILL, 1}) {
			t.Fatalf("fixture did not reach ziutek negotiation Write: %v", reply)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ziutek negotiation never entered raw Write")
	}
	select {
	case received := <-result:
		t.Fatalf("negotiation Write returned without mandatory Close: %v", received.err)
	default:
	}
	now.Store(deadline.UnixNano())
	callbackDone := make(chan struct{})
	go func() { timer.fire(); close(callbackDone) }()
	t.Cleanup(func() {
		c.interrupt()
		awaitYAMLTestSignal(t, callbackDone, "ziutek watchdog cleanup")
	})
	awaitYAMLTestSignal(t, callbackDone, "ziutek watchdog interruption")
	awaitYAMLTestSignal(t, conn.writeExited, "blocked negotiation Write retirement")
	received := awaitMachineTransportCoverageReception(t, result)
	if received.body != nil || !errors.Is(received.err, errYAMLUploadDeadline) || !timer.hasFired() || timer.fire() {
		t.Fatalf("negotiation expiry returned a body or retained callback: body=%q err=%v", received.body, received.err)
	}
	assertMachineTransportCoverageClosed(t, c, conn, deadline)
}

type machineTransportReceptionResult struct {
	body []byte
	err  error
}

func startMachineTransportCoverageReception(t *testing.T, c *Client, deadline time.Time, hooks yamlReceptionHooks) <-chan machineTransportReceptionResult {
	t.Helper()
	result := make(chan machineTransportReceptionResult, 1)
	done := make(chan struct{})
	go func() {
		body, err := c.receiveYAMLBodyWithHooks(deadline, hooks)
		result <- machineTransportReceptionResult{body: body, err: err}
		close(done)
	}()
	t.Cleanup(func() {
		c.interrupt()
		awaitYAMLTestSignal(t, done, "transport reception cleanup")
	})
	return result
}

func awaitMachineTransportCoverageReception(t *testing.T, result <-chan machineTransportReceptionResult) machineTransportReceptionResult {
	t.Helper()
	select {
	case received := <-result:
		return received
	case <-time.After(2 * time.Second):
		t.Fatal("transport reception did not retire")
		return machineTransportReceptionResult{}
	}
}

// This raw transport advances only at test barriers. Read deadlines are recorded,
// not enforced by wall time, so clock checks and watchdog Close are distinct oracles.
// A blocked Write has no release path other than mandatory connection Close.
type machineTransportCoverageConn struct {
	readEntered  chan struct{}
	chunks       chan []byte
	writeEntered chan []byte
	writeExited  chan struct{}
	closed       chan struct{}
	closeOnce    sync.Once
	mu           sync.Mutex
	deadlines    []time.Time
}

func newMachineTransportCoverageClient(t *testing.T, ziutek, blockWrite bool) (*Client, *machineTransportCoverageConn) {
	t.Helper()
	conn := &machineTransportCoverageConn{
		readEntered: make(chan struct{}), chunks: make(chan []byte), closed: make(chan struct{}),
	}
	if blockWrite {
		conn.writeEntered, conn.writeExited = make(chan []byte), make(chan struct{})
	}
	s := &Server{useZiutek: ziutek}
	reader, writer, err := s.defaultWrapConn(conn)
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{conn: conn, reader: bufio.NewReader(reader), writer: bufio.NewWriter(writer), server: s, done: make(chan struct{})}
	t.Cleanup(c.interrupt)
	return c, conn
}

func (c *machineTransportCoverageConn) Read(p []byte) (int, error) {
	select {
	case c.readEntered <- struct{}{}:
	case <-c.closed:
		return 0, net.ErrClosed
	}
	select {
	case chunk := <-c.chunks:
		if len(chunk) > len(p) {
			return 0, io.ErrShortBuffer
		}
		return copy(p, chunk), nil
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *machineTransportCoverageConn) Write(p []byte) (int, error) {
	if c.writeEntered == nil {
		return len(p), nil
	}
	select {
	case c.writeEntered <- bytes.Clone(p):
	case <-c.closed:
		return 0, net.ErrClosed
	}
	<-c.closed
	close(c.writeExited)
	return 0, net.ErrClosed
}

func (c *machineTransportCoverageConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (*machineTransportCoverageConn) LocalAddr() net.Addr  { return stubAddr("coverage-local") }
func (*machineTransportCoverageConn) RemoteAddr() net.Addr { return stubAddr("coverage-remote") }
func (c *machineTransportCoverageConn) SetDeadline(deadline time.Time) error {
	return c.SetReadDeadline(deadline)
}
func (*machineTransportCoverageConn) SetWriteDeadline(time.Time) error { return nil }
func (c *machineTransportCoverageConn) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, deadline)
	c.mu.Unlock()
	return nil
}

func (c *machineTransportCoverageConn) supply(t *testing.T, chunk []byte) {
	t.Helper()
	select {
	case c.chunks <- chunk:
	case <-c.closed:
		t.Fatal("transport closed before the next controlled chunk")
	case <-time.After(2 * time.Second):
		t.Fatal("transport did not accept the controlled chunk")
	}
}

func assertMachineTransportCoverageClosed(t *testing.T, c *Client, conn *machineTransportCoverageConn, deadline time.Time) {
	t.Helper()
	select {
	case <-c.done:
	default:
		t.Fatal("terminal upload left client done open")
	}
	select {
	case <-conn.closed:
	default:
		t.Fatal("terminal upload left raw connection open")
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.deadlines) != 1 || !conn.deadlines[0].Equal(deadline) {
		t.Fatalf("upload changed its absolute deadline: %v; want only %v", conn.deadlines, deadline)
	}
}
