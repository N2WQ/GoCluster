package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestYAMLBodyPreservesLiteralBytesAndTail(t *testing.T) {
	for _, ending := range []string{"\n", "\r\n"} {
		t.Run(fmtEndingName(ending), func(t *testing.T) {
			want := "request_id: noise-Ab1" + ending + "configuration:" + ending + "  grid: 'fn42aa'" + ending + "  enabled: false" + ending + "  names: [Ab-1, \"Mixed Case\"]" + ending
			c, conn := newYAMLTestClient("---" + ending + want + "..." + ending + "RESUME\n")
			c.echoInput = true
			var echo bytes.Buffer
			c.writer = bufio.NewWriter(&echo)
			hooks, timer := fixedYAMLTestHooks()
			got, err := c.receiveYAMLBodyWithHooks(hooks.now().Add(yamlUploadTimeout), hooks)
			if err != nil || string(got) != want {
				t.Fatalf("body=%q err=%v; want %q", got, err, want)
			}
			if echo.Len() != 0 {
				t.Fatalf("payload was echoed: %q", echo.String())
			}
			line, err := c.readCommandLine(128)
			if err != nil || line != "RESUME" {
				t.Fatalf("tail=%q err=%v", line, err)
			}
			if conn.closed.Load() || !timer.isStopped() {
				t.Fatal("successful reception closed connection or retained watchdog")
			}
			conn.mu.Lock()
			defer conn.mu.Unlock()
			if len(conn.deadlines) != 2 || !conn.deadlines[1].IsZero() {
				t.Fatalf("reception deadline not cleared: %v", conn.deadlines)
			}
		})
	}
}

func TestYAMLBodyLimitBoundaries(t *testing.T) {
	for _, size := range []int{65_536, 65_537} {
		for _, ending := range []string{"\n", "\r\n"} {
			name := fmtEndingName(ending)
			if size == 65_537 {
				name += "_over_limit"
			}
			t.Run(name, func(t *testing.T) {
				// This is a complete, ordinary YAML scalar at either size.
				body := "value: " + strings.Repeat("a", size-len("value: ")-len(ending)) + ending
				c, conn := newYAMLTestClient("---" + ending + body + "..." + ending + "RESUME\n")
				hooks, timer := fixedYAMLTestHooks()
				got, err := c.receiveYAMLBodyWithHooks(hooks.now().Add(yamlUploadTimeout), hooks)
				if size == 65_536 {
					if err != nil || string(got) != body {
						t.Fatalf("exact-limit body failed: len=%d err=%v", len(got), err)
					}
					if conn.closed.Load() {
						t.Fatal("exact-limit body closed connection")
					}
				} else if !errors.Is(err, errYAMLUploadTooLarge) || got != nil || !conn.closed.Load() {
					t.Fatalf("over-limit body not terminal: len=%d err=%v closed=%v", len(got), err, conn.closed.Load())
				}
				if !timer.isStopped() {
					t.Fatal("watchdog remained active")
				}
			})
		}
	}
}

func TestYAMLBodyFramingFailuresAreTerminal(t *testing.T) {
	for _, input := range []string{
		"--- EXTRA\nvalue: x\n...\nRESUME\n",
		" ---\nvalue: x\n...\nRESUME\n",
		"---\rvalue: x\n...\nRESUME\n",
		"---\nvalue: x\rRESUME\n...\n",
		"---\nvalue: x\n...\rRESUME\n",
		"---\nvalue: x\n",
		"---\nvalue: x\n...",
		"---\nvalue: x\n... \nRESUME\n",
		"---\nvalue: x\n" + string([]byte{IAC, SB, 31, 0}),
	} {
		t.Run(input, func(t *testing.T) {
			c, conn := newYAMLTestClient(input)
			hooks, timer := fixedYAMLTestHooks()
			got, err := c.receiveYAMLBodyWithHooks(hooks.now().Add(yamlUploadTimeout), hooks)
			if !errors.Is(err, errYAMLUploadFraming) || got != nil || !conn.closed.Load() || !timer.isStopped() {
				t.Fatalf("framing failure not terminal: body=%q err=%v closed=%v stopped=%v", got, err, conn.closed.Load(), timer.isStopped())
			}
			select {
			case <-c.done:
			default:
				t.Fatal("terminal reception left session open for tail dispatch")
			}
		})
	}
}

func TestYAMLOversizedCommandLikePayloadIsTerminal(t *testing.T) {
	body := "value: " + strings.Repeat("a", 65_537-len("value: ")-len("\nRESUME\n")) + "\nRESUME\n"
	c, conn := newYAMLTestClient("---\n" + body + "...\nPAUSE\n")
	hooks, timer := fixedYAMLTestHooks()
	got, err := c.receiveYAMLBodyWithHooks(hooks.now().Add(yamlUploadTimeout), hooks)
	if !errors.Is(err, errYAMLUploadTooLarge) || got != nil || !conn.closed.Load() || !timer.isStopped() {
		t.Fatalf("command-like oversized payload was not terminal: err=%v closed=%v", err, conn.closed.Load())
	}
	select {
	case <-c.done:
	default:
		t.Fatal("payload tail could reenter an open session")
	}
}

func TestYAMLBodyDotPrefixesAreOrdinaryData(t *testing.T) {
	want := ".\n..\n...x\n ...\n....\n"
	c, _ := newYAMLTestClient("---\n" + want + "...\n")
	hooks, _ := fixedYAMLTestHooks()
	got, err := c.receiveYAMLBodyWithHooks(hooks.now().Add(yamlUploadTimeout), hooks)
	if err != nil || string(got) != want {
		t.Fatalf("dot prefixes interpreted as terminators: %q, %v", got, err)
	}
}

func TestYAMLBufferedCompletionChecksAbsoluteDeadline(t *testing.T) {
	const tail = "RESUME\n"
	frame := "---\nvalue: urban\n...\n" + tail
	c, conn := newYAMLTestClient(frame)
	if _, err := c.reader.Peek(len(frame)); err != nil {
		t.Fatal(err)
	}
	hooks, timer := fixedYAMLTestHooks()
	deadline := hooks.now().Add(yamlUploadTimeout)
	hooks.now = func() time.Time {
		if c.reader.Buffered() == len(tail) {
			return deadline // The final LF was read, but its acceptance is late.
		}
		return deadline.Add(-time.Second)
	}
	got, err := c.receiveYAMLBodyWithHooks(deadline, hooks)
	if !errors.Is(err, errYAMLUploadDeadline) || got != nil || !conn.closed.Load() || !timer.isStopped() {
		t.Fatalf("buffered late completion accepted: body=%q err=%v closed=%v stopped=%v", got, err, conn.closed.Load(), timer.isStopped())
	}
	if timer.hasFired() {
		t.Fatal("fixture fired watchdog instead of proving independent absolute check")
	}
}

func TestYAMLReceptionRejectsAlreadyExpiredDeadline(t *testing.T) {
	c, conn := newYAMLTestClient("---\nvalue: x\n...\n")
	hooks, _ := fixedYAMLTestHooks()
	created := false
	hooks.afterFunc = func(time.Duration, func()) yamlWatchdog {
		created = true
		return &manualYAMLTestTimer{}
	}
	got, err := c.receiveYAMLBodyWithHooks(hooks.now(), hooks)
	if !errors.Is(err, errYAMLUploadDeadline) || got != nil || !conn.closed.Load() || created {
		t.Fatalf("expired entry accepted: err=%v closed=%v watchdog=%v", err, conn.closed.Load(), created)
	}
}

func TestYAMLReceptionJoinsAlreadyRunningWatchdog(t *testing.T) {
	c, conn := newYAMLTestClient("---\nvalue: x\n...\n")
	conn.closeEntered = make(chan struct{})
	conn.closeRelease = make(chan struct{})
	var releaseClose sync.Once
	t.Cleanup(func() { releaseClose.Do(func() { close(conn.closeRelease) }) })
	hooks, timer := fixedYAMLTestHooks()
	timer.stopCalled = make(chan struct{}, 1)
	registered := make(chan struct{})
	startReception := make(chan struct{})
	var releaseReception sync.Once
	t.Cleanup(func() { releaseReception.Do(func() { close(startReception) }) })
	original := hooks.afterFunc
	hooks.afterFunc = func(duration time.Duration, callback func()) yamlWatchdog {
		watchdog := original(duration, callback)
		close(registered)
		<-startReception
		return watchdog
	}
	result := make(chan error, 1)
	go func() {
		_, err := c.receiveYAMLBodyWithHooks(hooks.now().Add(yamlUploadTimeout), hooks)
		result <- err
	}()
	awaitYAMLTestSignal(t, registered, "watchdog registration")
	callbackDone := make(chan struct{})
	go func() {
		timer.fire()
		close(callbackDone)
	}()
	awaitYAMLTestSignal(t, conn.closeEntered, "watchdog socket close")
	releaseReception.Do(func() { close(startReception) })
	awaitYAMLTestSignal(t, timer.stopCalled, "watchdog disarm")
	select {
	case err := <-result:
		t.Fatalf("reception returned before callback cleanup: %v", err)
	default:
	}
	releaseClose.Do(func() { close(conn.closeRelease) })
	awaitYAMLTestSignal(t, callbackDone, "callback completion")
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expired reception succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reception did not finish after callback completion")
	}
}

func TestYAMLReceptionCancellationInterruptsBlockedRead(t *testing.T) {
	for range 4 {
		serverConn, peerConn := net.Pipe()
		c := &Client{conn: serverConn, reader: bufio.NewReader(serverConn), done: make(chan struct{})}
		result := make(chan error, 1)
		go func() {
			_, err := c.receiveYAMLBody(time.Now().Add(yamlUploadTimeout))
			result <- err
		}()
		c.interrupt()
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("canceled reception succeeded")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("canceled reception remained blocked")
		}
		_ = peerConn.Close()
	}
}

func TestYAMLReceptionDeadlineClosesBlockedSocket(t *testing.T) {
	serverConn, peerConn := net.Pipe()
	defer peerConn.Close()
	c := &Client{conn: serverConn, reader: bufio.NewReader(serverConn), done: make(chan struct{})}
	defer c.interrupt()
	_, err := c.receiveYAMLBody(time.Now().Add(30 * time.Millisecond))
	if !errors.Is(err, errYAMLUploadDeadline) {
		t.Fatalf("blocked reception did not expire: %v", err)
	}
	select {
	case <-c.done:
	default:
		t.Fatal("timeout left session open")
	}
}

func TestYAMLRepeatedReceptionsRetireWatchdogs(t *testing.T) {
	c, conn := newYAMLTestClient(strings.Repeat("---\nvalue: Ab-1\n...\n", 8))
	var timers []*manualYAMLTestTimer
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	hooks := yamlReceptionHooks{now: func() time.Time { return now }, afterFunc: func(_ time.Duration, callback func()) yamlWatchdog {
		for _, previous := range timers {
			if !previous.isStopped() {
				t.Fatal("more than one active reception watchdog")
			}
		}
		timer := &manualYAMLTestTimer{callback: callback}
		timers = append(timers, timer)
		return timer
	}}
	for range 8 {
		got, err := c.receiveYAMLBodyWithHooks(now.Add(yamlUploadTimeout), hooks)
		if err != nil || string(got) != "value: Ab-1\n" {
			t.Fatalf("repeated reception failed: %q %v", got, err)
		}
	}
	if len(timers) != 8 {
		t.Fatalf("expected one watchdog per reception, got %d", len(timers))
	}
	for _, timer := range timers {
		if timer.fire() {
			t.Fatal("retired watchdog could run")
		}
	}
	if conn.closed.Load() {
		t.Fatal("retired watchdog closed live session")
	}
}

func TestYAMLBodyNativeAndZiutekTransport(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		name := "native"
		if ziutek {
			name = "ziutek"
		}
		t.Run(name, func(t *testing.T) {
			body := []byte("request_id: noise-Ab1\r\nvalue: 'Case: # / \\\"")
			body = append(body, 0x15, IAC)
			body = append(body, []byte("'\r\n")...)
			wire := []byte("put yaml config\r\n---\r\n")
			wire = append(wire, IAC, WILL, 1)
			wire = append(wire, bytes.ReplaceAll(body, []byte{IAC}, []byte{IAC, IAC})...)
			wire = append(wire, []byte("...\r\nRESUME\n")...)
			conn := &yamlTestConn{input: bytes.NewReader(wire)}
			s := &Server{useZiutek: ziutek}
			readerConn, writerConn, err := s.defaultWrapConn(conn)
			if err != nil {
				t.Fatal(err)
			}
			c := &Client{conn: conn, reader: bufio.NewReader(readerConn), writer: bufio.NewWriter(writerConn), server: s, done: make(chan struct{})}
			line, err := c.readCommandLine(128)
			if err != nil || line != "put yaml config" {
				t.Fatalf("header=%q err=%v", line, err)
			}
			hooks, _ := fixedYAMLTestHooks()
			got, err := c.receiveYAMLBodyWithHooks(hooks.now().Add(yamlUploadTimeout), hooks)
			if err != nil || !bytes.Equal(got, body) {
				t.Fatalf("body mismatch: got %q want %q err=%v", got, body, err)
			}
			line, err = c.readCommandLine(128)
			if err != nil || line != "RESUME" {
				t.Fatalf("tail=%q err=%v", line, err)
			}
		})
	}
}

func FuzzYAMLFrame(f *testing.F) {
	for _, seed := range []string{"value: Urban", "...", ".\n..\n...x", "value: x\r\nnext: false", "value: x\rRESUME", strings.Repeat("x", 65_535), strings.Repeat("x", 65_536)} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 65_537 {
			t.Skip()
		}
		// This oracle concerns framing. Negotiation has separate transport
		// fixtures; replace IAC to keep the source stream literal here.
		data = bytes.ReplaceAll(data, []byte{IAC}, []byte{'x'})
		frame := append([]byte("---\n"), data...)
		frame = append(frame, []byte("\n...\nRESUME\n")...)
		c, conn := newYAMLTestClient(string(frame))
		hooks, _ := fixedYAMLTestHooks()
		got, err := c.receiveYAMLBodyWithHooks(hooks.now().Add(yamlUploadTimeout), hooks)
		want, tail, valid := oracleYAMLFrame(frame)
		if valid && len(want) <= 65_536 {
			if err != nil || !bytes.Equal(got, want) || conn.closed.Load() {
				t.Fatalf("valid frame changed: body=%q want=%q err=%v", got, want, err)
			}
			rest, readErr := io.ReadAll(c.reader)
			if readErr != nil || !bytes.Equal(rest, tail) {
				t.Fatalf("tail consumed: got=%q want=%q err=%v", rest, tail, readErr)
			}
		} else if err == nil || got != nil || !conn.closed.Load() {
			t.Fatalf("invalid/oversized frame not terminal: len=%d err=%v closed=%v", len(got), err, conn.closed.Load())
		}
	})
}

// The oracle uses complete line slices rather than the receiver's streaming
// prefix probe, and includes all actual body ending bytes in its independent sum.
func oracleYAMLFrame(frame []byte) ([]byte, []byte, bool) {
	remaining := frame[len("---\n"):]
	start := 0
	for {
		n := bytes.IndexByte(remaining[start:], '\n')
		if n < 0 {
			return nil, nil, false
		}
		end := start + n
		line := remaining[start:end]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if bytes.ContainsRune(line, '\r') {
			return nil, nil, false
		}
		if bytes.Equal(line, []byte("...")) {
			return remaining[:start], remaining[end+1:], true
		}
		start = end + 1
	}
}

func newYAMLTestClient(input string) (*Client, *yamlTestConn) {
	conn := &yamlTestConn{input: strings.NewReader(input)}
	return &Client{conn: conn, reader: bufio.NewReader(conn), done: make(chan struct{})}, conn
}

func fixedYAMLTestHooks() (yamlReceptionHooks, *manualYAMLTestTimer) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	timer := &manualYAMLTestTimer{}
	return yamlReceptionHooks{now: func() time.Time { return now }, afterFunc: func(_ time.Duration, callback func()) yamlWatchdog {
		timer.mu.Lock()
		timer.callback = callback
		timer.mu.Unlock()
		return timer
	}}, timer
}

type manualYAMLTestTimer struct {
	mu         sync.Mutex
	callback   func()
	stopped    bool
	fired      bool
	stopCalled chan struct{}
}

func (t *manualYAMLTestTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopCalled != nil {
		t.stopCalled <- struct{}{}
	}
	if t.fired || t.stopped {
		return false
	}
	t.stopped = true
	return true
}

func (t *manualYAMLTestTimer) fire() bool {
	t.mu.Lock()
	if t.stopped || t.fired {
		t.mu.Unlock()
		return false
	}
	t.fired = true
	callback := t.callback
	t.mu.Unlock()
	callback()
	return true
}

func (t *manualYAMLTestTimer) isStopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}

func (t *manualYAMLTestTimer) hasFired() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.fired
}

type yamlTestConn struct {
	input        io.Reader
	closed       atomic.Bool
	mu           sync.Mutex
	deadlines    []time.Time
	closeEntered chan struct{}
	closeRelease chan struct{}
}

func (c *yamlTestConn) Read(p []byte) (int, error)  { return c.input.Read(p) }
func (c *yamlTestConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *yamlTestConn) Close() error {
	c.closed.Store(true)
	if c.closeEntered != nil {
		close(c.closeEntered)
		<-c.closeRelease
	}
	return nil
}
func (c *yamlTestConn) LocalAddr() net.Addr              { return stubAddr("yaml-local") }
func (c *yamlTestConn) RemoteAddr() net.Addr             { return stubAddr("yaml-remote") }
func (c *yamlTestConn) SetDeadline(time.Time) error      { return nil }
func (c *yamlTestConn) SetWriteDeadline(time.Time) error { return nil }
func (c *yamlTestConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, t)
	c.mu.Unlock()
	return nil
}

func awaitYAMLTestSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func fmtEndingName(ending string) string {
	if ending == "\r\n" {
		return "CRLF"
	}
	return "LF"
}
