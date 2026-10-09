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

// Use the production raw guard, selected codec and human byte exposure. A
// memory connection records deadlines and closure without a wall-clock wait.
func newHumanRecoveryClient(t *testing.T, input []byte, ziutek bool) (*Client, *yamlTestConn) {
	t.Helper()
	conn := &yamlTestConn{input: bytes.NewReader(input)}
	wire := &inputWireConn{Conn: conn}
	s := &Server{useZiutek: ziutek}
	reader, writer, err := s.defaultWrapConn(wire)
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{conn: conn, inputWire: wire, server: s, writer: bufio.NewWriter(writer), done: make(chan struct{})}
	var inputReader io.Reader = reader
	if ziutek {
		c.inputExposure = &singleByteInput{reader: reader}
		inputReader = c.inputExposure
	}
	c.reader = bufio.NewReader(inputReader)
	t.Cleanup(c.interrupt)
	return c, conn
}

func TestHumanRejectionPreservesNextPhysicalLine(t *testing.T) {
	tails := [][]byte{
		[]byte("!REJECT BAND ALL\n"),
		[]byte("!\x15REJECT BAND ALL\b\n"),
		[]byte("!REJECT BAND ALL\r\n"),
		[]byte("!REJECT BAND ALL\r\x00"),
		{'!', IAC, IAC, '\r', '\n'},
		{'!', IAC, SB, 31, '\r', '\n', IAC, IAC, '\n', IAC, SE, 'X', '\n'},
		[]byte(strings.Repeat("A", 129) + "REJECT BAND ALL\n"),
		[]byte("DX 14074 K1ABC CW !REJECT BAND ALL\n"),
	}
	for _, ziutek := range []bool{false, true} {
		for _, tail := range tails {
			c, conn := newHumanRecoveryClient(t, append(bytes.Clone(tail), []byte("SHOW OWN\n")...), ziutek)
			line, err := c.readCommandLine(128)
			var validation *InputValidationError
			var terminal *humanInputError
			if line != "" || !errors.As(err, &validation) || errors.As(err, &terminal) || conn.closed.Load() {
				t.Fatalf("ziutek=%v rejected=%q line=%q err=%v closed=%v", ziutek, tail, line, err, conn.closed.Load())
			}
			if next, err := c.readCommandLine(128); err != nil || next != "SHOW OWN" {
				t.Fatalf("ziutek=%v next=%q err=%v after %q", ziutek, next, err, tail)
			}
		}
	}
}

func TestHumanRecoveryCountsHiddenWireAndPrefetch(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		// Subnegotiation contains no application bytes. A decoded-byte limit
		// alone would process this unbounded wire stream before seeing its EOL.
		input := append([]byte{'!', IAC, SB, 31}, bytes.Repeat([]byte{'X'}, 65_536+4096)...)
		input = append(input, IAC, SE, '\n')
		input = append(input, []byte("SHOW OWN\n")...)
		c, conn := newHumanRecoveryClient(t, input, ziutek)
		_, err := c.readCommandLine(128)
		var terminal *humanInputError
		if !errors.As(err, &terminal) || !errors.Is(err, errHumanRecoveryWireLimit) || !conn.closed.Load() {
			t.Fatalf("ziutek=%v error=%v closed=%v", ziutek, err, conn.closed.Load())
		}
		// Includes the prefetch that originally exposed '!'. Raw data consumed
		// after that byte never exceeds the agreed recovery allowance.
		if c.inputWire.total-1 > 65_536 {
			t.Fatalf("ziutek=%v read %d tail wire bytes", ziutek, c.inputWire.total-1)
		}
	}
}

func TestHumanRecoveryReservesBulkDecodedReadAhead(t *testing.T) {
	// Machine-body mode permits decoded bulk reads. Leave a decoded outer
	// buffer behind, including escaped IAC, before entering human reception.
	input := append([]byte{'!'}, bytes.Repeat([]byte{IAC, IAC}, 180)...)
	input = append(input, IAC, SB, 31)
	input = append(input, bytes.Repeat([]byte{'X'}, 70_000)...)
	c, conn := newHumanRecoveryClient(t, input, true)
	if _, err := c.reader.Peek(1); err != nil || c.reader.Buffered() < 32 {
		t.Fatalf("bulk fixture did not create outer decoded read-ahead: buffered=%d err=%v", c.reader.Buffered(), err)
	}
	before := c.inputWire.total
	debt := 2*(c.reader.Buffered()-1) + 256 // One offending byte will be consumed.
	_, err := c.readCommandLine(128)
	var terminal *humanInputError
	if !errors.As(err, &terminal) || !conn.closed.Load() || c.inputWire.total-before > uint64(65_536-debt) {
		t.Fatalf("decoded debt bypassed wire bound: before=%d after=%d debt=%d err=%v", before, c.inputWire.total, debt, err)
	}
}

type humanTimeoutReader struct{ data []byte }

func (r *humanTimeoutReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, humanTestTimeout{}
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

type humanTestTimeout struct{}

func (humanTestTimeout) Error() string   { return "test timeout" }
func (humanTestTimeout) Timeout() bool   { return true }
func (humanTestTimeout) Temporary() bool { return true }

type humanTrickleReader struct {
	data []byte
	now  *atomic.Int64
}

func (r *humanTrickleReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	p[0] = r.data[0]
	r.data = r.data[1:]
	r.now.Add(int64(time.Second))
	return 1, nil
}

func TestHumanPartialTimeoutSurvivesEditing(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, input := range []string{"A\x15", "A\b", "AB\x17", string([]byte{IAC, SB, 31})} {
			c, conn := newHumanRecoveryClient(t, nil, ziutek)
			conn.input = &humanTimeoutReader{data: []byte(input)}
			line, err := c.readCommandLine(128)
			var terminal *humanInputError
			if line != "" || !errors.As(err, &terminal) || !conn.closed.Load() {
				t.Fatalf("ziutek=%v input=%q line=%q err=%v closed=%v", ziutek, input, line, err, conn.closed.Load())
			}
		}
		c, conn := newHumanRecoveryClient(t, nil, ziutek)
		input := &humanTimeoutReader{}
		conn.input = input
		_, err := c.readCommandLine(128)
		var terminal *humanInputError
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() || errors.As(err, &terminal) || conn.closed.Load() {
			t.Fatalf("empty idle read ceased to be retryable: %v", err)
		}
		input.data = []byte("SHOW OWN\n")
		if got, err := c.readCommandLine(128); err != nil || got != "SHOW OWN" {
			t.Fatalf("idle retry=%q err=%v", got, err)
		}
	}
}

func TestHumanSplitTerminatorIdleRemainsRetryable(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, completion := range []byte{'\n', 0} {
			c, conn := newHumanRecoveryClient(t, nil, ziutek)
			input := &humanTimeoutReader{data: []byte("SHOW OWN\r")}
			conn.input = input
			if got, err := c.readCommandLine(128); err != nil || got != "SHOW OWN" {
				t.Fatalf("first line=%q err=%v", got, err)
			}
			input.data = []byte{completion} // A separate raw Read after previous CR.
			_, err := c.readCommandLine(128)
			var terminal *humanInputError
			var timeout net.Error
			if !errors.As(err, &timeout) || !timeout.Timeout() || errors.As(err, &terminal) || conn.closed.Load() {
				t.Fatalf("ziutek=%v completion=%q safe idle became terminal: %v", ziutek, completion, err)
			}
			input.data = []byte("SHOW OWN\n")
			if got, err := c.readCommandLine(128); err != nil || got != "SHOW OWN" {
				t.Fatalf("split terminator idle retry=%q err=%v", got, err)
			}
		}
	}
}

func humanManualHooks() (humanRecoveryHooks, *manualYAMLTestTimer) {
	hooks, timer := fixedYAMLTestHooks()
	return humanRecoveryHooks{now: hooks.now, afterFunc: func(d time.Duration, f func()) humanRecoveryTimer {
		return hooks.afterFunc(d, f)
	}}, timer
}

func TestHumanRecoveryRestoresEarlierDeadlineAndRetiresTimer(t *testing.T) {
	c, conn := newHumanRecoveryClient(t, []byte("ignored\nSHOW OWN\n"), false)
	hooks, timer := humanManualHooks()
	previous := hooks.now().Add(time.Second)
	if err := c.setReadDeadline(previous); err != nil {
		t.Fatal(err)
	}
	if err := c.recoverHumanLineWithHooks(hooks); err != nil {
		t.Fatal(err)
	}
	if !timer.isStopped() || timer.fire() || conn.closed.Load() || !c.readDeadline.Equal(previous) {
		t.Fatal("retired timer or deadline changed the next reception")
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.deadlines) != 3 || !conn.deadlines[1].Equal(previous) || !conn.deadlines[2].Equal(previous) {
		t.Fatalf("earlier deadline was extended: %v", conn.deadlines)
	}
}

func TestHumanRecoveryDefaultDeadlineIsFiveSeconds(t *testing.T) {
	c, conn := newHumanRecoveryClient(t, []byte("ignored\n"), false)
	hooks, _ := humanManualHooks()
	start := hooks.now()
	after := hooks.afterFunc
	var armed time.Duration
	hooks.afterFunc = func(d time.Duration, f func()) humanRecoveryTimer {
		armed = d
		return after(d, f)
	}
	if err := c.recoverHumanLineWithHooks(hooks); err != nil {
		t.Fatal(err)
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if armed != 5*time.Second || len(conn.deadlines) != 2 || !conn.deadlines[0].Equal(start.Add(5*time.Second)) || !conn.deadlines[1].IsZero() {
		t.Fatalf("wrong recovery contract: timer=%v deadlines=%v", armed, conn.deadlines)
	}
}

func TestHumanRecoveryRefusesExpiredBufferedBoundary(t *testing.T) {
	c, conn := newHumanRecoveryClient(t, []byte("ignored\nSHOW OWN\n"), false)
	if _, err := c.reader.Peek(1); err != nil {
		t.Fatal(err)
	}
	hooks, timer := humanManualHooks()
	start := hooks.now()
	var now atomic.Int64
	now.Store(start.UnixNano())
	hooks.now = func() time.Time { return time.Unix(0, now.Load()) }
	after := hooks.afterFunc
	hooks.afterFunc = func(d time.Duration, f func()) humanRecoveryTimer {
		result := after(d, f)
		now.Store(start.Add(5 * time.Second).UnixNano()) // Deliberately do not fire callback.
		return result
	}
	err := c.recoverHumanLineWithHooks(hooks)
	if !errors.Is(err, errHumanRecoveryDeadline) || !timer.isStopped() || conn.closed.Load() {
		t.Fatalf("buffered input bypassed absolute deadline: %v", err)
	}
	if b, err := c.reader.ReadByte(); err != nil || b != 'i' {
		t.Fatal("expired recovery consumed buffered data")
	}
}

func TestHumanRecoveryClockStopsTrickleBeforeBoundary(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		c, conn := newHumanRecoveryClient(t, nil, ziutek)
		hooks, timer := humanManualHooks()
		var now atomic.Int64
		now.Store(hooks.now().UnixNano())
		hooks.now = func() time.Time { return time.Unix(0, now.Load()) }
		input := &humanTrickleReader{data: []byte("xxxxxx\nSHOW OWN\n"), now: &now}
		conn.input = input                        // Fake transport records but does not enforce deadlines.
		err := c.recoverHumanLineWithHooks(hooks) // Watchdog deliberately never fires.
		if !errors.Is(err, errHumanRecoveryDeadline) || string(input.data) != "x\nSHOW OWN\n" || !timer.isStopped() {
			t.Fatalf("ziutek=%v trickle consumed beyond five seconds: remaining=%q err=%v", ziutek, input.data, err)
		}
	}
}

func TestHumanRecoveryJoinsStartedExpiryBeforeReturn(t *testing.T) {
	c, conn := newHumanRecoveryClient(t, []byte("ignored\n"), false)
	conn.closeEntered, conn.closeRelease = make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(conn.closeRelease) }) })
	hooks, timer := humanManualHooks()
	timer.stopCalled = make(chan struct{}, 1)
	after := hooks.afterFunc
	hooks.afterFunc = func(d time.Duration, f func()) humanRecoveryTimer {
		result := after(d, f)
		go timer.fire()
		<-conn.closeEntered
		return result
	}
	returned := make(chan error, 1)
	go func() { returned <- c.recoverHumanLineWithHooks(hooks) }()
	awaitYAMLTestSignal(t, timer.stopCalled, "expiry join barrier")
	select {
	case <-returned:
		t.Fatal("recovery returned while its mandatory expiry callback was running")
	default:
	}
	release.Do(func() { close(conn.closeRelease) })
	select {
	case err := <-returned:
		if !errors.Is(err, errHumanRecoveryDeadline) || !conn.closed.Load() {
			t.Fatalf("expiry result=%v closed=%v", err, conn.closed.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recovery did not join the released expiry callback")
	}
}

func TestHumanRecoveryAcceptanceRetiresStartedLosingCallback(t *testing.T) {
	c, conn := newHumanRecoveryClient(t, []byte("ignored\nignored again\nSHOW OWN\n"), false)
	hooks, timer := humanManualHooks()
	started, proceed := make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(proceed) }) })
	timer.stopCalled = make(chan struct{}, 1)
	after := hooks.afterFunc
	hooks.afterFunc = func(d time.Duration, f func()) humanRecoveryTimer {
		result := after(d, func() { close(started); <-proceed; f() })
		go timer.fire()
		<-started // Callback has started, then gets preempted before expiry claim.
		return result
	}
	returned := make(chan error, 1)
	go func() { returned <- c.recoverHumanLineWithHooks(hooks) }()
	awaitYAMLTestSignal(t, timer.stopCalled, "accepted boundary joins started callback")
	select {
	case <-returned:
		t.Fatal("acceptance returned before joining a started callback")
	default:
	}
	release.Do(func() { close(proceed) })
	select {
	case err := <-returned:
		if err != nil || conn.closed.Load() {
			t.Fatalf("losing expiry closed accepted connection: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("accepted recovery failed to join losing callback")
	}
	nextHooks, nextTimer := humanManualHooks()
	if err := c.recoverHumanLineWithHooks(nextHooks); err != nil || !nextTimer.isStopped() || conn.closed.Load() {
		t.Fatalf("retired callback affected next recovery: %v", err)
	}
	if got, err := c.readCommandLine(128); err != nil || got != "SHOW OWN" {
		t.Fatalf("next command=%q err=%v", got, err)
	}
}

func TestHumanPartialTimeoutDisconnectsLoginAndCommandCallers(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		for _, loggedIn := range []bool{false, true} {
			s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
				opts.Transport, opts.EchoMode = transport, "client"
				opts.PreloginTimeout = 250 * time.Millisecond
				opts.ReadIdleTimeout = 250 * time.Millisecond
				opts.LoginGreeting = "ready\n"
			})
			_, conn, done := startHandshakeTranscriptSession(t, s)
			t.Cleanup(func() { closeHandshakeTranscriptSession(t, conn, done) })
			if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(conn)
			readMachineEchoCoverageLiteral(t, reader, "login: ")
			if loggedIn {
				if _, err := io.WriteString(conn, "W1ABC-1\n"); err != nil {
					t.Fatal(err)
				}
				readMachineEchoCoverageLiteral(t, reader, "ready\r\n")
			}
			if _, err := io.WriteString(conn, "A\x15"); err != nil {
				t.Fatal(err)
			}
			if b, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
				t.Fatalf("transport=%s loggedIn=%v incomplete erased line retried: byte=%q err=%v", transport, loggedIn, b, err)
			}
			awaitYAMLTestSignal(t, done, "terminal caller retirement")
		}
	}
}

func TestHumanWatchdogInterruptsActualZiutekNegotiationWrite(t *testing.T) {
	c, conn := newMachineTransportCoverageClient(t, true, true)
	wire := &inputWireConn{Conn: conn}
	reader, writer, err := c.server.defaultWrapConn(wire)
	if err != nil {
		t.Fatal(err)
	}
	c.inputWire = wire
	c.inputExposure = &singleByteInput{reader: reader}
	c.reader, c.writer = bufio.NewReader(c.inputExposure), bufio.NewWriter(writer)
	result := make(chan error, 1)
	go func() { _, err := c.readCommandLine(128); result <- err }()
	t.Cleanup(func() { c.interrupt() })
	awaitYAMLTestSignal(t, conn.readEntered, "human raw read")
	conn.supply(t, []byte{'!', IAC, DO, 38})
	select {
	case <-conn.writeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("ziutek did not enter its real negotiation write")
	}
	select {
	case err := <-result:
		var terminal *humanInputError
		if !errors.As(err, &terminal) {
			t.Fatalf("stalled recovery remained retryable: %v", err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("recovery watchdog did not interrupt blocked negotiation Write")
	}
	awaitYAMLTestSignal(t, conn.writeExited, "retired codec writer")
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.deadlines) != 1 {
		t.Fatalf("offending byte was hidden before recovery armed: %v", conn.deadlines)
	}
}

func TestInputWireOrdinaryReadDoesNotAllocate(t *testing.T) {
	reader := bytes.NewReader([]byte("SHOW OWN\n"))
	conn := &yamlTestConn{input: reader}
	wire := &inputWireConn{Conn: conn}
	buffer := make([]byte, 32)
	if got := testing.AllocsPerRun(100, func() {
		_, _ = reader.Seek(0, io.SeekStart)
		_, _ = wire.Read(buffer)
	}); got != 0 {
		t.Fatalf("ordinary raw guard Read allocations=%v", got)
	}
}

func FuzzHumanRecoveryBoundary(f *testing.F) {
	f.Add([]byte("\r\nREJECT BAND ALL"))
	f.Add([]byte{IAC, SE, IAC, IAC, '\r'})
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 4096 {
			return
		}
		// Escaping every IAC makes arbitrary payload a valid SB body. CR/LF
		// inside it must not create a command boundary in either real codec.
		body := bytes.ReplaceAll(payload, []byte{IAC}, []byte{IAC, IAC})
		input := append([]byte{'!', IAC, SB, 31}, body...)
		input = append(input, IAC, SE, '\r', '\n')
		input = append(input, []byte("SHOW OWN\n")...)
		for _, ziutek := range []bool{false, true} {
			c, _ := newHumanRecoveryClient(t, input, ziutek)
			_, err := c.readCommandLine(128)
			var validation *InputValidationError
			var terminal *humanInputError
			if !errors.As(err, &validation) || errors.As(err, &terminal) {
				t.Fatalf("ziutek=%v framed rejection=%v", ziutek, err)
			}
			if got, err := c.readCommandLine(128); err != nil || got != "SHOW OWN" {
				t.Fatalf("ziutek=%v next=%q err=%v", ziutek, got, err)
			}
		}
	})
}
