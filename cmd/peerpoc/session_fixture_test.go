package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const probeFixtureBound = 8 * time.Second

func fixtureProbeConfig() probeConfig {
	return probeConfig{call: "N2WQ-33", password: "secret", loginTimeout: time.Hour,
		initTimeout: time.Hour, idleTimeout: time.Hour, keepalive: time.Hour,
		hop: 99, pc92Bitmap: 5, nodeVersion: "5457", legacyVersion: "5401",
		nodeCount: 1, backoffBase: time.Millisecond, backoffMax: time.Millisecond}
}

type probeWriteExit struct {
	data   string
	closed bool
}

type probeDeadlineEvent struct {
	kind string
	when time.Time
}

type probeReadReturn struct {
	entry int64
	data  string
}

// The decorator observes actual transport entry/exit. Fixture Close is cleanup
// only; successful owner retirement must occur before cleanup gets to run.
type fixtureProbeConn struct {
	net.Conn
	writeEntered chan string
	writeExited  chan probeWriteExit
	readEntered  chan int64
	readReturned chan probeReadReturn
	readSequence atomic.Int64
	closed       chan struct{}
	closeOnce    sync.Once
	isClosed     atomic.Bool
	writers      atomic.Int32
	overlap      atomic.Bool
	mu           sync.Mutex
	deadlines    []probeDeadlineEvent
	writeTrace   []string
	writeFault   func([]byte) (int, error, bool)
	readSetter   func(time.Time) error
	interrupt    func(time.Time) error
	byeSetter    func(time.Time) error
	afterClose   func()
}

func newFixtureProbeConn(conn net.Conn) *fixtureProbeConn {
	return &fixtureProbeConn{Conn: conn, writeEntered: make(chan string, 128),
		writeExited: make(chan probeWriteExit, 128), readEntered: make(chan int64, 128),
		readReturned: make(chan probeReadReturn, 128), closed: make(chan struct{})}
}

func (c *fixtureProbeConn) Read(p []byte) (int, error) {
	entry := c.readSequence.Add(1)
	c.readEntered <- entry
	n, err := c.Conn.Read(p)
	c.readReturned <- probeReadReturn{entry, string(p[:n])}
	return n, err
}

func (c *fixtureProbeConn) Write(p []byte) (int, error) {
	if c.writers.Add(1) != 1 {
		c.overlap.Store(true)
	}
	data := string(p)
	c.mu.Lock()
	c.writeTrace = append(c.writeTrace, data)
	c.mu.Unlock()
	c.writeEntered <- data
	defer func() {
		c.writers.Add(-1)
		c.writeExited <- probeWriteExit{data: data, closed: c.isClosed.Load()}
	}()
	if c.writeFault != nil {
		if n, err, handled := c.writeFault(p); handled {
			return n, err
		}
	}
	return c.Conn.Write(p)
}

func (c *fixtureProbeConn) recordDeadline(kind string, when time.Time) {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, probeDeadlineEvent{kind, when})
	c.mu.Unlock()
}

func (c *fixtureProbeConn) SetReadDeadline(when time.Time) error {
	if c.readSetter != nil {
		if err := c.readSetter(when); err != nil {
			return err
		}
	}
	c.recordDeadline("read", when)
	return c.Conn.SetReadDeadline(when)
}

func (c *fixtureProbeConn) SetDeadline(when time.Time) error {
	if c.interrupt != nil {
		if err := c.interrupt(when); err != nil {
			return err
		}
	}
	c.recordDeadline("interrupt", when)
	return c.Conn.SetDeadline(when)
}

func (c *fixtureProbeConn) SetWriteDeadline(when time.Time) error {
	if c.byeSetter != nil {
		if err := c.byeSetter(when); err != nil {
			return err
		}
	}
	c.recordDeadline("bye", when)
	return c.Conn.SetWriteDeadline(when)
}

func (c *fixtureProbeConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.isClosed.Store(true)
		err = c.Conn.Close()
		if c.afterClose != nil {
			c.afterClose()
		}
		close(c.closed)
	})
	return err
}

func (c *fixtureProbeConn) writes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.writeTrace...)
}

type fixtureProbeTicker struct {
	ticks       chan time.Time
	stopEntered chan struct{}
	stopRelease <-chan struct{}
	stopped     chan struct{}
	stops       atomic.Int32
}

func (t *fixtureProbeTicker) Ticks() <-chan time.Time { return t.ticks }

func (t *fixtureProbeTicker) Stop() {
	if t.stops.Add(1) == 1 {
		close(t.stopEntered)
		if t.stopRelease != nil {
			<-t.stopRelease
		}
		close(t.stopped)
	}
}

type fixtureProbeTickers struct {
	created     chan *fixtureProbeTicker
	count       atomic.Int32
	stopRelease <-chan struct{}
}

func newFixtureProbeTickers() *fixtureProbeTickers {
	return &fixtureProbeTickers{created: make(chan *fixtureProbeTicker, 32)}
}

func (f *fixtureProbeTickers) newTicker(time.Duration) probeTicker {
	t := &fixtureProbeTicker{ticks: make(chan time.Time, 1), stopEntered: make(chan struct{}),
		stopped: make(chan struct{}), stopRelease: f.stopRelease}
	f.count.Add(1)
	f.created <- t
	return t
}

type probeFixture struct {
	guard   context.Context
	cancel  context.CancelFunc
	conn    *fixtureProbeConn
	peer    net.Conn
	wire    *bufio.Reader
	tickers *fixtureProbeTickers
	session *probeSession
	result  chan error
	done    chan struct{}
}

func newProbeFixture(t *testing.T, cfg probeConfig, decorate func(*fixtureProbeConn), tickers *fixtureProbeTickers) *probeFixture {
	t.Helper()
	guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
	ctx, cancel := context.WithCancel(guard)
	client, remote := net.Pipe()
	conn := newFixtureProbeConn(client)
	if decorate != nil {
		decorate(conn)
	}
	if tickers == nil {
		tickers = newFixtureProbeTickers()
	}
	f := &probeFixture{guard: guard, cancel: cancel, conn: conn, peer: remote,
		wire: bufio.NewReader(remote), tickers: tickers, result: make(chan error, 1), done: make(chan struct{})}
	f.session = newProbeSession(ctx, cfg, conn, tickers.newTicker)
	go func() {
		defer close(f.done)
		f.result <- f.session.run()
	}()
	t.Cleanup(func() {
		cancel()
		_ = remote.Close()
		_ = conn.Close()
		select {
		case <-f.done:
		case <-time.After(probeFixtureBound):
			t.Error("fixture could not join session during failed-test cleanup")
		}
		endGuard()
	})
	return f
}

func awaitProbe[T any](ctx context.Context, t *testing.T, ch <-chan T, label string) T {
	t.Helper()
	select {
	case result := <-ch:
		if ctx.Err() != nil {
			t.Fatalf("%s arrived after fixture watchdog: %v", label, ctx.Err())
		}
		return result
	case <-ctx.Done():
		t.Fatalf("%s missed fixture bound: %v", label, ctx.Err())
		var zero T
		return zero
	}
}

func (f *probeFixture) finish(t *testing.T, cause error) {
	t.Helper()
	err := awaitProbe(f.guard, t, f.result, "session retirement")
	if !errors.Is(err, cause) {
		t.Fatalf("session cause = %v, want %v", err, cause)
	}
	awaitProbe(f.guard, t, f.done, "session worker completion")
	awaitProbe(f.guard, t, f.session.interruptDone, "interruptor join")
	awaitProbe(f.guard, t, f.conn.closed, "owned socket retirement")
}

func (f *probeFixture) send(t *testing.T, data string) {
	t.Helper()
	if err := f.peer.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(f.peer, data); err != nil {
		t.Fatalf("peer stimulus %q: %v", data, err)
	}
}

func (f *probeFixture) line(t *testing.T, want string) string {
	t.Helper()
	if err := f.peer.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := f.wire.ReadString('\n')
	if err != nil {
		t.Fatalf("wire line %q: %v", want, err)
	}
	if !strings.HasPrefix(got, want) || !strings.HasSuffix(got, "\r\n") {
		t.Fatalf("wire = %q, want prefix %q and CRLF", got, want)
	}
	return got
}

func (f *probeFixture) syncReader(t *testing.T) {
	t.Helper()
	f.send(t, "PC51^N2WQ-33^SYNC^1^\r\n")
	if got := f.line(t, "PC51^SYNC^N2WQ-33^0^"); got != "PC51^SYNC^N2WQ-33^0^\r\n" {
		t.Fatalf("reader barrier = %q", got)
	}
}

func (f *probeFixture) delivered(t *testing.T, payload string) int64 {
	t.Helper()
	for {
		receipt := awaitProbe(f.guard, t, f.conn.readReturned, "actual transport delivery of "+payload)
		if receipt.data == payload {
			return receipt.entry
		}
	}
}

func (f *probeFixture) readerAfter(t *testing.T, payload string) {
	t.Helper()
	completed := f.delivered(t, payload)
	for {
		if entry := awaitProbe(f.guard, t, f.conn.readEntered, "next actual reader entry"); entry > completed {
			return
		}
	}
}

func (f *probeFixture) initialize(t *testing.T, pc9x bool) {
	t.Helper()
	banner := "PC18^remote^legacy^\r\n"
	if pc9x {
		banner = "PC18^remote^pc9x^\r\n"
	}
	f.send(t, banner)
	if pc9x {
		f.line(t, "PC92^N2WQ-33^")
		f.line(t, "PC92^N2WQ-33^")
	} else {
		if got := f.line(t, "PC19^"); got != "PC19^1^N2WQ-33^0^5401^H99^\r\n" {
			t.Fatalf("legacy init = %q", got)
		}
	}
	if got := f.line(t, "PC20^"); got != "PC20^\r\n" {
		t.Fatalf("init completion = %q", got)
	}
}

func requireProbeNotClosed(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s completed while its owner barrier was parked", label)
	default:
	}
}

func requireProbeWrite(t *testing.T, f *probeFixture, prefix string) string {
	t.Helper()
	got := awaitProbe(f.guard, t, f.conn.writeEntered, "actual "+prefix+" write entry")
	if !strings.HasPrefix(got, prefix) {
		t.Fatalf("write entry = %q, want %q", got, prefix)
	}
	return got
}

func requireProbeStopped(t *testing.T, f *probeFixture, ticker *fixtureProbeTicker) {
	t.Helper()
	awaitProbe(f.guard, t, ticker.stopped, "ticker Stop completion")
	if ticker.stops.Load() != 1 {
		t.Fatalf("ticker Stop count = %d", ticker.stops.Load())
	}
	if f.session.keeperDone == nil {
		t.Fatal("missing owned keeper completion")
	}
	awaitProbe(f.guard, t, f.session.keeperDone, "keeper worker completion")
}

func probeWireError(label string) error { return fmt.Errorf("fixture %s failure", label) }
