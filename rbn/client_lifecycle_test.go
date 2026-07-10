package rbn

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const lifecycleTestTimeout = 2 * time.Second

func TestConnectFirstResultAndRetryModes(t *testing.T) {
	t.Run("Connect waits and aborts after first failure", func(t *testing.T) {
		c := newLifecycleTestClient()
		c.reconnectInitial = time.Millisecond
		c.reconnectMax = time.Millisecond
		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		c.dial = func(context.Context, string, string) (net.Conn, error) {
			entered <- struct{}{}
			<-release
			return nil, errors.New("first dial failed")
		}

		result := make(chan error, 1)
		go func() { result <- c.Connect() }()
		awaitSignal(t, entered, "first Connect dial")
		assertNoSignal(t, result, 20*time.Millisecond, "Connect returned before its first dial completed")
		close(release)
		if err := awaitValue(t, result, "Connect result"); err == nil {
			t.Fatal("Connect returned nil after a failed first dial")
		}
		assertNoSignal(t, entered, 20*time.Millisecond, "Connect retried after its first dial failure")
		c.Stop()
	})

	t.Run("ConnectWithInitialRetry returns first error and keeps retrying", func(t *testing.T) {
		c := newLifecycleTestClient()
		c.loginDelay = time.Nanosecond
		c.reconnectInitial = time.Millisecond
		c.reconnectMax = time.Millisecond
		var attempts atomic.Int32
		second := newControlledConn()
		secondAttempt := make(chan struct{})
		c.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
			switch attempts.Add(1) {
			case 1:
				return nil, errors.New("first dial failed")
			case 2:
				close(secondAttempt)
				return second, nil
			default:
				<-ctx.Done()
				return nil, ctx.Err()
			}
		}

		if err := c.ConnectWithInitialRetry(); err == nil {
			t.Fatal("ConnectWithInitialRetry did not return the first dial error")
		}
		awaitSignal(t, secondAttempt, "initial retry")
		awaitSignal(t, second.readEntered, "initial retry read loop")
		if !c.IsConnected() {
			t.Fatal("client is not connected after successful initial retry")
		}
		c.Stop()
		if got := attempts.Load(); got != 2 {
			t.Fatalf("dial attempts = %d, want 2", got)
		}
	})

	t.Run("Start is nonblocking while DialContext is blocked", func(t *testing.T) {
		c := newLifecycleTestClient()
		entered := make(chan struct{})
		dialCanceled := make(chan struct{})
		c.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
			close(entered)
			<-ctx.Done()
			close(dialCanceled)
			return nil, ctx.Err()
		}

		returned := make(chan struct{})
		go func() {
			if err := c.Start(context.Background()); err != nil {
				t.Errorf("Start: %v", err)
			}
			close(returned)
		}()
		awaitSignal(t, returned, "Start return")
		awaitSignal(t, entered, "blocked DialContext entry")
		c.Stop()
		awaitSignal(t, dialCanceled, "DialContext cancellation")
	})
}

func TestStartContextCancellationJoinsBlockedDial(t *testing.T) {
	c := newLifecycleTestClient()
	entered := make(chan struct{})
	exited := make(chan struct{})
	var attempts atomic.Int32
	c.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		attempts.Add(1)
		close(entered)
		<-ctx.Done()
		close(exited)
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	awaitSignal(t, entered, "DialContext entry")
	cancel()
	awaitSignal(t, exited, "DialContext exit")
	awaitChannelClosed(t, c.GetSpotChannel(), "spot channel after parent cancellation")
	if got := attempts.Load(); got != 1 {
		t.Fatalf("dial attempts = %d, want 1", got)
	}
}

func TestStopCancelsPreloginWithoutWriting(t *testing.T) {
	c := newLifecycleTestClient()
	c.loginDelay = time.Hour
	conn := newControlledConn()
	c.dial = func(context.Context, string, string) (net.Conn, error) { return conn, nil }

	if err := c.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !c.IsConnected() {
		t.Fatal("successful TCP dial was not reported connected")
	}
	started := time.Now()
	c.Stop()
	if elapsed := time.Since(started); elapsed > lifecycleTestTimeout {
		t.Fatalf("prelogin Stop took %s, want <= %s", elapsed, lifecycleTestTimeout)
	}
	if got := conn.writeCount.Load(); got != 0 {
		t.Fatalf("prelogin writes = %d, want 0", got)
	}
	if got := conn.closeCount.Load(); got != 1 {
		t.Fatalf("connection closes = %d, want 1", got)
	}
}

func TestMidstreamEOFReconnectsWithoutGenerationOverlap(t *testing.T) {
	c := newLifecycleTestClient()
	c.loginDelay = time.Nanosecond
	c.reconnectInitial = time.Millisecond
	c.reconnectMax = time.Millisecond
	first := newControlledConn()
	second := newControlledConn()
	secondDialed := make(chan struct{})
	var attempts atomic.Int32
	c.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		switch attempts.Add(1) {
		case 1:
			return first, nil
		case 2:
			if first.closeCount.Load() != 1 {
				t.Errorf("replacement dial began before first generation closed")
			}
			close(secondDialed)
			return second, nil
		default:
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}

	if err := c.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	awaitSignal(t, first.readEntered, "first generation read")
	first.failRead(io.EOF)
	awaitSignal(t, secondDialed, "replacement dial")
	awaitSignal(t, second.readEntered, "replacement read loop")
	if !c.IsConnected() {
		t.Fatal("replacement generation was not reported connected")
	}
	c.Stop()
	if got := first.closeCount.Load(); got != 1 {
		t.Fatalf("first generation closes = %d, want 1", got)
	}
	if got := second.closeCount.Load(); got != 1 {
		t.Fatalf("second generation closes = %d, want 1", got)
	}
}

func TestLoginAndKeepaliveFailuresReconnect(t *testing.T) {
	t.Run("login write failure", func(t *testing.T) {
		c := newLifecycleTestClient()
		c.loginDelay = time.Nanosecond
		c.reconnectInitial = time.Millisecond
		c.reconnectMax = time.Millisecond
		first := newControlledConn()
		first.failWriteAfter = 0
		secondDialed := make(chan struct{})
		var attempts atomic.Int32
		c.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
			if attempts.Add(1) == 1 {
				return first, nil
			}
			close(secondDialed)
			<-ctx.Done()
			return nil, ctx.Err()
		}

		if err := c.Connect(); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		awaitSignal(t, secondDialed, "retry after login failure")
		c.Stop()
		if got := first.writeCount.Load(); got != 1 {
			t.Fatalf("login writes = %d, want 1", got)
		}
		if got := first.closeCount.Load(); got != 1 {
			t.Fatalf("failed-login connection closes = %d, want 1", got)
		}
	})

	t.Run("keepalive write failure", func(t *testing.T) {
		c := newLifecycleTestClient()
		c.loginDelay = time.Nanosecond
		c.EnableKeepalive(time.Millisecond)
		c.reconnectInitial = time.Millisecond
		c.reconnectMax = time.Millisecond
		first := newControlledConn()
		first.failWriteAfter = 1
		secondDialed := make(chan struct{})
		var attempts atomic.Int32
		c.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
			if attempts.Add(1) == 1 {
				return first, nil
			}
			close(secondDialed)
			<-ctx.Done()
			return nil, ctx.Err()
		}

		if err := c.Connect(); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		awaitSignal(t, secondDialed, "retry after keepalive failure")
		c.Stop()
		if got := first.writeCount.Load(); got != 2 {
			t.Fatalf("generation writes = %d, want login plus one keepalive", got)
		}
		if got := first.closeCount.Load(); got != 1 {
			t.Fatalf("failed-keepalive connection closes = %d, want 1", got)
		}
	})
}

func TestStaleGenerationCleanupCannotMutateReplacement(t *testing.T) {
	c := newLifecycleTestClient()
	firstConn := newControlledConn()
	secondConn := newControlledConn()
	var attempts atomic.Int32
	c.dial = func(context.Context, string, string) (net.Conn, error) {
		if attempts.Add(1) == 1 {
			return firstConn, nil
		}
		return secondConn, nil
	}

	first, err := c.dialGeneration(c.ctx)
	if err != nil {
		t.Fatalf("dial first generation: %v", err)
	}
	if err := c.installGeneration(first); err != nil {
		t.Fatalf("install first generation: %v", err)
	}
	if err := c.writeStringLine(first, "first\r\n"); err != nil {
		t.Fatalf("write first generation: %v", err)
	}
	second, err := c.dialGeneration(c.ctx)
	if err != nil {
		t.Fatalf("dial second generation: %v", err)
	}
	if err := c.installGeneration(second); err != nil {
		t.Fatalf("install second generation: %v", err)
	}
	if err := c.writeStringLine(second, "second\r\n"); err != nil {
		t.Fatalf("write second generation: %v", err)
	}

	c.retireGeneration(first)
	if got := attempts.Load(); got != 2 {
		t.Fatalf("dial attempts = %d, want 2", got)
	}
	if got := firstConn.closeCount.Load(); got != 1 {
		t.Fatalf("stale generation closes = %d, want 1", got)
	}
	if got := secondConn.closeCount.Load(); got != 0 {
		t.Fatalf("replacement closes after stale cleanup = %d, want 0", got)
	}
	if got := firstConn.bytes(); got != "first\r\n" {
		t.Fatalf("stale generation bytes = %q", got)
	}
	if got := secondConn.bytes(); got != "second\r\n" {
		t.Fatalf("replacement bytes = %q", got)
	}
	if !c.IsConnected() {
		t.Fatal("stale cleanup cleared replacement connected state")
	}
	c.Stop()
}

func TestRetryDelayIsBoundedDeterministicAndDoesNotSpin(t *testing.T) {
	c := newLifecycleTestClient()
	c.reconnectInitial = 20 * time.Millisecond
	c.reconnectMax = 80 * time.Millisecond
	other := NewClient("other.invalid", 7000, "N0CALL", "OTHER", nil, false, 4)
	other.reconnectInitial = c.reconnectInitial
	other.reconnectMax = c.reconnectMax

	var sawAttemptVariation bool
	var sawEndpointVariation bool
	for attempt := 0; attempt < 12; attempt++ {
		got := c.retryDelay(attempt)
		if got < 16*time.Millisecond || got > c.reconnectMax {
			t.Fatalf("attempt %d delay %s outside bounded jitter range", attempt, got)
		}
		if again := c.retryDelay(attempt); again != got {
			t.Fatalf("attempt %d delay changed from %s to %s", attempt, got, again)
		}
		if attempt > 0 && got != c.retryDelay(attempt-1) {
			sawAttemptVariation = true
		}
		if other.retryDelay(attempt) != got {
			sawEndpointVariation = true
		}
	}
	if !sawAttemptVariation || !sawEndpointVariation {
		t.Fatalf("jitter variation: attempt=%v endpoint=%v", sawAttemptVariation, sawEndpointVariation)
	}
	other.Stop()

	attempted := make(chan time.Time, 8)
	c.dial = func(context.Context, string, string) (net.Conn, error) {
		attempted <- time.Now()
		return nil, errors.New("offline")
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	times := []time.Time{
		awaitValue(t, attempted, "dial attempt 1"),
		awaitValue(t, attempted, "dial attempt 2"),
		awaitValue(t, attempted, "dial attempt 3"),
	}
	c.Stop()
	for i := 1; i < len(times); i++ {
		if gap := times[i].Sub(times[i-1]); gap < 12*time.Millisecond {
			t.Fatalf("retry gap %d = %s, indicates spin", i, gap)
		}
	}
	assertNoSignal(t, attempted, 30*time.Millisecond, "dial attempt after Stop")
}

func TestConcurrentStopJoinsBeforeClosingSpotChannel(t *testing.T) {
	c := newLifecycleTestClient()
	c.loginDelay = time.Nanosecond
	conn := newControlledConn()
	var attempts atomic.Int32
	c.dial = func(context.Context, string, string) (net.Conn, error) {
		attempts.Add(1)
		return conn, nil
	}
	raw := make(chan string, 1)
	c.SetRawPassthrough(raw)
	if err := c.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	awaitSignal(t, conn.readEntered, "active read before Stop")
	awaitSignal(t, conn.writeObserved, "login write before Stop")

	const callers = 8
	returned := make(chan struct{}, callers)
	for i := 0; i < callers; i++ {
		go func() {
			c.Stop()
			returned <- struct{}{}
		}()
	}
	deadline := time.NewTimer(lifecycleTestTimeout)
	defer deadline.Stop()
	for i := 0; i < callers; i++ {
		select {
		case <-returned:
			select {
			case <-conn.readExited:
			default:
				t.Fatal("Stop returned before the active reader exited")
			}
		case <-deadline.C:
			t.Fatalf("concurrent Stop did not join within %s", lifecycleTestTimeout)
		}
	}
	if got := conn.closeCount.Load(); got != 1 {
		t.Fatalf("connection closes = %d, want exactly 1", got)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("dial attempts = %d, want 1", got)
	}
	awaitChannelClosed(t, c.GetSpotChannel(), "spot channel after Stop")
	raw <- "still caller-owned"
	if got := <-raw; got != "still caller-owned" {
		t.Fatalf("raw channel value = %q", got)
	}
	assertNoSignal(t, conn.writeObserved, 30*time.Millisecond, "write after Stop")
}

func newLifecycleTestClient() *Client {
	return NewClient("example.invalid", 7000, "N0CALL", "TEST", nil, false, 4)
}

type controlledConn struct {
	closed         chan struct{}
	readFailure    chan error
	readEntered    chan struct{}
	readExited     chan struct{}
	writeObserved  chan struct{}
	readEnterOnce  sync.Once
	readExitOnce   sync.Once
	closedOnce     sync.Once
	mu             sync.Mutex
	written        []byte
	failWriteAfter int32
	writeCount     atomic.Int32
	closeCount     atomic.Int32
}

func newControlledConn() *controlledConn {
	return &controlledConn{
		closed:         make(chan struct{}),
		readFailure:    make(chan error, 1),
		readEntered:    make(chan struct{}),
		readExited:     make(chan struct{}),
		writeObserved:  make(chan struct{}, 8),
		failWriteAfter: -1,
	}
}

func (c *controlledConn) Read([]byte) (int, error) {
	c.readEnterOnce.Do(func() { close(c.readEntered) })
	defer c.readExitOnce.Do(func() { close(c.readExited) })
	select {
	case err := <-c.readFailure:
		return 0, err
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *controlledConn) Write(p []byte) (int, error) {
	count := c.writeCount.Add(1)
	select {
	case c.writeObserved <- struct{}{}:
	default:
	}
	if c.failWriteAfter >= 0 && count > c.failWriteAfter {
		return 0, errors.New("synthetic write failure")
	}
	c.mu.Lock()
	c.written = append(c.written, p...)
	c.mu.Unlock()
	return len(p), nil
}

func (c *controlledConn) Close() error {
	c.closeCount.Add(1)
	c.closedOnce.Do(func() { close(c.closed) })
	return nil
}

func (*controlledConn) LocalAddr() net.Addr              { return testAddr("local") }
func (*controlledConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (*controlledConn) SetDeadline(time.Time) error      { return nil }
func (*controlledConn) SetReadDeadline(time.Time) error  { return nil }
func (*controlledConn) SetWriteDeadline(time.Time) error { return nil }
func (c *controlledConn) failRead(err error)             { c.readFailure <- err }
func (c *controlledConn) bytes() string                  { c.mu.Lock(); defer c.mu.Unlock(); return string(c.written) }

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }

func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func awaitValue[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func assertNoSignal[T any](t *testing.T, ch <-chan T, wait time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(what)
	case <-time.After(wait):
	}
}

func awaitChannelClosed[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("%s remained open", what)
		}
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}
