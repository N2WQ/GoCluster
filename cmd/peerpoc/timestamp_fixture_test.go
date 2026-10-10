package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var probeNoonUTC = time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC)

type probeTimestampClock struct {
	unix  atomic.Int64
	calls atomic.Int32
}

func newProbeTimestampClock(now time.Time) *probeTimestampClock {
	c := &probeTimestampClock{}
	c.unix.Store(now.Unix())
	return c
}

func (c *probeTimestampClock) now() time.Time {
	c.calls.Add(1)
	return time.Unix(c.unix.Load(), 0).UTC()
}

func literalProbePC92(stamp string, add bool) string {
	if add {
		return "PC92^N2WQ-33^" + stamp + "^A^^5N2WQ-33:5457^H99^\r\n"
	}
	return "PC92^N2WQ-33^" + stamp + "^K^5N2WQ-33:5457^1^0^H99^\r\n"
}

func requireTimestampRecord(t *testing.T, f *probeFixture, stamp string, add bool) {
	t.Helper()
	if got := f.line(t, "PC92^"); got != literalProbePC92(stamp, add) {
		t.Fatalf("PC92 wire = %q, want literal %q", got, literalProbePC92(stamp, add))
	}
}

func initializeTimestampPeer(t *testing.T, f *probeFixture, addStamp, keepStamp string) {
	t.Helper()
	f.send(t, "PC18^remote^pc9x^\r\n")
	requireTimestampRecord(t, f, addStamp, true)
	requireTimestampRecord(t, f, keepStamp, false)
	if got := f.line(t, "PC20^"); got != "PC20^\r\n" {
		t.Fatalf("timestamp init completion = %q", got)
	}
}

func consumeTimestampTick(ctx context.Context, t *testing.T, ticker *fixtureProbeTicker) {
	t.Helper()
	select {
	case ticker.ticks <- time.Now(): // Unbuffered fixtures witness actual consumption.
	case <-ctx.Done():
		t.Fatalf("keeper did not consume controlled tick: %v", ctx.Err())
	}
}

type probeTimestampDiagnostics struct{ ended chan string }

func (d *probeTimestampDiagnostics) Write(data []byte) (int, error) {
	if strings.Contains(string(data), "session ended:") {
		d.ended <- string(data)
	}
	return len(data), nil
}

type probeTimestampCommand struct {
	guard          context.Context
	cancel         context.CancelFunc
	result         <-chan error
	connected      chan *probeFixture
	diagnostics    *probeTimestampDiagnostics
	tickers        *fixtureProbeTickers
	mu             sync.Mutex
	owners         []*probeFixture
	unexpectedDial atomic.Bool
}

// Each attempt has <=100 PC92 records, PC20, one ack and one BYE: <=103 Write
// entry/exit events. Controlled pipe inputs are <=256 bytes, with no retained
// partial line, so the actual 4096-byte reader acquires each in one Read.
// At most four admitted attempts plus one canceled unexpected dial fit the
// finite diagnostics channel. An extra dial parks without holding fixture mu
// or manufacturing repeat errors. No collector owner
// or dropped telemetry can substitute for session retirement.
func newTimestampCommand(t *testing.T, attempts int, now func() time.Time, decorate func(int, *fixtureProbeConn)) *probeTimestampCommand {
	t.Helper()
	guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
	ctx, cancel := context.WithCancel(guard)
	r := &probeTimestampCommand{guard: guard, cancel: cancel,
		connected: make(chan *probeFixture, attempts), tickers: newFixtureProbeTickers(),
		diagnostics: &probeTimestampDiagnostics{ended: make(chan string, 16)}}
	r.tickers.unbuffered = true
	oldOutput := log.Writer()
	log.SetOutput(r.diagnostics)
	t.Cleanup(func() { log.SetOutput(oldOutput); endGuard() })
	runtime := probeRuntime{timestampNow: now, newTicker: r.tickers.newTicker,
		dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			r.mu.Lock()
			if len(r.owners) >= attempts {
				r.unexpectedDial.Store(true)
				r.mu.Unlock()
				<-ctx.Done()
				return nil, context.Cause(ctx)
			}
			if len(r.owners) > 0 {
				select {
				case <-r.owners[len(r.owners)-1].conn.closed:
				default:
					r.unexpectedDial.Store(true)
					r.mu.Unlock()
					<-ctx.Done()
					return nil, context.Cause(ctx)
				}
			}
			defer r.mu.Unlock()
			client, remote := net.Pipe()
			conn := newFixtureProbeConn(client)
			if decorate != nil {
				decorate(len(r.owners), conn)
			}
			f := &probeFixture{guard: guard, conn: conn, peer: remote, wire: bufio.NewReader(remote), tickers: r.tickers}
			r.owners = append(r.owners, f)
			r.connected <- f
			return conn, nil
		}}
	cfg := fixtureProbeConfig()
	cfg.reconnect = true
	r.result = launchProbeCommand(ctx, t, cfg, runtime, func() {
		cancel()
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, f := range r.owners {
			if err := f.peer.Close(); err != nil {
				t.Errorf("close timestamp peer: %v", err)
			}
			if err := f.conn.Close(); err != nil {
				t.Errorf("close timestamp transport: %v", err)
			}
		}
	})
	return r
}

func (r *probeTimestampCommand) next(t *testing.T) *probeFixture {
	t.Helper()
	return awaitProbe(r.guard, t, r.connected, "actual timestamp command connection")
}

func (r *probeTimestampCommand) ended(t *testing.T, cause error) {
	t.Helper()
	got := awaitProbe(r.guard, t, r.diagnostics.ended, "existing per-attempt causal diagnostic")
	if !strings.Contains(got, "session ended: "+cause.Error()) {
		t.Fatalf("attempt diagnostic = %q, want %v", got, cause)
	}
}

func (r *probeTimestampCommand) stop(t *testing.T, last *probeFixture) {
	t.Helper()
	r.cancel()
	if err := awaitProbe(r.guard, t, r.result, "explicit timestamp command cancellation"); err != nil {
		t.Fatal(err)
	}
	awaitProbe(r.guard, t, last.conn.closed, "last timestamp socket retirement")
	if r.unexpectedDial.Load() {
		t.Fatal("timestamp command admitted an unexpected dial")
	}
}

func requireTimestampRefusal(t *testing.T, r *probeTimestampCommand, f *probeFixture, cause error) {
	t.Helper()
	for {
		select {
		case line := <-r.diagnostics.ended:
			if !strings.Contains(line, "session ended: "+cause.Error()) {
				t.Fatalf("refusal cause = %q, want %v", line, cause)
			}
			if r.guard.Err() != nil {
				t.Fatal("watchdog substituted for timestamp refusal")
			}
			awaitProbe(r.guard, t, f.conn.closed, "refused timestamp owner retirement")
			return
		case output := <-f.conn.writeEntered:
			if output == "BYE\r\n" {
				f.line(t, "BYE\r\n")
				continue
			}
			t.Fatalf("timestamp refusal instead entered output: %q", output)
		case <-r.guard.Done():
			t.Fatalf("timestamp refusal exceeded guard: %v", r.guard.Err())
		}
	}
}

func noonProbeStamp(index int) string {
	if index == 0 {
		return "43200"
	}
	return fmt.Sprintf("43200.%02d", index)
}
