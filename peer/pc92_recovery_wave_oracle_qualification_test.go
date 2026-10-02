//go:build qualification

package peer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// These controls drive the real wave caller over TCP. Merely checking a phase
// predicate would miss a caller that accidentally retries a rejected recovery.
func TestPC92V14RetryHandshakeFailurePropagation(t *testing.T) {
	const recoveryC = "PC92^N0LOCAL^1^C^5N0LOCAL:5457^1K1ABC:192.0.2.1^H99^\r\n"
	const recoveryA = "PC92^N0LOCAL^2^A^5N0LOCAL:5457^1K1ABC:192.0.2.1^H99^\r\n"
	for _, tc := range []struct {
		name, output, want string
		delay              time.Duration
		later              string
		startup, close     bool
	}{
		{name: "missing_A", output: "PC22^\r\n" + recoveryC, want: "deadline"},
		{name: "late_A", output: "PC22^\r\n" + recoveryC, delay: 5200 * time.Millisecond, later: recoveryA, want: "deadline"},
		{name: "A_before_C", output: "PC22^\r\n" + recoveryA, want: "not ordered"},
		{name: "repeat_PC22", output: "PC22^\r\n" + recoveryC, delay: 4 * time.Second, later: "PC22^\r\n", want: "repeated PC22"},
		{name: "malformed_C", output: "PC22^\r\nPC92^N0LOCAL^1^C^\r\n", want: "malformed"},
		{name: "malformed_A", output: "PC22^\r\n" + recoveryC + "PC92^N0LOCAL^2^A^INVALID^H99^\r\n", want: "malformed"},
		{name: "mismatched_A", output: "PC22^\r\n" + recoveryC + strings.ReplaceAll(recoveryA, "192.0.2.1", "192.0.2.2"), want: "immutable"},
		{name: "post_PC22_close", output: "PC22^\r\n", close: true, want: "EOF"},
		{name: "oversized_recovery", output: "PC22^\r\n" + strings.Repeat("x", MaxPeerFrameBytes+3) + "\n", want: "oversized"},
		{name: "oversized_startup", output: strings.Repeat("x", 1025), startup: true, want: "oversized"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			address, serverDone := retryHandshakeControlServer(ctx, t, func(conn net.Conn) error {
				if !tc.startup {
					if err := retryHandshakeControlPrelude(conn); err != nil {
						return err
					}
				}
				if _, err := io.WriteString(conn, tc.output); err != nil {
					return err
				}
				if tc.close {
					return nil
				}
				if tc.delay != 0 {
					if err := qualificationWait(ctx, tc.delay); err != nil {
						return err
					}
					// An already rejected connection may refuse this late output.
					_, _ = io.WriteString(conn, tc.later)
				}
				<-ctx.Done()
				return nil
			})
			var released atomic.Bool
			var count retryWaveCounters
			failures := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				retryWaveClient(ctx, address, "P0AAAA", nil, &released, &count, failures, nil)
			}()
			select {
			case err := <-failures:
				var phase *retryHandshakeError
				if !errors.As(err, &phase) || phase.recovery == tc.startup || !strings.Contains(err.Error(), tc.want) {
					t.Errorf("caller reported %v; want phase recovery=%t and %q", err, !tc.startup, tc.want)
				}
			case <-time.After(6 * time.Second):
				t.Error("caller hid invalid recovery or failed to enforce its original five-second deadline")
			}
			cancel()
			<-done
			if err := <-serverDone; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("control server: %v", err)
			}
			if count.attempts.Load() != 1 || count.expired.Load() != 0 || count.recovered.Load() != 0 {
				t.Errorf("invalid output became a retry or success: attempts=%d expired=%d recovered=%d", count.attempts.Load(), count.expired.Load(), count.recovered.Load())
			}
		})
	}
}

func TestPC92V14RetryHandshakeStartupRefusal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	address, serverDone := retryHandshakeControlServer(ctx, t, func(net.Conn) error { return nil })
	var count retryWaveCounters
	var released atomic.Bool
	failures := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		retryWaveClient(ctx, address, "P0AAAA", nil, &released, &count, failures, nil)
	}()
	deadline := time.Now().Add(time.Second)
	for count.expired.Load() == 0 && time.Now().Before(deadline) {
		_ = qualificationWait(ctx, time.Millisecond)
	}
	cancel()
	<-done
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	if count.expired.Load() == 0 {
		t.Fatal("pre-PC22 refusal was not retained as a failed candidate")
	}
	select {
	case err := <-failures:
		t.Fatalf("legitimate pre-PC22 refusal became fatal: %v", err)
	default:
	}
}

func TestPC92V14FinalHandshakePropagatesConcurrentFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	accepted := make(chan struct{})
	address, serverDone := retryHandshakeControlServer(ctx, t, func(net.Conn) error {
		close(accepted)
		<-ctx.Done()
		return nil
	})
	want := errors.New("mandatory mixed delivery failed during handshake")
	failures := make(chan error, 1)
	go func() {
		select {
		case <-accepted:
			failures <- want
		case <-ctx.Done():
		}
	}()
	started := time.Now()
	conn, reader, err := retryWaveFinalHandshake(ctx, address, "P0AAAA", failures)
	if !errors.Is(err, want) || conn != nil || reader != nil || time.Since(started) > time.Second {
		t.Errorf("final handshake hid concurrent failure: conn=%v reader=%v err=%v elapsed=%s", conn, reader, err, time.Since(started))
	}
	cancel()
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func retryHandshakeControlServer(ctx context.Context, t *testing.T, serve func(net.Conn) error) (string, <-chan error) {
	t.Helper()
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
		defer stop()
		if err := conn.SetDeadline(time.Now().Add(7 * time.Second)); err != nil {
			done <- err
			return
		}
		done <- serve(conn)
	}()
	return listener.Addr().String(), done
}

func retryHandshakeControlPrelude(conn net.Conn) error {
	if _, err := io.WriteString(conn, "login:"); err != nil {
		return err
	}
	reader := bufio.NewReader(conn)
	if _, err := reader.ReadString('\n'); err != nil {
		return err
	}
	if _, err := io.WriteString(conn, "PC18^DXSpider Version: 1.57 Build: 633 [pc9x 91]^5457^\r\n"); err != nil {
		return err
	}
	for _, want := range []string{"PC18^", "PC20^"} {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		if !strings.HasPrefix(line, want) {
			return fmt.Errorf("control received %q, want %s", line, want)
		}
	}
	return nil
}
