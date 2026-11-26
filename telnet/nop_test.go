package telnet

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestTelnetNOPSurvivesIdleDeadline(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
				opts.Transport, opts.EchoMode, opts.LoginGreeting = transport, "off", "READY\n"
				opts.PreloginTimeout = 5 * time.Second
				opts.ReadIdleTimeout = 250 * time.Millisecond
				opts.CommandRatePerSecond, opts.CommandBurst = 1, 1
			})
			listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			deadlines := make(chan struct{}, 32)
			s.startWithListener(nopDeadlineListener{Listener: listener, deadlines: deadlines})
			t.Cleanup(func() {
				s.Stop()
				waitSessionCeiling(t, func() bool { return s.GetClientCount() == 0 })
			})
			peer := ceilingConnect(t, listener.Addr().String(), "N0CALL")
			ceilingReadThrough(t, peer, "READY\r\n")
			awaitYAMLTestSignal(t, deadlines, "prelogin read deadline")
			awaitYAMLTestSignal(t, deadlines, "first command read deadline")
			if _, err := peer.Write([]byte{IAC, telnetNOP}); err != nil {
				t.Fatal(err)
			}
			// Observe a full idle read after the NOP, even if the write raced
			// the first read deadline. No command or response can arm these.
			awaitYAMLTestSignal(t, deadlines, "first idle read renewal")
			awaitYAMLTestSignal(t, deadlines, "second idle read renewal")
			if _, err := io.WriteString(peer, "BYE\r\n"); err != nil {
				t.Fatal(err)
			}
			got := readUntilContains(t, peer, "73!\r\n", 2*time.Second)
			if got != "73!\r\n" || s.CommandPacingWaits() != 0 {
				t.Fatalf("NOP affected the wire or command budget: reply=%q waits=%d", got, s.CommandPacingWaits())
			}
		})
	}
}

type nopDeadlineListener struct {
	net.Listener
	deadlines chan struct{}
}

func (l nopDeadlineListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return nopDeadlineConn{Conn: conn, deadlines: l.deadlines}, nil
}

type nopDeadlineConn struct {
	net.Conn
	deadlines chan struct{}
}

func (c nopDeadlineConn) SetReadDeadline(deadline time.Time) error {
	if err := c.Conn.SetReadDeadline(deadline); err != nil {
		return err
	}
	select {
	case c.deadlines <- struct{}{}:
	default:
	}
	return nil
}

func TestTelnetNOPRetainsPreloginDeadline(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			const budget = 5 * time.Second
			synctest.Test(t, func(t *testing.T) {
				s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
					opts.Transport, opts.EchoMode, opts.PreloginTimeout = transport, "off", budget
				})
				start := time.Now()
				_, peer, done := startHandshakeTranscriptSession(t, s)
				defer peer.Close()
				readUntilContains(t, peer, "login: ", time.Second)
				for range 4 {
					time.Sleep(time.Second)
					if _, err := peer.Write([]byte{IAC, telnetNOP}); err != nil {
						t.Fatalf("NOP closed prelogin early: %v", err)
					}
				}
				<-done
				_, _, _, _, timeouts, _, _ := s.PreloginMetricSnapshot()
				if elapsed := time.Since(start); elapsed != budget || timeouts != 1 {
					t.Fatalf("elapsed=%s login timeouts=%d, want %s and 1", elapsed, timeouts, budget)
				}
			})
		})
	}
}

func TestTelnetNOPPreservesLineAndYAMLBoundaries(t *testing.T) {
	nop := string([]byte{IAC, telnetNOP})
	for _, ziutek := range []bool{false, true} {
		for _, companion := range []string{"\n", "\x00"} {
			prefix := "SH" + nop + "OW OWN\r" + nop[:1]
			input := io.MultiReader(strings.NewReader(prefix), strings.NewReader(nop[1:]+companion+"RESUME\n"))
			c, conn := newHumanRecoveryClient(t, nil, ziutek)
			conn.input = input
			for _, want := range []string{"SHOW OWN", "RESUME"} {
				if got, err := c.readCommandLine(128); err != nil || got != want {
					t.Fatalf("ziutek=%t companion=%q: line=%q err=%v want=%q", ziutek, companion, got, err, want)
				}
			}
		}

		const header = "PATCH YAML FILTER SCHEMA 7"
		const body = "mode: 'MiXeD'\r\n"
		input := header + "\r" + nop + "\n-" + nop + "--\r\nmode: 'Mi" + nop + "XeD'\r" + nop + "\n...\r\nRESUME\n"
		c, _ := newHumanRecoveryClient(t, []byte(input), ziutek)
		if got, err := c.readCommandLine(128); err != nil || got != header {
			t.Fatalf("ziutek=%t header=%q err=%v", ziutek, got, err)
		}
		hooks, timer := fixedYAMLTestHooks()
		if got, err := c.receiveYAMLBodyWithHooks(hooks.now().Add(yamlUploadTimeout), hooks); err != nil || string(got) != body || !timer.isStopped() {
			t.Fatalf("ziutek=%t body=%q err=%v watchdog stopped=%t", ziutek, got, err, timer.isStopped())
		}
		if got, err := c.readCommandLine(128); err != nil || got != "RESUME" {
			t.Fatalf("ziutek=%t pipeline tail=%q err=%v", ziutek, got, err)
		}
	}
}

func TestTelnetNOPProgressAndRecoveryBudget(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		c, conn := newHumanRecoveryClient(t, nil, ziutek)
		// The earlier read may prefetch the IAC after a CR. Completing that
		// NOP and the CR companion still leaves the next command read idle.
		prefix := []byte{'S', 'H', 'O', 'W', ' ', 'O', 'W', 'N', '\r', IAC}
		input := &humanTimeoutReader{data: []byte{telnetNOP}}
		conn.input = io.MultiReader(bytes.NewReader(prefix), input)
		if got, err := c.readCommandLine(128); err != nil || got != "SHOW OWN" {
			t.Fatalf("ziutek=%t initial line=%q err=%v", ziutek, got, err)
		}
		_, err := c.readCommandLine(128)
		var terminal *humanInputError
		if !isTimeoutErr(err) || errors.As(err, &terminal) || conn.closed.Load() {
			t.Fatalf("ziutek=%t idle read became terminal: %v", ziutek, err)
		}
		conn.input = strings.NewReader("\nRESUME\n")
		if got, err := c.readCommandLine(128); err != nil || got != "RESUME" {
			t.Fatalf("ziutek=%t NOP cleared the pending CR companion: line=%q err=%v", ziutek, got, err)
		}

		for _, partial := range [][]byte{{IAC}, {IAC, DO}, {IAC, SB, 31}, {'A', '\b', IAC, telnetNOP}} {
			c, conn := newHumanRecoveryClient(t, nil, ziutek)
			conn.input = &humanTimeoutReader{data: partial}
			_, err := c.readCommandLine(128)
			if !errors.As(err, &terminal) || !conn.closed.Load() {
				t.Fatalf("ziutek=%t partial input %x remained retryable: %v", ziutek, partial, err)
			}
		}

		oversized := append([]byte{'!'}, bytes.Repeat([]byte{IAC, telnetNOP}, humanRecoveryWireBytes/2+1)...)
		oversized = append(oversized, "\nRESUME\n"...)
		c, conn = newHumanRecoveryClient(t, oversized, ziutek)
		if _, err := c.readCommandLine(128); !errors.Is(err, errHumanRecoveryWireLimit) || !conn.closed.Load() {
			t.Fatalf("ziutek=%t NOP escaped the raw recovery budget: %v", ziutek, err)
		}
	}
}

func TestInputWireNOPTranslatesOnlyTopLevelCommand(t *testing.T) {
	input := []byte{
		telnetNOP, IAC, IAC, telnetNOP, IAC, DO, telnetNOP,
		IAC, SB, 31, IAC, telnetNOP, IAC, SE, IAC, telnetNOP, 'A',
	}
	want := bytes.Clone(input)
	want[len(want)-2] = telnetGA
	for split := range len(input) + 1 {
		wire := &inputWireConn{ziutekNOP: true}
		got := bytes.Clone(input)
		wire.observeTelnet(got[:split])
		wire.observeTelnet(got[split:])
		if !bytes.Equal(got, want) || wire.state != 0 {
			t.Fatalf("split=%d observed=%x state=%d want=%x", split, got, wire.state, want)
		}
	}
}
