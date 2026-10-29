package telnet

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

// pacingWaitGate holds the command loop after it observes an exhausted bucket.
// Closing stop releases every gate even if a test fails while a waiter is held.
type pacingWaitGate struct {
	entered chan chan struct{}
	stop    chan struct{}
	start   time.Time
	elapsed atomic.Int64
}

func (g *pacingWaitGate) now() time.Time {
	return g.start.Add(time.Duration(g.elapsed.Load()))
}

func (g *pacingWaitGate) advanceAndRelease(release chan struct{}) {
	g.elapsed.Add(int64(time.Second))
	close(release)
}

func (g *pacingWaitGate) wait(*Client) {
	release := make(chan struct{})
	select {
	case g.entered <- release:
	case <-g.stop:
		return
	}
	select {
	case <-release:
	case <-g.stop:
	}
}

func (g *pacingWaitGate) next(t *testing.T) chan struct{} {
	t.Helper()
	select {
	case release := <-g.entered:
		return release
	case <-time.After(5 * time.Second):
		t.Fatal("command did not enter pacing wait")
		return nil
	}
}

func startPacingTransportSession(t *testing.T, transport string, timerStarted ...func(*Client)) (*Server, *Client, net.Conn, *bufio.Reader, <-chan struct{}, *pacingWaitGate) {
	t.Helper()
	return startPacingTransportSessionAtRate(t, transport, 1, timerStarted...)
}

func startPacingTransportSessionAtRate(t *testing.T, transport string, rate int, timerStarted ...func(*Client)) (*Server, *Client, net.Conn, *bufio.Reader, <-chan struct{}, *pacingWaitGate) {
	t.Helper()
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.Transport, opts.EchoMode = transport, "off"
		opts.LoginGreeting = "ready\n"
		opts.InputInvalidCharMessage = "INVALID\n"
		opts.CommandRatePerSecond, opts.CommandBurst = rate, 1
		opts.NoiseModel = pathreliability.DefaultConfig().NoiseModel()
		opts.DefaultDedupePolicy = "FAST"
		opts.DedupeFastEnabled, opts.DedupeMedEnabled, opts.DedupeSlowEnabled = true, true, true
	})
	gate := &pacingWaitGate{entered: make(chan chan struct{}, 1), stop: make(chan struct{}), start: time.Now()}
	s.testCommandPacingNow = gate.now
	s.testCommandPacingWait = gate.wait
	if len(timerStarted) != 0 {
		s.testCommandPacingTimerStarted = timerStarted[0]
	}
	_, conn, done := startHandshakeTranscriptSession(t, s)
	t.Cleanup(func() {
		close(gate.stop)
		closeHandshakeTranscriptSession(t, conn, done)
	})
	if err := conn.SetDeadline(time.Now().Add(12 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	readMachineEchoCoverageLiteral(t, reader, "login: ")
	if _, err := io.WriteString(conn, "W1ABC-1\r\n"); err != nil {
		t.Fatal(err)
	}
	readMachineEchoCoverageLiteral(t, reader, "ready\r\n")
	s.clientsMutex.RLock()
	client := s.clients["W1ABC-1"]
	s.clientsMutex.RUnlock()
	if client == nil {
		t.Fatal("session was not registered")
	}
	return s, client, conn, reader, done, gate
}

// Writes are concurrent because net.Pipe delivers bytes only as the server
// reads them; pacing may intentionally stop the server before the last byte.
func pacingTransportWrite(t *testing.T, conn net.Conn, input string) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := io.WriteString(conn, input)
		done <- err
	}()
	return done
}

func TestCommandPacingRecoveredLineAndBlankTransport(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s, _, conn, reader, _, gate := startPacingTransportSession(t, transport)
			// IAC GA is transport control, and CRLF is one blank keepalive, not two lines.
			input := string([]byte{IAC, 249}) + "\r\nBAD!RESUME\r\nGET YAML SETTINGS SCHEMA 6 ID after-invalid\r\n"
			sent := pacingTransportWrite(t, conn, input)
			first := gate.next(t)
			if got := s.CommandPacingWaits(); got != 1 {
				t.Fatalf("recovered invalid line waits=%d, want 1", got)
			}
			gate.advanceAndRelease(first)
			readMachineEchoCoverageLiteral(t, reader, "INVALID\r\n")
			second := gate.next(t)
			if got := s.CommandPacingWaits(); got != 2 {
				t.Fatalf("following GET waits=%d, want 2", got)
			}
			gate.advanceAndRelease(second)
			if frame := readMachineTestFrame(t, reader); !strings.Contains(frame, "request_id: after-invalid\r\n") {
				t.Fatalf("following request lost or reordered: %q", frame)
			}
			if err := <-sent; err != nil {
				t.Fatalf("write pipeline: %v", err)
			}
		})
	}
}

func TestCommandPacingMachineBodyIsNotDispatched(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s, client, conn, reader, _, gate := startPacingTransportSession(t, transport)
			if _, err := io.WriteString(conn, "PAUSE 120\r\n"); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := reader.ReadString('\n'); err != nil {
					t.Fatal(err)
				}
			}
			pause := client.readPauseUntilUnixNano.Load()
			if pause == 0 {
				t.Fatal("PAUSE did not establish the state used by the payload oracle")
			}
			// RESUME is body data. If the upload header is skipped and the body
			// leaks into dispatch, the pause state changes.
			pipeline := "PATCH YAML SETTINGS\r\n---\r\nRESUME\r\n...\r\nGET YAML SETTINGS SCHEMA 6 ID after-body\r\n"
			sent := pacingTransportWrite(t, conn, pipeline)
			first := gate.next(t)
			if got := s.CommandPacingWaits(); got != 1 || client.readPauseUntilUnixNano.Load() != pause {
				t.Fatalf("upload wait=%d, pause changed=%v", got, client.readPauseUntilUnixNano.Load() != pause)
			}
			gate.advanceAndRelease(first)
			if frame := readMachineTestFrame(t, reader); !strings.Contains(frame, "code: invalid_document\r\n") {
				t.Fatalf("invalid YAML was not handled as one document: %q", frame)
			}
			second := gate.next(t)
			if got := s.CommandPacingWaits(); got != 2 || client.readPauseUntilUnixNano.Load() != pause {
				t.Fatalf("following request waits=%d, body changed pause=%v", got, client.readPauseUntilUnixNano.Load() != pause)
			}
			gate.advanceAndRelease(second)
			if frame := readMachineTestFrame(t, reader); !strings.Contains(frame, "request_id: after-body\r\n") {
				t.Fatalf("following GET was lost: %q", frame)
			}
			if err := <-sent; err != nil {
				t.Fatalf("write pipeline: %v", err)
			}
		})
	}
}

func TestCommandPacingValidMachineUploadAndFollowingRequest(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s, _, conn, reader, _, gate := startPacingTransportSession(t, transport)
			if _, err := io.WriteString(conn, "GET YAML SETTINGS SCHEMA 6 ID before\r\n"); err != nil {
				t.Fatal(err)
			}
			revision := machineTestRevision(t, readMachineTestFrame(t, reader))
			pipeline := fmt.Sprintf("PATCH YAML SETTINGS\r\n---\r\nschema_version: 6\r\nrequest_id: patch-1\r\nif_revision: %s\r\nconfiguration:\r\n  noise_class: URBAN\r\n...\r\nGET YAML SETTINGS SCHEMA 6 ID after-patch\r\n", revision)
			sent := pacingTransportWrite(t, conn, pipeline)
			first := gate.next(t)
			gate.advanceAndRelease(first)
			if frame := readMachineTestFrame(t, reader); !strings.Contains(frame, "operation: PATCH\r\n") || !strings.Contains(frame, "persisted: true\r\n") {
				t.Fatalf("upload did not persist: %q", frame)
			}
			second := gate.next(t)
			if got := s.CommandPacingWaits(); got != 2 {
				t.Fatalf("upload and following GET waits=%d, want 2", got)
			}
			gate.advanceAndRelease(second)
			if frame := readMachineTestFrame(t, reader); !strings.Contains(frame, "request_id: after-patch\r\n") || !strings.Contains(frame, "noise_class: URBAN\r\n") {
				t.Fatalf("following GET did not observe the committed upload: %q", frame)
			}
			if err := <-sent; err != nil {
				t.Fatalf("write pipeline: %v", err)
			}
		})
	}
}

func TestCommandPacingShutdownCancelsWaitingCommand(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s, client, conn, _, done, gate := startPacingTransportSession(t, transport)
			if _, err := io.WriteString(conn, "\r\n"); err != nil {
				t.Fatal(err)
			}
			sent := pacingTransportWrite(t, conn, "PAUSE 120\r\n")
			release := gate.next(t)
			s.Stop()
			close(release)
			waitConfigurationTest(t, done)
			if client.readPauseUntilUnixNano.Load() != 0 || s.GetClientCount() != 0 {
				t.Fatal("shutdown dispatched a pending command or retained its session")
			}
			<-sent
		})
	}
}

func TestCommandPacingReplacementCancelsWaitAndStartsFull(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s, old, conn, _, done, gate := startPacingTransportSession(t, transport)
			if _, err := io.WriteString(conn, "\r\n"); err != nil {
				t.Fatal(err)
			}
			sent := pacingTransportWrite(t, conn, "PAUSE 120\r\n")
			release := gate.next(t)

			_, replacement, replacementDone := startHandshakeTranscriptSession(t, s)
			t.Cleanup(func() { closeHandshakeTranscriptSession(t, replacement, replacementDone) })
			if err := replacement.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			replacementReader := bufio.NewReader(replacement)
			readMachineEchoCoverageLiteral(t, replacementReader, "login: ")
			if _, err := io.WriteString(replacement, "W1ABC-1\r\n"); err != nil {
				t.Fatal(err)
			}
			readMachineEchoCoverageLiteral(t, replacementReader, "ready\r\n")
			close(release)
			waitConfigurationTest(t, done)
			if old.readPauseUntilUnixNano.Load() != 0 {
				t.Fatal("replaced session dispatched its waiting command")
			}
			<-sent
			if _, err := io.WriteString(replacement, "GET YAML SETTINGS SCHEMA 6 ID replacement\r\n"); err != nil {
				t.Fatal(err)
			}
			if frame := readMachineTestFrame(t, replacementReader); !strings.Contains(frame, "request_id: replacement\r\n") {
				t.Fatalf("replacement GET did not complete: %q", frame)
			}
			if got := s.CommandPacingWaits(); got != 1 {
				t.Fatalf("replacement did not start with its own full bucket: waits=%d", got)
			}
		})
	}
}

func TestCommandPacingActiveTimerCancellation(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		for _, action := range []string{"shutdown", "replacement", "client_interrupt"} {
			t.Run(transport+"/"+action, func(t *testing.T) {
				started := make(chan struct{}, 1)
				s, old, conn, _, done, gate := startPacingTransportSession(t, transport, func(*Client) {
					select {
					case started <- struct{}{}:
					default:
					}
				})
				if _, err := io.WriteString(conn, "\r\n"); err != nil {
					t.Fatal(err)
				}
				sent := pacingTransportWrite(t, conn, "PAUSE 120\r\n")
				release := gate.next(t)
				// The fake clock remains frozen. Releasing the hook therefore starts
				// a real timer rather than making the pending command eligible.
				close(release)
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("pacing timer was not installed")
				}
				switch action {
				case "shutdown":
					s.Stop()
				case "client_interrupt":
					old.interrupt()
				case "replacement":
					_, replacement, replacementDone := startHandshakeTranscriptSession(t, s)
					t.Cleanup(func() { closeHandshakeTranscriptSession(t, replacement, replacementDone) })
					if err := replacement.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
						t.Fatal(err)
					}
					replacementReader := bufio.NewReader(replacement)
					readMachineEchoCoverageLiteral(t, replacementReader, "login: ")
					if _, err := io.WriteString(replacement, "W1ABC-1\r\n"); err != nil {
						t.Fatal(err)
					}
					readMachineEchoCoverageLiteral(t, replacementReader, "ready\r\n")
				}
				waitConfigurationTest(t, done)
				if old.readPauseUntilUnixNano.Load() != 0 {
					t.Fatal("canceled timer dispatched the pending command")
				}
				<-sent
				if action != "replacement" && s.GetClientCount() != 0 {
					t.Fatal("canceled session remains registered")
				}
			})
		}
	}
}

func TestCommandPacingDisabledAllowsBurst(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s, _, conn, reader, _, _ := startPacingTransportSessionAtRate(t, transport, 0)
			var pipeline strings.Builder
			for i := range 35 {
				fmt.Fprintf(&pipeline, "GET YAML SETTINGS SCHEMA 6 ID off-%d\r\n", i)
			}
			sent := pacingTransportWrite(t, conn, pipeline.String())
			for i := range 35 {
				frame := readMachineTestFrame(t, reader)
				if want := fmt.Sprintf("request_id: off-%d\r\n", i); !strings.Contains(frame, want) {
					t.Fatalf("disabled pacing reordered or lost request %d: %q", i, frame)
				}
			}
			if err := <-sent; err != nil {
				t.Fatalf("write burst: %v", err)
			}
			if got := s.CommandPacingWaits(); got != 0 {
				t.Fatalf("disabled pacing reported %d waits", got)
			}
		})
	}
}

func TestCommandPacingSeparateSessionsHaveIndependentAllowances(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s, _, first, firstReader, _, gate := startPacingTransportSession(t, transport)
			if _, err := io.WriteString(first, "\r\n"); err != nil {
				t.Fatal(err)
			}
			firstSent := pacingTransportWrite(t, first, "GET YAML SETTINGS SCHEMA 6 ID first\r\n")
			release := gate.next(t)

			_, second, secondDone := startHandshakeTranscriptSession(t, s)
			t.Cleanup(func() { closeHandshakeTranscriptSession(t, second, secondDone) })
			if err := second.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			secondReader := bufio.NewReader(second)
			readMachineEchoCoverageLiteral(t, secondReader, "login: ")
			if _, err := io.WriteString(second, "W1ABC-2\r\n"); err != nil {
				t.Fatal(err)
			}
			readMachineEchoCoverageLiteral(t, secondReader, "ready\r\n")
			if _, err := io.WriteString(second, "GET YAML SETTINGS SCHEMA 6 ID second\r\n"); err != nil {
				t.Fatal(err)
			}
			if frame := readMachineTestFrame(t, secondReader); !strings.Contains(frame, "request_id: second\r\n") {
				t.Fatalf("second session could not use its initial credit: %q", frame)
			}
			if got := s.CommandPacingWaits(); got != 1 {
				t.Fatalf("second session shared the first bucket: waits=%d", got)
			}
			gate.advanceAndRelease(release)
			if frame := readMachineTestFrame(t, firstReader); !strings.Contains(frame, "request_id: first\r\n") {
				t.Fatalf("first session did not resume: %q", frame)
			}
			if err := <-firstSent; err != nil {
				t.Fatalf("write first request: %v", err)
			}
		})
	}
}

func TestCommandPacingMachineHeaderTerminalBoundaries(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		for _, tc := range []struct {
			name, header string
			complete     bool
		}{
			{"complete", "PUT YAML CONFIG EXTRA\r\n", true},
			{"reader_error", "PUT YAML CONFIG _\r\n", false},
		} {
			t.Run(transport+"/"+tc.name, func(t *testing.T) {
				s, _, conn, reader, done, gate := startPacingTransportSession(t, transport)
				if _, err := io.WriteString(conn, "\r\n"); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				sent := pacingTransportWrite(t, conn, tc.header+"---\r\nRESUME\r\n...\r\n")
				if tc.complete {
					release := gate.next(t)
					if got := s.CommandPacingWaits(); got != 1 {
						t.Fatalf("complete header waits=%d, want 1", got)
					}
					gate.advanceAndRelease(release)
				}
				if frame := readMachineTestFrame(t, reader); !strings.Contains(frame, "code: invalid_header\r\n") {
					t.Fatalf("terminal header response=%q", frame)
				}
				if _, err := reader.ReadByte(); err == nil {
					t.Fatal("terminal header allowed further responses")
				}
				waitConfigurationTest(t, done)
				if !tc.complete && s.CommandPacingWaits() != 0 {
					t.Fatal("terminal reader error entered pacing")
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("terminal failure changed saved profile: %v", err)
				}
				<-sent // The closing server may stop the write before the payload ends.
			})
		}
	}
}
