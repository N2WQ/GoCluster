package telnet

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

// The writer owns the socket while this marker is held; queuing a duplicate
// notice cannot close the old session until the writer is allowed to flush.
type retirementBlockedConn struct {
	net.Conn
	blocked chan struct{}
	release chan struct{}
	stop    chan struct{}
	once    sync.Once
}

func (c *retirementBlockedConn) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte("BLOCKMARKER")) {
		c.once.Do(func() { close(c.blocked) })
		select {
		case <-c.release:
		case <-c.stop:
		}
	}
	return c.Conn.Write(data)
}

func retirementSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestCommandPacingRetirementPreservesDuplicateWriter(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		for _, scenario := range []string{"notice", "shutdown", "queue_full", "membership_notice", "membership_shutdown", "list_notice", "list_shutdown"} {
			t.Run(transport+"/"+scenario, func(t *testing.T) {
				stop := make(chan struct{})
				reporterEntered, reporterRelease := make(chan struct{}), make(chan struct{})
				timerStarted, timerRelease := make(chan struct{}), make(chan struct{})
				retirementObserved := make(chan struct{}, 1)
				s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
					opts.Transport, opts.EchoMode, opts.LoginGreeting = transport, "off", "ready\n"
					opts.CommandRatePerSecond, opts.CommandBurst = 1_000_000_000, 1
					opts.DuplicateLoginMsg = "duplicate notice"
					opts.ControlQueue = 1
					opts.NoiseModel = pathreliability.DefaultConfig().NoiseModel()
					opts.DefaultDedupePolicy = "FAST"
					opts.DedupeFastEnabled, opts.DedupeMedEnabled, opts.DedupeSlowEnabled = true, true, true
					opts.ConnectionReporter = func(event ConnectionEvent) {
						if event.Action == "evict" && !strings.HasPrefix(scenario, "membership_") && !strings.HasPrefix(scenario, "list_") {
							close(reporterEntered)
							select {
							case <-reporterRelease:
							case <-stop:
							}
						}
					}
				})
				clock := &pacingWaitGate{start: time.Now()}
				s.testCommandPacingNow = clock.now
				s.testCommandPacingTimerStarted = func(*Client) {
					close(timerStarted)
					select {
					case <-timerRelease:
					case <-stop:
					}
				}
				s.testCommandPacingRetired = func(*Client) {
					select {
					case retirementObserved <- struct{}{}:
					default:
					}
				}

				serverConn, oldConn := net.Pipe()
				blocked := &retirementBlockedConn{Conn: serverConn, blocked: make(chan struct{}), release: make(chan struct{}), stop: stop}
				oldDone := make(chan struct{})
				go func() { s.handleClient(blocked, nil); close(oldDone) }()
				var replacement net.Conn
				var replacementDone <-chan struct{}
				t.Cleanup(func() {
					close(stop)
					_ = oldConn.Close()
					_ = serverConn.Close()
					if replacement != nil {
						_ = replacement.Close()
					}
					waitConfigurationTest(t, oldDone)
					if replacementDone != nil {
						waitConfigurationTest(t, replacementDone)
					}
				})
				if err := oldConn.SetDeadline(time.Now().Add(12 * time.Second)); err != nil {
					t.Fatal(err)
				}
				oldReader := bufio.NewReader(oldConn)
				readMachineEchoCoverageLiteral(t, oldReader, "login: ")
				if _, err := io.WriteString(oldConn, "W1ABC-1\r\n"); err != nil {
					t.Fatal(err)
				}
				readMachineEchoCoverageLiteral(t, oldReader, "ready\r\n")
				s.clientsMutex.RLock()
				old := s.clients["W1ABC-1"]
				s.clientsMutex.RUnlock()
				if old == nil {
					t.Fatal("old session not registered")
				}
				if err := old.Send("BLOCKMARKER\n"); err != nil {
					t.Fatal(err)
				}
				retirementSignal(t, blocked.blocked, "old socket write to block")
				if scenario == "queue_full" {
					if err := old.Send("queue filler\n"); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := io.WriteString(oldConn, "\r\n"); err != nil {
					t.Fatal(err)
				}
				sent := pacingTransportWrite(t, oldConn, "PAUSE 120\r\n")
				retirementSignal(t, timerStarted, "old pacing timer")
				preNotice := strings.HasPrefix(scenario, "membership_") || strings.HasPrefix(scenario, "list_")
				if preNotice {
					var once atomic.Bool
					callback := func() {
						if !once.CompareAndSwap(false, true) {
							return
						}
						close(reporterEntered)
						select {
						case <-reporterRelease:
						case <-stop:
						}
					}
					if strings.HasPrefix(scenario, "membership_") {
						s.SetPeerMembershipListener(callback)
					} else {
						s.SetClientListListener(callback)
					}
				}

				_, replacement, replacementDone = startHandshakeTranscriptSession(t, s)
				if err := replacement.SetDeadline(time.Now().Add(8 * time.Second)); err != nil {
					t.Fatal(err)
				}
				replacementReader := bufio.NewReader(replacement)
				readMachineEchoCoverageLiteral(t, replacementReader, "login: ")
				if _, err := io.WriteString(replacement, "W1ABC-1\r\n"); err != nil {
					t.Fatal(err)
				}
				retirementSignal(t, reporterEntered, "blocked replacement callback")
				s.clientsMutex.RLock()
				current := s.clients["W1ABC-1"]
				s.clientsMutex.RUnlock()
				if current == nil || current == old {
					t.Fatal("replacement did not acquire registry ownership")
				}
				retirementSignal(t, old.retired, "old session retirement")
				if preNotice && len(old.controlChan) != 0 {
					t.Fatal("duplicate notice was queued before the callback was released")
				}
				if scenario != "queue_full" {
					select {
					case <-old.done:
						t.Fatal("old socket closed before its blocked notice")
					default:
					}
				} else {
					retirementSignal(t, old.done, "queue-full interruption")
				}
				path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
				profile, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				// The 1 ns timer is already installed. Hold the observer long enough
				// to make its real deadline pass, then make its credit due as well.
				<-time.After(10 * time.Millisecond)
				clock.elapsed.Add(int64(time.Second))
				close(timerRelease)
				if scenario != "queue_full" {
					retirementSignal(t, retirementObserved, "pacer retirement observation")
					select {
					case <-oldDone:
						t.Fatal("reader closed the socket before the notice could flush")
					case <-old.done:
						t.Fatal("old socket closed while writer remained blocked")
					default:
					}
				}
				if old.readPauseUntilUnixNano.Load() != 0 {
					t.Fatal("retired session dispatched its waiting PAUSE")
				}

				if strings.HasSuffix(scenario, "shutdown") {
					s.Stop()
					retirementSignal(t, oldDone, "retired reader shutdown")
					close(blocked.release)
					close(reporterRelease)
					retirementSignal(t, replacementDone, "replacement shutdown")
				} else {
					close(reporterRelease)
					readMachineEchoCoverageLiteral(t, replacementReader, "ready\r\n")
					close(blocked.release)
					if strings.HasSuffix(scenario, "notice") {
						readMachineEchoCoverageLiteral(t, oldReader, "BLOCKMARKER\r\nduplicate notice\r\n")
						if _, err := oldReader.ReadByte(); err == nil {
							t.Fatal("duplicate notice did not close the old socket")
						}
					}
					retirementSignal(t, oldDone, "old handler retirement")
					if _, err := io.WriteString(replacement, "GET YAML SETTINGS SCHEMA 5 ID replacement\r\n"); err != nil {
						t.Fatal(err)
					}
					if frame := readMachineTestFrame(t, replacementReader); !strings.Contains(frame, "request_id: replacement\r\n") {
						t.Fatalf("replacement is not usable: %q", frame)
					}
				}
				<-sent
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(profile, after) {
					t.Fatalf("retired session changed replacement profile: %v", err)
				}
			})
		}
	}
}
