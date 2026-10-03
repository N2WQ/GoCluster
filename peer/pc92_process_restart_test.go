package peer

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

// The helper starts a fresh manager in an actual child process, with no clock
// seam or retained Go state. The parent owns its TCP peer and controls a crash.
// This isolates process restart from the deterministic same-second test while
// using the production startup wait, dialer, handshake and publication workers.
func TestPC92RestartProcessHelper(t *testing.T) {
	address := os.Getenv("GOCLUSTER_PC92_RESTART_ADDRESS")
	if address == "" {
		t.Skip("subprocess helper")
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.PeeringConfig{
		NodeVersion: "5457", NodeBuild: "633", PC92Bitmap: 5, HopCount: 99,
		WriteQueueSize: 128, MaxLineLength: 65536, PC92MaxBytes: 65536,
		Timeouts: config.PeeringTimeouts{LoginSeconds: 10, InitSeconds: 10},
		Backoff:  config.PeeringBackoff{BaseMS: 2000, MaxMS: 300000},
		Peers: []config.PeeringPeer{{Enabled: true, Host: host, Port: port,
			RemoteCallsign: "GB7REF", LoginCallsign: "N0CALL", PreferPC9x: true,
			Family: config.PeeringPeerFamilyDXSpider, Direction: config.PeeringPeerDirectionOutbound}},
	}
	m, err := NewManager(completeProtocolTestConfig(cfg, "N0CALL"), "N0CALL", nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.SetBuildIdentity("restart-test", "", "test", "2026-10-01", "go1.26"); err != nil {
		t.Fatal(err)
	}
	user := LocalUser{SessionID: 1, Login: os.Getenv("GOCLUSTER_PC92_RESTART_USER"), IP: "192.0.2.7"}
	m.SetMembershipProvider(func() LocalMembership {
		return LocalMembership{Revision: 1, RawCount: 1, Complete: true, Users: []LocalUser{user}}
	})
	if err := m.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	_, _ = io.Copy(io.Discard, os.Stdin)
}

func TestDXSpiderReferenceActualSenderProcessRestart(t *testing.T) {
	reference, _ := startDXReference(t, false, "N0CALL")
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var previousStamp float64
	var firstPID int
	for generation, call := range []string{"K1OLD", "K2NEW"} {
		func() {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, executable, "-test.run=^TestPC92RestartProcessHelper$", "-test.timeout=15s")
			child.Env = append(os.Environ(), "GOCLUSTER_PC92_RESTART_ADDRESS="+listener.Addr().String(), "GOCLUSTER_PC92_RESTART_USER="+call)
			var output bytes.Buffer
			child.Stdout, child.Stderr = &output, &output
			input, err := child.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				// Abrupt exit intentionally omits shutdown D. The external receiver
				// retains its origin watermark/membership throughout both processes.
				_ = child.Process.Kill()
				_ = input.Close()
				_ = child.Wait()
				if t.Failed() {
					t.Logf("child process output: %s", output.String())
				}
			}()
			if generation == 0 {
				firstPID = child.Process.Pid
			} else if child.Process.Pid == firstPID {
				t.Fatal("sender process identity was not replaced")
			}
			if err := listener.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			conn, err := listener.AcceptTCP()
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(conn)
			read := func() string {
				t.Helper()
				line, err := reader.ReadString('\n')
				if err != nil {
					t.Fatal(err)
				}
				return strings.TrimSpace(line)
			}
			if got := read(); got != "N0CALL" {
				t.Fatalf("outbound login=%q", got)
			}
			if _, err := io.WriteString(conn, "PC18^DXSpider Version: 1.57 Build: 633 [pc9x 91]^5457^\r\n"); err != nil {
				t.Fatal(err)
			}
			completed, recoveredC, recoveredA := false, false, false
			var result dxReferenceResult
			for frames := 0; frames < 12 && !recoveredA; frames++ {
				line := read()
				if line == "PC20^" {
					if _, err := io.WriteString(conn, "PC22^\r\n"); err != nil {
						t.Fatal(err)
					}
					completed = true
					continue
				}
				if !strings.HasPrefix(line, "PC92^N0CALL^") {
					t.Fatalf("unexpected sender frame %q", line)
				}
				fields := strings.Split(line, "^")
				stamp, err := strconv.ParseFloat(fields[2], 64)
				if err != nil {
					t.Fatal(err)
				}
				if generation > 0 && stamp == previousStamp {
					t.Fatalf("fresh process reused retained timestamp %s", fields[2])
				}
				result = reference.frame(line, "K1OLD", "K2NEW")
				referenceWatermark(t, result, fields[2])
				previousStamp = stamp
				if completed && fields[3] == "C" {
					recoveredC = true
				}
				if completed && recoveredC && fields[3] == "A" {
					recoveredA = true
				}
			}
			if !recoveredA {
				t.Fatal("fresh process did not publish complete C followed by A")
			}
			referenceOnlyUser(t, result, call, "192.0.2.7")
			t.Logf("generation=%d pid=%d actual receiver membership=%s watermark=%s", generation, child.Process.Pid, call, fmt.Sprint(result.Routes["N0CALL"]["lastid"]))
		}()
	}
}
