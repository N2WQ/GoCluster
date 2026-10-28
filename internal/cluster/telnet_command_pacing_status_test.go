package cluster

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	"dxcluster/telnet"
)

func TestNetworkStatusIncludesCommandPacingWaits(t *testing.T) {
	for name, server := range map[string]*telnet.Server{
		"no server": nil,
		"server":    {},
	} {
		t.Run(name, func(t *testing.T) {
			got := formatNetworkLines(server, nil)
			if len(got) < 2 || got[0] != "[yellow]Telnet[-]: 0 clients   [yellow]Drops[-]: Q0 C0 W0   [yellow]Prelogin[-]: A0 G0 R0 C0 T0" ||
				got[1] != "[yellow]Command pacing waits[-]: 0" {
				t.Fatalf("network status missing original metrics or command pacing count: %q", got)
			}
		})
	}
}

func TestNetworkStatusShowsRealCommandPacingWait(t *testing.T) {
	t.Chdir(t.TempDir())
	port := runtimeQualificationPort(t)
	s := telnet.NewServer(telnet.ServerOptions{
		Port: port, LoginPrompt: "login: ", LoginGreeting: "ready\n",
		HandshakeMode: "none", Transport: "native", EchoMode: "off",
		CommandRatePerSecond: 1, CommandBurst: 1,
		BroadcastWorkers: 1, BroadcastQueue: 8, WorkerQueue: 8, ClientBuffer: 8,
	}, nil)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	var conn net.Conn
	t.Cleanup(func() {
		if conn != nil {
			_ = conn.Close()
		}
		s.Stop()
		deadline := time.Now().Add(3 * time.Second)
		for s.GetClientCount() != 0 {
			if time.Now().After(deadline) {
				t.Error("Telnet client did not unregister")
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	var err error
	conn, err = (&net.Dialer{Timeout: 5 * time.Second}).DialContext(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	prompt := make([]byte, len("login: "))
	if _, err := io.ReadFull(reader, prompt); err != nil || string(prompt) != "login: " {
		t.Fatalf("login prompt %q: %v", prompt, err)
	}
	if _, err := io.WriteString(conn, "DL1ABC\r\n"); err != nil {
		t.Fatal(err)
	}
	if greeting, err := reader.ReadString('\n'); err != nil || greeting != "ready\r\n" {
		t.Fatalf("greeting %q: %v", greeting, err)
	}
	// Several keepalives ensure a wait even if the test host briefly stalls.
	if _, err := io.WriteString(conn, strings.Repeat("\r\n", 10)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for s.CommandPacingWaits() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("pipelined keepalives never entered a pacing wait")
		}
		time.Sleep(time.Millisecond)
	}
	got := formatNetworkLines(s, s.ListClientCallsigns())
	if len(got) < 2 || !strings.Contains(got[0], "[yellow]Telnet[-]: 1 clients") ||
		!strings.Contains(got[0], "[yellow]Drops[-]: Q0 C0 W0") || !strings.Contains(got[0], "[yellow]Prelogin[-]:") ||
		!regexp.MustCompile(`^\[yellow\]Command pacing waits\[-\]: [1-9][0-9]*$`).MatchString(got[1]) {
		t.Fatalf("network status missing original metrics or nonzero pacing count: %q", got)
	}
}
