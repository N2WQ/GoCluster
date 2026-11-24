// File role: Opt-in timing qualification for silent loss of a real Telnet TCP path.
// An isolated peer drops packets on its own established socket and leaves that
// socket open until the server releases its session slot. This measures kernel
// failure detection and normal server teardown without changing network policy.
package telnet

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// The external qualification peer and listen IP are explicit so the ordinary
// Windows and Linux lanes never start a listener reachable outside loopback.
func TestPhase3SilentTelnetFailureQualification(t *testing.T) {
	clientPath := os.Getenv("GOCLUSTER_A38_CLIENT")
	if clientPath == "" {
		t.Skip("set GOCLUSTER_A38_CLIENT to the isolated TCP qualification peer")
	}
	listenIP := os.Getenv("GOCLUSTER_A38_LISTEN_IP")
	peerInterface := os.Getenv("GOCLUSTER_A38_PEER_INTERFACE")
	if addr := net.ParseIP(listenIP); addr == nil || addr.To4() == nil || addr.IsUnspecified() || peerInterface == "" {
		t.Fatal("set GOCLUSTER_A38_LISTEN_IP to an explicit test IPv4 address and GOCLUSTER_A38_PEER_INTERFACE to its peer interface")
	}
	s := newSessionCeilingServer(t, 1)
	s.keepaliveInterval = 120 * time.Second // Shipped Telnet application keepalive.
	removed := make(chan time.Time, 1)
	s.testAfterClientRemoval = func(*Client) { removed <- time.Now() }
	var listener net.Listener
	port := "0"
	if runtime.GOOS != "windows" {
		var err error
		listener, err = (&net.ListenConfig{}).Listen(t.Context(), "tcp4", net.JoinHostPort(listenIP, "0"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		port = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	}
	args := []string{clientPath, listenIP, port, peerInterface}
	program := "python3"
	if runtime.GOOS == "windows" {
		distro := os.Getenv("GOCLUSTER_A38_WSL_DISTRO")
		if distro == "" {
			t.Fatal("set GOCLUSTER_A38_WSL_DISTRO to the Linux peer distribution")
		}
		program = "wsl.exe"
		args = append([]string{"-d", distro, "-u", "root", "--", "python3"}, args...)
	}
	peer := exec.CommandContext(t.Context(), program, args...)
	peer.Stderr = os.Stderr
	stdin, err := peer.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := peer.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = peer.Wait()
	})

	decoder := json.NewDecoder(bufio.NewReader(stdout))
	read := func(want string) phase3BlackholeObservation {
		t.Helper()
		var obs phase3BlackholeObservation
		if err := decoder.Decode(&obs); err != nil || obs.Phase != want {
			t.Fatalf("peer phase %q, want %q: %v", obs.Phase, want, err)
		}
		return obs
	}
	if runtime.GOOS == "windows" {
		listener = phase3DialWindowsPeer(t, listenIP, read("listening").Port)
	}
	s.startWithListener(listener)
	t.Cleanup(s.Stop)
	read("logged in")
	waitSessionCeiling(t, func() bool { return s.GetClientCount() == 1 })
	begin := time.Now()
	if _, err := stdin.Write([]byte("drop\n")); err != nil {
		t.Fatal(err)
	}
	read("dropping")
	enabled := time.Now()
	t.Logf("Dropping TCP packets on the isolated peer of %s; server platform %s/%s", listener.Addr(), runtime.GOOS, runtime.GOARCH)

	var retired time.Time
	select {
	case retired = <-removed:
	case <-time.After(25 * time.Minute):
		t.Fatal("silent loss did not release the Telnet slot in 25 minutes")
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	if _, err := stdin.Write([]byte("report\n")); err != nil {
		t.Fatal(err)
	}
	obs := read("report")
	if obs.Inbound < 2 || obs.PeerResetOrFinish != 0 || obs.PeerACK != 0 {
		t.Fatalf("not a verified silent drop: %+v", obs)
	}
	if s.GetClientCount() != 0 {
		t.Fatal("retired session still occupies capacity")
	}
	t.Logf("A38 %s: slot released %s to %s after drop, %d server TCP packets, no peer ACK/FIN/RST", runtime.GOOS, retired.Sub(enabled).Round(time.Millisecond), retired.Sub(begin).Round(time.Millisecond), obs.Inbound)
}

// Use outbound TCP setup on Windows, so the fixture needs no host inbound rule.
// GoCluster still receives a real Windows TCPConn through its normal accept loop
// and uses the same prelogin, keepalive, read and teardown code.
func phase3DialWindowsPeer(t *testing.T, host string, port int) net.Listener {
	t.Helper()
	if port <= 0 || port > 65535 {
		t.Fatalf("invalid peer port %d", port)
	}
	dial := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dial.DialContext(t.Context(), "tcp4", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	t.Log("Native Windows opened the TCP socket; GoCluster runs the Telnet server protocol on it")
	return &phase3OneTCPListener{conn: conn, address: conn.LocalAddr(), closed: make(chan struct{})}
}

type phase3OneTCPListener struct {
	conn    net.Conn // Consumed by the only accept loop.
	address net.Addr
	closed  chan struct{}
}

func (l *phase3OneTCPListener) Accept() (net.Conn, error) {
	if l.conn != nil {
		conn := l.conn
		l.conn = nil
		return conn, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *phase3OneTCPListener) Close() error {
	close(l.closed)
	return nil
}

func (l *phase3OneTCPListener) Addr() net.Addr { return l.address }

type phase3BlackholeObservation struct {
	Phase             string `json:"phase"`
	Port              int    `json:"port"`
	Inbound           int    `json:"inbound"`
	PeerACK           int    `json:"peer_ack"`
	PeerResetOrFinish int    `json:"peer_reset_or_finish"`
}
