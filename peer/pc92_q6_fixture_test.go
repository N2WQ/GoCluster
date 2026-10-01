//go:build qualification

package peer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/filter"
	"dxcluster/spot"
	"dxcluster/telnet"
)

// Q6 composes the real TCP listeners, session owners, and telnet membership
// provider. It does not substitute for the complete cluster pipeline in Q1-Q3.
// Its only fault seam changes authority UTC; all I/O and test durations remain
// real. The smaller preflight is explicitly ineligible as qualification evidence.
func q6Profile(t *testing.T) bool {
	t.Helper()
	switch os.Getenv("GOCLUSTER_PC92_Q6_PROFILE") {
	case "qualification":
		if os.Getenv("DXSPIDER_ROOT") == "" || os.Getenv("DXSPIDER_PERL") == "" {
			t.Fatal("Q6 qualification requires the actual pinned DXSpider component receiver")
		}
		return true
	case "preflight":
		t.Log("DIAGNOSTIC ONLY: reduced repetition/duration; not Q6 qualification")
		return false
	default:
		t.Skip("opt-in Q6: scripts/pc92-q6-qualification.ps1")
		return false
	}
}

type q6Wire struct {
	line string
	at   time.Time
}

type q6Peer struct {
	conn    *net.TCPConn
	reader  *bufio.Reader
	events  chan q6Wire
	done    chan struct{}
	spots   atomic.Int64
	error   atomic.Bool
	start   sync.Once
	call    string
	hold    atomic.Bool
	paused  chan struct{}
	resume  chan struct{}
	release sync.Once
}

// The external observer holds at most 256 bounded lines. Transit PC92 and spot
// payloads are counted rather than retained. Overflow invalidates the run.
func (p *q6Peer) monitor() {
	p.start.Do(func() {
		go func() {
			defer close(p.done)
			var partial string
			for {
				if p.hold.Load() {
					close(p.paused)
					<-p.resume
					p.hold.Store(false)
					_ = p.conn.SetReadDeadline(time.Time{})
				}
				bytes, err := p.reader.ReadSlice('\n')
				line := partial + string(bytes)
				partial = ""
				if len(line) > MaxPeerFrameBytes+2 {
					p.error.Store(true)
					return
				}
				if err != nil {
					if err == bufio.ErrBufferFull {
						p.error.Store(true)
					}
					if networkErr, ok := err.(net.Error); ok && networkErr.Timeout() && p.hold.Load() {
						partial = line
						continue
					}
					return
				}
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "PC11^") || strings.HasPrefix(line, "PC61^") || strings.HasPrefix(line, "PC26^") {
					p.spots.Add(1)
					continue
				}
				if strings.HasPrefix(line, "PC92^") && !strings.HasPrefix(line, "PC92^N0CALL^") && !strings.HasPrefix(line, "PC92^GB7PEND^") {
					continue
				}
				select {
				case p.events <- q6Wire{line, time.Now()}:
				default:
					p.error.Store(true)
					return
				}
			}
		}()
	})
}

func (p *q6Peer) send(t *testing.T, line string) {
	t.Helper()
	if err := p.conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(p.conn, line+"\r\n"); err != nil {
		t.Fatalf("send to %s: %v", p.call, err)
	}
}

func (p *q6Peer) await(t *testing.T, predicate func(string) bool, timeout time.Duration) q6Wire {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case event := <-p.events:
			if predicate(event.line) {
				return event
			}
		case <-p.done:
			t.Fatalf("%s closed before expected wire event (observer overflow=%v)", p.call, p.error.Load())
		case <-timer.C:
			t.Fatalf("%s timed out waiting for wire event", p.call)
		}
	}
}

func q6Action(action string) func(string) bool {
	return func(line string) bool {
		fields := strings.Split(line, "^")
		return len(fields) > 3 && fields[0] == "PC92" && fields[1] == "N0CALL" && fields[3] == action
	}
}

func (p *q6Peer) recovery(t *testing.T) (q6Wire, q6Wire) {
	t.Helper()
	ready := p.await(t, func(line string) bool { return line == "PC22^" }, 5*time.Second)
	c := p.await(t, q6Action("C"), 5*time.Second)
	a := p.await(t, func(line string) bool { return q6Action("A")(line) || q6Action("K")(line) }, 5*time.Second)
	if !q6Action("A")(a.line) || a.at.Before(c.at) || a.at.Sub(ready.at) > 5*time.Second {
		t.Fatalf("mandatory ordered C/A recovery exceeded 5s or K overtook A: ready=%s C=%+v next=%+v", ready.at, c, a)
	}
	return c, a
}

type q6Local struct {
	conn net.Conn
	done chan struct{}
}

type q6Rig struct {
	t         *testing.T
	m         *Manager
	server    *telnet.Server
	peerAddr  string
	localAddr string
	peers     []*q6Peer
	locals    []*q6Local
	primary   *q6Peer
	alternate *q6Peer
	legacy    *q6Peer
	seed      q6Wire
	ingested  atomic.Int64
	dropped   atomic.Int64
	counts    []atomic.Uint32
	badToken  atomic.Bool
}

// The opt-in wrapper supplies one process-lifetime persistence directory. The
// telnet component's Stop does not join its saveFilter handlers, so restoring a
// global directory between cases would race with an earlier unregister. Q6 runs
// alone in its test process; ordinary tests never execute this initializer.
var q6UserDirectoryOnce sync.Once

func q6Port(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func newQ6Rig(t *testing.T, zeroTimers bool, maxBytes, tokenCount int) *q6Rig {
	t.Helper()
	r := &q6Rig{t: t, counts: make([]atomic.Uint32, tokenCount)}
	q6UserDirectoryOnce.Do(func() {
		directory := os.Getenv("GOCLUSTER_PC92_Q6_USERS")
		if directory == "" {
			var err error
			directory, err = os.MkdirTemp("", "gocluster-q6-users-")
			if err != nil {
				t.Fatal(err)
			}
		}
		filter.UserDataDir = directory
		t.Logf("Q6 isolated process-lifetime user data: %s", directory)
	})
	localPort, peerPort := q6Port(t), q6Port(t)
	r.localAddr, r.peerAddr = fmt.Sprintf("127.0.0.1:%d", localPort), fmt.Sprintf("127.0.0.1:%d", peerPort)
	r.server = telnet.NewServer(telnet.ServerOptions{Port: localPort, ClusterCall: "N0CALL", MaxConnections: 64,
		LoginPrompt: "login:", HandshakeMode: "native", Transport: "native", BroadcastWorkers: 1,
		BroadcastQueue: 128, WorkerQueue: 128, ClientBuffer: 128, ControlQueue: 32,
		LoginTimeout: time.Minute, ReadIdleTimeout: time.Hour}, nil)
	if err := r.server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.server.Stop)
	cfg := config.PeeringConfig{Enabled: true, ListenPort: peerPort, LocalCallsign: "N0CALL", NodeVersion: "5457", NodeBuild: "633",
		LegacyVersion: "5401", PC92Bitmap: 5, HopCount: 99, MaxLineLength: 65536, PC92MaxBytes: maxBytes,
		WriteQueueSize: 128, KeepaliveSeconds: 600, ConfigSeconds: 1800, Timeouts: config.PeeringTimeouts{LoginSeconds: 60, InitSeconds: 60, IdleSeconds: 3600}}
	if zeroTimers {
		cfg.KeepaliveSeconds, cfg.ConfigSeconds = 0, 0
	}
	for _, call := range []string{"GB7REF", "GB7ALT", "GB7LEG", "GB7PEND"} {
		cfg.Peers = append(cfg.Peers, config.PeeringPeer{Enabled: true, RemoteCallsign: call, PreferPC9x: true,
			Family: config.PeeringPeerFamilyDXSpider, Direction: config.PeeringPeerDirectionInbound})
	}
	ingest := make(chan *spot.Spot, 4096)
	manager, err := NewManager(cfg, "N0CALL", ingest, 600, func(string) { r.dropped.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	r.m = manager
	if err := manager.SetBuildIdentity("v7-q6", "91abcdef", "2026-10-01", "false", "go1.26"); err != nil {
		t.Fatal(err)
	}
	manager.SetMembershipProvider(func() LocalMembership {
		current := r.server.CurrentPeerMembership()
		result := LocalMembership{Revision: current.Revision, Complete: current.Complete, RawCount: current.RawCount}
		for _, user := range current.Users {
			result.Users = append(result.Users, LocalUser{SessionID: user.SessionID, Login: user.Login, IP: user.IP})
		}
		return result
	})
	r.server.SetPeerMembershipListener(manager.NotifyMembershipChanged)
	ctx, cancel := context.WithCancel(context.Background())
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			select {
			case value := <-ingest:
				if tokenCount > 0 {
					index, valid := q6Token(value.Comment)
					if !valid || index >= len(r.counts) {
						r.badToken.Store(true)
					} else {
						r.counts[index].Add(1)
					}
				}
				r.ingested.Add(1)
			case <-ctx.Done():
				return
			}
		}
	}()
	t.Cleanup(func() {
		for _, peer := range r.peers {
			_ = peer.conn.Close()
			peer.release.Do(func() { close(peer.resume) })
			peer.monitor()
		}
		for _, local := range r.locals {
			_ = local.conn.Close()
		}
		manager.Stop()
		cancel()
		<-consumerDone
		for _, peer := range r.peers {
			select {
			case <-peer.done:
			case <-time.After(3 * time.Second):
				t.Error("external peer reader did not join")
			}
			if peer.error.Load() {
				t.Error("bounded external observer overflowed or saw oversized output")
			}
		}
		for _, local := range r.locals {
			<-local.done
		}
	})
	if err := manager.Start(ctx); err != nil {
		t.Fatal(err)
	}
	r.login("K1USER", "127.0.0.2")
	r.primary = r.connect("GB7REF", true, true)
	r.seed, _ = r.primary.recovery(t)
	r.alternate = r.connect("GB7ALT", true, true)
	r.alternate.recovery(t)
	r.legacy = r.connect("GB7LEG", false, true)
	r.legacy.await(t, func(line string) bool { return line == "PC22^" }, 5*time.Second)
	return r
}

func q6ReadUntil(t *testing.T, reader *bufio.Reader, suffix string) {
	t.Helper()
	var b strings.Builder
	for b.Len() < 65536 {
		value, err := reader.ReadByte()
		if err != nil {
			t.Fatal(err)
		}
		b.WriteByte(value)
		if strings.HasSuffix(b.String(), suffix) {
			return
		}
	}
	t.Fatal("prompt exceeded bounded reader")
}

func (r *q6Rig) connect(call string, pc9x, establish bool) *q6Peer {
	r.t.Helper()
	conn, err := net.DialTimeout("tcp", r.peerAddr, 3*time.Second)
	if err != nil {
		r.t.Fatal(err)
	}
	p := &q6Peer{conn: conn.(*net.TCPConn), reader: bufio.NewReaderSize(conn, 65538), events: make(chan q6Wire, 256), done: make(chan struct{}), call: call,
		paused: make(chan struct{}), resume: make(chan struct{})}
	r.peers = append(r.peers, p)
	_ = p.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	q6ReadUntil(r.t, p.reader, "login:")
	p.send(r.t, call)
	q6ReadUntil(r.t, p.reader, "PC18^")
	q6ReadUntil(r.t, p.reader, "\n")
	_ = p.conn.SetReadDeadline(time.Time{})
	p.monitor()
	capability := ""
	if pc9x {
		capability = " [pc9x 91]"
	}
	p.send(r.t, "PC18^DXSpider Version: 1.57 Build: 633"+capability+"^5457^")
	if establish {
		p.send(r.t, "PC20^")
	}
	return p
}

func (r *q6Rig) login(call, ip string) *q6Local {
	r.t.Helper()
	dialer := net.Dialer{Timeout: 3 * time.Second, LocalAddr: &net.TCPAddr{IP: net.ParseIP(ip)}}
	conn, err := dialer.Dial("tcp", r.localAddr)
	if err != nil {
		r.t.Fatal(err)
	}
	local := &q6Local{conn: conn, done: make(chan struct{})}
	r.t.Cleanup(func() { _ = conn.Close() })
	reader := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	q6ReadUntil(r.t, reader, "login:")
	if _, err := io.WriteString(conn, call+"\r\n"); err != nil {
		r.t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	go func() { defer close(local.done); _, _ = io.Copy(io.Discard, reader) }()
	r.locals = append(r.locals, local)
	r.wait("telnet membership admission", 5*time.Second, func(QualificationState) bool {
		for _, user := range r.server.CurrentPeerMembership().Users {
			if user.Login == call && user.IP == ip {
				return true
			}
		}
		return false
	})
	return local
}

func (r *q6Rig) snapshot() QualificationState {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := r.m.QualificationSnapshot(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	return s
}

func (r *q6Rig) wait(label string, timeout time.Duration, predicate func(QualificationState) bool) QualificationState {
	r.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		s := r.snapshot()
		if predicate(s) {
			return s
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("%s: timed out; last state=%+v", label, s)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (r *q6Rig) closed(peer *q6Peer) {
	r.t.Helper()
	select {
	case <-peer.done:
	case <-time.After(5 * time.Second):
		r.t.Fatalf("%s socket did not close", peer.call)
	}
}

func (r *q6Rig) legacyAlive() {
	r.t.Helper()
	r.legacy.send(r.t, "PC51^N0CALL^GB7LEG^1^")
	r.legacy.await(r.t, func(line string) bool { return line == "PC51^GB7LEG^N0CALL^0^" }, 3*time.Second)
	before := r.ingested.Load()
	r.legacy.send(r.t, q6Spot(0, time.Now()))
	r.wait("legacy spot ingestion remains available", 3*time.Second, func(QualificationState) bool { return r.ingested.Load() == before+1 })
	if r.server.CurrentPeerMembership().RawCount < 1 {
		r.t.Fatal("fault disconnected local users")
	}
}

// Failed retry is observed at the socket, not inferred from a sampled gate.
func (r *q6Rig) refused(call string) {
	r.t.Helper()
	conn, err := net.DialTimeout("tcp", r.peerAddr, 3*time.Second)
	if err != nil {
		r.t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
	reader := bufio.NewReader(conn)
	q6ReadUntil(r.t, reader, "login:")
	_, _ = io.WriteString(conn, call+"\r\nPC18^DXSpider Version: 1.57 [pc9x 91]^5457^\r\nPC20^\r\n")
	for {
		line, err := reader.ReadString('\n')
		if strings.Contains(line, "PC22^") || q6Action("C")(strings.TrimSpace(line)) {
			r.t.Fatalf("gated peer established: %q", line)
		}
		if err != nil {
			if networkErr, ok := err.(net.Error); ok && networkErr.Timeout() {
				r.t.Fatal("gated retry hung instead of closing")
			}
			return
		}
	}
}

func (r *q6Rig) recovered(call string) {
	r.t.Helper()
	p := r.connect(call, true, true)
	c, a := p.recovery(r.t)
	r.verifyRecovery(c, a)
}

func (r *q6Rig) verifyRecovery(c, a q6Wire) {
	r.t.Helper()
	if !strings.Contains(a.line, "K1USER:127.0.0.3") {
		r.t.Fatalf("recovery A omitted current local IP: %s", a.line)
	}
	if os.Getenv("DXSPIDER_ROOT") != "" && os.Getenv("DXSPIDER_PERL") != "" {
		// Exact socket-captured wires are replayed only after timing assertions.
		// This establishes receiver semantics without pretending the Perl harness
		// is a live network daemon or that its execution measured socket latency.
		ref, _ := startDXReference(r.t, false, "N0CALL")
		before := ref.frameAt(r.seed.line, r.seed.at, "K1USER")
		referenceOnlyUser(r.t, before, "K1USER", "127.0.0.2")
		afterC := ref.frameAt(c.line, c.at, "K1USER")
		referenceOnlyUser(r.t, afterC, "K1USER", "127.0.0.2")
		afterA := ref.frameAt(a.line, a.at, "K1USER")
		referenceOnlyUser(r.t, afterA, "K1USER", "127.0.0.3")
		r.t.Logf("actual pinned receiver replay: old IP=%v after C=%v after A=%v", before.Routes["K1USER"], afterC.Routes["K1USER"], afterA.Routes["K1USER"])
	} else {
		r.t.Log("diagnostic omitted actual reference replay; not qualification")
	}
	r.t.Logf("socket recovery C=%q A=%q state=%+v", c.line, a.line, r.snapshot())
}
