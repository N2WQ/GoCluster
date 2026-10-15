// File role: Bounded loopback MQTT and outbound-peer owners for S5 ingress
// witnesses. The caller owns Client, relays and assertions; these helpers own
// their sockets, protocol goroutines and joins. Workers never receive a T.
package cluster

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/peer"
	"dxcluster/spot"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	pskrZeroFrameBytes = 4096
	pskrZeroFrameLimit = 64
	pskrZeroByteLimit  = 64 << 10
	pskrZeroJoinLimit  = 5 * time.Second
)

// One accepted connection belongs to its protocol worker. Cancellation closes
// both accept and active I/O; registering a connection races safely with Close.
type pskrZeroSocket struct {
	ctx                  context.Context
	listener             *net.TCPListener
	mu                   sync.Mutex
	conn                 net.Conn
	closed               bool
	watchStop, watchDone chan struct{}
	watchOnce            sync.Once
}

func newPSKRZeroSocket(ctx context.Context) (*pskrZeroSocket, error) {
	if ctx == nil {
		return nil, errors.New("protocol owner requires a finite context deadline")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	deadline, ok := ctx.Deadline()
	if !ok || !deadline.After(time.Now()) {
		return nil, errors.New("protocol owner requires a live finite context deadline")
	}
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	if err := listener.SetDeadline(deadline); err != nil {
		_ = listener.Close()
		return nil, err
	}
	s := &pskrZeroSocket{ctx: ctx, listener: listener, watchStop: make(chan struct{}), watchDone: make(chan struct{})}
	go func() {
		defer close(s.watchDone)
		select {
		case <-ctx.Done():
			s.closeIO()
		case <-s.watchStop:
		}
	}()
	return s, nil
}

func (s *pskrZeroSocket) accept() (net.Conn, error) {
	conn, err := s.listener.AcceptTCP()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	s.conn = conn
	s.mu.Unlock()
	_ = s.listener.Close() // A reconnect cannot create a second fixture session.
	deadline, _ := s.ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (s *pskrZeroSocket) closeIO() {
	s.mu.Lock()
	s.closed = true
	conn := s.conn
	s.mu.Unlock()
	_ = s.listener.Close()
	if conn != nil {
		_ = conn.Close()
	}
}

func (s *pskrZeroSocket) close() error {
	s.closeIO()
	s.watchOnce.Do(func() { close(s.watchStop) })
	return pskrZeroJoin(s.watchDone)
}

func pskrZeroJoin(done <-chan struct{}) error {
	timer := time.NewTimer(pskrZeroJoinLimit)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return errors.New("protocol owner did not retire within five seconds")
	}
}

type pskrZeroBrokerResult struct {
	Connections, Subscriptions, Published int
	Disconnected                          bool
	Unsubscribed, ClientClosed            bool
}

type pskrZeroBroker struct {
	socket      *pskrZeroSocket
	topic       string
	corpus      [][]byte
	done        chan struct{}
	runResult   pskrZeroBrokerResult // Worker-owned until done closes.
	runErr      error
	closeOnce   sync.Once
	closeResult pskrZeroBrokerResult
	closeErr    error
	retired     bool
}

func startPSKRZeroBroker(ctx context.Context, topic string, corpus [][]byte) (*pskrZeroBroker, error) {
	if topic == "" || len(topic) > 128 || len(corpus) == 0 || len(corpus) > pskrZeroFrameLimit {
		return nil, errors.New("invalid bounded MQTT topic/corpus")
	}
	total := 0
	owned := make([][]byte, len(corpus))
	for i, payload := range corpus {
		total += len(payload)
		if len(payload) > pskrZeroFrameBytes || total > pskrZeroByteLimit {
			return nil, errors.New("MQTT corpus exceeds fixture byte bound")
		}
		owned[i] = bytes.Clone(payload)
	}
	socket, err := newPSKRZeroSocket(ctx)
	if err != nil {
		return nil, err
	}
	b := &pskrZeroBroker{socket: socket, topic: topic, corpus: owned, done: make(chan struct{})}
	go func() { defer close(b.done); b.runErr = b.serve() }()
	return b, nil
}

func (b *pskrZeroBroker) Endpoint() (string, int) {
	return "127.0.0.1", b.socket.listener.Addr().(*net.TCPAddr).Port
}

// Only failure before the first byte is a packet-boundary terminal. An error
// after any header/body byte must remain a parse/transport failure, even when
// the codec has consumed the buffered prefix before returning that error.
type pskrZeroPacketBoundaryError struct{ cause error }

func (e *pskrZeroPacketBoundaryError) Error() string { return e.cause.Error() }
func (e *pskrZeroPacketBoundaryError) Unwrap() error { return e.cause }

// Bound the codec allocation before RemainingLength reaches ReadPacket.
func pskrZeroReadPacket(reader *bufio.Reader) (packets.ControlPacket, error) {
	if _, err := reader.Peek(1); err != nil {
		return nil, &pskrZeroPacketBoundaryError{err}
	}
	remaining := 0
	for digit := 0; digit < 4; digit++ {
		header, err := reader.Peek(digit + 2)
		if err != nil {
			return nil, err
		}
		value := header[digit+1]
		remaining |= int(value&127) << (7 * digit)
		if remaining > pskrZeroFrameBytes {
			return nil, errors.New("MQTT frame exceeds fixture byte bound")
		}
		if value&128 == 0 {
			return packets.ReadPacket(reader)
		}
	}
	return nil, errors.New("MQTT remaining length exceeds four bytes")
}

func (b *pskrZeroBroker) clientShutdown(err error) bool {
	var boundary *pskrZeroPacketBoundaryError
	if !b.runResult.Unsubscribed || b.runResult.Published != len(b.corpus) || b.socket.ctx.Err() != nil || !errors.As(err, &boundary) {
		return false
	}
	b.socket.mu.Lock()
	forced := b.socket.closed
	b.socket.mu.Unlock()
	// Client.Stop does not await UNSUBACK, and Paho closes TCP immediately or
	// after its bounded best-effort quiesce. The fixture separately proves Stop
	// and owner retirement; it does not require a graceful FIN or DISCONNECT.
	// Accept only EOF or Windows WSAECONNABORTED/WSAECONNRESET (10053/10054),
	// never timeouts or arbitrary errors. These Go errno names are Windows-only.
	return !forced && (errors.Is(boundary.cause, io.EOF) ||
		runtime.GOOS == "windows" && (errors.Is(boundary.cause, syscall.Errno(10053)) || errors.Is(boundary.cause, syscall.Errno(10054))))
}

func (b *pskrZeroBroker) serve() error {
	conn, err := b.socket.accept()
	if err != nil {
		return fmt.Errorf("MQTT accept: %w", err)
	}
	defer conn.Close()
	b.runResult.Connections++
	reader := bufio.NewReaderSize(conn, pskrZeroFrameBytes+5)
	packet, err := pskrZeroReadPacket(reader)
	if err != nil {
		return fmt.Errorf("MQTT CONNECT: %w", err)
	}
	if _, ok := packet.(*packets.ConnectPacket); !ok {
		return fmt.Errorf("MQTT requires CONNECT, got %T", packet)
	}
	if err := packets.NewControlPacket(packets.Connack).Write(conn); err != nil {
		return err
	}
	for frame := 1; frame < pskrZeroFrameLimit; frame++ {
		packet, err := pskrZeroReadPacket(reader)
		if err != nil {
			if b.clientShutdown(err) {
				b.runResult.ClientClosed = true
				return nil
			}
			return fmt.Errorf("MQTT frame %d: %w", frame, err)
		}
		switch value := packet.(type) {
		case *packets.SubscribePacket:
			if b.runResult.Subscriptions != 0 || value.MessageID == 0 || len(value.Topics) != 1 || value.Topics[0] != b.topic || len(value.Qoss) != 1 || value.Qoss[0] != 0 {
				return errors.New("unexpected MQTT subscription or QoS")
			}
			b.runResult.Subscriptions++
			ack := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
			ack.MessageID, ack.ReturnCodes = value.MessageID, []byte{0}
			if err := ack.Write(conn); err != nil {
				return err
			}
			for _, payload := range b.corpus {
				publish := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
				publish.TopicName, publish.Payload = b.topic, payload
				if err := publish.Write(conn); err != nil {
					return err
				}
				b.runResult.Published++
			}
		case *packets.PingreqPacket:
			if err := packets.NewControlPacket(packets.Pingresp).Write(conn); err != nil {
				return err
			}
		case *packets.UnsubscribePacket:
			if b.runResult.Subscriptions != 1 || b.runResult.Unsubscribed || value.MessageID == 0 || len(value.Topics) != 1 || value.Topics[0] != b.topic {
				return errors.New("unexpected MQTT unsubscribe")
			}
			b.runResult.Unsubscribed = true
			ack := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
			ack.MessageID = value.MessageID
			if err := ack.Write(conn); err != nil {
				return err
			}
		case *packets.DisconnectPacket:
			if b.runResult.Subscriptions != 1 || b.runResult.Published != len(b.corpus) {
				return errors.New("MQTT disconnected before complete corpus")
			}
			b.runResult.Disconnected = true
			return nil
		default:
			return fmt.Errorf("unexpected MQTT packet %T", packet)
		}
	}
	return errors.New("MQTT fixture frame bound exhausted")
}

// Stop the public PSKR Client first. Disconnected means an actual DISCONNECT;
// ClientClosed separately records validated unsubscribe then natural TCP close.
// Neither is manufactured by cancellation, timeouts or fixture-forced teardown.
func (b *pskrZeroBroker) Close() (pskrZeroBrokerResult, error) {
	b.closeOnce.Do(func() {
		// Disconnect(250) can return just before the broker reads its queued
		// DISCONNECT. Allow natural completion before closing the fake socket.
		timer := time.NewTimer(pskrZeroJoinLimit)
		var completionErr error
		select {
		case <-b.done:
		case <-b.socket.ctx.Done():
			completionErr = b.socket.ctx.Err()
		case <-timer.C:
			completionErr = errors.New("MQTT protocol did not complete before forced close")
		}
		timer.Stop()
		watchErr := b.socket.close()
		if err := pskrZeroJoin(b.done); err != nil {
			b.closeErr = errors.Join(completionErr, watchErr, err)
			return // The worker may still own runResult/runErr; do not read them.
		}
		b.retired = watchErr == nil
		b.closeResult = b.runResult
		b.closeErr = errors.Join(completionErr, watchErr, b.runErr, b.socket.ctx.Err())
		if !b.closeResult.Disconnected && !b.closeResult.ClientClosed {
			b.closeErr = errors.Join(b.closeErr, errors.New("MQTT closed without observed client shutdown"))
		}
	})
	return b.closeResult, b.closeErr
}

type pskrZeroPeer struct {
	socket              *pskrZeroSocket
	manager             *peer.Manager
	done, changed       chan struct{}
	runErr              error // Worker-owned until done closes.
	mu                  sync.Mutex
	spots               []string
	closing, activeSeen bool
	finalIndex          int
	closeOnce           sync.Once
	closeErr            error
	retired             bool
}

func startPSKRZeroPeer(ctx context.Context) (*pskrZeroPeer, error) {
	socket, err := newPSKRZeroSocket(ctx)
	if err != nil {
		return nil, err
	}
	cfg := config.PeeringConfig{Enabled: true, MaxPeers: 1, LocalCallsign: "N0CALL", ListenPort: 0,
		ForwardSpots: true, HopCount: 99, NodeVersion: "5457", NodeBuild: "633", LegacyVersion: "5401", PC92Bitmap: 5,
		TelnetTransport: config.TelnetTransportNative, WriteQueueSize: 32, MaxLineLength: pskrZeroFrameBytes, PC92MaxBytes: pskrZeroFrameBytes,
		Timeouts: config.PeeringTimeouts{LoginSeconds: 5, InitSeconds: 5},
		Peers: []config.PeeringPeer{{Enabled: true, Direction: config.PeeringPeerDirectionOutbound, Family: config.PeeringPeerFamilyDXSpider,
			Host: "127.0.0.1", Port: socket.listener.Addr().(*net.TCPAddr).Port, LoginCallsign: "N0CALL", RemoteCallsign: "K1PEER", PreferPC9x: false}}}
	//nolint:contextcheck // The context-free constructor starts no fixture workers; Start(ctx) below owns their lifecycle.
	manager, err := peer.NewManager(cfg, "N0CALL", make(chan *spot.Spot, 1), 0, nil)
	if err != nil {
		return nil, errors.Join(err, socket.close())
	}
	if err := manager.SetBuildIdentity("s5-zero-fixture", "", "", "", ""); err != nil {
		manager.Stop()
		return nil, errors.Join(err, socket.close())
	}
	manager.SetMembershipProvider(func() peer.LocalMembership { return peer.LocalMembership{Complete: true} })
	p := &pskrZeroPeer{socket: socket, manager: manager, done: make(chan struct{}), changed: make(chan struct{}, 1), finalIndex: -1}
	go func() { defer close(p.done); p.runErr = p.serve() }()
	if err := manager.Start(ctx); err != nil {
		return nil, errors.Join(err, p.Close())
	}
	return p, nil
}

func (p *pskrZeroPeer) Manager() *peer.Manager { return p.manager }

func (p *pskrZeroPeer) serve() error {
	conn, err := p.socket.accept()
	if err != nil {
		return fmt.Errorf("peer accept: %w", err)
	}
	defer conn.Close()
	reader := bufio.NewReaderSize(conn, pskrZeroFrameBytes)
	frames, total := 0, 0
	read := func() (string, error) {
		line, err := reader.ReadSlice('\n')
		frames++
		total += len(line)
		if frames > pskrZeroFrameLimit || total > pskrZeroByteLimit {
			return "", errors.New("peer fixture frame/byte bound exhausted")
		}
		if err != nil {
			return "", err
		}
		return strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r"), nil
	}
	write := func(line string) error { _, err := io.WriteString(conn, line+"\r\n"); return err }
	for _, step := range []struct{ receive, reply string }{
		{"N0CALL", "PC18^DXSpider Version: 1.57 Build: 633^5457^"},
		{"PC19^1^N0CALL^0^5401^H99^", ""}, {"PC20^", "PC22^"},
	} {
		line, err := read()
		if err != nil {
			return fmt.Errorf("peer handshake: %w", err)
		}
		if line != step.receive {
			return fmt.Errorf("peer handshake got %q, want %q", line, step.receive)
		}
		if step.reply != "" {
			if err := write(step.reply); err != nil {
				return err
			}
		}
	}
	for {
		line, err := read()
		if err != nil {
			p.mu.Lock()
			closing := p.closing
			p.mu.Unlock()
			if closing && (errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)) {
				return nil
			}
			return fmt.Errorf("peer wire read: %w", err)
		}
		fields := strings.Split(line, "^")
		switch fields[0] {
		case "PC11", "PC61":
			p.mu.Lock()
			p.spots = append(p.spots, line)
			p.mu.Unlock()
			select {
			case p.changed <- struct{}{}:
			default:
			}
		case "PC51":
			if len(fields) < 4 {
				return errors.New("malformed peer ping")
			}
			if fields[3] == "1" {
				if err := write(fmt.Sprintf("PC51^%s^%s^0^", fields[2], fields[1])); err != nil {
					return err
				}
			}
		case "PC21", "PC19", "PC16", "PC17", "PC20", "PC22":
			// Bounded legacy/control traffic remains visible to the frame bound.
		default:
			return fmt.Errorf("unexpected peer frame %q", line)
		}
	}
}

func (p *pskrZeroPeer) WaitActive(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if err := errors.Join(ctx.Err(), p.socket.ctx.Err()); err != nil {
			return err
		}
		if p.manager.ActiveSessionCount() == 1 {
			p.mu.Lock()
			p.activeSeen = true
			p.mu.Unlock()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.socket.ctx.Done():
			return p.socket.ctx.Err()
		case <-p.done:
			return errors.Join(errors.New("peer ended before active session"), p.runErr)
		case <-tick.C:
		}
	}
}

// Return only actual spot frames through the manual fence. The reader remains
// alive; Close also checks for any later spot frame while the manager retires.
func (p *pskrZeroPeer) WaitFinal(ctx context.Context, manualDX string) ([]string, error) {
	if !config.IsRawPeeringCall(manualDX) {
		return nil, errors.New("invalid peer manual fence identity")
	}
	for {
		if err := errors.Join(ctx.Err(), p.socket.ctx.Err()); err != nil {
			return nil, err
		}
		p.mu.Lock()
		for i, line := range p.spots {
			fields := strings.Split(line, "^")
			if len(fields) > 2 && fields[2] == manualDX {
				p.finalIndex = i
				result := append([]string(nil), p.spots[:i+1]...)
				p.mu.Unlock()
				return result, nil
			}
		}
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.socket.ctx.Done():
			return nil, p.socket.ctx.Err()
		case <-p.done:
			return nil, errors.Join(errors.New("peer ended before manual fence"), p.runErr)
		case <-p.changed:
		}
	}
}

func (p *pskrZeroPeer) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closing = true
		p.mu.Unlock()
		managerDone := make(chan struct{})
		go func() { defer close(managerDone); p.manager.Stop() }()
		managerErr := pskrZeroJoin(managerDone) // Keep the fake reader alive through Stop.
		watchErr := p.socket.close()
		if managerErr != nil {
			managerErr = errors.Join(managerErr, pskrZeroJoin(managerDone))
		}
		if err := pskrZeroJoin(p.done); err != nil {
			p.closeErr = errors.Join(managerErr, watchErr, err)
			return
		}
		p.retired = managerErr == nil && watchErr == nil
		p.closeErr = errors.Join(managerErr, watchErr, p.runErr, p.socket.ctx.Err())
		p.mu.Lock()
		defer p.mu.Unlock()
		if !p.activeSeen || p.finalIndex < 0 || p.finalIndex != len(p.spots)-1 {
			p.closeErr = errors.Join(p.closeErr, errors.New("peer missing active/manual completion or received a spot after its fence"))
		}
	})
	return p.closeErr
}

func TestPSKRZeroProtocolDeadlines(t *testing.T) {
	if owner, err := startPSKRZeroBroker(context.Background(), "s5/test", [][]byte{[]byte("{}")}); err == nil || owner != nil {
		t.Fatalf("broker accepted missing deadline: owner=%v err=%v", owner, err)
	}
	if owner, err := startPSKRZeroPeer(context.Background()); err == nil || owner != nil {
		t.Fatalf("peer accepted missing deadline: owner=%v err=%v", owner, err)
	}
}

func TestPSKRZeroProtocolLifecycle(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("new listening witnesses use the approved native Windows executable")
	}
	pathShardingRequireExecutable(t)
	for _, fault := range []string{"broker_cancel", "broker_bad_packet", "peer_cancel", "peer_forced_close"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			if strings.HasPrefix(fault, "broker_") {
				b, err := startPSKRZeroBroker(ctx, "s5/test", [][]byte{[]byte("{}")})
				if err != nil {
					t.Fatal(err)
				}
				defer b.Close()
				if fault == "broker_cancel" {
					cancel()
				} else {
					host, port := b.Endpoint()
					conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp4", net.JoinHostPort(host, fmt.Sprint(port)))
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					deadline, _ := ctx.Deadline()
					if err := conn.SetDeadline(deadline); err != nil {
						t.Fatal(err)
					}
					if err := packets.NewControlPacket(packets.Pingreq).Write(conn); err != nil {
						t.Fatal(err)
					}
					if err := pskrZeroJoin(b.done); err != nil {
						t.Fatal(err)
					}
				}
				result, err := b.Close()
				if err == nil || !b.retired || result.Disconnected || result.ClientClosed || result.Unsubscribed || (fault == "broker_cancel" && !errors.Is(err, context.Canceled)) ||
					result.Subscriptions != 0 || result.Published != 0 ||
					(fault == "broker_cancel" && result.Connections != 0) ||
					(fault == "broker_bad_packet" && (result.Connections != 1 || !strings.Contains(err.Error(), "requires CONNECT"))) {
					t.Fatalf("wrong broker failure/retirement: result=%+v err=%v", result, err)
				}
				again, againErr := b.Close()
				if again != result || againErr != err { //nolint:errorlint // Repeated Close must return the cached error instance; errors.Is would accept a new wrapper.
					t.Fatal("broker Close is not idempotent")
				}
				return
			}
			p, err := startPSKRZeroPeer(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if err := p.WaitActive(ctx); err != nil {
				t.Fatal(err)
			}
			if fault == "peer_cancel" {
				cancel()
			} else {
				p.socket.closeIO()
				if err := pskrZeroJoin(p.done); err != nil {
					t.Fatal(err)
				}
				if p.runErr == nil || !strings.Contains(p.runErr.Error(), "peer wire read") {
					t.Fatalf("forced transport closure did not record its actual read failure: %v", p.runErr)
				}
				if _, err := p.WaitFinal(ctx, "K1BAR"); err == nil {
					t.Fatal("failed peer transport invented manual completion")
				}
			}
			err = p.Close()
			if err == nil || !p.retired || p.manager.ActiveSessionCount() != 0 || (fault == "peer_cancel" && !errors.Is(err, context.Canceled)) {
				t.Fatalf("peer abort claimed completion or retained session: err=%v active=%d", err, p.manager.ActiveSessionCount())
			}
			if p.Close() != err { //nolint:errorlint // Repeated Close must return the cached error instance; errors.Is would accept a new wrapper.
				t.Fatal("peer Close is not idempotent")
			}
		})
	}
}

func TestPSKRZeroBrokerTerminalErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	b := &pskrZeroBroker{socket: &pskrZeroSocket{ctx: ctx}, corpus: [][]byte{[]byte("{}")},
		runResult: pskrZeroBrokerResult{Subscriptions: 1, Published: 1, Unsubscribed: true}}
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{io.EOF, true}, {syscall.Errno(10053), runtime.GOOS == "windows"}, {syscall.Errno(10054), runtime.GOOS == "windows"},
		{fmt.Errorf("wrapped receive: %w", syscall.Errno(10054)), runtime.GOOS == "windows"},
		{io.ErrUnexpectedEOF, false}, {syscall.Errno(10060), false}, {net.ErrClosed, false},
		{&net.DNSError{IsTimeout: true}, false}, {errors.New("unrelated transport error"), false},
	} {
		if got := b.clientShutdown(&pskrZeroPacketBoundaryError{tc.err}); got != tc.want {
			t.Fatalf("terminal classification %v = %v want %v", tc.err, got, tc.want)
		}
	}
	terminals := []error{io.EOF, syscall.Errno(10053), syscall.Errno(10054)}
	for _, terminal := range terminals {
		if b.clientShutdown(terminal) {
			t.Fatalf("unmarked mid-packet error became client completion: %v", terminal)
		}
	}
	requireRejected := func(reason string) {
		for _, terminal := range terminals {
			if b.clientShutdown(&pskrZeroPacketBoundaryError{terminal}) {
				t.Fatalf("%s became client completion: %v", reason, terminal)
			}
		}
	}
	b.runResult.Unsubscribed = false
	requireRejected("closure without validated unsubscribe")
	b.runResult.Unsubscribed = true
	b.runResult.Published = 0
	requireRejected("closure before complete corpus")
	b.runResult.Published = 1
	b.socket.closed = true
	requireRejected("fixture-forced closure")
	b.socket.closed = false
	cancel()
	requireRejected("canceled owner")
}

func TestPSKRZeroBrokerShutdown(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("new listening witnesses use the approved native Windows executable")
	}
	pathShardingRequireExecutable(t)
	for _, fault := range []string{"premature_eof", "complete_without_unsubscribe", "unsubscribe_eof", "unsubscribe_cancel", "unsubscribe_forced", "unsubscribe_truncated_frame"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			b, err := startPSKRZeroBroker(ctx, "s5/test", [][]byte{[]byte("{}")})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			host, port := b.Endpoint()
			conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp4", net.JoinHostPort(host, fmt.Sprint(port)))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			deadline, _ := ctx.Deadline()
			if err := conn.SetDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReaderSize(conn, pskrZeroFrameBytes+5)
			write := func(packet packets.ControlPacket) {
				if err := packet.Write(conn); err != nil {
					t.Fatal(err)
				}
			}
			read := func() packets.ControlPacket {
				packet, err := pskrZeroReadPacket(reader)
				if err != nil {
					t.Fatal(err)
				}
				return packet
			}
			connect := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
			connect.ProtocolName, connect.ProtocolVersion, connect.CleanSession, connect.ClientIdentifier = "MQTT", 4, true, "s5-terminal-control"
			write(connect)
			if ack, ok := read().(*packets.ConnackPacket); !ok || ack.ReturnCode != 0 {
				t.Fatal("missing successful CONNACK")
			}
			if fault != "premature_eof" {
				subscribe := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
				subscribe.MessageID, subscribe.Topics, subscribe.Qoss = 1, []string{"s5/test"}, []byte{0}
				write(subscribe)
				if ack, ok := read().(*packets.SubackPacket); !ok || ack.MessageID != 1 || len(ack.ReturnCodes) != 1 || ack.ReturnCodes[0] != 0 {
					t.Fatal("missing exact SUBACK")
				}
				if pub, ok := read().(*packets.PublishPacket); !ok || pub.Qos != 0 || pub.TopicName != "s5/test" || string(pub.Payload) != "{}" {
					t.Fatal("missing exact corpus publication")
				}
			}
			unsubscribed := strings.HasPrefix(fault, "unsubscribe_")
			if unsubscribed {
				unsubscribe := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
				unsubscribe.MessageID, unsubscribe.Topics = 2, []string{"s5/test"}
				write(unsubscribe)
				if ack, ok := read().(*packets.UnsubackPacket); !ok || ack.MessageID != 2 {
					t.Fatal("missing exact UNSUBACK")
				}
			}
			switch fault {
			case "unsubscribe_cancel":
				cancel()
			case "unsubscribe_forced":
				b.socket.closeIO()
			case "unsubscribe_truncated_frame":
				// A complete header declares two missing body bytes. The codec
				// consumes the header then returns EOF with Buffered()==0.
				if _, err := conn.Write([]byte{0xc0, 0x02}); err != nil {
					t.Fatal(err)
				}
				_ = conn.Close()
			default:
				_ = conn.Close()
			}
			if err := pskrZeroJoin(b.done); err != nil {
				t.Fatal(err)
			}
			result, err := b.Close()
			wantComplete := fault == "unsubscribe_eof"
			wantPublished := 1
			if fault == "premature_eof" {
				wantPublished = 0
			}
			if (err == nil) != wantComplete || !b.retired || result.Connections != 1 || result.Subscriptions != wantPublished || result.Published != wantPublished ||
				result.Unsubscribed != unsubscribed || result.ClientClosed != wantComplete || result.Disconnected {
				t.Fatalf("wrong %s terminal/retirement: result=%+v err=%v", fault, result, err)
			}
			if fault == "unsubscribe_cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost its cause: %v", err)
			}
		})
	}
}
