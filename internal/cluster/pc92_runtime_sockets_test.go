//go:build qualification

package cluster

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Each socket has one reader and serialized writers. Reader state and framing
// buffers are bounded by the actual wire cap; no unbounded transcript is kept.
type qualificationSocket struct {
	driver                *qualificationDriver
	conn                  net.Conn
	reader                *bufio.Reader
	row                   *qualificationRecipient
	writeMu               sync.Mutex
	done                  chan struct{}
	paused, expectedClose atomic.Bool
	nextPing              time.Time // Read-owner state; monotonic fixture liveness, not measurement time.
}

func newQualificationSocket(d *qualificationDriver, conn net.Conn, reader *bufio.Reader, row *qualificationRecipient) *qualificationSocket {
	return &qualificationSocket{driver: d, conn: conn, reader: reader, row: row, done: make(chan struct{}), nextPing: time.Now().Add(300 * time.Second)}
}

// Healthy DXSpider peers initiate pings independently (the pinned DXProt.pm
// pingint is 5*60). The existing read owner services this during load and drain,
// including continuously busy reads. Faulted/retiring fixtures remain silent;
// one delayed service produces one ping, never a catch-up burst or a new worker.
func (s *qualificationSocket) pingIfDue(now time.Time) error {
	select {
	case <-s.driver.ctx.Done():
		return nil
	default:
	}
	if !s.row.peer || s.paused.Load() || s.expectedClose.Load() || s.driver.oracle.closing.Load() || now.Before(s.nextPing) {
		return nil
	}
	if err := s.write(fmt.Sprintf("PC51^%s^%s^1^", s.driver.cfg.Peering.LocalCallsign, s.driver.cfg.Peering.Peers[s.row.index].RemoteCallsign)); err != nil {
		return err
	}
	s.nextPing = now.Add(300 * time.Second)
	return nil
}

func (s *qualificationSocket) write(line string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if !strings.HasSuffix(line, "\n") {
		line += "\r\n"
	}
	if err := s.conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return err
	}
	_, err := io.WriteString(s.conn, line)
	return err
}

func (d *qualificationDriver) connectPeer(address string, index int) (*qualificationSocket, error) {
	call := d.cfg.Peering.Peers[index].RemoteCallsign
	conn, reader, err := runtimeQualificationLogin(d.ctx, address, "", call)
	if err != nil {
		return nil, err
	}
	link := newQualificationSocket(d, conn, reader, d.oracle.peers[index])
	if d.profile.burst && index >= d.profile.peers-8 {
		if tcp, ok := conn.(*net.TCPConn); ok {
			if err := tcp.SetReadBuffer(1024); err != nil {
				_ = conn.Close()
				return nil, err
			}
		}
	}
	if err := link.write("PC18^DXSpider Version: 1.57 Build: 633 [pc9x 91]^5457^\r\nPC20^\r\n"); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("%s handshake: %w", call, err)
		}
		if strings.TrimSpace(line) == "PC22^" {
			break
		}
	}
	_ = conn.SetDeadline(time.Time{})
	return link, nil
}

func runtimeQualificationPort(t *testing.T) int {
	t.Helper()
	lc := net.ListenConfig{}
	listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}

func runtimeQualificationLogin(ctx context.Context, address, localIP, call string) (net.Conn, *bufio.Reader, error) {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	if localIP != "" {
		dialer.LocalAddr = &net.TCPAddr{IP: net.ParseIP(localIP)}
	}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReaderSize(conn, 65538)
	prompt := make([]byte, 0, 256)
	for len(prompt) < 8192 {
		b, err := reader.ReadByte()
		if err != nil {
			_ = conn.Close()
			return nil, nil, fmt.Errorf("%s login prompt: %w", call, err)
		}
		prompt = append(prompt, b)
		if bytes.HasSuffix(prompt, []byte("login:")) {
			if _, err := io.WriteString(conn, call+"\r\n"); err != nil {
				_ = conn.Close()
				return nil, nil, err
			}
			_ = conn.SetDeadline(time.Time{})
			return conn, reader, nil
		}
	}
	_ = conn.Close()
	return nil, nil, fmt.Errorf("%s login prompt exceeded bound", call)
}

// Complete lines borrow the read buffer for the synchronous callback. Only
// fragmented lines are copied, with a hard wire-cap bound. A prompt can prefix
// data without a newline, so a bounded marker tail keeps original read times
// across arbitrary splits; the marker's first byte, never its completion,
// determines the observation time. Peer records have no telnet prompt markers.
type qualificationLineReader struct {
	line          []byte
	first, record time.Time
	tail          [8]byte
	tailTimes     [8]time.Time
	tailLen       int
	peer          bool
}

func (r *qualificationLineReader) consume(data []byte, at time.Time, accept func([]byte, time.Time)) error {
	for len(data) > 0 {
		end := bytes.IndexByte(data, '\n')
		part := data
		if end >= 0 {
			part = data[:end]
		}
		if len(r.line)+len(part) > 65538 {
			return fmt.Errorf("received record exceeds wire cap")
		}
		if len(r.line) == 0 {
			r.first = at
		}
		if !r.peer && r.record.IsZero() {
			r.observeMarker(part, at)
		}
		if end < 0 {
			r.appendFragment(part)
			return nil
		}
		line := part
		if len(r.line) > 0 {
			r.appendFragment(part)
			line = r.line
		}
		first := r.record
		if first.IsZero() {
			first = r.first
		}
		accept(bytes.TrimSuffix(line, []byte{'\r'}), first)
		r.line, r.tailLen, r.record = r.line[:0], 0, time.Time{}
		data = data[end+1:]
	}
	return nil
}

func (r *qualificationLineReader) appendFragment(part []byte) {
	if needed := len(r.line) + len(part); needed > cap(r.line) {
		// Capacity itself is bounded, rather than relying on append's growth
		// policy. Each socket retains at most one wire-sized backing array.
		next := make([]byte, len(r.line), min(65538, max(needed, 2*cap(r.line))))
		copy(next, r.line)
		r.line = next
	}
	r.line = append(r.line, part...)
}

func (r *qualificationLineReader) observeMarker(part []byte, at time.Time) {
	var crossing [15]byte
	copy(crossing[:], r.tail[:r.tailLen])
	prefix := min(len(part), 7)
	copy(crossing[r.tailLen:], part[:prefix])
	firstCrossing, firstInPart := r.tailLen, len(part)
	for _, marker := range []string{"DX de ", "To ", "WWV de ", "WCY de "} {
		if i := bytes.Index(crossing[:r.tailLen+prefix], []byte(marker)); i >= 0 && i < firstCrossing && i+len(marker) > r.tailLen {
			firstCrossing = i
		}
		if i := bytes.Index(part, []byte(marker)); i >= 0 && i < firstInPart {
			firstInPart = i
		}
	}
	if firstCrossing < r.tailLen {
		r.record = r.tailTimes[firstCrossing]
		return
	}
	if firstInPart < len(part) {
		r.record = at
		return
	}
	keepNew := min(len(part), len(r.tail))
	keepOld := min(r.tailLen, len(r.tail)-keepNew)
	copy(r.tail[:], r.tail[r.tailLen-keepOld:r.tailLen])
	copy(r.tailTimes[:], r.tailTimes[r.tailLen-keepOld:r.tailLen])
	copy(r.tail[keepOld:], part[len(part)-keepNew:])
	for i := keepOld; i < keepOld+keepNew; i++ {
		r.tailTimes[i] = at
	}
	r.tailLen = keepOld + keepNew
}

func (s *qualificationSocket) read() {
	defer close(s.done)
	buf := make([]byte, 8192)
	framer := qualificationLineReader{line: make([]byte, 0, 256), peer: s.row.peer}
	for {
		if s.driver.ctx.Err() != nil {
			return
		}
		if s.paused.Load() {
			if qualificationWaitContext(s.driver.ctx, 10*time.Millisecond) != nil {
				return
			}
			continue
		}
		now := time.Now()
		if err := s.pingIfDue(now); err != nil {
			if s.driver.ctx.Err() == nil && !s.driver.oracle.closing.Load() && !s.expectedClose.Load() {
				s.driver.oracle.fail("%s initiated keepalive: %v", s.row.name, err)
			}
			return
		}
		_ = s.conn.SetReadDeadline(now.Add(time.Second))
		n, err := s.reader.Read(buf)
		at := s.driver.oracle.measurementNow()
		if parseErr := framer.consume(buf[:n], at, s.observe); parseErr != nil {
			s.driver.oracle.fail("%s: %v", s.row.name, parseErr)
			return
		}
		if err == nil {
			continue
		}
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			continue
		}
		if !s.driver.oracle.closing.Load() && !s.expectedClose.Load() {
			s.driver.oracle.fail("%s disconnected unexpectedly: %v", s.row.name, err)
		}
		return
	}
}

func (s *qualificationSocket) observe(line []byte, at time.Time) {
	if s.row.peer {
		if bytes.HasPrefix(line, []byte("PC51^")) {
			parts := strings.Split(string(line), "^")
			if len(parts) > 3 && parts[3] == "1" {
				if err := s.write(fmt.Sprintf("PC51^%s^%s^0^", parts[2], parts[1])); err != nil && !s.expectedClose.Load() {
					s.driver.oracle.fail("%s keepalive: %v", s.row.name, err)
				}
			}
		}
		if bytes.HasPrefix(line, []byte("PC92^")) {
			if fixture := s.driver.topology.Load(); fixture != nil {
				if err := fixture.ObserveBytes(s.row.index, line); err != nil {
					s.driver.oracle.fail("PC92 receiver%d: %v", s.row.index, err)
				}
			}
			return
		}
	}
	var dx []byte
	spotLine := false
	if s.row.peer {
		if bytes.HasPrefix(line, []byte("PC11^")) || bytes.HasPrefix(line, []byte("PC61^")) || bytes.HasPrefix(line, []byte("PC26^")) {
			dx, spotLine = qualificationField(line, '^', 2), true
		}
	} else if i := bytes.Index(line, []byte("DX de ")); i >= 0 {
		dx, spotLine = qualificationField(line[i:], ' ', 4), true
	}
	if bytes.Contains(line, []byte("QID")) || spotLine {
		id, ok := qualificationTokenBytes(line)
		if !ok {
			s.driver.oracle.fail("%s unknown/malformed token in %q", s.row.name, line)
			return
		}
		s.driver.oracle.observedID(s.row, id, string(dx), at, false)
	}
}

// Space-delimited telnet presentation collapses runs; caret-delimited peer
// fields preserve empty values. Returned bytes remain borrowed by the caller.
func qualificationField(line []byte, delimiter byte, index int) []byte {
	for i := 0; len(line) > 0; i++ {
		if delimiter == ' ' {
			line = bytes.TrimLeft(line, " \t")
		}
		end := bytes.IndexByte(line, delimiter)
		if end < 0 {
			if i == index {
				return line
			}
			return nil
		}
		if i == index {
			return line[:end]
		}
		line = line[end+1:]
	}
	return nil
}

func TestQualificationSplitReadPromptTimestamp(t *testing.T) {
	r := qualificationLineReader{}
	start := time.Now()
	var lines []string
	var times []time.Time
	accept := func(line []byte, at time.Time) { lines = append(lines, string(line)); times = append(times, at) }
	for i, part := range []string{"DL1CAA de NODE> ", "DX ", "de DL1AAA: 14020 DL1AABB QID0000001\r\n", "To ALL de DL1AAA: QID0000002\r\n"} {
		if err := r.consume([]byte(part), start.Add(time.Duration(i)*time.Millisecond), accept); err != nil {
			t.Fatal(err)
		}
	}
	if len(lines) != 2 || !times[0].Equal(start.Add(time.Millisecond)) || !times[1].Equal(start.Add(3*time.Millisecond)) {
		t.Fatalf("prompt/split timing: %v %v", lines, times)
	}
}
