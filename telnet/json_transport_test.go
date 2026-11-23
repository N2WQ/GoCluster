package telnet

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"dxcluster/spot"
)

func jsonWireRecord(t *testing.T, r *bufio.Reader, kind string) []json.RawMessage {
	t.Helper()
	line, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatalf("waiting for %s: %v (%q)", kind, err, line)
	}
	var fields []json.RawMessage
	if bytes.ContainsAny(line, "\r\xff") || json.Unmarshal(line, &fields) != nil || len(fields) == 0 || string(fields[0]) != fmt.Sprintf("%q", kind) {
		t.Fatalf("expected %s JSON line; received %q", kind, line)
	}
	return fields
}

func TestJSONSwitchTCP(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s := newHandshakeTranscriptServerWithOptions(t, func(o *ServerOptions) {
				o.Transport, o.EchoMode, o.LoginGreeting = transport, "server", "READY\n"
				o.CommandRatePerSecond, o.CommandBurst = 1000, 1000
			})
			const call = "N0CALL-57"
			conn, r, done, _ := connectUpgradeSession(t, s, call)
			send := func(text string) {
				t.Helper()
				if _, err := io.WriteString(conn, text); err != nil {
					t.Fatal(err)
				}
			}
			textLine := func(want string) {
				t.Helper()
				line, err := r.ReadString('\n')
				if err != nil || line != want+"\r\n" {
					t.Fatalf("want %q, got %q (%v)", want, line, err)
				}
			}
			roundTrip := func(cmd, want string) { t.Helper(); send(cmd + "\r\n"); textLine(cmd); textLine(want) }
			roundTrip("SET JSON OFF", "JSON already off.")
			roundTrip("SET JSON ON VERSION 17", "JSON REFUSED unsupported version")
			roundTrip("SET JSON ON DEFLATE", "JSON REFUSED DEFLATE not available")
			roundTrip("SET/VE7CC", "VE7CC enabled for this session.")
			roundTrip("SET JSON ON", "JSON REFUSED VE7CC is on; send UNSET/VE7CC first")
			roundTrip("UNSET/VE7CC", "VE7CC disabled for this session.")

			// Send the second command, a negotiation, and an erase before the
			// first acknowledgement arrives. Ziutek processes DO in its own Read.
			send("SET JSON ON VERSION 1\r\n" + string([]byte{IAC, DO, 93}) + "SET JSON OX\bN\r\nSET/VE7CC\r\nUNSET/VE7CC\r\n")
			textLine("SET JSON ON VERSION 1")
			textLine("JSON ON 1")
			hello := jsonWireRecord(t, r, "hello")
			var identity struct {
				Login, Node, Server string
				Limits              struct{ SpotBytes, NodeBytes, DXCountryBytes, DECountryBytes int64 }
			}
			if len(hello) != 3 || string(hello[1]) != "1" || json.Unmarshal(hello[2], &identity) != nil || identity.Login != call || identity.Node != "N2WQ-2" || identity.Server != "GoCluster" || identity.Limits.SpotBytes <= 0 || identity.Limits.NodeBytes != 256 || identity.Limits.DXCountryBytes != 256 || identity.Limits.DECountryBytes != 256 {
				t.Fatalf("wrong hello: %s", hello)
			}
			for _, layout := range []string{"s", "station"} {
				f := jsonWireRecord(t, r, "schema")
				if len(f) != 4 || string(f[1]) != fmt.Sprintf("%q", layout) {
					t.Fatalf("schema %s", f)
				}
			}
			for _, reason := range []string{"already on", "JSON is on"} {
				f := jsonWireRecord(t, r, "notice")
				if len(f) != 3 || string(f[1]) != "\"refused\"" || !strings.Contains(string(f[2]), reason) {
					t.Fatalf("refusal %s", f)
				}
			}
			if f := jsonWireRecord(t, r, "text"); len(f) != 2 || !strings.Contains(string(f[1]), "already disabled") {
				t.Fatalf("VE7CC unset: %s", f)
			}

			// The second OFF is already on the wire when the first flushes.
			send("SET JSON OFF\r\nSET JSON OFF\r\n")
			f := jsonWireRecord(t, r, "notice")
			if len(f) != 3 || string(f[1]) != "\"json-off\"" || string(f[2]) != "\"\"" {
				t.Fatalf("last JSON line %s", f)
			}
			textLine("SET JSON OFF")
			textLine("JSON already off.")
			send("SET JSON ON\r\n")
			textLine("SET JSON ON")
			textLine("JSON ON 1")
			jsonWireRecord(t, r, "hello")
			jsonWireRecord(t, r, "schema")
			jsonWireRecord(t, r, "schema")
			closeHandshakeTranscriptSession(t, conn, done)
			conn, r, _, _ = connectUpgradeSession(t, s, call)
			roundTrip("SET JSON OFF", "JSON already off.")
		})
	}
}

// A short write without an error is a real failure. After any prefix of a
// boundary, the writer must retire the socket and release the waiting reader.
type jsonBoundaryConn struct {
	discardConn
	writes int
	failAt int
}

func (c *jsonBoundaryConn) Write(p []byte) (int, error) {
	c.writes++
	if c.writes == c.failAt {
		return len(p) - 1, nil
	}
	return len(p), nil
}

func TestJSONShortSwitchWrites(t *testing.T) {
	for _, fail := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			s, c := v7MachineTestClient(t)
			conn := &jsonBoundaryConn{failAt: fail}
			c.conn, c.writer = conn, bufio.NewWriter(conn)
			c.writerStopped = make(chan struct{})
			c.spotChan = make(chan *spotEnvelope, 8)
			done := writerV15Start(c)
			t.Cleanup(func() { c.close(""); writerV15Wait(t, done, "switch cleanup") })
			_, ok := s.handleJSONCommand(c, "SET JSON ON")
			if fail == 3 && ok {
				_, ok = s.handleJSONCommand(c, "SET JSON OFF")
			}
			if ok {
				t.Fatal("reader accepted partially flushed switch")
			}
			writerV15Wait(t, done, "short write exit")
			if conn.writes != fail {
				t.Fatalf("%d writes after failure at %d", conn.writes, fail)
			}
		})
	}
}

// The selected boundary goes to a real blocked pipe. Stop must interrupt
// its write even if the peer has never read a byte of that boundary.
type jsonBlockedBoundary struct {
	net.Conn
	target, calls int
	entered       chan struct{}
}

func (b *jsonBlockedBoundary) Write(p []byte) (int, error) {
	b.calls++
	if b.calls < b.target {
		return len(p), nil
	}
	if b.calls == b.target {
		close(b.entered)
	}
	return b.Conn.Write(p)
}

func TestJSONDisconnectAndStopAtSwitchBoundaries(t *testing.T) {
	for _, stop := range []bool{false, true} {
		for _, target := range []int{1, 2, 3} {
			t.Run(fmt.Sprintf("stop=%t/write=%d", stop, target), func(t *testing.T) {
				s, c := v7MachineTestClient(t)
				s.shutdown = make(chan struct{})
				s.clients = map[string]*Client{c.callsign: c}
				a, b := net.Pipe()
				defer b.Close()
				blocked := &jsonBlockedBoundary{Conn: a, target: target, entered: make(chan struct{})}
				c.conn, c.writer = blocked, bufio.NewWriter(blocked)
				c.writerStopped = make(chan struct{})
				writer := writerV15Start(c)
				finished := make(chan struct{})
				go func() {
					defer close(finished)
					if _, ok := s.handleJSONCommand(c, "SET JSON ON"); ok && target == 3 {
						s.handleJSONCommand(c, "SET JSON OFF")
					}
				}()
				t.Cleanup(func() {
					c.close("")
					writerV15Wait(t, writer, "writer cleanup")
					writerV15Wait(t, finished, "reader cleanup")
				})
				writerV15Wait(t, blocked.entered, "selected blocked flush")
				select {
				case <-finished:
					t.Fatal("reader crossed unflushed boundary")
				default:
				}
				if stop {
					s.Stop()
				} else if err := b.Close(); err != nil {
					t.Fatal(err)
				}
				writerV15Wait(t, writer, "blocked write teardown")
				writerV15Wait(t, finished, "switch waiter teardown")
			})
		}
	}
}

func TestJSONRetiredPendingSwitchKeepsTerminalFraming(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprint(active), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, c := v7MachineTestClient(t)
				c.retired, c.writerStopped = make(chan struct{}), make(chan struct{})
				a, b := net.Pipe()
				defer b.Close()
				c.conn, c.writer = a, bufio.NewWriter(a)
				s.jsonLimits = newJSONSpotLimits(128, 0, spot.CurrentTaxonomy())
				r := bufio.NewReader(b)
				writer := writerV15Start(c)
				defer func() { c.close(""); writerV15Wait(t, writer, "retired writer") }()
				if active {
					on := &jsonFormatSwitch{flushed: make(chan struct{})}
					c.controlChan <- controlMessage{jsonSwitch: on}
					if ack, err := r.ReadString('\n'); err != nil || ack != "JSON ON 1\r\n" {
						t.Fatalf("activation %q %v", ack, err)
					}
					jsonWireRecord(t, r, "hello")
					jsonWireRecord(t, r, "schema")
					jsonWireRecord(t, r, "schema")
					<-on.flushed
				}
				c.controlChan <- controlMessage{line: "before retirement\n"}
				// Read exactly one byte from the pipe. The writer is now blocked
				// in the earlier flush, even when the legacy batch timer is active.
				var prefix [1]byte
				if _, err := io.ReadFull(b, prefix[:]); err != nil {
					t.Fatal(err)
				}
				r = bufio.NewReader(io.MultiReader(bytes.NewReader(prefix[:]), r))
				result := make(chan bool, 1)
				cmd := "SET JSON ON"
				if active {
					cmd = "SET JSON OFF"
				}
				go func() { _, ok := s.handleJSONCommand(c, cmd); result <- ok }()
				synctest.Wait()
				if len(c.controlChan) != 1 {
					t.Fatal("switch did not reach FIFO")
				}
				close(c.retired)
				if err := c.SendAndClose("replacement notice\n"); err != nil {
					t.Fatal(err)
				}
				for _, line := range []string{"before retirement", "replacement notice"} {
					if active {
						f := jsonWireRecord(t, r, "text")
						if len(f) != 2 || string(f[1]) != fmt.Sprintf("%q", line) {
							t.Fatalf("terminal framing %s", f)
						}
					} else {
						out, err := r.ReadString('\n')
						if err != nil || out != line+"\r\n" {
							t.Fatalf("terminal line %q %v", out, err)
						}
					}
				}
				if <-result {
					t.Fatal("retired reader completed switch")
				}
			})
		})
	}
}

func TestJSONReplacementWithoutPacing(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s := newHandshakeTranscriptServerWithOptions(t, func(o *ServerOptions) {
				o.Transport, o.EchoMode, o.LoginGreeting = transport, "off", "READY\n"
				o.CommandRatePerSecond, o.CommandBurst = 0, 0
				o.DuplicateLoginMsg = "replacement notice"
			})
			const call = "N0CALL-57"
			// The pipe lets the peer read exactly one byte of a real writer
			// flush. The second login enters through the normal TCP server.
			_, old, done := startHandshakeTranscriptSession(t, s)
			t.Cleanup(func() { closeHandshakeTranscriptSession(t, old, done) })
			if err := old.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			readUntilContains(t, old, "login: ", 2*time.Second)
			send := func(cmd string) {
				t.Helper()
				if _, err := io.WriteString(old, cmd+"\n"); err != nil {
					t.Fatal(err)
				}
			}
			send(call)
			readUntilContains(t, old, "READY\r\n", 2*time.Second)
			if err := old.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			r := bufio.NewReader(old)
			send("SET JSON ON")
			if ack, err := r.ReadString('\n'); err != nil || ack != "JSON ON 1\r\n" {
				t.Fatalf("activation %q %v", ack, err)
			}
			jsonWireRecord(t, r, "hello")
			jsonWireRecord(t, r, "schema")
			jsonWireRecord(t, r, "schema")
			s.clientsMutex.RLock()
			c := s.clients[call]
			s.clientsMutex.RUnlock()
			if err := c.Send("older output\n"); err != nil {
				t.Fatal(err)
			}
			var first [1]byte
			if _, err := io.ReadFull(old, first[:]); err != nil {
				t.Fatal(err)
			}
			send("SET JSON OFF")
			deadline := time.Now().Add(time.Second)
			for len(c.controlChan) == 0 && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			if len(c.controlChan) != 1 {
				t.Fatal("OFF was not waiting behind the blocked write")
			}
			next, nextReader, _, _ := connectUpgradeSession(t, s, call)
			r = bufio.NewReader(io.MultiReader(bytes.NewReader(first[:]), r))
			for _, want := range []string{"older output", "replacement notice"} {
				f := jsonWireRecord(t, r, "text")
				if len(f) != 2 || string(f[1]) != fmt.Sprintf("%q", want) {
					t.Fatalf("retired writer %s", f)
				}
			}
			if _, err := r.ReadByte(); err != io.EOF {
				t.Fatalf("retired socket end %v", err)
			}
			if _, err := io.WriteString(next, "SET JSON OFF\n"); err != nil {
				t.Fatal(err)
			}
			if line, err := nextReader.ReadString('\n'); err != nil || line != "JSON already off.\r\n" {
				t.Fatalf("new session %q %v", line, err)
			}
		})
	}
}

func TestJSONControlEscapingAndBlankLines(t *testing.T) {
	s, c := v7MachineTestClient(t)
	conn := ve7ccNewCaptureConn(false)
	c.conn = conn
	w := jsonSessionWriter{out: jsonWriterOutput{client: c, buf: make([]byte, 0, 4096)}}
	for _, msg := range []controlMessage{
		{line: "one\xff\r\n\r\nthree\n"},
		{kind: controlBulletin, bulletinKind: "ANNOUNCE", bulletinFrom: "K1ABC", line: "quote \" and \x80\n"},
	} {
		if _, _, err := w.control(msg); err != nil {
			t.Fatal(err)
		}
	}
	got := string(<-conn.writes) + string(<-conn.writes)
	want := "[\"text\",\"one�\"]\n[\"text\",\"\"]\n[\"text\",\"three\"]\n[\"bulletin\",\"ANNOUNCE\",null,\"K1ABC\",\"quote \\\" and �\"]\n"
	if got != want || s.JSONSpotStats().InvalidUTF8Bytes != 2 {
		t.Fatalf("invalid bytes=%d wire=%q", s.JSONSpotStats().InvalidUTF8Bytes, got)
	}
}

func TestJSONSwitchFullQueue(t *testing.T) {
	s, c := v7MachineTestClient(t)
	for range cap(c.controlChan) {
		c.controlChan <- controlMessage{line: "earlier\n"}
	}
	if _, ok := s.handleJSONCommand(c, "SET JSON ON"); ok || c.jsonRequested.Load() {
		t.Fatal("full queue accepted transition")
	}
	select {
	case <-c.done:
	default:
		t.Fatal("reader stranded on full queue")
	}
}

func TestJSONLargeSpotCountsFirstFlush(t *testing.T) {
	s, c := v7MachineTestClient(t)
	s.latency = newLatencyMetrics()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	conn := &jsonBlockedBoundary{Conn: a, target: 2, entered: make(chan struct{})}
	c.conn = conn
	w := jsonSessionWriter{out: jsonWriterOutput{client: c, buf: make([]byte, 0, 64), enqueueTimes: make([]time.Time, 0, 64)}}
	sp := ve7ccWireFixture()
	sp.Comment = strings.Repeat("x", 8192)
	done := make(chan error, 1)
	go func() { done <- w.live(&spotEnvelope{spot: sp, enqueueAt: time.Now()}) }()
	writerV15Wait(t, conn.entered, "second socket flush")
	_, sample, _ := s.LatencySnapshots()
	if sample.N != 1 {
		t.Errorf("first flush received %d latency samples", sample.N)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("lost peer did not interrupt the incomplete row")
	}
}

func TestJSONQueuedSpotsAcrossSwitches(t *testing.T) {
	s, c := v7MachineTestClient(t)
	conn := ve7ccNewCaptureConn(true)
	ve7ccAttachWriter(c, conn)
	s.jsonLimits = newJSONSpotLimits(128, 0, spot.CurrentTaxonomy())
	c.writerStopped = make(chan struct{})
	first := ve7ccWireFixture()
	c.spotChan <- &spotEnvelope{spot: first}
	done := writerV15Start(c)
	t.Cleanup(func() { conn.unblock(); c.close(""); writerV15Wait(t, done, "switch cleanup") })
	writerV15Wait(t, conn.entered, "gated legacy spot")
	switchOn := &jsonFormatSwitch{flushed: make(chan struct{})}
	c.controlChan <- controlMessage{jsonSwitch: switchOn}
	second := ve7ccWireFixture()
	second.ID = 202
	c.spotChan <- &spotEnvelope{spot: second, enqueueAt: time.Now()}
	conn.unblock()
	writerV15Wait(t, switchOn.flushed, "on")
	read := func() string {
		t.Helper()
		select {
		case b := <-conn.writes:
			return string(b)
		case <-time.After(3 * time.Second):
			t.Fatal("writer stalled")
			return ""
		}
	}
	if got := read(); got != writerV15Oracle(first.FormatDXCluster()+"\n") {
		t.Fatalf("first text spot: %q", got)
	}
	if got := read(); got != "JSON ON 1\r\n" {
		t.Fatalf("ack %q", got)
	}
	intro := bufio.NewReader(strings.NewReader(read()))
	jsonWireRecord(t, intro, "hello")
	jsonWireRecord(t, intro, "schema")
	jsonWireRecord(t, intro, "schema")
	f := jsonWireRecord(t, bufio.NewReader(strings.NewReader(read())), "s")
	if string(f[1]) != "202" {
		t.Fatalf("queued spot id %s", f[1])
	}
	switchOff := &jsonFormatSwitch{flushed: make(chan struct{})}
	c.controlChan <- controlMessage{jsonSwitch: switchOff, kind: controlJSONOff}
	c.spotChan <- &spotEnvelope{spot: ve7ccWireFixture()}
	writerV15Wait(t, switchOff.flushed, "off")
	if got := read(); got != "[\"notice\",\"json-off\",\"\"]\n" {
		t.Fatalf("last JSON record %q", got)
	}
	if got := read(); got != writerV15Oracle(first.FormatDXCluster()+"\n") {
		t.Fatalf("last text spot %q", got)
	}
	c.close("")
	writerV15Wait(t, done, "end of tracked spots")
	select {
	case extra := <-conn.writes:
		t.Fatalf("duplicate delivery %q", extra)
	default:
	}
}
