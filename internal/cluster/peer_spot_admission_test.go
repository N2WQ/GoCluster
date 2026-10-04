package cluster

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/dedup"
	"dxcluster/peer"
	"dxcluster/spot"
	"dxcluster/telnet"
)

// This fixture exercises native Telnet decoding and the real primary deduper,
// then uses the production final-delivery path. PC51 acknowledges the preceding
// synchronous read-owner work, so an empty ingest queue proves rejection after
// processing rather than an absence sampled before the sentence was received.
func TestPeerSpotMalformedAdmissionDoesNotPoisonDelivery(t *testing.T) {
	for _, forward := range []bool{true, false} {
		name := "relay enabled"
		if !forward {
			name = "receive only"
		}
		t.Run(name, func(t *testing.T) {
			runPeerSpotAdmissionCases(t, forward)
		})
	}
}

func TestPeerSpotDateSpellingsSharePrimaryIdentity(t *testing.T) {
	for _, forward := range []bool{true, false} {
		for _, typeID := range []string{"PC11", "PC61", "PC26"} {
			t.Run(fmt.Sprintf("%s/forward=%t", typeID, forward), func(t *testing.T) {
				runPeerSpotDateIdentity(t, typeID, forward)
			})
		}
	}
}

func runPeerSpotDateIdentity(t *testing.T, typeID string, forward bool) {
	t.Helper()
	// The fixed historical instant distinguishes validated input from the
	// tolerant parser's fallback to now. Disable age filtering only in this
	// fixture; the production date/age gate is checked by peer handler tests.
	at := time.Date(2024, time.October, 4, 12, 34, 0, 0, time.UTC)
	ingest := make(chan *spot.Spot, 4)
	source, destination := newPeerAdmissionSockets(t, ingest, forward, 0)
	primary := dedup.NewDeduplicator(10*time.Minute, false, 4)
	primary.Start()
	t.Cleanup(primary.Stop)
	ring := buffer.NewRingBuffer(4)
	writer := newDeliveryTestArchiveWriter(t)
	display := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil)
	t.Cleanup(display.Stop)
	pipeline := newDeliveryTestPipeline(ring, writer, display)

	var firstKey [42]byte
	for i, input := range []struct{ date, dx string }{
		{" 4-Oct-2024", "K1DATE"},
		{"04-Oct-2024", "K1DATE"},
		{" 4-Oct-2024", "K1DONE"},
	} {
		fields := peerAdmissionFields(typeID, input.dx, at)
		fields[2] = input.date
		peerAdmissionSendAndAck(t, source, peerAdmissionSentence(typeID, fields, 5))
		var accepted *spot.Spot
		select {
		case accepted = <-ingest:
		default:
			t.Fatalf("date %q did not reach local handoff", input.date)
		}
		assertPeerAdmissionSpot(t, accepted, input.dx, at)
		key := accepted.DedupeKey()
		if i == 0 {
			firstKey = key
		} else if i == 1 && key != firstKey {
			t.Fatalf("equivalent original dates have different primary identities: %x != %x", key, firstKey)
		}
		select {
		case primary.GetInputChannel() <- accepted:
		case <-time.After(3 * time.Second):
			t.Fatal("primary input handoff timed out")
		}
		if forward && i != 1 {
			got := peerAdmissionReadSpot(t, destination)
			if want := peerAdmissionSentence(typeID, fields, 4); got != want {
				t.Fatalf("original date relay=%q want=%q", got, want)
			}
		}
	}

	// A distinct final spot is a FIFO processing barrier. Its output proves
	// that primary dedupe consumed the preceding spelling without emitting
	// it; no sleep or absence sampled before processing establishes the claim.
	for _, dx := range []string{"K1DATE", "K1DONE"} {
		var accepted *spot.Spot
		select {
		case accepted = <-primary.GetOutputChannel():
		case <-time.After(3 * time.Second):
			t.Fatal("primary output timed out")
		}
		assertPeerAdmissionSpot(t, accepted, dx, at)
		pipeline.deliverSpot(&outputSpotContext{spot: accepted})
		archived, ok := tryReadArchiveQueuedSpot(writer)
		if !ok {
			t.Fatal("validated timestamp missing from accepted archive queue")
		}
		assertPeerAdmissionSpot(t, archived, dx, at)
		broadcast, ok := tryReadTelnetBroadcastSpot(display)
		if !ok {
			t.Fatal("validated timestamp missing from display broadcast queue")
		}
		assertPeerAdmissionSpot(t, broadcast, dx, at)
		recent := ring.GetRecent(1)
		if len(recent) != 1 {
			t.Fatal("validated timestamp missing from recent display")
		}
		assertPeerAdmissionSpot(t, recent[0], dx, at)
	}
	processed, duplicates, size := primary.GetStats()
	if processed != 3 || duplicates != 1 || size != 2 || ring.GetCount() != 2 {
		t.Fatalf("primary date identity processed=%d duplicates=%d size=%d display=%d want=3/1/2/2", processed, duplicates, size, ring.GetCount())
	}
	select {
	case got := <-primary.GetOutputChannel():
		t.Fatalf("equivalent original date emitted an extra primary output: %+v", got)
	default:
	}
}

type peerAdmissionSocket struct {
	conn   net.Conn
	reader *bufio.Reader
	call   string
}

type peerAdmissionCase struct {
	name   string
	typeID string
	field  int
	value  string
}

func runPeerSpotAdmissionCases(t *testing.T, forward bool) {
	t.Helper()
	ingest := make(chan *spot.Spot, 4)
	source, destination := newPeerAdmissionSockets(t, ingest, forward, 3600)
	primary := dedup.NewDeduplicator(10*time.Minute, false, 4)
	primary.Start()
	t.Cleanup(primary.Stop)
	ring := buffer.NewRingBuffer(64)
	writer := newDeliveryTestArchiveWriter(t)
	display := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil)
	t.Cleanup(display.Stop)
	pipeline := newDeliveryTestPipeline(ring, writer, display)

	var acceptedCount uint64
	for i, test := range peerSpotAdmissionCases() {
		t.Run(test.name, func(t *testing.T) {
			at := time.Now().UTC().Truncate(time.Minute)
			fields := peerAdmissionFields(test.typeID, fmt.Sprintf("K1Q%02d", i), at)
			malformed := append([]string(nil), fields...)
			if test.field == len(malformed) {
				malformed = append(malformed, test.value)
			} else if test.field == 1 {
				malformed[test.field] = test.value + fields[test.field]
			} else {
				malformed[test.field] = test.value
			}
			peerAdmissionSendAndAck(t, source, peerAdmissionSentence(test.typeID, malformed, 5))
			select {
			case got := <-ingest:
				t.Fatalf("malformed original reached local handoff: %+v", got)
			default:
			}
			assertPeerAdmissionPrimaryStats(t, primary, acceptedCount)
			if ring.GetCount() != int(acceptedCount) {
				t.Fatalf("malformed original reached recent display: count=%d want=%d", ring.GetCount(), acceptedCount)
			}
			if got, ok := tryReadArchiveQueuedSpot(writer); ok {
				t.Fatalf("malformed original reached accepted archive: %+v", got)
			}
			if got, ok := tryReadTelnetBroadcastSpot(display); ok {
				t.Fatalf("malformed original reached display broadcast: %+v", got)
			}
			select {
			case got := <-primary.GetOutputChannel():
				t.Fatalf("malformed original reached primary output: %+v", got)
			default:
			}

			// Keep the calls, frequency and valid timestamp unchanged. Fields
			// outside the dedupe identity are repaired only in this new input;
			// the malformed input must not poison either admission path.
			peerAdmissionSendAndAck(t, source, peerAdmissionSentence(test.typeID, fields, 5))
			var accepted *spot.Spot
			select {
			case accepted = <-ingest:
			default:
				t.Fatal("valid original did not reach local handoff after malformed input")
			}
			assertPeerAdmissionSpot(t, accepted, fields[1], at)
			select {
			case primary.GetInputChannel() <- accepted:
			case <-time.After(3 * time.Second):
				t.Fatal("primary input handoff timed out")
			}
			select {
			case accepted = <-primary.GetOutputChannel():
			case <-time.After(3 * time.Second):
				t.Fatal("valid original was suppressed by primary dedupe")
			}
			assertPeerAdmissionSpot(t, accepted, fields[1], at)
			acceptedCount++
			assertPeerAdmissionPrimaryStats(t, primary, acceptedCount)
			pipeline.deliverSpot(&outputSpotContext{spot: accepted})
			if ring.GetCount() != int(acceptedCount) {
				t.Fatalf("valid recent display count=%d want=%d", ring.GetCount(), acceptedCount)
			}
			recent := ring.GetRecent(1)
			if len(recent) != 1 {
				t.Fatal("valid original missing from recent display")
			}
			assertPeerAdmissionSpot(t, recent[0], fields[1], at)
			archived, ok := tryReadArchiveQueuedSpot(writer)
			if !ok {
				t.Fatal("valid original missing from accepted archive queue")
			}
			assertPeerAdmissionSpot(t, archived, fields[1], at)
			broadcast, ok := tryReadTelnetBroadcastSpot(display)
			if !ok {
				t.Fatal("valid original missing from display broadcast")
			}
			assertPeerAdmissionSpot(t, broadcast, fields[1], at)
			if forward {
				got := peerAdmissionReadSpot(t, destination)
				want := peerAdmissionSentence(test.typeID, fields, 4)
				if got != want {
					t.Fatalf("valid same-identity relay=%q want=%q", got, want)
				}
			}
		})
	}
}

func peerSpotAdmissionCases() []peerAdmissionCase {
	common := []struct {
		name  string
		field int
		value string
	}{
		{"exponent frequency", 0, "1.402e4"},
		{"padded DX call", 1, " "},
		{"invalid calendar date", 2, "32-Oct-2026"},
		{"invalid UTC time", 3, "2460Z"},
		{"empty comment", 4, ""},
		{"C0 comment byte", 4, "bad\x01comment"},
		{"C1 comment byte", 4, "bad\x85comment"},
		{"escaped IAC comment byte", 4, "bad\xffcomment"},
		{"padded spotter", 5, " W1XYZ"},
		{"empty origin", 6, ""},
	}
	cases := make([]peerAdmissionCase, 0, 35)
	for _, typeID := range []string{"PC11", "PC61", "PC26"} {
		for _, item := range common {
			cases = append(cases, peerAdmissionCase{typeID + "/" + item.name, typeID, item.field, item.value})
		}
		fields := 7
		if typeID != "PC11" {
			fields = 8
		}
		cases = append(cases, peerAdmissionCase{typeID + "/extra field", typeID, fields, "EXTRA"})
	}
	cases = append(cases,
		peerAdmissionCase{"PC61/invalid original IP", "PC61", 7, "not-an-ip"},
		peerAdmissionCase{"PC26/invalid merge request", "PC26", 7, "NOPE"},
	)
	return cases
}

func peerAdmissionFields(typeID, dx string, at time.Time) []string {
	fields := []string{"14020.00", dx, at.Format("02-Jan-2006"), at.Format("1504Z"), "valid original", "W1XYZ", "K2ORG"}
	switch typeID {
	case "PC61":
		fields = append(fields, "192.0.2.7")
	case "PC26":
		fields = append(fields, " ")
	}
	return fields
}

func peerAdmissionSentence(typeID string, fields []string, hop int) string {
	return fmt.Sprintf("%s^%s^H%d^~", typeID, strings.Join(fields, "^"), hop)
}

func assertPeerAdmissionPrimaryStats(t *testing.T, primary *dedup.Deduplicator, accepted uint64) {
	t.Helper()
	processed, duplicates, size := primary.GetStats()
	if processed != accepted || duplicates != 0 || size != int(accepted) {
		t.Fatalf("primary stats processed=%d duplicates=%d size=%d want=%d/0/%d", processed, duplicates, size, accepted, accepted)
	}
}

func assertPeerAdmissionSpot(t *testing.T, got *spot.Spot, dx string, at time.Time) {
	t.Helper()
	if got == nil || got.DXCallNorm != dx || got.DECallNorm != "W1XYZ" || got.Frequency != 14020 || !got.Time.Equal(at) || got.SourceType != spot.SourcePeer || got.SourceNode != "K2ORG" {
		t.Fatalf("accepted spot identity=%+v want DX=%s DE=W1XYZ frequency=14020 time=%s source=peer/K2ORG", got, dx, at.Format(time.RFC3339))
	}
}

func newPeerAdmissionSockets(t *testing.T, ingest chan<- *spot.Spot, forward bool, maxAgeSeconds int) (*peerAdmissionSocket, *peerAdmissionSocket) {
	t.Helper()
	lc := net.ListenConfig{}
	reserved, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := reserved.Addr().(*net.TCPAddr).Port
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := peerRuntimeTestConfig().Peering
	cfg.MaxPeers, cfg.ListenPort, cfg.ForwardSpots = 2, port, forward
	cfg.TelnetTransport = config.TelnetTransportNative
	cfg.Timeouts = config.PeeringTimeouts{LoginSeconds: 5, InitSeconds: 5, IdleSeconds: 30}
	cfg.Peers = []config.PeeringPeer{
		{Enabled: true, Direction: config.PeeringPeerDirectionInbound, Family: config.PeeringPeerFamilyDXSpider, RemoteCallsign: "N1REM"},
		{Enabled: true, Direction: config.PeeringPeerDirectionInbound, Family: config.PeeringPeerFamilyDXSpider, RemoteCallsign: "N2DST", PreferPC9x: true},
	}
	manager, err := peer.NewManager(cfg, cfg.LocalCallsign, ingest, maxAgeSeconds, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Stop)
	if err := manager.SetBuildIdentity("test", "", "", "", ""); err != nil {
		t.Fatal(err)
	}
	manager.SetMembershipProvider(func() peer.LocalMembership { return peer.LocalMembership{Complete: true} })
	if err := manager.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	address := fmt.Sprintf("127.0.0.1:%d", port)
	source := peerAdmissionLogin(t, address, "N1REM", false)
	destination := peerAdmissionLogin(t, address, "N2DST", true)
	// PC22 is written during the handshake. The following ping is handled
	// only after registration, establishing the fixture without polling.
	peerAdmissionSendAndAck(t, source, "")
	peerAdmissionSendAndAck(t, destination, "")
	return source, destination
}

func peerAdmissionLogin(t *testing.T, address, call string, modern bool) *peerAdmissionSocket {
	t.Helper()
	dialer := net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(t.Context(), "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	socket := &peerAdmissionSocket{conn: conn, reader: bufio.NewReaderSize(conn, 65538), call: call}
	if got := peerAdmissionReadLine(t, socket); got != "login:" {
		t.Fatalf("peer login prompt=%q", got)
	}
	peerAdmissionWrite(t, socket, call)
	if got := peerAdmissionReadLine(t, socket); !strings.HasPrefix(got, "PC18^") {
		t.Fatalf("peer startup=%q want PC18", got)
	}
	banner := "PC18^DXSpider Version: 1.57 Build: 633"
	if modern {
		banner += " [pc9x 91]"
	}
	peerAdmissionWrite(t, socket, banner+"^5457^\r\nPC20^")
	for i := 0; i < 16; i++ {
		if peerAdmissionReadLine(t, socket) == "PC22^" {
			return socket
		}
	}
	t.Fatal("peer handshake exceeded bounded startup exchange")
	return nil
}

func peerAdmissionSendAndAck(t *testing.T, socket *peerAdmissionSocket, sentence string) {
	t.Helper()
	ping := fmt.Sprintf("PC51^N0CALL-1^%s^1^", socket.call)
	if sentence != "" {
		ping = sentence + "\r\n" + ping
	}
	peerAdmissionWrite(t, socket, ping)
	want := fmt.Sprintf("PC51^%s^N0CALL-1^0^", socket.call)
	for i := 0; i < 16; i++ {
		got := peerAdmissionReadLine(t, socket)
		if got == want {
			return
		}
		if strings.HasPrefix(got, "PC11^") || strings.HasPrefix(got, "PC61^") || strings.HasPrefix(got, "PC26^") {
			t.Fatalf("unexpected peer spot before processing ACK: %q", got)
		}
	}
	t.Fatal("processing ACK missing from bounded exchange")
}

func peerAdmissionWrite(t *testing.T, socket *peerAdmissionSocket, sentence string) {
	t.Helper()
	if err := socket.conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// A decoded literal IAC is represented by doubled IAC on native Telnet
	// ingress. Sending unescaped FF would test transport consumption instead
	// of the shared original-comment rejection rule.
	wire := strings.ReplaceAll(sentence, "\xff", "\xff\xff") + "\r\n"
	if _, err := io.WriteString(socket.conn, wire); err != nil {
		t.Fatal(err)
	}
}

func peerAdmissionReadSpot(t *testing.T, socket *peerAdmissionSocket) string {
	t.Helper()
	for i := 0; i < 16; i++ {
		line := peerAdmissionReadLine(t, socket)
		if strings.HasPrefix(line, "PC11^") || strings.HasPrefix(line, "PC61^") || strings.HasPrefix(line, "PC26^") {
			return line
		}
	}
	t.Fatal("peer relay missing from bounded exchange")
	return ""
}

func peerAdmissionReadLine(t *testing.T, socket *peerAdmissionSocket) string {
	t.Helper()
	if err := socket.conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line, err := socket.reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
}
