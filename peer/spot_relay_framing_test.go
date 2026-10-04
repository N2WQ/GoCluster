package peer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Read owns this recorder; no concurrent observation of its buffer occurs.
// It observes the real writer socket before the receiving native transport
// strips sentence terminators, so wire fidelity and receiver fidelity differ.
type peerSpotRecordingConn struct {
	net.Conn
	wire bytes.Buffer
}

func (c *peerSpotRecordingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.wire.Write(p[:n])
	return n, err
}

func readNativePeerSpotWriter(t *testing.T, destination *session, remote net.Conn) (string, *Frame) {
	t.Helper()
	destination.startWorker(destination.writerLoop)
	recorded := &peerSpotRecordingConn{Conn: remote}
	reader := newLineReader(recorded, MaxPeerFrameBytes, MaxPeerFrameBytes, nil)
	t.Cleanup(reader.release)
	line, err := reader.ReadLine(time.Now().Add(3 * time.Second))
	if err != nil {
		t.Fatalf("receiving native reader failed: %v (wire=%q)", err, recorded.wire.String())
	}
	frame, err := ParseFrame(line)
	if err != nil {
		t.Fatalf("receiving frame parser failed: %v (line=%q)", err, line)
	}
	if _, err := validateOriginalPeerSpot(frame); err != nil {
		t.Fatalf("relay did not retain original validity: %v", err)
	}
	if !strings.HasSuffix(recorded.wire.String(), "^~\r\n") {
		t.Fatalf("production writer lost its terminal marker or CRLF: %q", recorded.wire.String())
	}
	return recorded.wire.String(), frame
}

func peerSpotFramingPayload(comment string) string {
	return "14074.1^K1ABC^ 9-Oct-2026^1200Z^" + comment + "^W1XYZ^GB7REF"
}

func TestPeerSpotTildeNativeRelay(t *testing.T) {
	for _, comment := range []string{"~", "~CQ", "CQ~", "~~", "CQ~~TEST", "CQ~PC61~TEST", "~CQ~~PC00~TEST~"} {
		for _, kind := range []string{"PC11", "PC61", "PC26"} {
			for _, forward := range []bool{false, true} {
				for _, modern := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%q/forward=%t/modern=%t", kind, comment, forward, modern), func(t *testing.T) {
						payload := peerSpotFramingPayload(comment)
						switch kind {
						case "PC61":
							payload += "^2001:0dB8:0000:0000::0001"
						case "PC26":
							payload += "^ "
						}
						result := observeNativePeerSpot(t, kind+"^"+payload+"^H3^~", forward, modern)
						if result.local == nil || result.local.Comment != comment || result.frame.Fields[4] != comment {
							t.Fatalf("native input comment was truncated or rejected: %+v", result)
						}
						wantWire, wantEntries := "", 0
						if forward {
							wantEntries = 1
							if kind != "PC26" || modern {
								outKind, outPayload := kind, payload
								if kind == "PC61" && !modern {
									outKind, outPayload = "PC11", peerSpotFramingPayload(comment)
								}
								wantWire = outKind + "^" + outPayload + "^H2^~\r\n"
							}
						}
						if result.wire != wantWire || result.peerEntries != wantEntries {
							t.Fatalf("tilde relay bytes/gates changed: wire=%q entries=%d wantWire=%q wantEntries=%d", result.wire, result.peerEntries, wantWire, wantEntries)
						}
						if wantWire != "" && (result.received == nil || result.received.Fields[4] != comment || result.received.Hop != 2) {
							t.Fatalf("second native receiver lost the comment/hop: %+v", result.received)
						}
					})
				}
			}
		}
	}
}

func TestPeerSpotTildeRestrictedToCommentField(t *testing.T) {
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		base := strings.Split(strings.TrimSuffix(originalSpotTestWire(kind, "~CQ~~TEST~"), "^H3^"), "^")
		for index := 1; index < len(base); index++ {
			if index == 5 {
				continue // fifth caret begins the admitted comment payload
			}
			fields := append([]string(nil), base...)
			fields[index] += "~"
			if _, err := ParseFrame(strings.Join(fields, "^") + "^H3^~"); err == nil {
				t.Errorf("%s admitted internal tilde outside comment, field=%d", kind, index-1)
			}
		}
	}
}

// RFC 4291 section 2.2 supplies the first forms. Additional literal vectors
// distinguish valid original spellings from RFC 5952 canonical presentation;
// address class or public routability is not the selected admission policy.
func originalPeerIPv6Vectors() []string {
	return []string{
		"ABCD:EF01:2345:6789:ABCD:EF01:2345:6789",
		"2001:DB8:0:0:8:800:200C:417A", "2001:DB8::8:800:200C:417A", "FF01::101",
		"0:0:0:0:0:0:0:1", "::1", "0:0:0:0:0:0:0:0", "::",
		"0:0:0:0:0:0:13.1.68.3", "0:0:0:0:0:FFFF:129.144.52.38", "::13.1.68.3", "::FFFF:129.144.52.38",
		"2001:0dB8:0000:0000:0008:0800:200c:417a",
		"2001:db8:1::2:3:4:5", "2001:db8::1:0:0:0:1",
		"::ffff:192.0.2.1", "2001:db8::192.0.2.1", "fe80::1", "ff02::1", "2001:db8::1:23",
	}
}

func TestPeerSpotRFC4291IPv6NativeRelay(t *testing.T) {
	for _, ip := range originalPeerIPv6Vectors() {
		for _, modern := range []bool{false, true} {
			t.Run(ip+fmt.Sprintf("/modern=%t", modern), func(t *testing.T) {
				// Each observation has a fresh manager: IP and comments are
				// intentionally absent from the existing peer dedupe identity.
				payload := peerSpotFramingPayload("~CQ~~TEST~")
				result := observeNativePeerSpot(t, "PC61^"+payload+"^"+ip+"^H3^~", true, modern)
				if result.local == nil || result.local.SpotterIP != ip || result.frame.Fields[7] != ip || result.peerEntries != 1 {
					t.Fatalf("legitimate original IPv6 rejected/changed: IP=%q observation=%+v", ip, result)
				}
				want := "PC11^" + payload + "^H2^~\r\n"
				if modern {
					want = "PC61^" + payload + "^" + ip + "^H2^~\r\n"
				}
				if result.wire != want || result.received == nil {
					t.Fatalf("original IP bytes did not survive writer/native receiver: got=%q want=%q", result.wire, want)
				}
				if modern {
					if result.received.Type != "PC61" || result.received.Fields[7] != ip {
						t.Fatal("modern relay canonicalized or removed original IP text")
					}
				} else if result.received.Type != "PC11" || len(result.received.Fields) != 7 {
					t.Fatal("legacy conversion removed more than the IP field")
				}
			})
		}
	}
}

func TestOriginalPeerSpotRejectsMalformedIPv6(t *testing.T) {
	for _, ip := range []string{
		"1:2:3:4:5:6:7", "1:2:3:4:5:6:7:8:9", "1:2:3:4:5:6:7:8::", "1::2::3", ":::1",
		"2001:db8::g", "2001:db8::00001", "2001:db8:", ":2001:db8::1",
		"::ffff:192.0.2.256", "::ffff:192.0.2", "::ffff:192.00.2.1", "::ffff:192.0.2.1:80",
		"1:2:3:4:5:192.0.2.1", "[2001:db8::1]", "[2001:db8::1]:23", "fe80::1%eth0", "2001:db8::1/64",
		" 2001:db8::1", "2001:db8::1 ", "\t2001:db8::1", "2001:db8::1\n",
	} {
		t.Run(ip, func(t *testing.T) {
			frame := &Frame{Type: "PC61", Hop: 3, Fields: []string{
				"14074.1", "K1ABC", " 9-Oct-2026", "1200Z", "~CQ~~TEST~", "W1XYZ", "GB7REF", ip,
			}}
			if _, err := validateOriginalPeerSpot(frame); err == nil {
				t.Fatalf("malformed/wrapped original address admitted: %q", ip)
			}
		})
	}
}

func TestDXSpiderReferenceGeneratedPeerSpotComments(t *testing.T) {
	at := time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC)
	ip := "2001:0dB8:0000:0000::0007"
	for _, comment := range []string{"~CQ", "CQ~", "~", "CQ~~TEST", "~CQ~~PC61~TEST~"} {
		generated := generateReferencePeerSpotsWithInput(t, at, comment, ip)
		for _, kind := range []string{"PC11", "PC61", "PC26"} {
			payload, suffix := peerSpotFramingPayload(comment), "^H3^~"
			switch kind {
			case "PC61":
				payload += "^" + ip
			case "PC26":
				suffix = "^ ^~"
			}
			if want := kind + "^" + payload + suffix; generated[kind] != want {
				t.Fatalf("unmodified pinned generator changed controlled input: got=%q want=%q", generated[kind], want)
			}
			for _, forward := range []bool{false, true} {
				for _, modern := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%q/forward=%t/modern=%t", kind, comment, forward, modern), func(t *testing.T) {
						result := observeNativePeerSpot(t, generated[kind], forward, modern)
						if result.local == nil || !result.local.Time.Equal(at) || result.local.Comment != comment || result.frame.Fields[4] != comment {
							t.Fatalf("actual sender comment/timestamp lost during local admission: %+v", result)
						}
						want, entries := "", 0
						if forward && kind != "PC26" {
							outKind, outPayload := kind, payload
							if kind == "PC61" && !modern {
								outKind, outPayload = "PC11", peerSpotFramingPayload(comment)
							}
							want, entries = outKind+"^"+outPayload+"^H2^~\r\n", 1
						}
						if result.wire != want || result.peerEntries != entries {
							t.Fatalf("actual generator forwarding/no-hop gates changed: %+v want=%q entries=%d", result, want, entries)
						}
						if kind == "PC26" && (result.frame.Hop != 0 || result.received != nil) {
							t.Fatal("unmodified no-hop PC26 acquired relay eligibility")
						}
						if want != "" && result.received.Fields[4] != comment {
							t.Fatal("actual sender comment lost at receiving peer")
						}
					})
				}
			}
		}
	}
}

func TestDXSpiderReferenceGeneratedCaretCommentReplacement(t *testing.T) {
	at := time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC)
	generated := generateReferencePeerSpotsWithInput(t, at, "^CQ^^TEST^", "203.0.113.7")
	// Only PC11/PC61 replace carets. PC26's unmodified generator does not;
	// literal PC26 tildes are covered separately rather than repaired here.
	for _, kind := range []string{"PC11", "PC61"} {
		result := observeNativePeerSpot(t, generated[kind], true, true)
		want := kind + "^" + peerSpotFramingPayload("~CQ~~TEST~")
		if kind == "PC61" {
			want += "^203.0.113.7"
		}
		want += "^H2^~\r\n"
		if result.local == nil || result.frame.Fields[4] != "~CQ~~TEST~" || result.received == nil || result.received.Fields[4] != "~CQ~~TEST~" || result.wire != want {
			t.Fatalf("pinned sender's actual caret substitution did not survive relay: %+v want=%q", result, want)
		}
	}
}

func TestDXSpiderReferenceGeneratedPC26CommentAddedTransportHop(t *testing.T) {
	at := time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC)
	comment := "~CQ~~PC61~TEST~"
	generated := generateReferencePeerSpotsWithInput(t, at, comment, "203.0.113.7")["PC26"]
	// Explicit transport adaptation, separate from the actual sender's
	// no-hop admission observations above. DXSpider itself emits no H3.
	adapted := strings.TrimSuffix(generated, "~") + "H3^~"
	for _, modern := range []bool{false, true} {
		t.Run(fmt.Sprintf("modern=%t", modern), func(t *testing.T) {
			result := observeNativePeerSpot(t, adapted, true, modern)
			if result.local == nil || !result.local.Time.Equal(at) || result.local.Comment != comment || result.frame.Hop != 3 || result.peerEntries != 1 {
				t.Fatalf("adapted PC26 failed local/peer eligibility: %+v", result)
			}
			want := ""
			if modern {
				want = "PC26^" + peerSpotFramingPayload(comment) + "^ ^H2^~\r\n"
			}
			if result.wire != want || (modern && (result.received == nil || result.received.Fields[4] != comment)) {
				t.Fatalf("adapted PC26 writer/receiving reader or destination gate changed: got=%q want=%q", result.wire, want)
			}
		})
	}
}

// The write worker and pipe are test-owned and joined on every failure path.
// Input deliberately has terminal tildes without CRLF; a broken splitter
// cannot obtain the expected separate records by waiting for a newline/EOF.
// The end marker proves there were no extra fragments before write completion;
// closing net.Pipe early would instead fail SetReadDeadline on buffered reads.
func newNativePeerSpotInputReader(t *testing.T, wire string, maxLine, chunk int) (*lineReader, <-chan error) {
	t.Helper()
	wire += "PC00^TESTEND^H0^~"
	local, remote := net.Pipe()
	reader := newLineReader(local, maxLine, maxLine, nil)
	written, joined := make(chan error, 1), make(chan struct{})
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close(); <-joined; reader.release() })
	go func() {
		defer close(joined)
		if err := remote.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			written <- err
			return
		}
		for offset := 0; offset < len(wire); offset += chunk {
			if _, err := io.WriteString(remote, wire[offset:min(offset+chunk, len(wire))]); err != nil {
				written <- err
				return
			}
		}
		written <- nil
	}()
	return reader, written
}

func assertNativePeerSpotInputSequence(t *testing.T, reader *lineReader, generated map[string]string) {
	t.Helper()
	for _, kind := range []string{"PC61", "PC11", "PC26"} {
		line, err := reader.ReadLine(time.Now().Add(5 * time.Second))
		want := strings.TrimSuffix(generated[kind], "~")
		if err != nil || line != want {
			t.Fatalf("actual sender fragmented/consecutive framing changed: got=%q want=%q err=%v", line, want, err)
		}
		frame, err := ParseFrame(line)
		if err != nil {
			t.Fatal(err)
		}
		wantFields := []string{"14074.1", "K1ABC", " 9-Oct-2026", "1200Z", "~CQ~~PC61~TEST~", "W1XYZ", "GB7REF"}
		switch kind {
		case "PC61":
			wantFields = append(wantFields, "203.0.113.7")
		case "PC26":
			wantFields = append(wantFields, " ")
		}
		if !reflect.DeepEqual(frame.Fields, wantFields) {
			t.Fatalf("actual sender's original fields changed: got=%q want=%q", frame.Fields, wantFields)
		}
		if _, err := validateOriginalPeerSpot(frame); err != nil {
			t.Fatal(err)
		}
	}
	if marker, err := reader.ReadLine(time.Now().Add(5 * time.Second)); marker != "PC00^TESTEND^H0^" || err != nil {
		t.Fatalf("comment tildes produced an extra record before the end marker: line=%q err=%v", marker, err)
	}
}

func TestDXSpiderReferenceGeneratedCommentFragmentationAndConsecutiveFrames(t *testing.T) {
	at := time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC)
	generated := generateReferencePeerSpotsWithInput(t, at, "~CQ~~PC61~TEST~", "203.0.113.7")
	wire := generated["PC61"] + generated["PC11"] + generated["PC26"]
	for _, chunk := range []int{1, 2, 3, 4, 5, 7, 17, len(wire)} {
		t.Run(fmt.Sprintf("chunk=%d", chunk), func(t *testing.T) {
			reader, written := newNativePeerSpotInputReader(t, wire, MaxPeerFrameBytes, chunk)
			assertNativePeerSpotInputSequence(t, reader, generated)
			if err := <-written; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDXSpiderReferenceGeneratedCommentOverflowRecovery(t *testing.T) {
	at := time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC)
	generated := generateReferencePeerSpotsWithInput(t, at, "~CQ~~PC61~TEST~", "203.0.113.7")
	overlong := generateReferencePeerSpotsWithInput(t, at, strings.Repeat("X", 100)+"~PC00~~"+strings.Repeat("Y", 100), "203.0.113.7")["PC61"]
	const limit = 128
	for _, sentence := range generated {
		if len(sentence) > limit {
			t.Fatal("recovery fixture does not fit the lowered reader limit")
		}
	}
	wire := overlong + generated["PC61"] + generated["PC11"] + generated["PC26"]
	for _, chunk := range []int{1, 17, len(wire)} {
		t.Run(fmt.Sprintf("chunk=%d", chunk), func(t *testing.T) {
			reader, written := newNativePeerSpotInputReader(t, wire, limit, chunk)
			if _, err := reader.ReadLine(time.Now().Add(5 * time.Second)); err == nil {
				t.Fatal("oversized reference-generated spot was admitted")
			} else {
				var tooLong ErrLineTooLong
				if !errors.As(err, &tooLong) {
					t.Fatalf("overflow refused for the wrong reason: %v", err)
				}
			}
			assertNativePeerSpotInputSequence(t, reader, generated)
			if err := <-written; err != nil {
				t.Fatal(err)
			}
		})
	}
}
