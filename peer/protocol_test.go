package peer

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestParseFrameStripsSingleHopSuffix(t *testing.T) {
	frame, err := ParseFrame("PC92^OH2J^76586.01^A^^1RV1CC:178.70.200.33^H95^")
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	if frame.Hop != 95 {
		t.Fatalf("expected hop=95, got %d", frame.Hop)
	}
	if got, want := len(frame.Fields), 5; got != want {
		t.Fatalf("expected %d payload fields, got %d (%v)", want, got, frame.Fields)
	}
	last := frame.Fields[len(frame.Fields)-1]
	if last != "1RV1CC:178.70.200.33" {
		t.Fatalf("expected last payload field to be entry, got %q", last)
	}
}

func TestParseFrameRejectsCaretHeavyAuthorityFramesBeforeSplit(t *testing.T) {
	for _, header := range []string{"PC92^", " pC92^", "PC93^", "pc93^"} {
		wire := header + strings.Repeat("^", 16000) + "H99^"
		if _, err := ParseFrame(wire); err == nil {
			t.Fatalf("accepted caret-heavy %q", header)
		}
	}
	if _, err := ParseFrame("PC93^K1ABC^1^K2ABC^K3ABC^*^H123^^^EXTRA^H99^"); err == nil {
		t.Fatal("accepted unknown PC93 payload slot")
	}
}

func TestParseFrameStripsStackedHopSuffixAndUsesRightmostHop(t *testing.T) {
	frame, err := ParseFrame("PC92^OH2J^76586.01^A^^1RV1CC:178.70.200.33^H95^H94^H93^")
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	if frame.Hop != 93 {
		t.Fatalf("expected hop=93 from rightmost token, got %d", frame.Hop)
	}
	if got, want := len(frame.Fields), 5; got != want {
		t.Fatalf("expected %d payload fields, got %d (%v)", want, got, frame.Fields)
	}
}

func TestEncodeCanonicalizesTrailingHopSuffix(t *testing.T) {
	frame, err := ParseFrame("PC92^OH2J^76586.01^A^^1RV1CC:178.70.200.33^H95^")
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	got := frame.Encode(frame.Hop - 1)
	want := "PC92^OH2J^76586.01^A^^1RV1CC:178.70.200.33^H94^"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestPayloadFieldsStripsTrailingHopSuffixRun(t *testing.T) {
	in := []string{"NODE", "123", "A", "", "9CALL:ver", "H95", "H94", "H93", ""}
	out := PayloadFields(in)
	if got, want := len(out), 5; got != want {
		t.Fatalf("expected %d payload fields, got %d (%v)", want, got, out)
	}
	if out[len(out)-1] != "9CALL:ver" {
		t.Fatalf("expected final payload entry, got %q", out[len(out)-1])
	}
}

func TestParseFrameMalformedTrailingHopLikeTokensAreStripped(t *testing.T) {
	frame, err := ParseFrame("PC92^NODE^123^A^^9CALL:ver^H99^H9x^")
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	if frame.Hop != 99 {
		t.Fatalf("expected hop from rightmost numeric token, got %d", frame.Hop)
	}
	if got, want := len(frame.Fields), 5; got != want {
		t.Fatalf("expected %d payload fields, got %d (%v)", want, got, frame.Fields)
	}
}

func TestParseFramePreservesPC93TextAndEmptyMetadata(t *testing.T) {
	for _, text := range []string{"H123", "H9x", ""} {
		wire := "PC93^GB7DJK^123^K1ABC^G1TLH^*^" + text + "^^^H99^H0^"
		frame, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"GB7DJK", "123", "K1ABC", "G1TLH", "*", text, "", ""}
		if frame.Hop != 0 || !reflect.DeepEqual(frame.payloadFields(), want) {
			t.Fatalf("payload or H0 lost: %+v", frame)
		}
		wantWire := "PC93^GB7DJK^123^K1ABC^G1TLH^*^" + text + "^^^H0^"
		if got := frame.Encode(0); got != wantWire {
			t.Fatalf("%q != %q", got, wantWire)
		}
		short, err := ParseFrame("PC93^GB7DJK^123^K1ABC^G1TLH^*^" + text + "^H99^")
		if err != nil || len(short.Fields) != 6 || short.Fields[5] != text {
			t.Fatalf("short text lost: %+v %v", short, err)
		}
	}
}

func TestTelnetNegotiationEveryReadBoundary(t *testing.T) {
	wire := []byte{'A', telnetIAC, telnetDO, 1, 'B', telnetIAC, telnetWILL, 3, telnetIAC, telnetSB, 24, 'x', telnetIAC, telnetIAC, 'y', telnetIAC, telnetSE, 'C', telnetIAC, telnetIAC, 'D'}
	want := []byte{'A', 'B', 'C', telnetIAC, 'D'}
	wantReplies := []byte{telnetIAC, telnetWONT, 1, telnetIAC, telnetDONT, 3}
	for split := 0; split <= len(wire); split++ {
		p := &telnetParser{}
		a, ar := p.Feed(wire[:split])
		b, br := p.Feed(wire[split:])
		if !bytes.Equal(append(a, b...), want) || !bytes.Equal(bytes.Join(append(ar, br...), nil), wantReplies) {
			t.Fatalf("split%d: %v/%v replies%v/%v", split, a, b, ar, br)
		}
	}
	p := &telnetParser{}
	var got, replies []byte
	for _, b := range wire {
		out, rs := p.Feed([]byte{b})
		got = append(got, out...)
		replies = append(replies, bytes.Join(rs, nil)...)
	}
	if !bytes.Equal(got, want) || !bytes.Equal(replies, wantReplies) {
		t.Fatalf("byte reads: %v %v", got, replies)
	}
}
