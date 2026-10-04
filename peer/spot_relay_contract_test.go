package peer

import (
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

func originalSpotTestWire(kind string, comment string) string {
	fields := []string{"14074.0249", "K1ABC-123", "01-oCt-2026", "1200Z", comment, "W1XYZ-#", "H1ABC"}
	switch kind {
	case "PC61":
		fields = append(fields, "2001:0db8:0:0::0001")
	case "PC26":
		fields = append(fields, " ")
	}
	return kind + "^" + strings.Join(fields, "^") + "^H3^"
}

func TestOriginalPeerSpotValidity(t *testing.T) {
	bad := []struct {
		name  string
		index int
		value string
	}{
		{"signed frequency", 0, "+14074"}, {"negative frequency", 0, "-1"},
		{"exponent frequency", 0, "1e4"}, {"hex frequency", 0, "0x1p10"},
		{"nonfinite frequency", 0, "NaN"}, {"infinite frequency", 0, "Inf"},
		{"empty frequency", 0, ""}, {"frequency padding", 0, "14074 "},
		{"leading decimal", 0, ".1"}, {"trailing decimal", 0, "1."},
		{"multiple decimals", 0, "1.2.3"}, {"key overflow", 0, "4294967296"},
		{"rounded key overflow", 0, "4294967295.999"},
		{"lowercase DX", 1, "k1abc"}, {"padded DX", 1, " K1ABC"},
		{"correctable DX", 1, "K1ABC."}, {"invalid DX identity", 1, "FT8"},
		{"lowercase DE", 5, "w1xyz"}, {"padded DE", 5, "W1XYZ "},
		{"correctable DE", 5, "W1XYZ."}, {"missing DE", 5, ""},
		{"invalid date", 2, "31-Feb-2026"}, {"nonleap date", 2, "29-Feb-2025"},
		{"date padding", 2, "01-Oct-2026 "}, {"date shape", 2, "1-Oct-2026"},
		{"date fallback", 2, ""}, {"invalid month", 2, "01-Foo-2026"},
		{"time fallback", 3, ""}, {"invalid hour", 3, "2400Z"},
		{"invalid minute", 3, "1260Z"}, {"lowercase zone", 3, "1200z"},
		{"time padding", 3, " 1200Z"}, {"short time", 3, "200Z"},
		{"empty comment", 4, ""}, {"C0 comment", 4, "A\x00B"},
		{"newline comment", 4, "A\nB"}, {"C1 comment", 4, "A\x9fB"},
		{"UTF8 C1 continuation", 4, "A\xc3\x81B"}, {"IAC comment", 4, "A\xffB"},
		{"empty origin", 6, ""}, {"invalid origin", 6, "ORIGIN"},
		{"padded origin", 6, "H1ABC "}, {"lowercase origin", 6, "h1abc"},
	}
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		frame, err := ParseFrame(originalSpotTestWire(kind, "  FT8 -10 dB 1100Z  CQ\tDX \xfe  "))
		if err != nil {
			t.Fatal(err)
		}
		stamp, err := validateOriginalPeerSpot(frame)
		wantTime := time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC)
		if err != nil || stamp != wantTime {
			t.Fatalf("%s valid original: stamp=%v err=%v", kind, stamp, err)
		}
		for _, test := range bad {
			t.Run(kind+"/"+test.name, func(t *testing.T) {
				fields := append([]string(nil), frame.Fields...)
				fields[test.index] = test.value
				if _, err := validateOriginalPeerSpot(&Frame{Type: kind, Fields: fields, Hop: 3}); err == nil {
					t.Fatalf("accepted malformed original %q", fields)
				}
			})
		}
	}
	for _, good := range []string{"0", "0000.000", "0.004", "14074.0001", "2450000", "4294967295.99"} {
		if !validOriginalPeerFrequency(good) {
			t.Errorf("rejected representable plain-decimal frequency %q", good)
		}
	}
	for _, ip := range []string{"", "bad", "192.0.2.1 ", "[2001:db8::1]", "fe80::1%eth0", "192.0.2.1/24", "192.0.2.1:23"} {
		frame, _ := ParseFrame(originalSpotTestWire("PC61", "CQ"))
		frame.Fields[7] = ip
		if _, err := validateOriginalPeerSpot(frame); err == nil {
			t.Errorf("accepted invalid plain IP %q", ip)
		}
	}
	for _, ip := range []string{"0.0.0.0", "192.0.2.1", "::", "2001:0db8:0:0::0001", "::ffff:192.0.2.1"} {
		frame, _ := ParseFrame(originalSpotTestWire("PC61", "CQ"))
		frame.Fields[7] = ip
		if _, err := validateOriginalPeerSpot(frame); err != nil {
			t.Errorf("rejected plain IP %q: %v", ip, err)
		}
	}
}

func TestOriginalPeerCommentByteRule(t *testing.T) {
	for value := 0; value <= 255; value++ {
		// Independently enumerate allowed bytes, rather than repeat the guard.
		allowed := value == 9 || (value >= 32 && value <= 127 && value != '^') || (value >= 160 && value <= 254)
		comment := "A" + string([]byte{byte(value)}) + "B"
		if got := validOriginalPeerComment(comment); got != allowed {
			t.Errorf("byte %02x: accepted=%v want=%v", value, got, allowed)
		}
	}
	for _, comment := range []string{" ", "\t", " \t ", "A\xfeB", "A\xc3\xa9B", "~", "~CQ", "CQ~", "CQ~~TEST"} {
		if !validOriginalPeerComment(comment) {
			t.Errorf("rejected allowed comment %q", comment)
		}
	}
}

func TestOriginalPeerSpotPermittedBytesSurviveNativeTransport(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	reader := NewLineReader(server, MaxPeerFrameBytes, MaxPeerFrameBytes, nil)
	var wires []string
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		for value := 0; value <= 255; value++ {
			allowed := value == 9 || (value >= 32 && value <= 127 && value != '^') || (value >= 160 && value <= 254)
			if !allowed {
				continue
			}
			wires = append(wires, originalSpotTestWire(kind, "A"+string([]byte{byte(value)})+"B"))
		}
	}
	written := make(chan error, 1)
	go func() {
		if err := client.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			written <- err
			return
		}
		for _, wire := range wires {
			if _, err := io.WriteString(client, wire+"~\r\n"); err != nil {
				written <- err
				return
			}
		}
		written <- nil
	}()
	for _, want := range wires {
		line, err := reader.ReadLine(time.Now().Add(5 * time.Second))
		if err != nil || line != want {
			t.Fatalf("native transport changed permitted bytes: got=%q want=%q err=%v", line, want, err)
		}
		frame, err := ParseFrame(line)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := validateOriginalPeerSpot(frame); err != nil {
			t.Fatalf("permitted native payload rejected: %v (%q)", err, line)
		}
		if got := originalPeerSpotSentence(frame, 2, false); got != strings.TrimSuffix(want, "H3^")+"H2^~" {
			t.Fatalf("permitted native payload changed during relay: %q", got)
		}
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

func TestPeerSpotEnvelopeAndPC26NoHopRoundTrip(t *testing.T) {
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		wire := originalSpotTestWire(kind, "H9X")
		for _, suffix := range []string{"H3^", "h03^~\r\n\r\n~~", "H9^h2^H0^"} {
			line := strings.ToLower(kind) + wire[4:len(wire)-3] + suffix
			frame, err := ParseFrame(line)
			if err != nil {
				t.Fatalf("transport tolerance %q: %v", line, err)
			}
			if _, err := validateOriginalPeerSpot(frame); err != nil {
				t.Fatal(err)
			}
			if suffix == "H9^h2^H0^" && frame.Hop != 0 {
				t.Fatal("rightmost numeric hop lost")
			}
		}
		for _, line := range []string{
			" " + wire, wire + " ", wire + "\t\r\n", wire + "^",
			wire[:len(wire)-3] + "H3^H9x^", wire[:len(wire)-3] + "H3^H100^",
			wire[:len(wire)-3] + " H3^", wire[:len(wire)-3] + "H3 ^",
			wire[:len(wire)-3] + "H3^^H2^", wire[:len(wire)-3] + "EXTRA^H2^",
			wire[:len(wire)-1],
		} {
			if _, err := ParseFrame(line); err == nil {
				t.Errorf("accepted malformed sentence %q", line)
			}
		}
	}
	base := "PC26^14074^K1ABC^01-Oct-2026^1200Z^H1ABC^W1XYZ^H2ABC"
	for _, requested := range []string{"omitted", "", " ", "*", "H1ABC", "W1XYZ-123", "W1XYZ-#"} {
		wire := base + "^"
		if requested != "omitted" {
			wire = base + "^" + requested + "^"
		}
		frame, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := validateOriginalPeerSpot(frame); err != nil {
			t.Fatal(err)
		}
		encoded := frame.Encode(-1)
		reparsed, err := ParseFrame(encoded)
		if err != nil || frame.Hop != 0 || !reflect.DeepEqual(frame.Fields, reparsed.Fields) || encoded != wire {
			t.Fatalf("no-hop payload changed: %q -> %q: %+v %v", wire, encoded, reparsed, err)
		}
	}
	for _, requested := range []string{"  ", "\t", "NOCALL", "h1abc", "H123", "H9x"} {
		frame, err := ParseFrame(base + "^" + requested + "^")
		if err == nil {
			_, err = validateOriginalPeerSpot(frame)
		}
		if err == nil {
			t.Errorf("accepted malformed requested merge call %q", requested)
		}
	}
}

// Keep the input untrimmed. The generic historical suffix fuzzer trims it and
// cannot establish admission behavior for outer sentence whitespace.
func FuzzOriginalPeerSpotAdmission(f *testing.F) {
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		for _, comment := range []string{"CQ", " \t ", "A\xfeB", "A\xffB", "A\xc3\x81B", "A~B"} {
			wire := originalSpotTestWire(kind, comment)
			f.Add(wire)
			f.Add(" " + wire)
			f.Add(wire + "\t")
		}
	}
	f.Add("PC26^14074^K1ABC^29-Feb-2024^2359Z^CQ^W1XYZ^H2ABC^H1ABC^")
	f.Add("PC26^14074^K1ABC^29-Feb-2024^2359Z^CQ^W1XYZ^H2ABC^^")
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		for _, date := range []string{" 1-Oct-2026", " 9-Oct-2026", " 0-Oct-2026", "\t1-Oct-2026", "  1-Oct-2026"} {
			f.Add(strings.Replace(originalSpotTestWire(kind, "CQ"), "01-oCt-2026", date, 1))
		}
	}
	f.Fuzz(func(t *testing.T, line string) {
		frame, err := ParseFrame(line)
		if err != nil || !isPeerSpotFrame(frame.Type) {
			return
		}
		if _, err := validateOriginalPeerSpot(frame); err != nil {
			return
		}
		raw := strings.TrimRight(line, "\r\n~")
		if strings.TrimSpace(raw) != raw {
			t.Fatalf("admitted padded sentence %q", line)
		}
		for _, legacy := range []bool{false, true} {
			encoded := originalPeerSpotSentence(frame, frame.Hop, legacy)
			if encoded == "" {
				continue // per-variant size refusal is part of the contract
			}
			if len(encoded) > MaxPeerFrameBytes || !strings.HasSuffix(encoded, "^~") {
				t.Fatalf("invalid output envelope %q", encoded)
			}
			reparsed, err := ParseFrame(encoded)
			if err != nil {
				t.Fatalf("cannot parse admitted relay: %v", err)
			}
			want := frame.Fields
			if legacy && frame.Type == "PC61" {
				want = want[:7]
				if reparsed.Type != "PC11" {
					t.Fatal("legacy PC61 conversion lost")
				}
			}
			if !reflect.DeepEqual(want, reparsed.Fields) || reparsed.Hop != frame.Hop {
				t.Fatalf("original payload changed: %q => %q", want, reparsed.Fields)
			}
			if _, err := validateOriginalPeerSpot(reparsed); err != nil {
				t.Fatalf("relay lost shared validity: %v", err)
			}
		}
	})
}
