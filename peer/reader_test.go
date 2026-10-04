package peer

import (
	"bytes"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestLineReaderDropsOversizePC92(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	maxLine := 1024
	pc92Max := 64
	reader := newLineReaderWithTransport(server, maxLine, pc92Max, server.Read, nil, nil)

	pc92Payload := strings.Repeat("A", pc92Max+10)
	pc92Line := "PC92^" + pc92Payload + "^H99^~"
	pc51Line := "PC51^N2WQ-77^N2WQ-73^0^~"

	go func() {
		_, _ = client.Write([]byte(pc92Line + pc51Line))
	}()

	deadline := time.Now().Add(2 * time.Second)
	_, err := reader.ReadLine(deadline)
	if err == nil {
		t.Fatal("expected oversize PC92 to return an error")
	}
	var tooLong ErrLineTooLong
	if !errors.As(err, &tooLong) {
		t.Fatalf("expected ErrLineTooLong, got %v", err)
	}
	if tooLong.Reason != overlongReasonPC92MaxBytes {
		t.Fatalf("expected reason=%q, got %q", overlongReasonPC92MaxBytes, tooLong.Reason)
	}
	if tooLong.Limit != pc92Max {
		t.Fatalf("expected limit=%d, got %d", pc92Max, tooLong.Limit)
	}

	line, err := reader.ReadLine(deadline)
	if err != nil {
		t.Fatalf("expected next line, got error: %v", err)
	}
	if line != "PC51^N2WQ-77^N2WQ-73^0^" {
		t.Fatalf("unexpected line after drop: %q", line)
	}
}

func TestLineReaderAllHeaderFormsAndTerminatorBounds(t *testing.T) {
	for _, header := range []string{"PC92^", "pC92^", "  pc92^", "PC93^"} {
		for _, terminator := range []string{"~", "\r", "\n", "\r\n"} {
			for _, chunk := range []int{1, 4096} {
				for _, length := range []int{64, 65} {
					line := header + strings.Repeat("X", length-len(header))
					payload := bytes.NewReader([]byte(line + terminator + "PC51^K1ABC^K2ABC^0^~"))
					server, client := net.Pipe()
					r := newLineReaderWithTransport(server, 64, 64, func(p []byte) (int, error) {
						if len(p) > chunk {
							p = p[:chunk]
						}
						return payload.Read(p)
					}, nil, nil)
					got, err := r.ReadLine(time.Now().Add(time.Second))
					if length == 64 && (err != nil || got != line) {
						t.Fatalf("header%q term%q chunk%d exact: %q %v", header, terminator, chunk, got, err)
					}
					if length == 65 {
						var over ErrLineTooLong
						if !errors.As(err, &over) || over.Limit != 64 {
							t.Fatalf("header%q term%q chunk%d overflow: %q %v", header, terminator, chunk, got, err)
						}
					}
					got, err = r.ReadLine(time.Now().Add(time.Second))
					if err != nil || got != "PC51^K1ABC^K2ABC^0^" {
						t.Fatalf("resync %q %v", got, err)
					}
					if cap(r.buf) > 65 {
						t.Fatalf("reader retained %d bytes", cap(r.buf))
					}
					_ = server.Close()
					_ = client.Close()
				}
			}
		}
	}
}

func TestLineReaderBoundedRepeatedOverflowAndIAC(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	data := []byte(strings.Repeat("PC92^"+strings.Repeat("A", 200)+"~\r\n", 10))
	data = append(data, []byte{'P', 'C', telnetIAC, telnetDO, 1, '5', '1', '^', 'K', '1', 'A', 'B', 'C', '^', 'K', '2', 'A', 'B', 'C', '^', '0', '^', '~'}...)
	source := bytes.NewReader(data)
	r := newLineReaderWithTransport(server, 64, 32, func(p []byte) (int, error) { return source.Read(p[:1]) }, &telnetParser{}, nil)
	for i := 0; i < 10; i++ {
		_, err := r.ReadLine(time.Now().Add(time.Second))
		var over ErrLineTooLong
		if !errors.As(err, &over) {
			t.Fatalf("cycle%d: %v", i, err)
		}
		if cap(r.buf) > 65 || len(r.buf) > 65 {
			t.Fatalf("unbounded state cycle%d", i)
		}
	}
	line, err := r.ReadLine(time.Now().Add(time.Second))
	if err != nil || line != "PC51^K1ABC^K2ABC^0^" {
		t.Fatalf("fragmented telnet resync %q %v", line, err)
	}
}

func TestLineReaderReleasesLargeBackingBuffer(t *testing.T) {
	for _, remainder := range []string{"", "PC51^K1ABC^K2ABC^0^~"} {
		server, client := net.Pipe()
		r := newLineReaderWithTransport(server, MaxPeerFrameBytes, MaxPeerFrameBytes, nil, nil, nil)
		line := "PC99^" + strings.Repeat("x", 60000)
		r.appendData([]byte(line + "~" + remainder))
		got, err, ready := r.tryReadLine()
		if !ready || err != nil || got != line {
			t.Fatalf("line lost %v %v", ready, err)
		}
		if cap(r.buf) > 4096 || string(r.buf) != remainder {
			t.Fatalf("large backing retained cap=%d remainder=%q", cap(r.buf), r.buf)
		}
		_ = server.Close()
		_ = client.Close()
	}
}

func FuzzLineReaderRetainedBound(f *testing.F) {
	f.Add("PC92^K1ABC^1^C^5K1ABC^H99^~PC51^K1ABC^K2ABC^0^\r\n", uint16(1))
	f.Add(strings.Repeat("x", 300)+"~PC51^K1ABC^K2ABC^0^~", uint16(4096))
	f.Add("PC61^14074^K1ABC^04-Oct-2026^1200Z^CQ~TEST^W1XYZ^N0CALL^::1^H3^~Z~", uint16(1))
	f.Add("pC11^14074^K1ABC^04-Oct-2026^1200Z^"+strings.Repeat("x", 300)+"~Q~^W1XYZ^N0CALL^H3^~Z~", uint16(4096))
	f.Fuzz(func(t *testing.T, input string, chunk uint16) {
		if len(input) > MaxPeerFrameBytes {
			return
		}
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		source := strings.NewReader(input)
		width := int(chunk)%4096 + 1
		r := newLineReaderWithTransport(server, 128, 64, func(p []byte) (int, error) { return source.Read(p[:min(len(p), width)]) }, &telnetParser{}, nil)
		for attempts := 0; attempts <= len(input)+1; attempts++ {
			line, err := r.ReadLine(time.Now().Add(time.Second))
			if cap(r.buf) > 129 || len(r.buf) > 129 {
				t.Fatalf("reader retained len=%d cap=%d", len(r.buf), cap(r.buf))
			}
			limit := 128
			if frameTypeFromBuffer([]byte(line)) == "PC92" {
				limit = 64
			}
			if len(line) > limit {
				t.Fatalf("published overlong line len%d >%d", len(line), limit)
			}
			if err != nil {
				var over ErrLineTooLong
				if errors.As(err, &over) {
					continue
				}
				return
			}
		}
		t.Fatal("reader did not make bounded progress")
	})
}

func TestLineReaderFlagsMaxLineReason(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	maxLine := 64
	reader := newLineReaderWithTransport(server, maxLine, 0, server.Read, nil, nil)

	go func() {
		_, _ = client.Write([]byte(strings.Repeat("X", maxLine+16)))
		time.Sleep(200 * time.Millisecond)
	}()

	deadline := time.Now().Add(2 * time.Second)
	_, err := reader.ReadLine(deadline)
	if err == nil {
		t.Fatal("expected max-line overflow to return an error")
	}
	var tooLong ErrLineTooLong
	if !errors.As(err, &tooLong) {
		t.Fatalf("expected ErrLineTooLong, got %v", err)
	}
	if tooLong.Reason != overlongReasonMaxLine {
		t.Fatalf("expected reason=%q, got %q", overlongReasonMaxLine, tooLong.Reason)
	}
	if tooLong.Limit != maxLine {
		t.Fatalf("expected limit=%d, got %d", maxLine, tooLong.Limit)
	}
}
