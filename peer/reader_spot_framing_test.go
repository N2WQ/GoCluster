package peer

import (
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// Literal templates are independent of Frame.Encode and the relay serializer.
// PC26's no-hop spelling deliberately remains separate from relay fixtures.
func spotReaderSentence(frameType, comment string) string {
	base := frameType + "^14074.1^K1ABC^ 4-Oct-2026^1200Z^" + comment + "^W1XYZ^N0CALL^"
	switch frameType {
	case "PC61":
		return base + "2001:0DB8:0000::1^H3^"
	case "PC26":
		return base
	default:
		return base + "H3^"
	}
}

// The dummy connection supplies deadline behavior; the source specifies every
// read boundary, including n > 0 with EOF. It never derives expected lines.
func spotFramingReader(t testing.TB, wire string, maxLine int, chunks ...int) *lineReader {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	position, next := 0, 0
	return newLineReaderWithTransport(local, maxLine, maxLine, func(dst []byte) (int, error) {
		if position == len(wire) {
			return 0, io.EOF
		}
		width := len(dst)
		if len(chunks) > 0 {
			width = min(width, chunks[min(next, len(chunks)-1)])
			next++
		}
		n := copy(dst[:width], wire[position:])
		position += n
		if position == len(wire) {
			return n, io.EOF
		}
		return n, nil
	}, &telnetParser{}, nil)
}

func requireSpotFramingLines(t testing.TB, r *lineReader, expected ...string) {
	t.Helper()
	for _, want := range expected {
		got, err := r.ReadLine(time.Now().Add(time.Second))
		if err != nil || got != want {
			t.Fatalf("line=%q error=%v; want=%q", got, err, want)
		}
	}
	if line, err := r.ReadLine(time.Now().Add(time.Second)); line != "" || !errors.Is(err, io.EOF) {
		t.Fatalf("trailing line=%q error=%v, want EOF", line, err)
	}
}

func TestSpotReaderCommentTildesAtEveryReadBoundary(t *testing.T) {
	for _, frameType := range []string{"PC11", "PC61", "PC26"} {
		for _, comment := range []string{"~", "~~", "~CQ", "CQ~", "CQ~TEST", "CQ~PC51"} {
			line := spotReaderSentence(frameType, comment)
			wire := line + "~\r\n~~PC51^W1AAA^W2AAA^0^~"
			t.Run(frameType+"/"+comment, func(t *testing.T) {
				// Whole reads, byte reads and every two-part split independently
				// exercise the comment caret and the genuine ending.
				for split := 0; split <= len(wire); split++ {
					var chunks []int
					if split == 0 {
						chunks = []int{1}
					} else {
						chunks = []int{split, 4096}
					}
					r := spotFramingReader(t, wire, 512, chunks...)
					requireSpotFramingLines(t, r, line, "PC51^W1AAA^W2AAA^0^")
				}
			})
		}
	}
}

func TestSpotReaderBareTerminalTildeOnOpenSocket(t *testing.T) {
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	reader := newLineReader(local, 512, 512, nil)
	line := spotReaderSentence("PC61", "CQ~TEST")
	written := make(chan error, 1)
	go func() { _, err := remote.Write([]byte(line + "~")); written <- err }()
	// No newline or EOF is available to conceal a broken tilde delimiter.
	got, err := reader.ReadLine(time.Now().Add(time.Second))
	if err != nil || got != line {
		t.Fatalf("bare ending line=%q error=%v", got, err)
	}
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer did not complete on the still-open socket")
	}
}

func TestSpotReaderTransportToleranceAndStrictWhitespace(t *testing.T) {
	for _, line := range []string{
		"pc11^14074^K1ABC^04-Oct-2026^1200Z^CQ~TEST^W1XYZ^H1ABC^h03^H2^",
		"pC61^14074^K1ABC^04-Oct-2026^1200Z^~^W1XYZ^N0CALL^::1^h03^",
		"PC26^14074^K1ABC^04-Oct-2026^1200Z^~~^W1XYZ^N0CALL^^",
		"PC26^14074^K1ABC^04-Oct-2026^1200Z^CQ~TEST^W1XYZ^N0CALL^H1ABC^H3^",
	} {
		for _, ending := range []string{"~", "\r", "\n", "\r\n", "~~~\r\n"} {
			r := spotFramingReader(t, line+ending, 512, 1)
			requireSpotFramingLines(t, r, line)
			if frame, err := ParseFrame(line); err != nil || !strings.Contains(frame.Fields[4], "~") {
				t.Fatalf("accepted spelling frame=%+v error=%v", frame, err)
			}
		}
	}
	for _, padding := range []string{" ", "\t", "\u2003", " \u00a0\t"} {
		line := padding + spotReaderSentence("PC11", "CQ~PC51")
		r := spotFramingReader(t, line+"~Z~", 512, 1)
		requireSpotFramingLines(t, r, line, "Z")
		if _, err := ParseFrame(line); err == nil {
			t.Fatalf("reader repaired forbidden padding %q", padding)
		}
	}
}

func TestSpotReaderOverflowPreservesPartialHeaderAndCommentPosition(t *testing.T) {
	for _, frameType := range []string{"PC11", "PC61", "PC26"} {
		for _, padding := range []string{"", " ", "\u2003", "\u00a0\t"} {
			for _, maxLine := range []int{1, 2, 3, 4, 5, 48, 96} {
				for _, width := range []int{1, 4096} {
					// Q would fit even the smallest cap if an internal ~ ended
					// discard mode; only the actual following Z is a new line.
					line := padding + strings.Replace(spotReaderSentence(frameType, strings.Repeat("X", 120)+"~Q~"), "PC", "pC", 1)
					r := spotFramingReader(t, line+"~Z~", maxLine, width)
					_, err := r.ReadLine(time.Now().Add(time.Second))
					var over ErrLineTooLong
					if !errors.As(err, &over) || over.Limit != maxLine || over.Reason != overlongReasonMaxLine {
						t.Fatalf("type%s padding%q max%d width%d: %v", frameType, padding, maxLine, width, err)
					}
					requireSpotFramingLines(t, r, "Z")
					if len(r.buf) > maxLine+1 || cap(r.buf) > maxLine+1 || cap(r.readBuf) != 4096 {
						t.Fatalf("retained state exceeds cap after max%d overflow", maxLine)
					}
				}
			}
		}
	}
}

func TestSpotReaderExactLimitAndRepeatedOverflow(t *testing.T) {
	const limit = 128
	for _, length := range []int{limit, limit + 1} {
		base := spotReaderSentence("PC61", "~Q~")
		line := spotReaderSentence("PC61", strings.Repeat("X", length-len(base))+"~Q~")
		for _, width := range []int{1, 4096} {
			r := spotFramingReader(t, strings.Repeat(line+"~", 4)+"PC92^NODE^1^C^5NODE^H3^~Z~", limit, width)
			r.pc92Max = 16
			for repeat := 0; repeat < 4; repeat++ {
				got, err := r.ReadLine(time.Now().Add(time.Second))
				if length == limit {
					if err != nil || got != line {
						t.Fatalf("exact-limit line=%q error=%v", got, err)
					}
				} else {
					var over ErrLineTooLong
					if !errors.As(err, &over) || got != "" {
						t.Fatalf("overflow line=%q error=%v", got, err)
					}
				}
				if cap(r.buf) > limit+1 {
					t.Fatal("overflow retained excess backing capacity")
				}
			}
			_, err := r.ReadLine(time.Now().Add(time.Second))
			var over ErrLineTooLong
			if !errors.As(err, &over) || over.Reason != overlongReasonPC92MaxBytes || over.Limit != 16 {
				t.Fatalf("PC92 limit changed: %v", err)
			}
			requireSpotFramingLines(t, r, "Z")
		}
	}
}

func TestSpotReaderEOFAndClosedConnection(t *testing.T) {
	complete := spotReaderSentence("PC61", "CQ~TEST")
	for _, suffix := range []string{"", "PC", "PC61^14074^K1ABC^04-Oct-2026^1200Z^CQ~"} {
		r := spotFramingReader(t, complete+"~"+complete+"~"+suffix, 512, 4096)
		requireSpotFramingLines(t, r, complete, complete)
	}
	r := spotFramingReader(t, spotReaderSentence("PC11", strings.Repeat("X", 200)+"~Q~"), 32, 1)
	if _, err := r.ReadLine(time.Now().Add(time.Second)); err == nil {
		t.Fatal("unfinished overflowing frame was admitted")
	}
	requireSpotFramingLines(t, r)
	r.release()
	if r.dropping || r.buf != nil || r.readBuf != nil || r.conn != nil || r.readFn != nil || r.parser != nil {
		t.Fatal("release retained discard transport ownership")
	}
	local, remote := net.Pipe()
	reader := newLineReader(local, 512, 512, nil)
	defer local.Close()
	_ = remote.Close()
	if got, err := reader.ReadLine(time.Now().Add(time.Second)); got != "" || err == nil {
		t.Fatalf("closed peer line=%q error=%v", got, err)
	}
}

func FuzzPeerSpotReaderFraming(f *testing.F) {
	f.Add("CQ~TEST", uint8(0), uint16(1), false)
	f.Add("~~", uint8(1), uint16(4095), true)
	f.Add("~Q~", uint8(2), uint16(4), true)
	f.Fuzz(func(t *testing.T, comment string, family uint8, chunk uint16, overflow bool) {
		if len(comment) > 512 {
			return
		}
		// Independent grammar for generated valid comments. Never ask the
		// validator or reader whether an expected payload should survive.
		for i := range len(comment) {
			b := comment[i]
			if b <= 8 || (b >= 10 && b <= 31) || (b >= 128 && b <= 159) || b == 255 || b == '^' {
				return
			}
		}
		frameType := []string{"PC11", "PC61", "PC26"}[int(family)%3]
		line := spotReaderSentence(frameType, comment)
		limit := 1024
		if overflow {
			limit = int(family)%5 + 1
		}
		r := spotFramingReader(t, line+"~Z~", limit, int(chunk)%4096+1)
		if overflow {
			if got, err := r.ReadLine(time.Now().Add(time.Second)); got != "" {
				t.Fatalf("published overflowing line %q", got)
			} else {
				var over ErrLineTooLong
				if !errors.As(err, &over) {
					t.Fatalf("overflow returned %v", err)
				}
			}
		} else {
			got, err := r.ReadLine(time.Now().Add(time.Second))
			if err != nil || got != line {
				t.Fatalf("type%s comment%q width%d line=%q error=%v", frameType, comment, chunk, got, err)
			}
		}
		requireSpotFramingLines(t, r, "Z")
		if cap(r.buf) > limit+1 {
			t.Fatalf("retained %d bytes over limit%d", cap(r.buf), limit)
		}
	})
}
