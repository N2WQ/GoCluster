package linebuffer

import "testing"

func TestAppendAndExtractLinesSplitsCRLFAndKeepsRemainder(t *testing.T) {
	remaining, lines := AppendAndExtractLines(nil, []byte("a\r\nb\nc"), 1024)
	if len(lines) != 2 || lines[0] != "a" || lines[1] != "b" {
		t.Fatalf("unexpected lines: %#v", lines)
	}
	if string(remaining) != "c" {
		t.Fatalf("unexpected remainder %q", string(remaining))
	}
}

func TestAppendAndExtractLinesFlushesOversizedRemainder(t *testing.T) {
	remaining, lines := AppendAndExtractLines(nil, []byte("abcdef"), 4)
	if len(lines) != 1 || lines[0] != "abcdef" {
		t.Fatalf("unexpected lines: %#v", lines)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected empty remainder, got %q", string(remaining))
	}
}
