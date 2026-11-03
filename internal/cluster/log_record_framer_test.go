package cluster

import (
	"bytes"
	"strings"
	"testing"

	"dxcluster/internal/asynclog"
)

func TestLogRecordFramer(t *testing.T) {
	limit := asynclog.MaxRecordBytes
	tests := []struct {
		name   string
		stream bool
		writes []string
		want   []string
		drops  int
	}{
		{"complete single and multiline calls", false, []string{"one\n", "two\nthree\n"}, []string{"one", "two\nthree"}, 0},
		{"stream multiple lines in one write", true, []string{"one\ntwo\n"}, []string{"one", "two"}, 0},
		{"fragmented with remainder", true, []string{"one", " two\nnext", " line\n"}, []string{"one two", "next line"}, 0},
		{"exact limit LF", false, []string{strings.Repeat("x", limit) + "\n"}, []string{strings.Repeat("x", limit)}, 0},
		{"exact limit CRLF split", true, []string{strings.Repeat("x", limit), "\r", "\n"}, []string{strings.Repeat("x", limit)}, 0},
		{"overlong complete multiline call", false, []string{strings.Repeat("x", limit) + "\ny\n"}, nil, 1},
		{"discard overlong fragment then recover", true, []string{strings.Repeat("x", limit), "y", "more\nnext\n"}, []string{"next"}, 1},
		{"fragment exceeds limit on terminator", true, []string{strings.Repeat("x", limit), "z\nnext\n"}, []string{"next"}, 1},
		{"pending fragment on close", true, []string{"unfinished"}, nil, 1},
		{"overlong fragment on close counted once", true, []string{strings.Repeat("x", limit+1)}, nil, 1},
		{"empty record", true, []string{"\n"}, []string{""}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var f logRecordFramer
			var got []string
			drops := 0
			emit := func(p []byte) { got = append(got, string(p)) }
			drop := func() { drops++ }
			for _, p := range tt.writes {
				if tt.stream {
					f.writeStream([]byte(p), emit, drop)
				} else {
					f.writeRecord([]byte(p), emit, drop)
				}
			}
			f.close(drop)
			f.close(drop)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d records, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("record %d differs", i)
				}
			}
			if drops != tt.drops {
				t.Fatalf("got %d drops, want %d", drops, tt.drops)
			}
		})
	}
}

func BenchmarkLogRecordFramer(b *testing.B) {
	var f logRecordFramer
	p := bytes.Repeat([]byte("a"), 200)
	p = append(p, '\n')
	emit := func([]byte) {}
	drop := func() { b.Fatal("unexpected drop") }
	b.ReportAllocs()
	for b.Loop() {
		f.writeRecord(p, emit, drop)
	}
}

func TestLogRecordFramerKeepsMixedSourcesIndependent(t *testing.T) {
	var f logRecordFramer
	var records []string
	drops := 0
	emit := func(p []byte) { records = append(records, string(p)) }
	drop := func() { drops++ }
	f.writeStream([]byte("part"), emit, drop)
	f.writeRecord([]byte("whole\n"), emit, drop)
	f.writeStream([]byte("ial\n"), emit, drop)
	f.writeStream([]byte(strings.Repeat("x", asynclog.MaxRecordBytes+1)), emit, drop)
	f.writeRecord([]byte("another\n"), emit, drop)
	f.writeStream([]byte("ignored\nlast\n"), emit, drop)
	f.close(drop)
	if got := strings.Join(records, "|"); got != "whole|partial|another|last" || drops != 1 {
		t.Fatalf("records %q, drops %d", got, drops)
	}
}

func TestLogRecordFramerBoundsRawBatchAndResynchronizes(t *testing.T) {
	var f logRecordFramer
	var records []string
	drops := 0
	emit := func(p []byte) { records = append(records, string(p)) }
	drop := func() { drops++ }
	input := strings.Repeat("x\n", asynclog.QueueSize) + "lost\n"
	if !f.writeStream([]byte(input), emit, drop) {
		t.Fatal("bounded batch did not report unknown additional rows")
	}
	if len(records) != asynclog.QueueSize || drops != 1 {
		t.Fatalf("records=%d drops=%d", len(records), drops)
	}
	// The ignored remainder may have contained a line ending. Discard through
	// the next LF rather than ever joining text across that uncertain gap.
	f.writeStream([]byte("unsafe\nnext\n"), emit, drop)
	if len(records) != asynclog.QueueSize+1 || records[len(records)-1] != "next" || drops != 1 {
		t.Fatalf("resynchronization records=%d last=%q drops=%d", len(records), records[len(records)-1], drops)
	}
}
