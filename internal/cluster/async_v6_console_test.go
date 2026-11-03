package cluster

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"dxcluster/internal/asynclog"
)

type v6ConsoleResultWriter struct {
	n   int
	err error
}

func (w v6ConsoleResultWriter) Write([]byte) (int, error) { return w.n, w.err }

func TestV6ConsolePreservesFormatAndResults(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var out bytes.Buffer
	s := &v6ConsoleSink{writer: &out}
	if got := s.Write(at, 0, []byte("one\ntwo")); got != asynclog.Written || out.String() != "2026/01/02 03:04:05 one\n2026/01/02 03:04:05 two\n" {
		t.Fatalf("result %v, output %q", got, out.String())
	}
	out.Reset()
	if got := s.Write(at, 0, []byte(strings.Repeat("\n", asynclog.MaxRecordBytes))); got != asynclog.Written || out.Len() != len(s.buffer) {
		t.Fatalf("worst-case result %v, size %d", got, out.Len())
	}
	s.writer = v6ConsoleResultWriter{n: 1, err: errors.New("failed")}
	if got := s.Write(at, 0, []byte("one")); got != asynclog.NotWritten {
		t.Fatalf("partial output reported as %v", got)
	}
}
