//go:build qualification

package peer

import (
	"bytes"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestQualificationReaderOwnedBackingAndReturnedLine(t *testing.T) {
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	input := bytes.NewBufferString(strings.Repeat("x", 65536) + "\n")
	r := newLineReaderWithTransport(local, 65536, 65536, input.Read, nil, nil)
	if backing, raw := r.allocation.snapshot(); backing != 8192 || raw != 0 {
		t.Fatalf("initial backing=%d raw=%d", backing, raw)
	}
	line, err := r.ReadLine(time.Now().Add(time.Second))
	if err != nil || len(line) != 65536 {
		t.Fatalf("read length=%d err=%v", len(line), err)
	}
	if backing, raw := r.allocation.snapshot(); backing != 4096 || raw != 65536 {
		t.Fatalf("returned maximum line backing=%d raw=%d", backing, raw)
	}
	if _, err := r.ReadLine(time.Now().Add(time.Second)); err != io.EOF {
		t.Fatalf("drain err=%v", err)
	}
	if _, raw := r.allocation.snapshot(); raw != 0 {
		t.Fatal("next read retained the previous returned-line reservation")
	}
	r.release()
	if backing, raw := r.allocation.snapshot(); backing != 0 || raw != 0 {
		t.Fatalf("released backing=%d raw=%d", backing, raw)
	}
}
