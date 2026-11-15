package peer

import (
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

type pendingInputConn struct{ io.Reader }

func (*pendingInputConn) Write([]byte) (int, error)        { return 0, errors.New("unexpected offline write") }
func (*pendingInputConn) Close() error                     { return nil }
func (*pendingInputConn) LocalAddr() net.Addr              { return nil }
func (*pendingInputConn) RemoteAddr() net.Addr             { return nil }
func (*pendingInputConn) SetDeadline(time.Time) error      { return nil }
func (*pendingInputConn) SetReadDeadline(time.Time) error  { return nil }
func (*pendingInputConn) SetWriteDeadline(time.Time) error { return nil }

func TestLineReaderPendingInputAtEOF(t *testing.T) {
	for _, tc := range []struct {
		input   string
		pending bool
	}{{"", false}, {"PC51^N0CALL^W1XYZ^1^~\r\n", false}, {"PC11^14020^", true}, {"\xff", true}, {"\xff\xfa\x01", true}, {"\xff\xfb\x01", false}} {
		r := NewLineReader(&pendingInputConn{strings.NewReader(tc.input)}, MaxPeerFrameBytes, MaxPeerFrameBytes, nil)
		for {
			_, err := r.ReadLine(time.Time{})
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if r.PendingInput() != tc.pending {
			t.Fatalf("pending %v want%v", r.PendingInput(), tc.pending)
		}
	}
}
