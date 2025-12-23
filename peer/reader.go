package peer

import (
	"fmt"
	"net"
	"sync"
	"time"
)

type lineReader struct {
	conn    net.Conn
	parser  *telnetParser
	buf     []byte
	writeMu *sync.Mutex
	maxLine int
}

func newLineReader(conn net.Conn, writeMu *sync.Mutex, maxLine int) *lineReader {
	return &lineReader{
		conn:    conn,
		parser:  &telnetParser{},
		buf:     make([]byte, 0, maxLine),
		writeMu: writeMu,
		maxLine: maxLine,
	}
}

func (r *lineReader) ReadLine(deadline time.Time) (string, error) {
	if err := r.conn.SetReadDeadline(deadline); err != nil {
		return "", err
	}
	for {
		chunk := make([]byte, 1024)
		n, err := r.conn.Read(chunk)
		if n > 0 {
			out, replies := r.parser.Feed(chunk[:n])
			if len(replies) > 0 {
				for _, rep := range replies {
					r.writeMu.Lock()
					_, _ = r.conn.Write(rep)
					r.writeMu.Unlock()
				}
			}
			r.buf = append(r.buf, out...)
			if len(r.buf) > r.maxLine && r.maxLine > 0 {
				// Drop the current buffer to avoid unbounded growth; caller can choose to continue.
				r.buf = r.buf[:0]
				return "", ErrLineTooLong
			}
			if idx := bytesIndexLine(r.buf); idx >= 0 {
				line := string(trimLine(r.buf[:idx]))
				r.buf = append([]byte{}, r.buf[idx:]...)
				return line, nil
			}
		}
		if err != nil {
			return "", err
		}
	}
}

func bytesIndexLine(b []byte) int {
	for i, c := range b {
		if c == '\n' {
			return i + 1
		}
	}
	return -1
}

func trimLine(b []byte) []byte {
	for len(b) > 0 {
		if b[len(b)-1] == '\n' || b[len(b)-1] == '\r' {
			b = b[:len(b)-1]
		} else {
			break
		}
	}
	return b
}

var ErrLineTooLong = fmt.Errorf("line too long")
