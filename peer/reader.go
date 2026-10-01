package peer

import (
	"bytes"
	"errors"
	"net"
	"time"
)

// TelnetParser strips telnet negotiation bytes and emits optional replies.
type TelnetParser interface {
	Feed(input []byte) (output []byte, replies [][]byte)
}

// LineReader wraps the peer line reader for use outside the peer package.
type LineReader struct {
	inner *lineReader
}

// NewLineReader constructs a LineReader with the default telnet parser.
func NewLineReader(conn net.Conn, maxLine int, pc92Max int, replyFn func([]byte)) *LineReader {
	return &LineReader{inner: newLineReader(conn, maxLine, pc92Max, replyFn)}
}

// NewLineReaderWithTransport constructs a LineReader with a custom reader/parser.
func NewLineReaderWithTransport(conn net.Conn, maxLine int, pc92Max int, readFn func([]byte) (int, error), parser TelnetParser, replyFn func([]byte)) *LineReader {
	return &LineReader{inner: newLineReaderWithTransport(conn, maxLine, pc92Max, readFn, parser, replyFn)}
}

// ReadLine reads a single line/frame using the underlying peer reader.
func (r *LineReader) ReadLine(deadline time.Time) (string, error) {
	if r == nil || r.inner == nil {
		return "", errors.New("nil line reader")
	}
	return r.inner.ReadLine(deadline)
}

type lineReader struct {
	allocation     readerAllocationState
	acquireScratch func(time.Time) (frameParseLease, error)
	conn           net.Conn
	readFn         func([]byte) (int, error)
	parser         TelnetParser
	buf            []byte
	replyFn        func([]byte)
	maxLine        int
	pc92Max        int
	dropping       bool
	readBuf        []byte
	readErr        error
}

// release runs after the session reader has returned and all workers joined.
// Queued controller work can still retain the old session identity; it must
// not also retain a maximum-size unfinished line or transport closures.
func (r *lineReader) release() {
	r.buf, r.readBuf = nil, nil
	r.readFn, r.replyFn = nil, nil
	r.acquireScratch = nil
	r.parser, r.conn, r.readErr = nil, nil, nil
	r.allocation.setBuffer(0)
	r.allocation.setReadBuffer(0)
	r.allocation.setRaw(0)
}

// ErrLineTooLong carries a preview and length when a frame exceeds maxLine.
type ErrLineTooLong struct {
	Preview string
	Length  int
	Reason  string
	Limit   int
}

// Error provides a generic error string for overlong lines.
// Key aspects: Keeps the error message stable for callers.
// Upstream: lineReader.tryReadLine.
// Downstream: None.
func (e ErrLineTooLong) Error() string {
	return "line too long"
}

const (
	// MaxPeerFrameBytes is the qualified transport envelope. Configuration may
	// lower it, but no reader or direct parser can raise it.
	MaxPeerFrameBytes = 64 << 10
	// One bounded read's native Telnet replies/output, aggregate growth and
	// extraction copy share the manager's existing parser scratch allowance.
	readerScratchBytes         int64 = 288 << 10
	overlongReasonPC92MaxBytes       = "pc92_max_bytes"
	overlongReasonMaxLine            = "max_line_length"
)

// Purpose: Construct a lineReader with the default telnet parser.
// Key aspects: Uses conn.Read and a refuse-all telnet parser.
// Upstream: Peer session setup.
// Downstream: newLineReaderWithTransport.
func newLineReader(conn net.Conn, maxLine int, pc92Max int, replyFn func([]byte)) *lineReader {
	return newLineReaderWithTransport(conn, maxLine, pc92Max, conn.Read, &telnetParser{}, replyFn)
}

// Purpose: Construct a lineReader with a custom transport reader/parser.
// Key aspects: Allows pre-stripped IAC data by passing parser=nil.
// Upstream: Peer session setup for external telnet transport.
// Downstream: lineReader.ReadLine.
func newLineReaderWithTransport(conn net.Conn, maxLine int, pc92Max int, readFn func([]byte) (int, error), parser TelnetParser, replyFn func([]byte)) *lineReader {
	if maxLine <= 0 || maxLine > MaxPeerFrameBytes {
		maxLine = MaxPeerFrameBytes
	}
	if pc92Max <= 0 || pc92Max > maxLine {
		pc92Max = maxLine
	}
	if readFn == nil {
		readFn = conn.Read
	}
	r := &lineReader{
		conn:    conn,
		readFn:  readFn,
		parser:  parser,
		buf:     make([]byte, 0, min(maxLine+1, 4096)),
		replyFn: replyFn,
		maxLine: maxLine,
		pc92Max: pc92Max,
		readBuf: make([]byte, 4096),
	}
	r.allocation.setBuffer(cap(r.buf))
	r.allocation.setReadBuffer(cap(r.readBuf))
	return r
}

// ReadLine reads a single line/frame with deadline and telnet filtering.
// Key aspects: Aggregates reads into a buffer and handles overlong lines.
// Upstream: Peer session read loop.
// Downstream: tryReadLine, bytesIndexTerminator.
func (r *lineReader) ReadLine(deadline time.Time) (string, error) {
	r.allocation.setRaw(0)
	var scratch frameParseLease
	releaseScratch := func() {
		if scratch.budget != nil {
			scratch.release()
			scratch = frameParseLease{}
		}
	}
	defer releaseScratch()
	acquireScratch := func() error {
		if scratch.budget != nil || r.acquireScratch == nil {
			return nil
		}
		var err error
		scratch, err = r.acquireScratch(deadline)
		return err
	}
	if err := r.conn.SetReadDeadline(deadline); err != nil {
		return "", err
	}
	for {
		if !r.dropping {
			if len(r.buf) > 0 {
				if err := acquireScratch(); err != nil {
					return "", err
				}
			}
			line, err, ready := r.tryReadLine()
			if ready {
				r.allocation.setRaw(len(line))
				return line, err
			}
		}
		if r.readErr != nil {
			return "", r.readErr
		}
		// Never hold shared scratch over a blocking socket read. A candidate
		// withholding bytes cannot deprive other readers of parser progress.
		releaseScratch()
		readBuf := r.readBuf
		if !r.dropping {
			limit, _ := r.lineLimit()
			// One lookahead byte distinguishes an exact-limit frame followed by
			// its terminator from overflow, without retaining a whole extra read.
			if available := limit + 1 - len(r.buf); available < len(readBuf) {
				readBuf = readBuf[:available]
			}
		} else if len(readBuf) > r.maxLine+1 {
			readBuf = readBuf[:r.maxLine+1]
		}
		n, err := r.readFn(readBuf)
		if n > 0 {
			if err := acquireScratch(); err != nil {
				return "", err
			}
			data := r.readBuf[:n]
			if r.parser != nil {
				out, replies := r.parser.Feed(data)
				if len(replies) > 0 && r.replyFn != nil {
					for _, rep := range replies {
						r.replyFn(rep)
					}
				}
				data = out
			}
			if r.dropping {
				if idx, size := bytesIndexTerminator(data); idx >= 0 {
					r.dropping = false
					r.appendData(data[idx+size:])
				}
			} else {
				r.appendData(data)
			}
		}
		r.readErr = err
	}
}

// Purpose: Attempt to extract a full line from the current buffer.
// Key aspects: Respects terminators, PC92 max size, and resync rules.
// Upstream: ReadLine.
// Downstream: trimLeadingTerminators, bytesIndexTerminator, frameTypeFromBuffer.
//
//nolint:revive // Keep return ordering for existing call sites.
func (r *lineReader) tryReadLine() (string, error, bool) {
	for {
		trimmed := trimLeadingTerminators(r.buf)
		if removed := len(r.buf) - len(trimmed); removed > 0 {
			r.consume(removed)
		}
		if len(r.buf) == 0 {
			return "", nil, false
		}
		// Prefer explicit terminators (~, CRLF, CR, LF) when present.
		if idx, size := bytesIndexTerminator(r.buf); idx >= 0 {
			limit, reason := r.lineLimit()
			if idx > limit {
				preview := linePreview(r.buf[:idx])
				r.consume(idx + size)
				return "", ErrLineTooLong{
					Preview: preview,
					Length:  idx,
					Reason:  reason,
					Limit:   limit,
				}, true
			}
			line := string(trimLine(r.buf[:idx]))
			r.consume(idx + size)
			return line, nil, true
		}
		// Resync: discard leading noise until a valid PCxx^ frame start that follows a terminator.
		// This avoids splitting on "^PC" sequences that might appear inside payload fields.
		if start := bytesIndexFrameStart(r.buf); start > 0 {
			r.consume(start)
			continue
		}
		limit, reason := r.lineLimit()
		if len(r.buf) > limit {
			length := len(r.buf)
			preview := linePreview(r.buf)
			r.buf = nil
			r.allocation.setBuffer(0)
			r.dropping = true
			return "", ErrLineTooLong{
				Preview: preview,
				Length:  length,
				Reason:  reason,
				Limit:   limit,
			}, true
		}
		return "", nil, false
	}
}

func (r *lineReader) lineLimit() (int, string) {
	if frameTypeFromBuffer(r.buf) == "PC92" && r.pc92Max <= r.maxLine {
		return r.pc92Max, overlongReasonPC92MaxBytes
	}
	return r.maxLine, overlongReasonMaxLine
}

func (r *lineReader) consume(n int) {
	remaining := len(r.buf) - n
	if cap(r.buf) > 4096 && remaining <= 4096 {
		if remaining == 0 {
			r.buf = nil
		} else {
			buf := make([]byte, remaining, min(r.maxLine+1, 4096))
			copy(buf, r.buf[n:])
			r.buf = buf
		}
		r.allocation.setBuffer(cap(r.buf))
		return
	}
	copy(r.buf, r.buf[n:])
	r.buf = r.buf[:len(r.buf)-n]
}

func (r *lineReader) appendData(data []byte) {
	needed := len(r.buf) + len(data)
	if needed > cap(r.buf) {
		capacity := min(r.maxLine+1, max(needed, 2*cap(r.buf)))
		buf := make([]byte, len(r.buf), capacity)
		copy(buf, r.buf)
		r.buf = buf
		r.allocation.setBuffer(cap(r.buf))
	}
	r.buf = append(r.buf, data...)
}

func linePreview(b []byte) string {
	if len(b) > 512 {
		b = b[:512]
	}
	return string(b)
}

// Purpose: Trim trailing CR/LF from a line buffer.
// Key aspects: Stops at first non-terminator from the end.
// Upstream: tryReadLine.
// Downstream: None.
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

// Purpose: Remove leading terminator bytes so frames start cleanly.
// Key aspects: Drops CR/LF/~ in sequence.
// Upstream: tryReadLine.
// Downstream: isTerminator.
func trimLeadingTerminators(b []byte) []byte {
	for len(b) > 0 {
		if isTerminator(b[0]) {
			b = b[1:]
			continue
		}
		break
	}
	return b
}

// Purpose: Report whether a byte is a line/frame terminator.
// Key aspects: Recognizes CR, LF, and '~'.
// Upstream: trimLeadingTerminators, bytesIndexFrameStart.
// Downstream: None.
func isTerminator(b byte) bool {
	return b == '\n' || b == '\r' || b == '~'
}

// Purpose: Find the first line terminator and its width.
// Key aspects: Prefers '~' and handles CRLF pairs.
// Upstream: tryReadLine.
// Downstream: None.
func bytesIndexTerminator(b []byte) (int, int) {
	for i := 0; i < len(b); i++ {
		switch b[i] {
		case '~':
			return i, 1
		case '\n':
			return i, 1
		case '\r':
			if i+1 < len(b) && b[i+1] == '\n' {
				return i, 2
			}
			return i, 1
		}
	}
	return -1, 0
}

// Purpose: Find a valid PCxx^ frame start in the buffer.
// Key aspects: Only considers starts at buffer start or after a terminator.
// Upstream: tryReadLine resync.
// Downstream: isFrameStartAt, isTerminator.
func bytesIndexFrameStart(b []byte) int {
	if isFrameStartAt(b, 0) {
		return 0
	}
	for i := 1; i < len(b); i++ {
		if !isTerminator(b[i-1]) {
			continue
		}
		if isFrameStartAt(b, i) {
			return i
		}
	}
	return -1
}

// Purpose: Check whether a buffer offset begins with a PCxx^ header.
// Key aspects: Validates "PC" + two digits + caret.
// Upstream: bytesIndexFrameStart, frameTypeFromBuffer.
// Downstream: None.
func isFrameStartAt(b []byte, i int) bool {
	if i+4 >= len(b) {
		return false
	}
	if (b[i] != 'P' && b[i] != 'p') || (b[i+1] != 'C' && b[i+1] != 'c') {
		return false
	}
	if b[i+2] < '0' || b[i+2] > '9' || b[i+3] < '0' || b[i+3] > '9' {
		return false
	}
	return b[i+4] == '^'
}

// Purpose: Extract the PC frame type from the start of a buffer.
// Key aspects: Returns empty string if no valid frame start.
// Upstream: tryReadLine.
// Downstream: isFrameStartAt.
func frameTypeFromBuffer(b []byte) string {
	b = bytes.TrimSpace(b)
	if !isFrameStartAt(b, 0) {
		return ""
	}
	return string(bytes.ToUpper(b[:4]))
}
