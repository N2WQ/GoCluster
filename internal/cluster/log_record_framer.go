package cluster

import (
	"bytes"

	"dxcluster/internal/asynclog"
)

// logRecordFramer owns at most one unfinished input record. The owner must
// serialize calls and must only use nonblocking callbacks. A trusted complete
// log.Logger call is one record even when its message contains newlines. Raw
// byte streams use writeStream, which always separates newline-delimited lines.
// Oversized fragments are discarded through that boundary and counted once.
type logRecordFramer struct {
	buf        [asynclog.MaxRecordBytes + 1]byte // one possible CR before LF
	n          int
	discarding bool
	closed     bool
}

// writeRecord accepts exactly one trusted standard-logger call. The standard
// logger supplies a terminating newline, which is framing rather than payload.
func (f *logRecordFramer) writeRecord(p []byte, emit func([]byte), drop func()) {
	if len(p) == 0 {
		return
	}
	if f.closed {
		drop()
		return
	}
	// The independent raw stream may have an unfinished fragment. A standard
	// logger call must neither consume it nor end an oversize discard.
	if len(p) != 0 && p[len(p)-1] == '\n' {
		p = p[:len(p)-1]
		if len(p) != 0 && p[len(p)-1] == '\r' {
			p = p[:len(p)-1]
		}
	}
	if len(p) > asynclog.MaxRecordBytes {
		drop()
	} else {
		emit(p)
	}
}

// writeStream borrows record bytes only until emit returns. It never retains p.
func (f *logRecordFramer) writeStream(p []byte, emit func([]byte), drop func()) bool {
	if f.closed {
		if len(p) != 0 {
			drop()
		}
		return false
	}
	processed := 0
	for len(p) != 0 {
		if processed == asynclog.QueueSize {
			f.dropStream(drop)
			return true
		}
		if f.discarding {
			if i := bytes.IndexByte(p, '\n'); i >= 0 {
				f.discarding = false
				p = p[i+1:]
				processed++
				continue
			}
			return false
		}
		i := bytes.IndexByte(p, '\n')
		fragment := p
		if i >= 0 {
			fragment = p[:i]
		}
		if len(fragment) > len(f.buf)-f.n {
			f.n = 0
			drop()
			if i < 0 {
				f.discarding = true
				return false
			}
			p = p[i+1:]
			processed++
			continue
		}
		f.n += copy(f.buf[f.n:], fragment)
		if i < 0 {
			if f.n > asynclog.MaxRecordBytes && f.buf[f.n-1] != '\r' {
				f.n = 0
				f.discarding = true
				drop()
			}
			return false
		}
		if f.n != 0 && f.buf[f.n-1] == '\r' {
			f.n--
		}
		if f.n > asynclog.MaxRecordBytes {
			drop()
		} else {
			emit(f.buf[:f.n])
		}
		f.n = 0
		p = p[i+1:]
		processed++
	}
	return false
}

// dropStream abandons a bounded raw input whose remaining row boundaries are
// unknown. Resynchronization waits for a later LF rather than joining text
// from opposite sides of the loss. An already-discarding row was counted.
func (f *logRecordFramer) dropStream(drop func()) {
	if f.closed {
		drop()
		return
	}
	if !f.discarding {
		f.n = 0
		f.discarding = true
		drop()
	}
}

// close counts an incomplete record once. An oversized fragment has already
// been counted, so shutdown does not count it again.
func (f *logRecordFramer) close(drop func()) {
	if f.closed {
		return
	}
	if f.n != 0 {
		drop()
	}
	f.n = 0
	f.discarding = false
	f.closed = true
}
