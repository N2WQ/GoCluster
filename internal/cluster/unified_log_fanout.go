package cluster

import (
	"bytes"
	"sync"
	"time"

	"dxcluster/internal/asynclog"
)

// logAdmission implementations must perform bounded in-memory admission only.
// They must not wait for display or filesystem operations while called here.
type logAdmission interface {
	emit(time.Time, []byte)
	drop()
}

type streamEmitter interface {
	// Emit copies an admitted payload before returning. It never waits for I/O.
	Emit(stream uint8, at time.Time, payload []byte) bool
}

// systemLogFanout serializes the one bounded fragment buffer with destination
// changes. Each completed call is copied once into each destination's shared
// queue; neither the callbacks nor this lock cover destination I/O.
type systemLogFanout struct {
	mu         sync.Mutex
	framer     logRecordFramer
	normalized [asynclog.MaxRecordBytes]byte
	console    logAdmission
	file       logAdmission
}

func (f *systemLogFanout) setConsole(target logAdmission) {
	f.mu.Lock()
	f.console = target
	f.mu.Unlock()
}

func (f *systemLogFanout) setFile(target logAdmission) {
	f.mu.Lock()
	f.file = target
	f.mu.Unlock()
}

// Write accepts one complete call from Go's standard logger. The only runtime
// writer installation is log.SetOutput; raw streams use WriteStream instead.
func (f *systemLogFanout) Write(p []byte) (int, error) {
	return f.write(p, false)
}

func (f *systemLogFanout) WriteStream(p []byte) (int, error) {
	return f.write(p, true)
}

func (f *systemLogFanout) write(p []byte, stream bool) (int, error) {
	f.mu.Lock()
	now := time.Now().UTC()
	// Dashboard messages may exceed one file record; admit the complete
	// display message separately from the bounded file record.
	newSurface, v6Dashboard := f.console.(v6DashboardAdmission)
	if v6Dashboard && !f.framer.closed && len(p) != 0 {
		if stream {
			newSurface.emitStream(now, p)
		} else {
			newSurface.emit(now, p)
		}
	}
	emit := func(record []byte) {
		record = normalizeSystemRecord(f.normalized[:0], record)
		if f.console != nil && !v6Dashboard {
			f.console.emit(now, record)
		}
		if f.file != nil {
			f.file.emit(now, record)
		}
	}
	drop := func() {
		if f.console != nil && !v6Dashboard {
			f.console.drop()
		}
		if f.file != nil {
			f.file.drop()
		}
	}
	if stream {
		if len(p) > asynclog.QueueSize*asynclog.MaxRecordBytes {
			f.framer.dropStream(drop)
		} else {
			f.framer.writeStream(p, emit, drop)
		}
	} else {
		f.framer.writeRecord(p, emit, drop)
	}
	f.mu.Unlock()
	return len(p), nil
}

// The previous line fanout removed trailing CR bytes from every physical line.
// Keep that display/file format after checking the original message size.
func normalizeSystemRecord(dst, input []byte) []byte {
	for {
		i := bytes.IndexByte(input, '\n')
		line := input
		if i >= 0 {
			line = input[:i]
		}
		line = bytes.TrimRight(line, "\r")
		dst = append(dst, line...)
		if i < 0 {
			return dst
		}
		dst = append(dst, '\n')
		input = input[i+1:]
	}
}

// writeFileOnly preserves the existing route for periodic diagnostics that
// must not be duplicated to the console. Oversized calls count once.
func (f *systemLogFanout) writeFileOnly(line string, now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return
	}
	if f.framer.closed || len(line) > asynclog.MaxRecordBytes {
		f.file.drop()
		return
	}
	f.file.emit(now, []byte(line))
}

func (f *systemLogFanout) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.framer.close(func() {
		if f.console != nil {
			f.console.drop()
		}
		if f.file != nil {
			f.file.drop()
		}
	})
	// A captured or stale fanout must not keep its former coordinator or
	// dashboard generation alive after retirement.
	f.console, f.file = nil, nil
}
