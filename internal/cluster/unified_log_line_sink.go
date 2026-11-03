package cluster

import (
	"sync"
	"time"

	"dxcluster/internal/asynclog"
)

// unifiedLogLineSink is a borrowed category route into the common owner.
// Its Close does not stop a shared destination; runtime owns one global stop.
type unifiedLogLineSink struct {
	mu      sync.Mutex
	emitter streamEmitter
	stream  uint8
}

func (s *unifiedLogLineSink) WriteLine(line string, now time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.emitter == nil || len(line) > asynclog.MaxRecordBytes {
		return
	}
	s.emitter.Emit(s.stream, now, []byte(line))
}

func (s *unifiedLogLineSink) Close() error {
	if s != nil {
		s.mu.Lock()
		s.emitter = nil
		s.mu.Unlock()
	}
	return nil
}
