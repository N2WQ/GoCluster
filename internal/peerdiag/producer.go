// Package peerdiag converts bounded peer events into records for the shared logger.
package peerdiag

import (
	"math"
	"strconv"
	"sync"
	"time"

	"dxcluster/internal/asynclog"
)

type Kind uint8

const (
	Diagnostic Kind = iota + 1
	Connection
	Overlong
)

// Fields are borrowed for Emit only. Each field has a fixed inspection and
// output limit; no error, session, or network frame is retained.
type Fields struct {
	Action, Peer, Endpoint, Reason, Detail, Direction string
	DX, DE                                            string
	Count, Limit                                      int64
}

// StreamEmitter is trusted shared-logger integration. Emit must copy the
// borrowed payload before it returns and must not wait on a destination.
// Status returns actual current owner state and generation, or zero generation
// when the category has no bound owner. It must use bounded in-memory locking
// only: Stop waits for any in-progress Status borrow to finish.
type StreamEmitter interface {
	Emit(stream uint8, at time.Time, payload []byte) bool
	Status(stream uint8) StreamStatus
}

// StreamStatus is a bounded snapshot of one current async destination.
// ChargedBytes represents the whole peer diagnostic allocation, so combining
// two streams uses the larger value instead of counting the shared charge twice.
type StreamStatus struct {
	Generation                      uint64
	Queued                          int
	Written, Dropped, Suppressed    uint64
	Closed, Degraded, CleanupFailed bool
	ChargedBytes                    int64
}

type Stats struct {
	Disabled, Degraded, CleanupFailed                         bool
	Queued, DeduperKeys                                       int // DeduperKeys is -1 when unavailable from the common sink.
	Dropped, Unconfirmed, Written, Suppressed, DisabledEvents uint64
	CurrentWritten                                            uint64
	Generation                                                uint64
	ChargedBytes                                              int64
}

// One process-wide scratch buffer avoids multiplying peer-owned backing when
// stopped manager handles remain referenced. Integration charges this static
// buffer and producer state to the peer logger reservation.
var formatBuffer struct {
	sync.Mutex
	data [asynclog.MaxRecordBytes]byte
}

// Service's emitter owns admission, loss accounting, deduplication, I/O, and
// shutdown. Producer admission and formatting hold only bounded in-memory locks.
type Service struct {
	mu               sync.Mutex
	emitter          StreamEmitter
	daily, overlong  uint8
	enabled, stopped bool
	valid            bool
	disabledEvents   uint64
}

// Retired is pointer-free metadata needed to reconstruct stopped diagnostics.
// It does not retain a Service, emitter, queue, or coordinator.
type Retired struct {
	disabledEvents   uint64
	enabled, present bool
}

func New(enabled bool, emitter StreamEmitter, dailyStream, overlongStream uint8) *Service {
	return &Service{enabled: enabled, emitter: emitter, daily: dailyStream, overlong: overlongStream,
		valid: dailyStream < asynclog.MaxStreams && overlongStream < asynclog.MaxStreams && dailyStream != overlongStream}
}

// Emit formats only bounded primitive fields and submits one atomic record.
// Peer overlong samples stay enabled even when the daily peer log is disabled.
func (s *Service) Emit(kind Kind, fields Fields) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := s.daily
	if kind == Overlong {
		stream = s.overlong
	}
	if s.stopped {
		return false
	}
	if kind != Overlong && !s.enabled {
		if s.disabledEvents < math.MaxUint64 {
			s.disabledEvents++
		}
		return false
	}
	if !s.valid || s.emitter == nil || kind < Diagnostic || kind > Overlong {
		return false
	}
	at := time.Now().UTC()
	name := "peer_diagnostic"
	switch kind {
	case Connection:
		name = "peer_connection"
	case Overlong:
		name = "peer_overlong"
	}
	formatBuffer.Lock()
	defer formatBuffer.Unlock()
	line := formatBuffer.data[:0]
	line = appendField(line, "event", name, 32)
	line = appendField(line, "direction", fields.Direction, 16)
	line = appendField(line, "action", fields.Action, 64)
	line = appendField(line, "peer", fields.Peer, 64)
	line = appendField(line, "endpoint", fields.Endpoint, 256)
	line = appendField(line, "reason", fields.Reason, 128)
	line = appendField(line, "detail", fields.Detail, 512)
	line = appendField(line, "dx", fields.DX, 128)
	line = appendField(line, "de", fields.DE, 128)
	if fields.Count != 0 {
		line = append(line, " count="...)
		line = strconv.AppendInt(line, fields.Count, 10)
	}
	if fields.Limit != 0 {
		line = append(line, " limit="...)
		line = strconv.AppendInt(line, fields.Limit, 10)
	}
	return s.emitter.Emit(stream, at, line)
}

func appendField(dst []byte, key, value string, limit int) []byte {
	if value == "" {
		return dst
	}
	if len(dst) != 0 {
		dst = append(dst, ' ')
	}
	dst = append(dst, key...)
	dst = append(dst, '=')
	for i := 0; i < len(value) && i < limit; i++ {
		ch := value[i]
		if ch <= ' ' || ch == 127 {
			ch = '_'
		}
		dst = append(dst, ch)
	}
	return dst
}

// Stop closes peer producer admission and releases its coordinator reference.
// The shared manager owns the one shutdown deadline and admitted records.
func (s *Service) Stop() {
	s.Retire()
}

// Retire disconnects the emitter after current borrows finish and returns
// compact metadata for a manager that must serve post-stop status queries.
func (s *Service) Retire() Retired {
	if s == nil {
		return Retired{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	s.emitter = nil
	return Retired{disabledEvents: s.disabledEvents, enabled: s.enabled, present: true}
}

func addCount(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}
	return a + b
}

// Snapshot retains only compact pointer-free retirement state. Current-owner
// counts are unavailable after retirement and must not describe a successor.
func (r Retired) Snapshot() Stats {
	if !r.present {
		return Stats{Disabled: true}
	}
	return Stats{Disabled: !r.enabled, Degraded: true, DisabledEvents: r.disabledEvents, DeduperKeys: -1}
}

// Snapshot reports the current stream generations. A stopped service retains
// no emitter reference and does not report successor-generation activity.
func (s *Service) Snapshot() Stats {
	if s == nil {
		return Stats{Disabled: true}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Service) snapshotLocked() Stats {
	enabled, emitter, daily, overlong, disabledEvents, valid, stopped := s.enabled, s.emitter, s.daily, s.overlong, s.disabledEvents, s.valid, s.stopped
	result := Stats{Disabled: !enabled, DisabledEvents: disabledEvents, DeduperKeys: -1}
	if !valid {
		result.Degraded = true
		return result
	}
	if emitter == nil {
		result.Degraded = true
		return result
	}
	state := emitter.Status(daily)
	other := emitter.Status(overlong)
	result.Dropped = addCount(state.Dropped, other.Dropped)
	result.Written = addCount(state.Written, other.Written)
	result.Suppressed = addCount(state.Suppressed, other.Suppressed)
	result.ChargedBytes = max(state.ChargedBytes, other.ChargedBytes)
	result.Generation = state.Generation
	if state.Generation != 0 {
		result.CurrentWritten = state.Written
	}
	result.Queued = state.Queued + other.Queued
	result.Degraded = stopped || (enabled && (state.Degraded || state.CleanupFailed || state.Generation == 0 || state.Closed)) || other.Degraded || other.CleanupFailed || other.Generation == 0 || other.Closed
	result.CleanupFailed = (enabled && state.CleanupFailed) || other.CleanupFailed
	return result
}
