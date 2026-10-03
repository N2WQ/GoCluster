// Package peerdiag isolates peer diagnostic storage from protocol ownership.
// Producers copy bounded typed values into a fixed mailbox; only the companion
// process touches diagnostic files. No producer calls an arbitrary formatter.
package peerdiag

import (
	"encoding/binary"
	"strconv"
	"sync"
	"time"
)

const (
	QueueSize    = 256
	RecordBytes  = 2048
	BackingLimit = 3 << 20
)

type Kind uint16

const (
	Diagnostic Kind = iota + 1
	Connection
	Overlong
	LossSummary
)

// Fields are borrowed only during Emit. The event never retains these string
// headers, an error interface, a session, or an input frame. Values are copied
// only after admission; all formatting uses fixed code and primitive values.
type Fields struct {
	Action, Peer, Endpoint, Reason, Detail, Direction string
	DX, DE                                            string
	Count, Limit                                      int64
}

// Event is exactly one 2 KiB mailbox/IPC record, including its header.
type Event struct {
	Sequence uint64
	UnixNano int64
	Length   uint16
	Kind     Kind
	_        uint32
	Data     [RecordBytes - 24]byte
}

type Stats struct {
	Disabled, Degraded, CleanupFailed                         bool
	Queued, DeduperKeys                                       int
	Dropped, Unconfirmed, Written, Suppressed, DisabledEvents uint64
	Generation                                                uint64
	ChargedBytes                                              int64
}

type Mailbox struct {
	mu          sync.Mutex
	events      *[QueueSize]Event
	head, count int
	sequence    uint64
	closed      bool
	disabled    bool
	stats       Stats
	notify      chan struct{}
}

func NewMailbox(enabled bool) *Mailbox {
	return &Mailbox{events: new([QueueSize]Event), disabled: !enabled, stats: Stats{Disabled: !enabled}, notify: make(chan struct{}, 1)}
}

func (m *Mailbox) Emit(kind Kind, fields Fields) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.emitLocked(kind, fields)
}

func (m *Mailbox) emitLocked(kind Kind, fields Fields) bool {
	if m.disabled && kind != Overlong {
		m.stats.DisabledEvents++
		return false
	}
	if m.closed || m.count == QueueSize {
		m.stats.Dropped++
		return false
	}
	event := &m.events[(m.head+m.count)%QueueSize]
	*event = Event{Kind: kind, UnixNano: time.Now().UTC().UnixNano()}
	m.sequence++
	event.Sequence = m.sequence
	line := event.Data[:0]
	name := "peer_diagnostic"
	if kind == Connection {
		name = "peer_connection"
	}
	if kind == Overlong {
		name = "peer_overlong"
	}
	if kind == LossSummary {
		name = "peer_diagnostic_loss"
	}
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
	event.Length = uint16(len(line))
	m.count++
	select {
	case m.notify <- struct{}{}:
	default:
	}
	return true
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
	// Bound input inspection as well as output. A hostile long whitespace
	// prefix must not make diagnostic formatting proportional to input length.
	for i := 0; i < len(value) && i < limit; i++ {
		ch := value[i]
		if ch <= ' ' || ch == 127 {
			ch = '_'
		}
		dst = append(dst, ch)
	}
	return dst
}

// Next transfers one owned fixed record to the single consumer. The supervisor
// is the production consumer; direct consumers are useful for isolated tests.
func (m *Mailbox) Next() (Event, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.count == 0 {
		return Event{}, false
	}
	event := m.events[m.head]
	m.events[m.head] = Event{}
	m.head = (m.head + 1) % QueueSize
	m.count--
	return event, true
}

func (m *Mailbox) closeAdmission() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	m.stats.Dropped += uint64(m.count)
	if m.events != nil {
		clear(m.events[:])
	}
	m.count = 0
}

func (m *Mailbox) Snapshot() Stats {
	if m == nil {
		return Stats{Disabled: true}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.stats
	stats.Queued = m.count
	return stats
}

func (e *Event) encode(dst *[RecordBytes]byte) {
	binary.LittleEndian.PutUint64(dst[0:8], e.Sequence)
	binary.LittleEndian.PutUint64(dst[8:16], uint64(e.UnixNano))
	binary.LittleEndian.PutUint16(dst[16:18], e.Length)
	binary.LittleEndian.PutUint16(dst[18:20], uint16(e.Kind))
	clear(dst[20:24])
	copy(dst[24:], e.Data[:])
}

func decodeEvent(src *[RecordBytes]byte) (Event, bool) {
	e := Event{Sequence: binary.LittleEndian.Uint64(src[:8]), UnixNano: int64(binary.LittleEndian.Uint64(src[8:16])), Length: binary.LittleEndian.Uint16(src[16:18]), Kind: Kind(binary.LittleEndian.Uint16(src[18:20]))}
	if e.Length > uint16(len(e.Data)) || e.Kind < Diagnostic || e.Kind > LossSummary || e.Sequence == 0 {
		return Event{}, false
	}
	copy(e.Data[:], src[24:])
	return e, true
}
