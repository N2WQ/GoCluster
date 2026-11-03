// Package asynclog provides bounded asynchronous delivery to a fixed set of
// process-wide destinations. Filesystem and display work belongs in Sink.
package asynclog

import (
	"errors"
	"math"
	"sync"
	"time"
	"unsafe"
)

const (
	MaxStreams      = 32
	QueueSize       = 256
	SingleQueueSize = 128
	MaxRecordBytes  = 2048
	MaxMessageBytes = QueueSize * MaxRecordBytes
	MaxGrouped      = 6
)

var (
	ErrStream    = errors.New("invalid logging stream")
	ErrOwned     = errors.New("logging stream remains owned")
	ErrFactory   = errors.New("nil logging sink factory")
	ErrExhausted = errors.New("logging stream generation exhausted")
	ErrGrouped   = errors.New("grouped logging buffers exhausted")
)

// Sink is called only by its stream's worker. Write borrows payload until it
// returns. A blocked Write or Close prevents a replacement for this stream,
// but never blocks admission to another stream or the Stop caller.
type Sink interface {
	Write(at time.Time, frame uint8, payload []byte) WriteResult
	Close() error
}

type WriteResult uint8

const (
	Written WriteResult = iota + 1
	Suppressed
	NotWritten
)

// Handle contains no sink or queue pointer. Each value refers to one stream
// generation, so stale handles cannot deliver records to a replacement.
type Handle struct {
	stream uint8
	id     uint64
}

type record struct {
	at     time.Time
	length uint16
	parts  uint16 // Nonzero only on the first record of a logical message.
	frame  uint8
	data   [MaxRecordBytes]byte
}

type slot struct {
	mu          sync.Mutex
	once        sync.Once
	wake        chan struct{}
	id          uint64
	active      bool
	stopping    bool
	poisoned    bool
	degraded    bool
	factory     func() Sink
	done        chan struct{}
	retained    Sink // A failed Close may still own resources; do not replace it.
	queue       []record
	head, count int
	groups      int
	written     uint64
	dropped     uint64
	suppressed  uint64
	scratch     [MaxRecordBytes]byte // Exclusively owned by the stream worker.
	groupIndex  int
}

var slots [MaxStreams]slot
var singleQueues [MaxStreams][SingleQueueSize]record

var grouped struct {
	sync.Mutex
	used  [MaxGrouped]bool
	data  [MaxGrouped][MaxMessageBytes]byte
	queue [MaxGrouped][QueueSize]record
}

// StaticBackingBytes is the permanently allocated slot array, including all
// queues and worker scratch. It excludes Go runtime allocation overhead,
// channels, sinks, factories, and resources owned by integration callers.
const StaticBackingBytes = uint64(unsafe.Sizeof([MaxStreams]slot{})) + uint64(unsafe.Sizeof(singleQueues)) + uint64(unsafe.Sizeof(grouped))

// SlotBackingBytes is the slot array's fixed backing attributable to one stream.
const SlotBackingBytes = uint64(unsafe.Sizeof(slot{})) + uint64(unsafe.Sizeof([SingleQueueSize]record{}))

type Stats struct {
	Generation    uint64
	Queued        int
	Written       uint64
	Dropped       uint64
	Suppressed    uint64
	Closed        bool
	Degraded      bool
	CleanupFailed bool
	RetainedBytes uint64 // Fixed slot backing only; see StaticBackingBytes.
}

var closedDone = func() <-chan struct{} { ch := make(chan struct{}); close(ch); return ch }()

func (s *slot) startWorker() {
	s.once.Do(func() {
		s.wake = make(chan struct{}, 1)
		go s.run()
	})
}

// Start reserves one stream until its factory, outstanding write, and Close
// have returned. A factory runs in that stream's permanent worker, so even a
// stuck open or directory creation cannot block the Start caller.
func Start(stream uint8, factory func() Sink) (Handle, error) {
	return start(stream, factory, true)
}

// StartSingle uses the same queue with one-record delivery and no grouped
// reassembly buffer. All file and console messages have this bound.
func StartSingle(stream uint8, factory func() Sink) (Handle, error) {
	return start(stream, factory, false)
}

func start(stream uint8, factory func() Sink, large bool) (Handle, error) {
	if int(stream) >= MaxStreams {
		return Handle{}, ErrStream
	}
	if factory == nil {
		return Handle{}, ErrFactory
	}
	s := &slots[stream]
	s.startWorker()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active || s.poisoned {
		return Handle{}, ErrOwned
	}
	if s.id == math.MaxUint64 {
		return Handle{}, ErrExhausted
	}
	groupIndex := -1
	if large {
		grouped.Lock()
		for i := range grouped.used {
			if !grouped.used[i] {
				grouped.used[i] = true
				groupIndex = i
				break
			}
		}
		grouped.Unlock()
		if groupIndex < 0 {
			return Handle{}, ErrGrouped
		}
	}
	s.id++
	s.active, s.stopping, s.degraded = true, false, false
	s.groupIndex = groupIndex
	s.queue = singleQueues[stream][:]
	if groupIndex >= 0 {
		s.queue = grouped.queue[groupIndex][:]
	}
	s.factory = factory
	s.done = make(chan struct{})
	s.head, s.count, s.groups = 0, 0, 0
	s.written, s.dropped, s.suppressed = 0, 0, 0
	h := Handle{stream: stream, id: s.id}
	s.signal()
	return h, nil
}

func (s *slot) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func increment(v *uint64) {
	if *v != math.MaxUint64 {
		*v++
	}
}

// Emit atomically admits the entire message as one or more fixed 2 KiB
// records. It copies before returning and never waits for output or space.
// UTC normalization avoids retaining arbitrary caller-owned time locations.
func (h Handle) Emit(at time.Time, frame uint8, payload []byte) bool {
	return emit(h, at, frame, payload)
}

// EmitString copies directly from the caller's string into the fixed queue.
// It avoids allocating one temporary copy for every concurrent publisher.
func (h Handle) EmitString(at time.Time, frame uint8, payload string) bool {
	return emit(h, at, frame, payload)
}

func emit[T ~string | ~[]byte](h Handle, at time.Time, frame uint8, payload T) bool {
	if h.id == 0 || int(h.stream) >= MaxStreams {
		return false
	}
	s := &slots[h.stream]
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active || s.id != h.id || s.stopping {
		return false
	}
	if len(payload) > MaxMessageBytes || (s.groupIndex < 0 && len(payload) > MaxRecordBytes) {
		increment(&s.dropped)
		return false
	}
	parts := (len(payload) + MaxRecordBytes - 1) / MaxRecordBytes
	if parts == 0 {
		parts = 1
	}
	if parts > len(s.queue)-s.count {
		increment(&s.dropped)
		return false
	}
	at = at.UTC()
	for i := 0; i < parts; i++ {
		r := &s.queue[(s.head+s.count+i)%len(s.queue)]
		*r = record{at: at, frame: frame}
		if i == 0 {
			r.parts = uint16(parts)
		}
		start := i * MaxRecordBytes
		end := min(start+MaxRecordBytes, len(payload))
		r.length = uint16(copy(r.data[:], payload[start:end]))
	}
	s.count += parts
	s.groups++
	s.signal()
	return true
}

// Stop prevents new admissions without waiting for delivery or cleanup. The
// worker drains admitted messages; Done confirms that cleanup really returned.
func (h Handle) Stop() {
	if h.id == 0 || int(h.stream) >= MaxStreams {
		return
	}
	s := &slots[h.stream]
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active || s.id != h.id || s.stopping {
		return
	}
	s.stopping = true
	s.signal()
}

// Done is generation-specific and closes after the factory and sink cleanup
// have returned. A zero handle is already done. Callers retaining old Done
// channels own that retention after the generation is complete.
func (h Handle) Done() <-chan struct{} {
	if h.id == 0 || int(h.stream) >= MaxStreams {
		return closedDone
	}
	s := &slots[h.stream]
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active || s.id != h.id || s.done == nil {
		return closedDone
	}
	return s.done
}

func (h Handle) Stats() Stats {
	result := Stats{Generation: h.id, Closed: true, RetainedBytes: SlotBackingBytes}
	if h.id == 0 || int(h.stream) >= MaxStreams {
		return result
	}
	s := &slots[h.stream]
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id == h.id {
		result.Queued, result.Written, result.Dropped, result.Suppressed = s.groups, s.written, s.dropped, s.suppressed
		result.Closed = !s.active || s.stopping
		result.Degraded = s.degraded
		result.CleanupFailed = s.poisoned
	}
	return result
}

func (s *slot) run() {
	for range s.wake {
		s.mu.Lock()
		if !s.active || s.factory == nil {
			s.mu.Unlock()
			continue
		}
		factory, id, skip := s.factory, s.id, s.stopping && s.groups == 0
		s.factory = nil
		s.mu.Unlock()

		var sink Sink
		factoryOK := true
		if !skip {
			sink, factoryOK = callFactory(factory)
		}
		// Do not retain captured configuration through the worker's factory.
		factory = nil //nolint:ineffassign,wastedassign // Drop captured configuration before a successor can start.
		if sink != nil {
			s.deliver(sink, id)
		}
		clean := factoryOK && callClose(sink)
		if clean {
			// A later generation may start as soon as the slot is released.
			// Drop the old sink before making that release visible.
			sink = nil
		}
		s.mu.Lock()
		if s.id == id && s.active {
			// A failed factory or sink may leave admitted messages undelivered.
			// Count those logical messages before releasing the queue.
			for i := 0; i < s.groups; i++ {
				increment(&s.dropped)
			}
			clear(s.queue)
			s.head, s.count, s.groups = 0, 0, 0
			if s.groupIndex >= 0 {
				grouped.Lock()
				grouped.used[s.groupIndex] = false
				grouped.Unlock()
				s.groupIndex = -1
			}
			s.queue = nil
			if !clean {
				s.poisoned, s.retained = true, sink
				sink = nil
			}
			s.active, s.stopping = false, true
			close(s.done)
			s.done = nil
		}
		s.mu.Unlock()
	}
}

func (s *slot) deliver(sink Sink, id uint64) {
	for {
		s.mu.Lock()
		if !s.active || s.id != id || (s.stopping && s.groups == 0) {
			s.mu.Unlock()
			return
		}
		if s.groups == 0 {
			s.mu.Unlock()
			<-s.wake
			continue
		}
		first := &s.queue[s.head]
		at, frame, parts := first.at, first.frame, int(first.parts)
		scratch := s.scratch[:]
		if s.groupIndex >= 0 {
			scratch = grouped.data[s.groupIndex][:]
		}
		n := 0
		for i := 0; i < parts; i++ {
			r := &s.queue[(s.head+i)%len(s.queue)]
			n += copy(scratch[n:], r.data[:r.length])
			*r = record{}
		}
		s.head = (s.head + parts) % len(s.queue)
		s.count -= parts
		s.groups--
		s.mu.Unlock()
		result, ok := callWrite(sink, at, frame, scratch[:n])
		s.mu.Lock()
		if s.id == id && s.active {
			switch result {
			case Written:
				increment(&s.written)
				s.degraded = false
			case Suppressed:
				increment(&s.suppressed)
			default:
				increment(&s.dropped)
				s.degraded = true
			}
		}
		s.mu.Unlock()
		if !ok {
			return
		}
	}
}

// A defective sink must not permanently kill its process-wide worker. Its
// Close still runs after a panic, and uncertain cleanup poisons the slot.
func callFactory(factory func() Sink) (sink Sink, ok bool) {
	defer func() {
		if recover() != nil {
			sink, ok = nil, false
		}
	}()
	sink = factory()
	return sink, true
}

func callWrite(s Sink, at time.Time, frame uint8, payload []byte) (result WriteResult, ok bool) {
	defer func() {
		if recover() != nil {
			result, ok = NotWritten, false
		}
	}()
	return s.Write(at, frame, payload), true
}

func callClose(s Sink) (ok bool) {
	if s == nil {
		return true
	}
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return s.Close() == nil
}
