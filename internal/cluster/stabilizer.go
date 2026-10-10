// File role: Owns bounded delayed spot scheduling and terminal admission.
// Crawler notes: One scheduler owns the heap/timer; Stop drops held work and
// closes released output. Pipeline retirement also joins its separate consumer.
// Related docs: docs/decisions/ADR-0275-runtime-output-retirement.md.
package cluster

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/spot"
)

const (
	stabilizerTimeoutRelease  = "release"
	stabilizerTimeoutSuppress = "suppress"
)

type telnetStabilizerItem struct {
	spot             *spot.Spot
	due              time.Time
	seq              uint64
	checksCompleted  int
	delayReason      string
	resolverKey      spot.ResolverSignalKey
	hasResolverKey   bool
	evidenceEnqueued bool
}

// telnetStabilizerEnvelope carries the minimum context needed to retry a held
// telnet spot without re-running upstream stages that already made a decision.
// Keeping this envelope small makes the delay queue easier to reason about
// during overflow or shutdown investigations.
type telnetStabilizerEnvelope struct {
	spot             *spot.Spot
	checksCompleted  int
	delayReason      string
	resolverKey      spot.ResolverSignalKey
	hasResolverKey   bool
	evidenceEnqueued bool
}

type telnetStabilizerHeap []*telnetStabilizerItem

func (h telnetStabilizerHeap) Len() int { return len(h) }

func (h telnetStabilizerHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].seq < h[j].seq
	}
	return h[i].due.Before(h[j].due)
}

func (h telnetStabilizerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *telnetStabilizerHeap) Push(x any) {
	item, ok := x.(*telnetStabilizerItem)
	if !ok {
		return
	}
	*h = append(*h, item)
}

func (h *telnetStabilizerHeap) Pop() any {
	old := *h
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// telnetSpotStabilizer delays risky telnet spots for a bounded duration. A
// single scheduler goroutine owns the heap and timer to keep goroutine count
// bounded under bursty traffic. The lifecycle fence orders startup and bounded
// input publication against terminal retirement, never spanning scheduler I/O
// or joins. Released envelopes belong to the pipeline's separate consumer.
type telnetSpotStabilizer struct {
	delay      time.Duration
	maxPending int64

	in      chan *telnetStabilizerEnvelope
	release chan *telnetStabilizerEnvelope
	stop    chan struct{}
	done    chan struct{}

	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
	stopOnce    sync.Once

	pending atomic.Int64
}

func newTelnetSpotStabilizer(delay time.Duration, maxPending int) *telnetSpotStabilizer {
	if delay <= 0 {
		delay = 5 * time.Second
	}
	if maxPending <= 0 {
		maxPending = 20000
	}
	return &telnetSpotStabilizer{
		delay:      delay,
		maxPending: int64(maxPending),
		in:         make(chan *telnetStabilizerEnvelope, 1024),
		release:    make(chan *telnetStabilizerEnvelope, 1024),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// Start begins the single owner goroutine for delayed telnet releases. The
// stabilizer deliberately does not allocate one timer or goroutine per spot, so
// bursty contested calls cannot multiply scheduler pressure.
func (s *telnetSpotStabilizer) Start() {
	if s == nil {
		return
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.started || s.stopped {
		return
	}
	s.started = true
	go s.run()
}

// Stop permanently fences admission and joins the scheduler once. Unreleased
// input/heap work keeps the existing shutdown-drop disposition and retires its
// reserved count; already released envelopes remain consumer-owned, buffered
// in the closed release channel. The pipeline must separately join that
// consumer before closing downstream resources.
func (s *telnetSpotStabilizer) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		s.lifecycleMu.Lock()
		s.stopped = true
		close(s.stop)
		started := s.started
		s.lifecycleMu.Unlock()
		if started {
			<-s.done
			return
		}
		// Pre-Start admission is valid, but terminal retirement must not wait
		// for a scheduler that cannot now start.
		s.retireInput()
		close(s.release)
		close(s.done)
	})
}

// ReleaseChan exposes only released envelopes so the output pipeline remains
// the sole component that decides final telnet fanout.
func (s *telnetSpotStabilizer) ReleaseChan() <-chan *telnetStabilizerEnvelope {
	if s == nil {
		return nil
	}
	return s.release
}

// Pending reports reserved queue slots, including envelopes waiting on the
// input channel. Support checks use it to distinguish normal delay from
// backpressure/overflow.
func (s *telnetSpotStabilizer) Pending() int64 {
	if s == nil {
		return 0
	}
	return s.pending.Load()
}

func (s *telnetSpotStabilizer) Enqueue(sp *spot.Spot) bool {
	return s.EnqueueWithChecks(sp, 0, "")
}

func (s *telnetSpotStabilizer) EnqueueWithChecks(sp *spot.Spot, checksCompleted int, delayReason string) bool {
	return s.EnqueueWithContext(sp, checksCompleted, delayReason, spot.ResolverSignalKey{}, false, false)
}

func (s *telnetSpotStabilizer) EnqueueWithContext(
	sp *spot.Spot,
	checksCompleted int,
	delayReason string,
	resolverKey spot.ResolverSignalKey,
	hasResolverKey bool,
	evidenceEnqueued bool,
) bool {
	if s == nil || sp == nil {
		return false
	}
	if checksCompleted < 0 {
		checksCompleted = 0
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.stopped {
		return false
	}
	if !s.reserveSlot() {
		return false
	}
	select {
	case s.in <- &telnetStabilizerEnvelope{
		spot:             sp,
		checksCompleted:  checksCompleted,
		delayReason:      delayReason,
		resolverKey:      resolverKey,
		hasResolverKey:   hasResolverKey,
		evidenceEnqueued: evidenceEnqueued,
	}:
		return true
	default:
		s.pending.Add(-1)
		return false
	}
}

// retireInput runs only after admission is fenced and scheduler processing has
// finished (or never started). Receive every held envelope to release its
// references and retire its own reservation; a reset would hide mismatches.
func (s *telnetSpotStabilizer) retireInput() {
	for {
		select {
		case <-s.in:
			s.pending.Add(-1)
		default:
			return
		}
	}
}

// reserveSlot applies the hard pending cap before enqueue so the stabilizer
// fails open by refusing more delayed work rather than growing memory.
func (s *telnetSpotStabilizer) reserveSlot() bool {
	for {
		cur := s.pending.Load()
		if cur >= s.maxPending {
			return false
		}
		if s.pending.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// run owns the heap and timer so delayed telnet release is deterministic: due
// time first, insertion sequence second. Pending is decremented only when an
// envelope leaves this queue boundary.
func (s *telnetSpotStabilizer) run() {
	defer close(s.done)
	defer close(s.release)

	var (
		queue  telnetStabilizerHeap
		nextID uint64
		timer  *time.Timer
		timerC <-chan time.Time
	)
	heap.Init(&queue)
	defer func() {
		for i := range queue {
			queue[i] = nil
			s.pending.Add(-1)
		}
		s.retireInput()
	}()

	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	resetTimer := func(delay time.Duration) {
		if delay < 0 {
			delay = 0
		}
		if timer == nil {
			timer = time.NewTimer(delay)
			timerC = timer.C
			return
		}
		stopTimer()
		timer.Reset(delay)
		timerC = timer.C
	}
	defer stopTimer()

	for {
		select {
		case <-s.stop:
			return
		default:
		}
		if queue.Len() == 0 {
			stopTimer()
			timerC = nil
		} else {
			next := queue[0]
			resetTimer(time.Until(next.due))
		}

		select {
		case <-s.stop:
			return
		case envelope := <-s.in:
			if envelope == nil || envelope.spot == nil {
				s.pending.Add(-1)
				continue
			}
			heap.Push(&queue, &telnetStabilizerItem{
				spot:             envelope.spot,
				due:              time.Now().UTC().Add(s.delay),
				seq:              nextID,
				checksCompleted:  envelope.checksCompleted,
				delayReason:      envelope.delayReason,
				resolverKey:      envelope.resolverKey,
				hasResolverKey:   envelope.hasResolverKey,
				evidenceEnqueued: envelope.evidenceEnqueued,
			})
			nextID++
		case <-timerC:
			now := time.Now().UTC()
			for queue.Len() > 0 {
				next := queue[0]
				if next.due.After(now) {
					break
				}
				itemAny := heap.Pop(&queue)
				item, ok := itemAny.(*telnetStabilizerItem)
				s.pending.Add(-1)
				if !ok || item == nil || item.spot == nil {
					continue
				}
				select {
				case s.release <- &telnetStabilizerEnvelope{
					spot:             item.spot,
					checksCompleted:  item.checksCompleted,
					delayReason:      item.delayReason,
					resolverKey:      item.resolverKey,
					hasResolverKey:   item.hasResolverKey,
					evidenceEnqueued: item.evidenceEnqueued,
				}:
				case <-s.stop:
					return
				}
			}
		}
	}
}
