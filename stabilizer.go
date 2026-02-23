package main

import (
	"container/heap"
	"sync/atomic"
	"time"

	"dxcluster/spot"
)

const (
	stabilizerTimeoutRelease  = "release"
	stabilizerTimeoutSuppress = "suppress"
)

type telnetStabilizerItem struct {
	spot            *spot.Spot
	due             time.Time
	seq             uint64
	checksCompleted int
}

type telnetStabilizerEnvelope struct {
	spot            *spot.Spot
	checksCompleted int
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
	item, _ := x.(*telnetStabilizerItem)
	*h = append(*h, item)
}

func (h *telnetStabilizerHeap) Pop() any {
	old := *h
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// telnetSpotStabilizer delays risky telnet spots for a bounded duration. A
// single scheduler goroutine owns the heap and timer to keep goroutine count
// bounded under bursty traffic.
type telnetSpotStabilizer struct {
	delay      time.Duration
	maxPending int64

	in      chan *telnetStabilizerEnvelope
	release chan *telnetStabilizerEnvelope
	stop    chan struct{}
	done    chan struct{}

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

func (s *telnetSpotStabilizer) Start() {
	if s == nil {
		return
	}
	go s.run()
}

func (s *telnetSpotStabilizer) Stop() {
	if s == nil {
		return
	}
	select {
	case <-s.done:
		return
	default:
	}
	close(s.stop)
	<-s.done
}

func (s *telnetSpotStabilizer) ReleaseChan() <-chan *telnetStabilizerEnvelope {
	if s == nil {
		return nil
	}
	return s.release
}

func (s *telnetSpotStabilizer) Pending() int64 {
	if s == nil {
		return 0
	}
	return s.pending.Load()
}

func (s *telnetSpotStabilizer) Enqueue(sp *spot.Spot) bool {
	return s.EnqueueWithChecks(sp, 0)
}

func (s *telnetSpotStabilizer) EnqueueWithChecks(sp *spot.Spot, checksCompleted int) bool {
	if s == nil || sp == nil {
		return false
	}
	if checksCompleted < 0 {
		checksCompleted = 0
	}
	if !s.reserveSlot() {
		return false
	}
	select {
	case s.in <- &telnetStabilizerEnvelope{spot: sp, checksCompleted: checksCompleted}:
		return true
	default:
		s.pending.Add(-1)
		return false
	}
}

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
				spot:            envelope.spot,
				due:             time.Now().UTC().Add(s.delay),
				seq:             nextID,
				checksCompleted: envelope.checksCompleted,
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
				item, _ := itemAny.(*telnetStabilizerItem)
				s.pending.Add(-1)
				if item == nil || item.spot == nil {
					continue
				}
				select {
				case s.release <- &telnetStabilizerEnvelope{
					spot:            item.spot,
					checksCompleted: item.checksCompleted,
				}:
				case <-s.stop:
					return
				}
			}
		}
	}
}
