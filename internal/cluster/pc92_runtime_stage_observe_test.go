//go:build qualification

package cluster

import (
	"math"
	"time"

	"dxcluster/internal/qualificationstage"
)

func (s *qualificationStageTrace) observe(event qualificationstage.Event) {
	if !s.enter() {
		return
	}
	defer s.active.Add(-1)
	at := s.oracle.clockNow().UnixNano()
	id, ok := qualificationToken(event.Comment)
	if !ok {
		s.fail(1)
		return
	}
	_, _, start, ok := s.input(id)
	if !ok {
		return
	}
	if at <= 0 || at < start || at < s.armed {
		s.fail(4)
		return
	}
	if event.Stage == qualificationstage.WorkerDispatched {
		if event.Worker < 0 || event.Worker >= s.workers {
			s.fail(5)
			return
		}
		bit := uint32(1) << uint(event.Worker)
		if bit&s.expected == 0 || s.dispatched[id].Or(bit)&bit != 0 {
			s.fail(6)
		}
		return
	}
	slot := int(event.Stage)
	if event.Stage == qualificationstage.WorkerStarted {
		if event.Worker < 0 || event.Worker >= s.workers || s.dispatched[id].Load()&(uint32(1)<<uint(event.Worker)) == 0 {
			s.fail(5)
			return
		}
		slot = stageCommon + event.Worker
	} else if slot < 0 || slot >= stageCommon || event.Worker != -1 {
		s.fail(5)
		return
	}
	if !s.rows[id*s.stride+slot].CompareAndSwap(0, at) {
		s.fail(6)
	}
}

// Enqueue uses the existing endpoint's original QPC tick. The observation does
// not reset that tick after parsing or after the optional diagnostic work.
func (s *qualificationStageTrace) enqueue(client int, comment string, at int64) {
	if !s.enter() {
		return
	}
	defer s.active.Add(-1)
	id, ok := qualificationToken(comment)
	if !ok {
		s.fail(1)
		return
	}
	_, minute, start, ok := s.input(id)
	if !ok {
		return
	}
	if client < 0 || client >= len(s.oracle.clients) {
		s.fail(5)
		return
	}
	ticks, ok := s.deltas(id, client, at, start)
	if !ok {
		s.fail(7)
		return
	}
	elapsed, ok := s.duration(at - start)
	if !ok {
		return
	}
	late := elapsed > 5*time.Millisecond
	s.record(0, minute, 6, ticks[6])
	if late {
		for segment, value := range ticks {
			s.record(1, minute, segment, value)
		}
	}
}

func (s *qualificationStageTrace) deltas(id, client int, at, start int64) ([stageSegments]int64, bool) {
	var ticks [stageSegments]int64
	previous := start
	for i := range stageCommon {
		next := s.rows[id*s.stride+i].Load()
		if next == 0 || next < previous {
			return ticks, false
		}
		ticks[i], previous = next-previous, next
	}
	worker := int(s.clientWorker[client])
	next := s.rows[id*s.stride+stageCommon+worker].Load()
	if next == 0 || next < previous || at < next {
		return ticks, false
	}
	ticks[5], ticks[6] = next-previous, at-next
	return ticks, true
}

func (s *qualificationStageTrace) record(group, minute, segment int, ticks int64) {
	if _, ok := s.duration(ticks); !ok {
		return
	}
	s.hist[group][0][segment].observeCounter(time.Duration(ticks), s.oracle.clockFrequency)
	s.hist[group][minute][segment].observeCounter(time.Duration(ticks), s.oracle.clockFrequency)
}

func (s *qualificationStageTrace) duration(ticks int64) (time.Duration, bool) {
	if ticks < 0 || ticks == math.MaxInt64 || ticks/s.oracle.clockFrequency > math.MaxInt64/int64(time.Second)-1 {
		s.fail(4)
		return 0, false
	}
	return qualificationTickDuration(time.Duration(ticks), s.oracle.clockFrequency, true), true
}
