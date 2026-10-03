//go:build qualification

package cluster

import "dxcluster/telnet"

// All report values are fixed scalars. Stage indices are the five common
// handoffs, broadcast-consumer to worker, and worker to client admission.
// Group zero has one observation per token/worker/client as applicable;
// group one attributes each over-five-ms client admission to all seven stages.
type qualificationStageReport struct {
	Enabled, Complete                 bool
	Schema, Inputs, Workers, Clients  int
	Mask                              uint32
	BackingBytes                      uint64
	Epoch, Frequency, Armed, Finished int64
	RequiredSpots, Missing, Failures  uint64
	FailureCodes                      [16]uint64
	Segments                          [2][stageCohorts][stageSegments]qualificationLatency
}

func (s *qualificationStageTrace) finish(used int, fanout *telnet.QualificationStageFanout) *qualificationStageReport {
	s.freeze()
	defer s.retire()
	r := s.report
	r.Enabled, r.Schema = true, 1
	r.Inputs, r.Workers, r.Clients, r.Mask = len(s.oracle.inputs), s.workers, len(s.oracle.clients), s.expected
	r.BackingBytes, r.Epoch, r.Frequency = s.backing, s.oracle.measurementEpoch.UnixNano(), s.oracle.clockFrequency
	r.Armed, r.Finished = s.armed, s.finished
	if s.finished < s.armed || used < 0 || used > len(s.oracle.inputs) || s.matchFanout(fanout, false) != nil {
		s.fail(8)
	} else {
		for id := range used {
			s.validateRow(id, r)
		}
	}
	for i := range s.failures {
		r.FailureCodes[i] = s.failures[i].Load()
		if ^uint64(0)-r.Failures < r.FailureCodes[i] {
			r.Failures = ^uint64(0)
		} else {
			r.Failures += r.FailureCodes[i]
		}
	}
	for group := range 2 {
		for minute := range stageCohorts {
			for segment := range stageSegments {
				r.Segments[group][minute][segment] = s.hist[group][minute][segment].result()
			}
		}
	}
	// Reconcile every existing endpoint count, including its conservative QPC
	// margin, instead of using a surviving-only diagnostic denominator.
	for minute := range stageCohorts {
		var endpoints, late uint64
		for _, client := range s.oracle.clients {
			if minute >= len(client.enqueueLatency) {
				continue
			}
			endpoints += uint64(client.enqueueLatency[minute].count.Load())
			late += uint64(client.enqueueLatency[minute].over5.Load())
		}
		if uint64(r.Segments[0][minute][6].Count) != endpoints {
			r.reject()
		}
		for segment := range stageSegments {
			if uint64(r.Segments[1][minute][segment].Count) != late {
				r.reject()
			}
		}
	}
	r.Complete = r.Failures == 0 && r.Missing == 0
	return r
}

func (r *qualificationStageReport) reject() {
	if r.Failures != ^uint64(0) {
		r.Failures++
	}
}

func (s *qualificationStageTrace) validateRow(id int, r *qualificationStageReport) {
	in := &s.oracle.inputs[id]
	if in.started.Load() <= 0 {
		s.fail(2)
		return
	}
	if !in.spot {
		return
	}
	r.RequiredSpots++
	_, minute, previous, ok := s.input(id)
	if !ok {
		return
	}
	for slot := range stageCommon {
		at := s.rows[id*s.stride+slot].Load()
		if at == 0 {
			r.Missing++
			continue
		}
		if at < previous || at > s.finished {
			s.fail(4)
			continue
		}
		s.record(0, minute, slot, at-previous)
		previous = at
	}
	if s.dispatched[id].Load() != s.expected {
		s.fail(9)
	}
	for worker := range s.workers {
		at := s.rows[id*s.stride+stageCommon+worker].Load()
		if s.expected&(uint32(1)<<uint(worker)) == 0 {
			if at != 0 {
				s.fail(5)
			}
			continue
		}
		if at == 0 {
			r.Missing++
			continue
		}
		if at < previous || at > s.finished {
			s.fail(4)
			continue
		}
		s.record(0, minute, 5, at-previous)
	}
}
