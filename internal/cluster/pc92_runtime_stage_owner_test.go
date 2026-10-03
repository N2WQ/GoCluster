//go:build qualification

package cluster

import (
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"dxcluster/internal/qualificationstage"
	"dxcluster/telnet"
)

const (
	stageCommon          = 5
	stageCohorts         = 16
	stageSegments        = 7
	stageLimit           = 32 << 20
	stageReportLimit     = 16 << 10
	stageControllerLimit = 4 << 10
)

type qualificationStageHistograms [2][stageCohorts][stageSegments]qualificationHistogram

// Rows are child-only. No packet or returned report contains these arrays.
// A second owner cannot start before the first clears its retired references.
type qualificationStageTrace struct {
	oracle          *qualificationOracle
	rows            []atomic.Int64
	dispatched      []atomic.Uint32
	hist            *qualificationStageHistograms
	report          *qualificationStageReport
	registration    *qualificationstage.Registration
	workers, stride int
	expected        uint32
	clientWorker    [100]int32
	sessions        [100]uint64
	closed          atomic.Bool
	retired         atomic.Bool
	active          atomic.Int64
	failures        [16]atomic.Uint64
	backing         uint64
	armed, finished int64
}

var qualificationStageReserved atomic.Bool

// The reservation includes current warm samples, both report owners and six
// complete RPC buffers. Raw stage rows never enter those encodings.
func qualificationStageBacking(inputs, workers int) (uint64, error) {
	if inputs <= 0 || inputs > 151928 || workers <= 0 || workers > 32 {
		return 0, fmt.Errorf("unsupported stage dimensions %d/%d", inputs, workers)
	}
	// In this warm child: one primary dedup owner, one output-pipeline owner,
	// one broadcast dispatcher, and the actual broadcast workers can mark.
	// The inherited QPC wrapper allocates two eight-byte objects per call;
	// allow separate sixteen-byte tiny backing blocks for both objects.
	control := 2*unsafe.Sizeof(qualificationStageTrace{}) + 2*unsafe.Sizeof(telnet.QualificationStageFanout{}) + 256 + uintptr(workers+3)*32
	if unsafe.Sizeof(qualificationStageReport{}) > stageReportLimit || control > 2*stageControllerLimit {
		return 0, fmt.Errorf("stage fixed owner layout exceeds reviewed bound")
	}
	raw := uint64(inputs)*uint64(stageCommon+workers)*8 + uint64(inputs)*4 + 8192
	warm := 2*uint64(unsafe.Sizeof(qualificationWarmProfile{})) + 6*qualificationRPCMaxBytes
	aux := uint64(unsafe.Sizeof(qualificationStageHistograms{})) + 8192 + 4*stageReportLimit + 2*stageControllerLimit
	total := raw + warm + aux
	if total > stageLimit {
		return 0, fmt.Errorf("optional diagnostic backing %d exceeds %d", total, stageLimit)
	}
	return total, nil
}

func newQualificationStageTrace(o *qualificationOracle, fanout *telnet.QualificationStageFanout) (*qualificationStageTrace, error) {
	if !qualificationStageReserved.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("stage owner already reserved")
	}
	ready := false
	defer func() {
		if !ready {
			qualificationStageReserved.Store(false)
		}
	}()
	backing, err := qualificationStageBacking(len(o.inputs), fanout.Workers)
	if err != nil {
		return nil, err
	}
	if o.measurementEpoch.UnixNano() <= 0 || o.clockNow == nil || o.clockFrequency < 1_000_000 || o.clockFrequency > math.MaxInt64/int64(time.Second)-1 {
		return nil, fmt.Errorf("invalid stage clock domain")
	}
	s := &qualificationStageTrace{oracle: o, workers: fanout.Workers, stride: stageCommon + fanout.Workers, expected: fanout.Mask, backing: backing}
	if err = s.matchFanout(fanout, true); err != nil {
		return nil, err
	}
	registration, ok := qualificationstage.Reserve()
	if !ok {
		return nil, fmt.Errorf("stage hook already reserved")
	}
	s.registration = registration
	s.rows = make([]atomic.Int64, len(o.inputs)*s.stride)
	s.dispatched = make([]atomic.Uint32, len(o.inputs))
	s.hist = new(qualificationStageHistograms)
	s.report = new(qualificationStageReport)
	s.armed = o.clockNow().UnixNano()
	qualificationstage.Publish(registration, s.observe)
	ready = true
	return s, nil
}

func (s *qualificationStageTrace) matchFanout(f *telnet.QualificationStageFanout, initial bool) error {
	if f.Workers != s.workers || f.Mask != s.expected || f.Count != len(s.oracle.clients) || f.Count > len(s.sessions) || f.Mask == 0 {
		return fmt.Errorf("warm stage fanout dimensions changed")
	}
	var seen [100]bool
	var mask uint32
	for _, client := range f.Clients[:f.Count] {
		index := -1
		for i, session := range s.oracle.sessionIDs {
			if session != 0 && session == client.SessionID {
				index = i
				break
			}
		}
		if index < 0 || seen[index] || client.Worker >= uint32(s.workers) {
			return fmt.Errorf("warm stage fanout identity changed")
		}
		seen[index] = true
		mask |= uint32(1) << client.Worker
		if initial {
			s.sessions[index], s.clientWorker[index] = client.SessionID, int32(client.Worker)
		} else if s.sessions[index] != client.SessionID || s.clientWorker[index] != int32(client.Worker) {
			return fmt.Errorf("warm stage shard assignment changed")
		}
	}
	if mask != s.expected {
		return fmt.Errorf("warm stage nonempty shard mask differs")
	}
	return nil
}

func (s *qualificationStageTrace) enter() bool {
	if s.closed.Load() {
		return false
	}
	s.active.Add(1)
	if s.closed.Load() {
		s.active.Add(-1)
		return false
	}
	return true
}

func (s *qualificationStageTrace) freeze() {
	if s.closed.Swap(true) {
		return
	}
	qualificationstage.Remove(s.registration)
	for s.active.Load() != 0 {
		runtime.Gosched()
	}
	// A preloaded callback may increment later, but its closed recheck prevents
	// all row access. This is not a claim that such callbacks have been joined.
	s.finished = s.oracle.clockNow().UnixNano()
}

func (s *qualificationStageTrace) retire() {
	if s.retired.Swap(true) {
		return
	}
	s.freeze()
	s.rows, s.dispatched, s.hist, s.registration = nil, nil, nil, nil
	s.report = nil
	s.oracle = nil
	qualificationStageReserved.Store(false)
}

func (s *qualificationStageTrace) input(id int) (int, int64, bool) {
	if id < 0 || id >= len(s.oracle.inputs) {
		s.fail(1)
		return 0, 0, false
	}
	in := &s.oracle.inputs[id]
	stamp := in.started.Load()
	if stamp <= 0 || !in.spot {
		s.fail(2)
		return 0, 0, false
	}
	epoch := s.oracle.measurementEpoch.UnixNano()
	if stamp-1 > math.MaxInt64-epoch {
		s.fail(4)
		return 0, 0, false
	}
	// Divide raw ticks before any duration conversion. The bounded clock
	// frequency makes the minute divisor safe; no nanosecond product can wrap.
	minute := int((stamp-1)/(s.oracle.clockFrequency*60)) + 1
	if minute < 1 || minute >= stageCohorts {
		s.fail(3)
		return 0, 0, false
	}
	return minute, epoch + stamp - 1, true
}

func (s *qualificationStageTrace) fail(code int) {
	if s.failures[code].Add(1) == 0 {
		s.failures[0].Store(1)
	}
}
