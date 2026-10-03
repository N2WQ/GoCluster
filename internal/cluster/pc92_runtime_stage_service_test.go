//go:build qualification

package cluster

import (
	"fmt"
	"os"
)

func (s *qualificationChild) armStages() error {
	switch os.Getenv("GOCLUSTER_PC92_RUNTIME_STAGES") {
	case "", "0":
		return nil
	case "1":
	default:
		return fmt.Errorf("invalid optional stage diagnostic selection")
	}
	if s.profile.name != "warm-diagnostic" || s.profile.shipped || len(s.oracle.clients) != 100 {
		return fmt.Errorf("optional stage observation requires unchanged warm diagnostic")
	}
	fanout, err := s.runtime.telnetServer.QualificationStageFanout()
	if err != nil {
		return err
	}
	trace, err := newQualificationStageTrace(s.oracle, &fanout)
	if err != nil {
		return err
	}
	s.oracle.stages.Store(trace)
	return nil
}

func (s *qualificationChild) finishStages(used int) *qualificationStageReport {
	trace := s.oracle.stages.Swap(nil)
	if trace == nil {
		return nil
	}
	fanout, err := s.runtime.telnetServer.QualificationStageFanout()
	if err != nil {
		trace.fail(8)
	}
	return trace.finish(used, &fanout)
}

func (s *qualificationChild) retireStages() {
	if trace := s.oracle.stages.Swap(nil); trace != nil {
		trace.retire()
	}
}

func validQualificationStageReport(r *qualificationStageReport, o *qualificationOracle, spots int, now int64) bool {
	if r == nil || !r.Enabled || !r.Complete || r.Schema != 1 || r.Inputs != len(o.inputs) || r.Clients != len(o.clients) || r.RequiredSpots != uint64(spots) || r.Failures != 0 || r.Missing != 0 {
		return false
	}
	if r.Epoch != o.measurementEpoch.UnixNano() || r.Frequency != o.clockFrequency || r.Armed < r.Epoch || r.Finished < r.Armed || now <= 0 || (r.Finished > now && r.Finished-now > 1) {
		return false
	}
	backing, err := qualificationStageBacking(r.Inputs, r.Workers)
	return err == nil && r.BackingBytes == backing && r.Mask != 0 && (r.Workers == 32 || r.Mask>>uint(r.Workers) == 0)
}
