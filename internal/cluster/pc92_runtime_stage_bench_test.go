//go:build qualification

package cluster

import (
	"testing"
	"time"

	"dxcluster/internal/qualificationstage"
)

// Each measured operation is one real callback. Clearing its fixed slot avoids
// timing only the duplicate-error path. The clock control is shared with the
// installed case; Windows runs use the actual qualification QPC mechanism.
func BenchmarkPC92StageTraceObserver(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "absent"
		if enabled {
			name = "installed"
		}
		b.Run(name, func(b *testing.B) {
			s, _, _ := stageFixture(b, 1)
			now, frequency, err := qualificationCounterClock()
			if err != nil {
				b.Skip("actual qualification clock unavailable", err)
			}
			s.oracle.clockNow, s.oracle.clockFrequency = now, frequency
			s.oracle.measurementEpoch = now()
			s.armed = s.oracle.measurementEpoch.UnixNano()
			if !enabled {
				qualificationstage.Remove(s.registration)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				s.rows[0].Store(0)
				qualificationstage.Observe(qualificationstage.PrimaryReady, "QID0000000", -1)
			}
			b.StopTimer()
			if enabled && s.rows[0].Load() == 0 {
				b.Fatal("installed observer not exercised")
			}
		})
	}
}

func TestPC92StageTraceInputMinuteAndFinalIdentity(t *testing.T) {
	s, f, c := stageFixture(t, 1)
	s.oracle.inputs[0].started.Store(int64(61*time.Second) + 1)
	base := int64(61 * time.Second)
	for stage, tick := range []int64{110, 125, 140, 155, 170} {
		c.Store(base + tick)
		s.observe(qualificationstage.Event{Stage: qualificationstage.Stage(stage), Comment: "QID0000000", Worker: -1})
	}
	for worker := range 2 {
		s.observe(qualificationstage.Event{Stage: qualificationstage.WorkerDispatched, Comment: "QID0000000", Worker: worker})
		c.Store(base + 190)
		s.observe(qualificationstage.Event{Stage: qualificationstage.WorkerStarted, Comment: "QID0000000", Worker: worker})
	}
	c.Store(base + 220)
	o := s.oracle
	r := s.finish(1, &f)
	if !r.Complete || r.Segments[0][2][0].Count != 1 || r.Segments[0][1][0].Count != 0 {
		t.Fatal("arrival minute replaced input minute", r)
	}
	if !validQualificationStageReport(r, o, 1, c.Load()) {
		t.Fatal("valid identity rejected")
	}
	copyReport := *r
	copyReport.Frequency++
	if validQualificationStageReport(&copyReport, o, 1, c.Load()) {
		t.Fatal("different clock accepted")
	}
	copyReport = *r
	copyReport.RequiredSpots--
	if validQualificationStageReport(&copyReport, o, 1, c.Load()) {
		t.Fatal("different input count accepted")
	}
}
