//go:build qualification && windows

package cluster

import (
	"testing"

	"dxcluster/internal/qualificationstage"
)

func TestPC92StageTraceRealClockAllocation(t *testing.T) {
	s, _, _ := stageFixture(t)
	now, frequency, err := qualificationCounterClock()
	if err != nil {
		t.Fatal(err)
	}
	s.oracle.clockNow, s.oracle.clockFrequency = now, frequency
	s.oracle.measurementEpoch = now()
	s.armed = s.oracle.measurementEpoch.UnixNano()
	allocations := testing.AllocsPerRun(1000, func() {
		s.rows[0].Store(0)
		qualificationstage.Observe(qualificationstage.PrimaryReady, "QID0000000", -1)
	})
	if allocations > 2 {
		t.Fatalf("real QPC observation allocations exceeded measured bound: %v", allocations)
	}
	t.Logf("actual inherited QPC marker allocations=%v; synthetic-clock checker isolates observer-core allocation", allocations)
}
