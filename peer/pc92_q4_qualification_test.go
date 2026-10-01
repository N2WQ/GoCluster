//go:build qualification

package peer

import (
	"testing"
	"time"
)

func TestQ4StagingPlanIndependentBoundaries(t *testing.T) {
	for _, plan := range QualificationStagingPlans() {
		records, bytes := 0, 0
		for i, n := range plan.Records {
			if n == 0 {
				continue
			}
			wire, err := QualificationStagingWire(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), plan.WireBytes)
			if err != nil || len(wire) != plan.WireBytes {
				t.Fatalf("%s invalid wire %v", plan.Name, err)
			}
			owned := pointerAllocationBytes(256*16) + n*allocationBytes(len(wire))
			if i == 0 && plan.TailBytes > 0 {
				n++
				owned += allocationBytes(plan.TailBytes)
			}
			if n > 256 || owned > 512<<10 {
				t.Fatalf("%s violates individual allocation boundary", plan.Name)
			}
			records += n
			bytes += owned
		}
		if records != plan.ExpectedRecords || bytes != plan.ExpectedBytes {
			t.Fatalf("%s count%d bytes%d differ from independent%d/%d", plan.Name, records, bytes, plan.ExpectedRecords, plan.ExpectedBytes)
		}
	}
}
