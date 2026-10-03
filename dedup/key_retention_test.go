package dedup

import (
	"fmt"
	"testing"
	"time"
)

func TestCompleteKeyCleanupChurn(t *testing.T) {
	primary := NewDeduplicator(time.Minute, false, 1)
	secondary := NewSecondaryDeduper(time.Minute, false)
	base := time.Unix(60, 0)
	const perCycle = 4096
	for cycle := range 3 {
		for i := range perCycle {
			s := policySpot(base, false, -10)
			s.DXCall = fmt.Sprintf("K%06d", cycle*perCycle+i)
			s.DXCallNorm = s.DXCall
			primary.processSpot(s)
			requirePrimaryOutput(t, primary, s)
			if !secondary.ShouldForward(s) {
				t.Fatal("new complete key suppressed during churn")
			}
		}
		if _, _, size := primary.GetStats(); size != perCycle {
			t.Fatalf("primary population=%d", size)
		}
		if _, _, size := secondary.GetStats(); size != perCycle {
			t.Fatalf("secondary population=%d", size)
		}
		primary.cleanupAt(base.Add(time.Minute+1), nil)
		secondary.cleanupAt(base.Add(time.Minute + 1))
		if processed, duplicates, size := primary.GetStats(); processed != uint64((cycle+1)*perCycle) || duplicates != 0 || size != 0 {
			t.Fatalf("primary cleanup stats=%d/%d/%d", processed, duplicates, size)
		}
		if processed, duplicates, size := secondary.GetStats(); processed != uint64((cycle+1)*perCycle) || duplicates != 0 || size != 0 {
			t.Fatalf("secondary cleanup stats=%d/%d/%d", processed, duplicates, size)
		}
	}
}
