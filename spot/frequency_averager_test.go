package spot

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestFrequencyAveragerFirstIdleTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fa := NewFrequencyAverager()
		const tick, window = time.Second, time.Minute
		cutoff := time.Now().Add(tick - window)
		fa.Average("OLD", 14001, cutoff.Add(-time.Nanosecond), window, 1)
		fa.Average("EDGE", 14002, cutoff, window, 1)
		fa.StartCleanup(tick, window)
		defer fa.StopCleanup()
		synctest.Wait()
		time.Sleep(tick)
		synctest.Wait()
		if _, ok := fa.lastSeen["OLD"]; ok || len(fa.entries["OLD"]) != 0 {
			t.Fatal("expired call survived the first idle tick")
		}
		if _, ok := fa.lastSeen["EDGE"]; !ok {
			t.Fatal("call at the inclusive cutoff was removed")
		}
		if avg, agree, total := fa.Average("EDGE", 14003, time.Now(), window, 1); avg != 14002.5 || agree != 2 || total != 2 {
			t.Fatalf("boundary sample lost: average %v, corroborating %d, total %d", avg, agree, total)
		}
	})
}

func TestFrequencyAveragerTrafficSweepCadence(t *testing.T) {
	fa := NewFrequencyAverager()
	now := time.Now()
	fa.Average("OLD", 14001, now.Add(-2*time.Minute), time.Minute, 1)
	for i := 1; i < 127; i++ {
		fa.Average("ACTIVE", 14002, now, time.Minute, 1)
	}
	if _, ok := fa.lastSeen["OLD"]; !ok {
		t.Fatal("traffic swept before update 128")
	}
	fa.Average("ACTIVE", 14002, now, time.Minute, 1)
	if _, ok := fa.lastSeen["OLD"]; ok {
		t.Fatal("traffic failed to sweep on update 128")
	}
}
