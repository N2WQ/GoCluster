package floodcontrol

import (
	"sync"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestFloodDirectAdmissionMixedRailsAndUnlockedSink(t *testing.T) {
	cfg := testFloodConfig(config.FloodActionObserve, 8)
	cfg.Rails.SourceNode = cfg.Rails.DECall
	cfg.Rails.SourceNode.Action = config.FloodActionDrop
	c := New(cfg, make(chan *spot.Spot, 2), nil, nil)
	calls := 0
	c.SetAdmissionSink(func(*spot.Spot) bool {
		if !c.mu.TryLock() {
			t.Fatal("policy lock held during primary admission")
		}
		c.mu.Unlock()
		calls++
		return true
	})
	first := testSpot("W1AAA", spot.SourcePeer)
	first.SourceNode = "K1NODE"
	second := testSpot("W1AAA", spot.SourcePeer)
	second.SourceNode = "K1NODE"
	if !c.Admit(time.Unix(1, 0), first) || c.Admit(time.Unix(1, 0), second) {
		t.Fatal("direct aggregate verdict wrong")
	}
	processed, observed, suppressed, dropped, _, _ := c.GetStats()
	if calls != 1 || processed != 2 || observed != 0 || suppressed != 0 || dropped != 1 {
		t.Fatal("mixed action admitted or charged twice")
	}
}

func TestFloodDirectAdmissionConcurrentState(t *testing.T) {
	c := New(testFloodConfig(config.FloodActionSuppress, 8), make(chan *spot.Spot, 32), nil, nil)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() { c.Admit(time.Unix(1, 0), testSpot("W1AAA", spot.SourcePeer)) })
	}
	wg.Wait()
	processed, _, suppressed, _, _, actors := c.GetStats()
	if processed != 32 || suppressed != 31 || actors != 1 {
		t.Fatalf("concurrent one-charge state %d/%d/%d", processed, suppressed, actors)
	}
}
