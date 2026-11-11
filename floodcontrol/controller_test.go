package floodcontrol

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/dedup"
	"dxcluster/spot"
)

func TestControllerObserveStillForwards(t *testing.T) {
	downstream := make(chan *spot.Spot, 8)
	controller := New(testFloodConfig(config.FloodActionObserve, 4), downstream, nil, nil)
	controller.Start()
	defer controller.Stop()

	controller.Input() <- testSpot("W1AAA", spot.SourceManual)
	controller.Input() <- testSpot("W1AAA", spot.SourceManual)

	mustReceiveSpot(t, downstream)
	mustReceiveSpot(t, downstream)

	_, observed, suppressed, dropped, overflow, _ := controller.GetStats()
	if observed != 1 {
		t.Fatalf("expected observed=1, got %d", observed)
	}
	if suppressed != 0 || dropped != 0 || overflow != 0 {
		t.Fatalf("unexpected counters suppress=%d drop=%d overflow=%d", suppressed, dropped, overflow)
	}
}

func TestControllerPartitionsBySourceType(t *testing.T) {
	downstream := make(chan *spot.Spot, 8)
	controller := New(testFloodConfig(config.FloodActionSuppress, 4), downstream, nil, nil)
	controller.Start()
	defer controller.Stop()

	controller.Input() <- testSpot("W1AAA", spot.SourceManual)
	controller.Input() <- testSpot("W1AAA", spot.SourceRBN)
	controller.Input() <- testSpot("W1AAA", spot.SourceManual)

	mustReceiveSpot(t, downstream)
	mustReceiveSpot(t, downstream)
	mustNotReceiveSpot(t, downstream)

	_, _, suppressed, _, _, _ := controller.GetStats()
	if suppressed != 1 {
		t.Fatalf("expected suppressed=1, got %d", suppressed)
	}
}

func TestControllerOverflowFailsOpenForUnseenActors(t *testing.T) {
	downstream := make(chan *spot.Spot, 8)
	controller := New(testFloodConfig(config.FloodActionSuppress, 1), downstream, nil, nil)
	controller.Start()
	defer controller.Stop()

	controller.Input() <- testSpot("W1AAA", spot.SourceManual)
	controller.Input() <- testSpot("W1BBB", spot.SourceManual)
	controller.Input() <- testSpot("W1BBB", spot.SourceManual)

	mustReceiveSpot(t, downstream)
	mustReceiveSpot(t, downstream)
	mustReceiveSpot(t, downstream)

	_, _, suppressed, _, overflow, _ := controller.GetStats()
	if suppressed != 0 {
		t.Fatalf("expected suppressed=0 with fail-open overflow, got %d", suppressed)
	}
	if overflow == 0 {
		t.Fatalf("expected overflow > 0")
	}
}

func TestControllerSuppressesBeforePrimaryDedupe(t *testing.T) {
	d := dedup.NewDeduplicator(time.Minute, false, 8)
	d.Start()
	defer d.Stop()

	controller := New(testFloodConfig(config.FloodActionSuppress, 4), d.GetInputChannel(), nil, nil)
	controller.Start()
	defer controller.Stop()

	controller.Input() <- testSpot("W1AAA", spot.SourceManual)
	mustReceiveSpot(t, d.GetOutputChannel())

	controller.Input() <- testSpot("W1AAA", spot.SourceManual)
	mustNotReceiveSpot(t, d.GetOutputChannel())

	processed, duplicates, _ := d.GetStats()
	if processed != 1 {
		t.Fatalf("expected primary dedupe to process only the forwarded spot, got %d", processed)
	}
	if duplicates != 0 {
		t.Fatalf("expected primary dedupe duplicates=0, got %d", duplicates)
	}
}

func TestControllerCleanupPreservesLiveThreshold(t *testing.T) {
	for _, action := range []string{config.FloodActionObserve, config.FloodActionSuppress, config.FloodActionDrop} {
		t.Run(action, func(t *testing.T) {
			cfg := testFloodConfig(action, 4)
			cfg.Rails.DECall.ThresholdsPerSourceType["MANUAL"] = 3
			downstream := make(chan *spot.Spot, 8)
			controller := New(cfg, downstream, nil, nil)
			base := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
			for i := 0; i < 3; i++ {
				controller.process(base.Add(time.Duration(i)*time.Second), testSpot("W1AAA", spot.SourceManual))
			}
			controller.cleanup(base.Add(3 * time.Second))
			controller.cleanup(base.Add(3 * time.Second))
			controller.process(base.Add(4*time.Second), testSpot("W1AAA", spot.SourceManual))
			controller.cleanup(base.Add(5 * time.Second))
			controller.process(base.Add(6*time.Second), testSpot("W1AAA", spot.SourceManual))
			processed, observed, suppressed, dropped, overflow, actors := controller.GetStats()
			wantObserved, wantSuppressed, wantDropped, wantForwarded := uint64(0), uint64(0), uint64(0), 3
			switch action {
			case config.FloodActionObserve:
				wantObserved, wantForwarded = 2, 5
			case config.FloodActionSuppress:
				wantSuppressed = 2
			case config.FloodActionDrop:
				wantDropped = 2
			}
			if processed != 5 || observed != wantObserved || suppressed != wantSuppressed || dropped != wantDropped || overflow != 0 || actors != 1 || len(downstream) != wantForwarded {
				t.Fatalf("after cleanup: processed=%d observed=%d suppressed=%d dropped=%d overflow=%d actors=%d forwarded=%d; want decisions=2 and forwarded=%d", processed, observed, suppressed, dropped, overflow, actors, len(downstream), wantForwarded)
			}
			if got := len(controller.rails[0].partitions["MANUAL"]["W1AAA"].times); got != 4 {
				t.Fatalf("live timestamps=%d; want threshold+1=4", got)
			}
		})
	}
}

func TestControllerCleanupSourceThresholdAndExpiry(t *testing.T) {
	cfg := testFloodConfig(config.FloodActionObserve, 4)
	cfg.Rails.DECall.ThresholdsPerSourceType["MANUAL"] = 3
	cfg.Rails.DECall.ThresholdsPerSourceType["RBN"] = 5
	controller := New(cfg, make(chan *spot.Spot, 16), nil, nil)
	base := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
	for _, entry := range []struct {
		source spot.SourceType
		count  int
	}{{spot.SourceManual, 3}, {spot.SourceRBN, 5}} {
		for i := 0; i < entry.count; i++ {
			controller.process(base, testSpot("W1AAA", entry.source))
		}
	}
	// The cutoff is inclusive, independently for each source partition.
	cutoff := base.Add(time.Minute)
	controller.cleanup(cutoff)
	for partition, want := range map[string]int{"MANUAL": 3, "RBN": 5} {
		if got := len(controller.rails[0].partitions[partition]["W1AAA"].times); got != want {
			t.Fatalf("%s retained=%d at inclusive cutoff; want %d", partition, got, want)
		}
	}
	controller.process(cutoff, testSpot("W1AAA", spot.SourceManual))
	controller.process(cutoff, testSpot("W1AAA", spot.SourceRBN))
	_, observed, _, _, _, _ := controller.GetStats()
	if observed != 2 {
		t.Fatalf("inclusive-cutoff observations=%d; want 2", observed)
	}
	controller.cleanup(cutoff.Add(time.Nanosecond))
	for _, partition := range []string{"MANUAL", "RBN"} {
		if got := len(controller.rails[0].partitions[partition]["W1AAA"].times); got != 1 {
			t.Fatalf("%s retained=%d after expiry; want only the newest event", partition, got)
		}
	}
	controller.cleanup(cutoff.Add(time.Minute + time.Nanosecond))
	for _, rail := range controller.rails {
		if len(rail.partitions) != 0 {
			t.Fatalf("expired rail %s still owns partitions: %+v", rail.name, rail.partitions)
		}
	}
}

func TestControllerCleanupUsesSelectedDXMode(t *testing.T) {
	for _, entry := range []struct {
		mode      string
		threshold int
	}{{"conservative", 2}, {"moderate", 3}, {"aggressive", 4}} {
		t.Run(entry.mode, func(t *testing.T) {
			cfg := testFloodConfig(config.FloodActionSuppress, 4)
			cfg.Rails.DECall.Enabled = false
			cfg.Rails.DXCall.Enabled = true
			cfg.Rails.DXCall.ActiveMode = entry.mode
			cfg.Rails.DXCall.ThresholdsByMode.Conservative["MANUAL"] = 2
			cfg.Rails.DXCall.ThresholdsByMode.Moderate["MANUAL"] = 3
			cfg.Rails.DXCall.ThresholdsByMode.Aggressive["MANUAL"] = 4
			controller := New(cfg, make(chan *spot.Spot, 8), nil, nil)
			base := time.Date(2027, 1, 2, 3, 0, 0, 0, time.UTC)
			for i := 0; i < entry.threshold; i++ {
				controller.process(base, testSpot("W1AAA", spot.SourceManual))
			}
			controller.cleanup(base.Add(time.Second))
			controller.process(base.Add(time.Second), testSpot("W1AAA", spot.SourceManual))
			_, _, suppressed, _, _, _ := controller.GetStats()
			if suppressed != 1 {
				t.Fatalf("%s suppressed=%d after cleanup; want 1", entry.mode, suppressed)
			}
		})
	}
}

func testFloodConfig(action string, maxEntries int) config.FloodControlConfig {
	return config.FloodControlConfig{
		Enabled:            true,
		LogIntervalSeconds: 30,
		PartitionMode:      config.FloodControlPartitionExactSourceType,
		Rails: config.FloodControlRailsConfig{
			DECall: config.FloodRailConfig{
				Enabled:                true,
				Action:                 action,
				WindowSeconds:          60,
				MaxEntriesPerPartition: maxEntries,
				ThresholdsPerSourceType: map[string]int{
					"MANUAL":      1,
					"UPSTREAM":    0,
					"PEER":        0,
					"RBN":         1,
					"FT8":         0,
					"FT4":         0,
					"PSKREPORTER": 0,
				},
			},
			SourceNode: disabledRail(),
			SpotterIP:  disabledRail(),
			DXCall: config.FloodRailConfig{
				Enabled:                false,
				Action:                 action,
				WindowSeconds:          60,
				MaxEntriesPerPartition: maxEntries,
				ActiveMode:             "conservative",
				ThresholdsByMode: config.FloodRailThresholdsByMode{
					Conservative: disabledThresholds(),
					Moderate:     disabledThresholds(),
					Aggressive:   disabledThresholds(),
				},
			},
		},
	}
}

func disabledRail() config.FloodRailConfig {
	return config.FloodRailConfig{
		Enabled:                 false,
		Action:                  config.FloodActionObserve,
		WindowSeconds:           60,
		MaxEntriesPerPartition:  4,
		ThresholdsPerSourceType: disabledThresholds(),
	}
}

func disabledThresholds() map[string]int {
	return map[string]int{
		"MANUAL":      0,
		"UPSTREAM":    0,
		"PEER":        0,
		"RBN":         0,
		"FT8":         0,
		"FT4":         0,
		"PSKREPORTER": 0,
	}
}

func testSpot(deCall string, sourceType spot.SourceType) *spot.Spot {
	now := time.Now().UTC()
	return &spot.Spot{
		DXCall:     "K1ABC",
		DXCallNorm: "K1ABC",
		DECall:     deCall,
		DECallNorm: deCall,
		SourceType: sourceType,
		Time:       now,
		Frequency:  7010.0,
	}
}

func mustReceiveSpot(t *testing.T, ch <-chan *spot.Spot) *spot.Spot {
	t.Helper()
	select {
	case s := <-ch:
		if s == nil {
			t.Fatalf("received nil spot")
		}
		return s
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for spot")
		return nil
	}
}

func mustNotReceiveSpot(t *testing.T, ch <-chan *spot.Spot) {
	t.Helper()
	select {
	case s := <-ch:
		t.Fatalf("unexpected spot received: %+v", s)
	case <-time.After(150 * time.Millisecond):
	}
}
