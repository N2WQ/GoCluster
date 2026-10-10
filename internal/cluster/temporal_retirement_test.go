package cluster

import (
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

// Timing values come from elapsed-time arithmetic, not decoder execution.
// Classification controls retain the existing scored selection and thresholds;
// they establish force behavior, not callsign truth or calibration.
func TestRuntimeTemporalRetirementUsesActualAgeBeyondFutureDue(t *testing.T) {
	for _, tc := range []struct {
		name   string
		margin int
		action correctionflow.TemporalDecisionAction
	}{
		{"passing gates", 0, correctionflow.TemporalDecisionActionApply},
		{"failed margin", 5000, correctionflow.TemporalDecisionActionFallbackResolver},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := runtimeTemporalTestConfig(tc.margin)
			cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 90000, 90000
			controller := newRuntimeTemporalController(cfg)
			start := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
			key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
			accepted, _, reason := controller.Observe(start, key, "K1ABC", runtimeTemporalPending{
				spot:      spot.NewSpot("K1ABC", "W1XYZ", 7010, "CW"),
				selection: runtimeTemporalTestSelection(key, "K1ABC", 3200, "K1ABD", 1800),
			})
			if !accepted {
				t.Fatalf("accepted positive long lag control was refused: %s", reason)
			}
			actual := start.Add(500 * time.Millisecond)
			if releases := controller.Drain(actual, false); len(releases) != 0 {
				t.Fatal("ordinary processing bypassed future lag")
			}
			due, ok := controller.NextDue()
			if !ok || !due.Equal(start.Add(25*time.Hour)) || !due.After(actual.Add(24*time.Hour)) {
				t.Fatal("fixture does not exceed the previous synthetic shutdown horizon")
			}
			releases := controller.Drain(actual, true)
			if len(releases) != 1 || releases[0].decision.CommitLatency != 500*time.Millisecond || releases[0].decision.Action != tc.action {
				t.Fatalf("forced actual-time retirement: %+v", releases)
			}
			if len(controller.pending) != 0 || len(controller.queue) != 0 {
				t.Fatal("forced retirement stranded future-due request ownership")
			}
			if _, ok := controller.NextDue(); ok || len(controller.Drain(actual, true)) != 0 {
				t.Fatal("retired request can be released a second time")
			}
		})
	}
}

func TestRuntimeTemporalRetirementPreservesFutureDueSequence(t *testing.T) {
	cfg := runtimeTemporalTestConfig(0)
	cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 90000, 90000
	controller := newRuntimeTemporalController(cfg)
	start := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	for _, call := range []string{"K1ABC", "K1ABD"} {
		key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
		accepted, _, reason := controller.Observe(start, key, call, runtimeTemporalPending{
			spot:      spot.NewSpot(call, "W1XYZ", 7010, "CW"),
			selection: runtimeTemporalTestSelection(key, call, 3200, "K1ZZZ", 1800),
		})
		if !accepted {
			t.Fatal(reason)
		}
	}
	releases := controller.Drain(start.Add(500*time.Millisecond), true)
	if len(releases) != 2 || releases[0].pending.id != 1 || releases[1].pending.id != 2 || len(controller.pending) != 0 || len(controller.queue) != 0 {
		t.Fatalf("forced future-due retirement changed deterministic ownership/order: %+v", releases)
	}
}

func TestOutputPipelineRetirementReleasesFutureTemporalHold(t *testing.T) {
	cfg := runtimeTemporalTestConfig(5000)
	cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 90000, 90000
	ring := buffer.NewRingBuffer(4)
	p := newTestFTOutputPipeline(cfg, ring)
	output := make(chan *spot.Spot)
	p.outputChan = output
	start := time.Now().UTC().Add(-500 * time.Millisecond)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	accepted, _, reason := p.temporal.Observe(start, key, "K1ABC", runtimeTemporalPending{
		spot:      spot.NewSpot("K1ABC", "W1XYZ", 7010, "CW"),
		selection: runtimeTemporalTestSelection(key, "K1ABC", 2500, "K1ABD", 2500),
	})
	if !accepted {
		t.Fatal(reason)
	}
	close(output)
	p.run()
	if recent := ring.GetRecent(1); len(recent) != 1 || recent[0].DXCallNorm != "K1ABC" {
		t.Fatalf("pipeline did not process its retired temporal hold: %+v", recent)
	}
	if len(p.temporal.pending) != 0 || len(p.temporal.queue) != 0 || p.temporalTimer != nil || p.temporalTimerCh != nil || p.ftTimer != nil || p.ftTimerCh != nil {
		t.Fatal("completed pipeline retained hold/timer ownership")
	}
}
