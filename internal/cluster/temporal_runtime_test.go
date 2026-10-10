package cluster

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestRuntimeTemporalControllerDrainRequeuesDeferredDecisionToMaxWait(t *testing.T) {
	controller := newRuntimeTemporalController(runtimeTemporalTestConfig(5000))
	start := time.Now().UTC()
	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	accepted, id, reason := controller.Observe(
		start,
		key,
		"K1ABC",
		runtimeTemporalPending{
			spot:      spot.NewSpot("K1ABC", "W1XYZ", 7010.0, "CW"),
			selection: runtimeTemporalTestSelection(key, "K1ABC", 2500, "K1ABD", 2500),
		},
	)
	if !accepted {
		t.Fatalf("Observe() rejected id=%d reason=%s", id, reason)
	}

	releases := controller.Drain(start.Add(2*time.Second), false)
	if len(releases) != 0 {
		t.Fatalf("expected lag-time drain to defer and requeue, got %d releases", len(releases))
	}

	nextDue, ok := controller.NextDue()
	if !ok {
		t.Fatalf("expected requeued temporal item after defer")
	}
	wantDue := start.Add(3 * time.Second)
	if !nextDue.Equal(wantDue) {
		t.Fatalf("NextDue() = %s, want %s", nextDue, wantDue)
	}

	releases = controller.Drain(start.Add(3*time.Second), false)
	if len(releases) != 1 {
		t.Fatalf("expected max-wait drain to release one item, got %d", len(releases))
	}
	if releases[0].decision.Action != correctionflow.TemporalDecisionActionFallbackResolver {
		t.Fatalf("expected fallback_resolver at max wait, got %s", releases[0].decision.Action)
	}
	if _, ok := controller.NextDue(); ok {
		t.Fatalf("expected temporal queue to be empty after release")
	}
}

func TestRuntimeTemporalControllerDrainPreservesDueSequenceForEqualLag(t *testing.T) {
	controller := newRuntimeTemporalController(runtimeTemporalTestConfig(0))
	start := time.Now().UTC()
	key1 := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	key2 := spot.NewResolverSignalKey(7020.0, "40m", "CW", 500)

	observations := []struct {
		key     spot.ResolverSignalKey
		subject string
		call    string
		freq    float64
	}{
		{key: key1, subject: "K1ABC", call: "K1ABC", freq: 7010.0},
		{key: key2, subject: "K1ABD", call: "K1ABD", freq: 7020.0},
	}
	for idx, obs := range observations {
		accepted, id, reason := controller.Observe(
			start,
			obs.key,
			obs.subject,
			runtimeTemporalPending{
				spot:      spot.NewSpot(obs.call, "W1XYZ", obs.freq, "CW"),
				selection: runtimeTemporalTestSelection(obs.key, obs.subject, 3200, "K1ZZZ", 1800),
			},
		)
		if !accepted {
			t.Fatalf("Observe() rejected item %d id=%d reason=%s", idx, id, reason)
		}
	}

	releases := controller.Drain(start.Add(2*time.Second), false)
	if len(releases) != 2 {
		t.Fatalf("expected two releases at equal lag, got %d", len(releases))
	}
	if releases[0].pending.id != 1 || releases[1].pending.id != 2 {
		t.Fatalf("expected deterministic release order [1 2], got [%d %d]", releases[0].pending.id, releases[1].pending.id)
	}
}

func runtimeTemporalTestConfig(minMargin int) config.CallCorrectionConfig {
	return config.CallCorrectionConfig{
		DistanceModelCW:   "morse",
		DistanceModelRTTY: "baudot",
		Enabled:           true,
		TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
			Enabled:             true,
			Scope:               "all_correction_candidates",
			LagSeconds:          2,
			MaxWaitSeconds:      3,
			BeamSize:            4,
			MaxObsCandidates:    4,
			StayBonus:           120,
			SwitchPenalty:       160,
			FamilySwitchPenalty: 60,
			Edit1SwitchPenalty:  90,
			MinScore:            0,
			MinMarginScore:      minMargin,
			OverflowAction:      "fallback_resolver",
			MaxPending:          100,
			MaxActiveKeys:       16,
			MaxEventsPerKey:     32,
		},
	}
}

// All fixtures build a probable two-candidate selection. Its weighted total
// is the sum of those candidates, rather than a separately tunable input.
func runtimeTemporalTestSelection(
	key spot.ResolverSignalKey,
	winner string,
	winnerWeighted int,
	runner string,
	runnerWeighted int,
) correctionflow.ResolverPrimarySelection {
	winnerSupport := winnerWeighted / 1000
	if winnerSupport <= 0 {
		winnerSupport = 1
	}
	runnerSupport := runnerWeighted / 1000
	if runnerSupport <= 0 {
		runnerSupport = 1
	}
	totalSupport := winnerSupport + runnerSupport
	if totalSupport <= 0 {
		totalSupport = 2
	}
	return correctionflow.ResolverPrimarySelection{
		SnapshotOK: true,
		Snapshot: spot.ResolverSnapshot{
			Key:                        key,
			State:                      spot.ResolverStateProbable,
			Winner:                     winner,
			RunnerUp:                   runner,
			WinnerSupport:              winnerSupport,
			RunnerSupport:              runnerSupport,
			Margin:                     winnerSupport - runnerSupport,
			TotalReporters:             totalSupport,
			WinnerWeightedSupportMilli: winnerWeighted,
			RunnerWeightedSupportMilli: runnerWeighted,
			TotalWeightedSupportMilli:  winnerWeighted + runnerWeighted,
			CandidateRanks: []spot.ResolverCandidateSupport{
				{
					Call:                 winner,
					Support:              winnerSupport,
					WeightedSupportMilli: winnerWeighted,
				},
				{
					Call:                 runner,
					Support:              runnerSupport,
					WeightedSupportMilli: runnerWeighted,
				},
			},
		},
	}
}
