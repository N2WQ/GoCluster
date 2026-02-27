package correctionflow

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestTemporalDecoderAppliesLateEvidence(t *testing.T) {
	decoder := NewTemporalDecoder(config.CallCorrectionConfig{
		DistanceModelCW:   "morse",
		DistanceModelRTTY: "baudot",
		TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
			Enabled:             true,
			Scope:               "all_correction_candidates",
			LagSeconds:          2,
			MaxWaitSeconds:      6,
			BeamSize:            8,
			MaxObsCandidates:    8,
			StayBonus:           120,
			SwitchPenalty:       160,
			FamilySwitchPenalty: 60,
			Edit1SwitchPenalty:  90,
			MinScore:            0,
			MinMarginScore:      0,
			OverflowAction:      "fallback_resolver",
			MaxPending:          100,
			MaxActiveKeys:       16,
			MaxEventsPerKey:     32,
		},
	})
	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	start := time.Now().UTC()

	observations := []TemporalObservation{
		{
			ID:          1,
			ObservedAt:  start,
			Key:         key,
			SubjectCall: "K1ABD",
			Selection:   testTemporalSelection(key, spot.ResolverStateProbable, "K1ABD", 2550, "K1ABC", 2450, 5000),
		},
		{
			ID:          2,
			ObservedAt:  start.Add(1 * time.Second),
			Key:         key,
			SubjectCall: "K1ABD",
			Selection:   testTemporalSelection(key, spot.ResolverStateConfident, "K1ABC", 4500, "K1ABD", 500, 5000),
		},
		{
			ID:          3,
			ObservedAt:  start.Add(2 * time.Second),
			Key:         key,
			SubjectCall: "K1ABD",
			Selection:   testTemporalSelection(key, spot.ResolverStateConfident, "K1ABC", 4600, "K1ABD", 400, 5000),
		},
	}
	for _, obs := range observations {
		if ok, reason := decoder.Observe(obs); !ok {
			t.Fatalf("Observe() rejected id=%d reason=%s", obs.ID, reason)
		}
	}

	decision := decoder.Evaluate(1, start.Add(2*time.Second), false)
	if decision.Action != TemporalDecisionActionApply {
		t.Fatalf("expected apply decision, got action=%s reason=%s", decision.Action, decision.Reason)
	}
	if decision.Winner != "K1ABC" {
		t.Fatalf("expected late evidence to commit K1ABC, got %q", decision.Winner)
	}
}

func TestTemporalDecoderDefersBeforeLag(t *testing.T) {
	decoder := NewTemporalDecoder(config.CallCorrectionConfig{
		TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
			Enabled:             true,
			Scope:               "all_correction_candidates",
			LagSeconds:          2,
			MaxWaitSeconds:      4,
			BeamSize:            4,
			MaxObsCandidates:    4,
			StayBonus:           120,
			SwitchPenalty:       160,
			FamilySwitchPenalty: 60,
			Edit1SwitchPenalty:  90,
			MinScore:            0,
			MinMarginScore:      0,
			OverflowAction:      "fallback_resolver",
			MaxPending:          100,
			MaxActiveKeys:       16,
			MaxEventsPerKey:     32,
		},
	})
	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	start := time.Now().UTC()
	if ok, reason := decoder.Observe(TemporalObservation{
		ID:          1,
		ObservedAt:  start,
		Key:         key,
		SubjectCall: "K1ABD",
		Selection:   testTemporalSelection(key, spot.ResolverStateProbable, "K1ABC", 3200, "K1ABD", 1800, 5000),
	}); !ok {
		t.Fatalf("Observe() rejected reason=%s", reason)
	}

	decision := decoder.Evaluate(1, start.Add(500*time.Millisecond), false)
	if decision.Action != TemporalDecisionActionDefer {
		t.Fatalf("expected defer before lag, got action=%s reason=%s", decision.Action, decision.Reason)
	}
}

func TestTemporalDecoderFallsBackAtMaxWaitWhenMarginLow(t *testing.T) {
	decoder := NewTemporalDecoder(config.CallCorrectionConfig{
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
			MinMarginScore:      5000,
			OverflowAction:      "fallback_resolver",
			MaxPending:          100,
			MaxActiveKeys:       16,
			MaxEventsPerKey:     32,
		},
	})
	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	start := time.Now().UTC()
	if ok, reason := decoder.Observe(TemporalObservation{
		ID:          1,
		ObservedAt:  start,
		Key:         key,
		SubjectCall: "K1ABC",
		Selection:   testTemporalSelection(key, spot.ResolverStateProbable, "K1ABC", 2500, "K1ABD", 2500, 5000),
	}); !ok {
		t.Fatalf("Observe() rejected reason=%s", reason)
	}
	if ok, reason := decoder.Observe(TemporalObservation{
		ID:          2,
		ObservedAt:  start.Add(1 * time.Second),
		Key:         key,
		SubjectCall: "K1ABC",
		Selection:   testTemporalSelection(key, spot.ResolverStateProbable, "K1ABC", 2500, "K1ABD", 2500, 5000),
	}); !ok {
		t.Fatalf("Observe() rejected id=2 reason=%s", reason)
	}

	beforeTimeout := decoder.Evaluate(1, start.Add(2*time.Second), false)
	if beforeTimeout.Action != TemporalDecisionActionDefer {
		t.Fatalf("expected defer at lag when margin low and max_wait not reached, got %s", beforeTimeout.Action)
	}

	atTimeout := decoder.Evaluate(1, start.Add(3*time.Second), false)
	if atTimeout.Action != TemporalDecisionActionFallbackResolver {
		t.Fatalf("expected fallback_resolver at max wait, got action=%s reason=%s", atTimeout.Action, atTimeout.Reason)
	}
}

func TestTemporalDecoderDeterministicTieBreakUsesLexicalOrder(t *testing.T) {
	decoder := NewTemporalDecoder(config.CallCorrectionConfig{
		TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
			Enabled:             true,
			Scope:               "all_correction_candidates",
			LagSeconds:          1,
			MaxWaitSeconds:      2,
			BeamSize:            4,
			MaxObsCandidates:    4,
			StayBonus:           120,
			SwitchPenalty:       160,
			FamilySwitchPenalty: 60,
			Edit1SwitchPenalty:  90,
			MinScore:            0,
			MinMarginScore:      0,
			OverflowAction:      "fallback_resolver",
			MaxPending:          100,
			MaxActiveKeys:       16,
			MaxEventsPerKey:     32,
		},
	})
	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	start := time.Now().UTC()
	if ok, reason := decoder.Observe(TemporalObservation{
		ID:          1,
		ObservedAt:  start,
		Key:         key,
		SubjectCall: "K1ABD",
		Selection:   testTemporalSelection(key, spot.ResolverStateProbable, "K1ABC", 2500, "K1ABD", 2500, 5000),
	}); !ok {
		t.Fatalf("Observe() rejected reason=%s", reason)
	}

	decision := decoder.Evaluate(1, start.Add(1*time.Second), false)
	if decision.Action != TemporalDecisionActionApply {
		t.Fatalf("expected apply, got action=%s reason=%s", decision.Action, decision.Reason)
	}
	if decision.Winner != "K1ABC" {
		t.Fatalf("expected lexical tie-break winner K1ABC, got %q", decision.Winner)
	}
}

func testTemporalSelection(
	key spot.ResolverSignalKey,
	state spot.ResolverState,
	winner string,
	winnerWeighted int,
	runner string,
	runnerWeighted int,
	totalWeighted int,
) ResolverPrimarySelection {
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
	return ResolverPrimarySelection{
		SnapshotOK: true,
		Snapshot: spot.ResolverSnapshot{
			Key:                        key,
			State:                      state,
			Winner:                     winner,
			RunnerUp:                   runner,
			WinnerSupport:              winnerSupport,
			RunnerSupport:              runnerSupport,
			Margin:                     winnerSupport - runnerSupport,
			TotalReporters:             totalSupport,
			WinnerWeightedSupportMilli: winnerWeighted,
			RunnerWeightedSupportMilli: runnerWeighted,
			TotalWeightedSupportMilli:  totalWeighted,
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
