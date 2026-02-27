package main

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestMaybeApplyResolverCorrectionReplayNoSnapshot(t *testing.T) {
	cfg := config.CallCorrectionConfig{
		Enabled:             true,
		MinConsensusReports: 2,
		MaxEditDistance:     3,
	}
	spotEntry := spot.NewSpotNormalized("K1AB", "W1XYZ", 7010.0, "CW")
	now := time.Now().UTC()

	outcome := maybeApplyResolverCorrectionReplay(
		spotEntry,
		nil,
		spot.ResolverEvidence{},
		false,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		now,
	)

	if outcome.Applied {
		t.Fatalf("expected no apply without snapshot")
	}
	if outcome.Suppress {
		t.Fatalf("expected no suppress without snapshot")
	}
	if got := outcome.Confidence.Final; got != "?" {
		t.Fatalf("expected confidence '?', got %q", got)
	}
}

func TestMaybeApplyResolverCorrectionReplayAppliesWinner(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              128,
		MaxActiveKeys:          128,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    8,
		InactiveTTL:            5 * time.Minute,
		EvalMinInterval:        10 * time.Millisecond,
		SweepInterval:          10 * time.Millisecond,
		HysteresisWindows:      1,
		MaxEditDistance:        3,
		DistanceModelCW:        "morse",
		DistanceModelRTTY:      "baudot",
		FreqGuardRunnerUpRatio: 0.5,
	})
	driver, err := spot.NewSignalResolverDriver(resolver)
	if err != nil {
		t.Fatalf("new resolver driver: %v", err)
	}

	now := time.Now().UTC()
	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 400)
	evidenceWinnerA := spot.ResolverEvidence{
		ObservedAt:    now.Add(-2 * time.Second),
		Key:           key,
		DXCall:        "K1ABC",
		Spotter:       "W1AAA",
		FrequencyKHz:  7010.0,
		RecencyWindow: 60 * time.Second,
	}
	evidenceWinnerB := spot.ResolverEvidence{
		ObservedAt:    now.Add(-1 * time.Second),
		Key:           key,
		DXCall:        "K1ABC",
		Spotter:       "W1BBB",
		FrequencyKHz:  7010.0,
		RecencyWindow: 60 * time.Second,
	}
	if !resolver.Enqueue(evidenceWinnerA) || !resolver.Enqueue(evidenceWinnerB) {
		t.Fatalf("seed evidence enqueue failed")
	}
	driver.Step(now)

	cfg := config.CallCorrectionConfig{
		Enabled:              true,
		MinConsensusReports:  2,
		MinAdvantage:         1,
		MinConfidencePercent: 45,
		MaxEditDistance:      3,
		DistanceModelCW:      "morse",
		DistanceModelRTTY:    "baudot",
		FrequencyToleranceHz: 400,
	}
	spotEntry := spot.NewSpotNormalized("K1AB", "W1XYZ", 7010.0, "CW")

	outcome := maybeApplyResolverCorrectionReplay(
		spotEntry,
		resolver,
		spot.ResolverEvidence{Key: key},
		true,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		now,
	)

	if !outcome.Applied {
		t.Fatalf("expected resolver apply")
	}
	if outcome.Suppress {
		t.Fatalf("expected no suppress on apply")
	}
	if outcome.Winner != "K1ABC" {
		t.Fatalf("expected winner K1ABC, got %q", outcome.Winner)
	}
	if spotEntry.DXCallNorm != "K1ABC" {
		t.Fatalf("expected spot DXCallNorm K1ABC, got %q", spotEntry.DXCallNorm)
	}
	if spotEntry.Confidence != "C" {
		t.Fatalf("expected confidence C, got %q", spotEntry.Confidence)
	}
}
