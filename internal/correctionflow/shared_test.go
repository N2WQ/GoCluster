package correctionflow

import (
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestFormatConfidenceParity(t *testing.T) {
	tests := []struct {
		name           string
		percent        int
		totalReporters int
		want           string
	}{
		{name: "single reporter unknown", percent: 100, totalReporters: 1, want: "?"},
		{name: "multiple reporters probable", percent: 10, totalReporters: 2, want: "P"},
		{name: "multiple reporters very likely", percent: 51, totalReporters: 2, want: "V"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := FormatConfidence(tc.percent, tc.totalReporters); got != tc.want {
				t.Fatalf("FormatConfidence(%d, %d) = %q, want %q", tc.percent, tc.totalReporters, got, tc.want)
			}
		})
	}
}

func TestResolverConfidenceGlyphForCallUsesEmittedCallSupport(t *testing.T) {
	snapshot := spot.ResolverSnapshot{
		State:                     spot.ResolverStateConfident,
		Winner:                    "K1ABC",
		TotalReporters:            3,
		TotalWeightedSupportMilli: 1200,
		CandidateRanks: []spot.ResolverCandidateSupport{
			{Call: "K1ABC", Support: 2, WeightedSupportMilli: 900},
			{Call: "K1ABD", Support: 1, WeightedSupportMilli: 300},
		},
	}

	if got := ResolverConfidenceGlyphForCall(snapshot, true, "K1ABC"); got != "V" {
		t.Fatalf("winner glyph = %q, want V", got)
	}
	if got := ResolverConfidenceGlyphForCall(snapshot, true, "K1ABD"); got != "P" {
		t.Fatalf("runner glyph = %q, want P", got)
	}
	if got := ResolverConfidenceGlyphForCall(snapshot, true, "K1ZZZ"); got != "?" {
		t.Fatalf("unsupported call glyph = %q, want ?", got)
	}
}

func TestResolverCallConfidencePercentFallbackToWinnerFields(t *testing.T) {
	snapshot := spot.ResolverSnapshot{
		State:                      spot.ResolverStateProbable,
		Winner:                     "K1ABC",
		WinnerSupport:              1,
		TotalReporters:             2,
		WinnerWeightedSupportMilli: 900,
		TotalWeightedSupportMilli:  1500,
	}

	percent, ok := ResolverCallConfidencePercent(snapshot, "K1ABC")
	if !ok {
		t.Fatalf("expected winner percent to be available from fallback fields")
	}
	if percent != 60 {
		t.Fatalf("expected weighted fallback percent 60, got %d", percent)
	}
	if winner := ResolverWinnerConfidence(snapshot); winner != 60 {
		t.Fatalf("ResolverWinnerConfidence = %d, want 60", winner)
	}
}

func TestSelectResolverPrimarySnapshotUsesNeighborhoodWinner(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)
	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	neighborKey := spot.NewResolverSignalKey(7009.5, "40m", "CW", 500)
	now := time.Now().UTC()
	events := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAB", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAC", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAD", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range events {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue neighborhood evidence")
		}
	}
	waitForSelectionSnapshots(t, resolver, []selectionSnapshotExpectation{
		{Key: key, Winner: "K1ABD", MinWinnerSupport: 1},
		{Key: neighborKey, Winner: "K1ABC", MinWinnerSupport: 3},
	}, 500*time.Millisecond)

	selection := SelectResolverPrimarySnapshot(resolver, key, config.CallCorrectionConfig{
		ResolverNeighborhoodEnabled:      true,
		ResolverNeighborhoodBucketRadius: 1,
		FreqGuardRunnerUpRatio:           0.6,
	})
	if !selection.SnapshotOK {
		t.Fatalf("expected neighborhood selection snapshot")
	}
	if selection.NeighborhoodSplit {
		t.Fatalf("did not expect neighborhood split")
	}
	if !selection.WinnerOverride {
		t.Fatalf("expected winner override from neighborhood competition")
	}
	if got := selection.Snapshot.Winner; got != "K1ABC" {
		t.Fatalf("expected neighborhood winner K1ABC, got %q", got)
	}
}

func TestSelectResolverPrimarySnapshotMarksNeighborhoodSplit(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)
	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	neighborKey := spot.NewResolverSignalKey(7009.5, "40m", "CW", 500)
	now := time.Now().UTC()
	events := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAB", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAC", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAD", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0AAE", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0AAF", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0AAG", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range events {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue neighborhood split evidence")
		}
	}
	waitForSelectionSnapshots(t, resolver, []selectionSnapshotExpectation{
		{Key: key, Winner: "K1ABC", MinWinnerSupport: 4},
		{Key: neighborKey, Winner: "K1ABD", MinWinnerSupport: 3},
	}, 500*time.Millisecond)

	selection := SelectResolverPrimarySnapshot(resolver, key, config.CallCorrectionConfig{
		ResolverNeighborhoodEnabled:      true,
		ResolverNeighborhoodBucketRadius: 1,
		FreqGuardRunnerUpRatio:           0.7,
	})
	if !selection.SnapshotOK {
		t.Fatalf("expected neighborhood selection snapshot")
	}
	if !selection.NeighborhoodSplit {
		t.Fatalf("expected neighborhood split when runner-up is comparable")
	}
	if got := selection.Snapshot.State; got != spot.ResolverStateSplit {
		t.Fatalf("expected split snapshot state, got %q", got)
	}
	if got := selection.Snapshot.Winner; got != "" {
		t.Fatalf("expected split snapshot to clear winner, got %q", got)
	}
}

func TestSelectResolverPrimarySnapshotForCallIgnoresUnrelatedNeighbor(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)
	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	neighborKey := spot.NewResolverSignalKey(7009.5, "40m", "CW", 500)
	now := time.Now().UTC()
	events := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "JA1XYZ", Spotter: "N0AAB", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "JA1XYZ", Spotter: "N0AAC", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "JA1XYZ", Spotter: "N0AAD", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range events {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue evidence")
		}
	}
	waitForSelectionSnapshots(t, resolver, []selectionSnapshotExpectation{
		{Key: key, Winner: "K1ABD", MinWinnerSupport: 1},
		{Key: neighborKey, Winner: "JA1XYZ", MinWinnerSupport: 3},
	}, 500*time.Millisecond)

	selection := SelectResolverPrimarySnapshotForCall(resolver, key, config.CallCorrectionConfig{
		ResolverNeighborhoodEnabled:         true,
		ResolverNeighborhoodBucketRadius:    1,
		ResolverNeighborhoodMaxDistance:     1,
		ResolverNeighborhoodAllowTruncation: true,
		FreqGuardRunnerUpRatio:              0.6,
		DistanceModelCW:                     "morse",
		DistanceModelRTTY:                   "baudot",
	}, "K1ABD")
	if !selection.SnapshotOK {
		t.Fatalf("expected snapshot")
	}
	if got := selection.Snapshot.Winner; got != "K1ABD" {
		t.Fatalf("expected exact winner K1ABD, got %q", got)
	}
	if selection.WinnerOverride {
		t.Fatalf("did not expect neighborhood winner override for unrelated neighbor")
	}
	if selection.NeighborhoodSplit {
		t.Fatalf("did not expect neighborhood split for unrelated neighbor")
	}
	if selection.NeighborhoodExcludedUnrelated == 0 {
		t.Fatalf("expected unrelated exclusion counter > 0")
	}
}

func TestSelectResolverPrimarySnapshotForCallPreservesTruncationBenefit(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)
	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	neighborKey := spot.NewResolverSignalKey(7009.5, "40m", "CW", 500)
	now := time.Now().UTC()
	events := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "W1A", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "W1ABC", Spotter: "N0AAB", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "W1ABC", Spotter: "N0AAC", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "W1ABC", Spotter: "N0AAD", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range events {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue evidence")
		}
	}
	waitForSelectionSnapshots(t, resolver, []selectionSnapshotExpectation{
		{Key: key, Winner: "W1A", MinWinnerSupport: 1},
		{Key: neighborKey, Winner: "W1ABC", MinWinnerSupport: 3},
	}, 500*time.Millisecond)

	selection := SelectResolverPrimarySnapshotForCall(resolver, key, config.CallCorrectionConfig{
		ResolverNeighborhoodEnabled:         true,
		ResolverNeighborhoodBucketRadius:    1,
		ResolverNeighborhoodMaxDistance:     1,
		ResolverNeighborhoodAllowTruncation: true,
		FreqGuardRunnerUpRatio:              0.6,
		DistanceModelCW:                     "morse",
		DistanceModelRTTY:                   "baudot",
		FamilyPolicy: config.CallCorrectionFamilyPolicyConfig{
			Truncation: config.CallCorrectionTruncationFamilyConfig{
				Enabled:          true,
				MaxLengthDelta:   2,
				MinShorterLength: 3,
				AllowPrefixMatch: true,
				AllowSuffixMatch: true,
			},
		},
	}, "W1A")
	if !selection.SnapshotOK {
		t.Fatalf("expected snapshot")
	}
	if got := selection.Snapshot.Winner; got != "W1ABC" {
		t.Fatalf("expected truncation-family neighborhood winner W1ABC, got %q", got)
	}
	if !selection.WinnerOverride {
		t.Fatalf("expected neighborhood winner override for truncation-family pair")
	}
	if selection.NeighborhoodSplit {
		t.Fatalf("did not expect split for truncation-family override case")
	}
}

type selectionSnapshotExpectation struct {
	Key              spot.ResolverSignalKey
	Winner           string
	MinWinnerSupport int
}

func waitForSelectionSnapshots(t *testing.T, resolver *spot.SignalResolver, expected []selectionSnapshotExpectation, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for _, candidate := range expected {
			snap, ok := resolver.Lookup(candidate.Key)
			if !ok || snap.Winner == "" {
				ready = false
				break
			}
			if candidate.Winner != "" && !strings.EqualFold(snap.Winner, candidate.Winner) {
				ready = false
				break
			}
			if candidate.MinWinnerSupport > 0 && snap.WinnerSupport < candidate.MinWinnerSupport {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for resolver snapshots")
}

func TestEditDistanceOneSubstitutionVariants(t *testing.T) {
	variants := EditDistanceOneSubstitutionVariants("K1ABC")
	if len(variants) == 0 {
		t.Fatalf("expected substitution variants")
	}
	if len(variants) != len("K1ABC")*(len(editNeighborAlphabet)-1) {
		t.Fatalf("unexpected variant count %d", len(variants))
	}
	found := false
	for _, candidate := range variants {
		if candidate == "K1ABD" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected variant K1ABD")
	}
}
