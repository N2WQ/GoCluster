package main

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestObserveAppliedOutputCountsGlyphs(t *testing.T) {
	var metrics replayABMetrics
	metrics.ObserveAppliedOutput(replayConfidenceOutcome{Final: "P"})

	if metrics.Output.ConfidenceCounts.P != 1 {
		t.Fatalf("expected output P count 1, got %d", metrics.Output.ConfidenceCounts.P)
	}
}

func TestObserveResolverSnapshotTracksStateAndProjectedConfidence(t *testing.T) {
	var metrics replayABMetrics

	metrics.ObserveResolverSnapshot(spot.ResolverSnapshot{
		State:          spot.ResolverStateConfident,
		Winner:         "K1ABC",
		WinnerSupport:  1,
		TotalReporters: 10,
	}, true)
	metrics.ObserveResolverSnapshot(spot.ResolverSnapshot{}, false)

	if metrics.Resolver.Samples != 2 {
		t.Fatalf("expected resolver samples 2, got %d", metrics.Resolver.Samples)
	}
	if metrics.Resolver.MissingSnapshot != 1 {
		t.Fatalf("expected missing snapshot 1, got %d", metrics.Resolver.MissingSnapshot)
	}
	if metrics.Resolver.StateCounts.Confident != 1 {
		t.Fatalf("expected confident state count 1, got %d", metrics.Resolver.StateCounts.Confident)
	}
	if metrics.Resolver.ProjectedConfidenceCounts.P != 1 {
		t.Fatalf("expected projected P count 1, got %d", metrics.Resolver.ProjectedConfidenceCounts.P)
	}
}

func TestStabilizerDelayProxyObserveSinglePolicy(t *testing.T) {
	store := spot.NewRecentBandStore(10 * time.Minute)
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:            true,
		StabilizerMaxChecks:          4,
		StabilizerAmbiguousMaxChecks: 2,
	}
	s := spot.NewSpotNormalized("K1ABC", "W1XYZ", 7010.0, "CW")
	now := time.Now().UTC()

	if !stabilizerDelayProxyEligible(s, store, cfg) {
		t.Fatalf("expected stabilizer delay proxy eligibility")
	}

	decision := evaluateStabilizerDelay(s, store, cfg, now, spot.ResolverSnapshot{
		State: spot.ResolverStateSplit,
	}, true)
	if !decision.ShouldDelay {
		t.Fatalf("expected split snapshot to trigger delay")
	}

	var proxy replayStabilizerDelayProxyMetrics
	proxy.Observe(decision)
	if proxy.WouldDelay != 1 {
		t.Fatalf("expected would_delay=1, got %d", proxy.WouldDelay)
	}
	if proxy.ReasonAmbiguousResolver != 1 {
		t.Fatalf("expected reason_ambiguous_resolver=1, got %d", proxy.ReasonAmbiguousResolver)
	}
	if proxy.ReasonUnknownOrNonRecent != 0 || proxy.ReasonPLowConfidence != 0 || proxy.ReasonEditNeighbor != 0 {
		t.Fatalf("unexpected reason distribution: %+v", proxy)
	}
}

func TestObserveResolverSelectionNeighborhoodCounters(t *testing.T) {
	var metrics replayABMetrics
	metrics.ObserveResolverSelection(correctionflow.ResolverPrimarySelection{
		UsedNeighborhood:                  true,
		WinnerOverride:                    true,
		NeighborhoodSplit:                 true,
		NeighborhoodExcludedUnrelated:     2,
		NeighborhoodExcludedDistance:      1,
		NeighborhoodExcludedAnchorMissing: 1,
	})

	if metrics.Resolver.NeighborhoodUsed != 1 {
		t.Fatalf("expected neighborhood_used=1, got %d", metrics.Resolver.NeighborhoodUsed)
	}
	if metrics.Resolver.NeighborhoodOverride != 1 {
		t.Fatalf("expected neighborhood_winner_override=1, got %d", metrics.Resolver.NeighborhoodOverride)
	}
	if metrics.Resolver.NeighborhoodSplit != 1 {
		t.Fatalf("expected neighborhood_conflict_split=1, got %d", metrics.Resolver.NeighborhoodSplit)
	}
	if metrics.Resolver.NeighborhoodExcludedUnrelated != 2 {
		t.Fatalf("expected neighborhood_excluded_unrelated=2, got %d", metrics.Resolver.NeighborhoodExcludedUnrelated)
	}
	if metrics.Resolver.NeighborhoodExcludedDistance != 1 {
		t.Fatalf("expected neighborhood_excluded_distance=1, got %d", metrics.Resolver.NeighborhoodExcludedDistance)
	}
	if metrics.Resolver.NeighborhoodExcludedAnchorMissing != 1 {
		t.Fatalf("expected neighborhood_excluded_anchor_missing=1, got %d", metrics.Resolver.NeighborhoodExcludedAnchorMissing)
	}
}

func TestObserveResolverRecentPlus1GateCounters(t *testing.T) {
	var metrics replayABMetrics
	metrics.ObserveResolverRecentPlus1Gate(spot.ResolverPrimaryGateResult{
		RecentPlus1Considered: true,
		RecentPlus1Applied:    true,
	}, true)
	metrics.ObserveResolverRecentPlus1Gate(spot.ResolverPrimaryGateResult{
		RecentPlus1Considered: true,
		RecentPlus1Applied:    false,
		RecentPlus1Reject:     "winner_recent_insufficient",
	}, true)
	metrics.ObserveResolverRecentPlus1Gate(spot.ResolverPrimaryGateResult{
		RecentPlus1Considered: true,
		RecentPlus1Applied:    false,
		RecentPlus1Reject:     "subject_not_weaker",
	}, true)
	metrics.ObserveResolverRecentPlus1Gate(spot.ResolverPrimaryGateResult{
		RecentPlus1Considered: true,
		RecentPlus1Applied:    false,
		RecentPlus1Reject:     "edit_neighbor_contested",
	}, true)
	metrics.ObserveResolverRecentPlus1Gate(spot.ResolverPrimaryGateResult{
		RecentPlus1Considered: true,
		RecentPlus1Applied:    false,
		RecentPlus1Reject:     "distance_or_family",
	}, true)
	metrics.ObserveResolverRecentPlus1Gate(spot.ResolverPrimaryGateResult{
		RecentPlus1Considered: true,
		RecentPlus1Applied:    false,
		RecentPlus1Reject:     "unexpected_reason",
	}, true)

	if metrics.Resolver.RecentPlus1Applied != 1 {
		t.Fatalf("expected recent_plus1_applied=1, got %d", metrics.Resolver.RecentPlus1Applied)
	}
	if metrics.Resolver.RecentPlus1Rejected != 5 {
		t.Fatalf("expected recent_plus1_rejected=5, got %d", metrics.Resolver.RecentPlus1Rejected)
	}
	if metrics.Resolver.RecentPlus1RejectWinner != 1 {
		t.Fatalf("expected recent_plus1_reject_winner_recent_insufficient=1, got %d", metrics.Resolver.RecentPlus1RejectWinner)
	}
	if metrics.Resolver.RecentPlus1RejectSubject != 1 {
		t.Fatalf("expected recent_plus1_reject_subject_not_weaker=1, got %d", metrics.Resolver.RecentPlus1RejectSubject)
	}
	if metrics.Resolver.RecentPlus1RejectEdit != 1 {
		t.Fatalf("expected recent_plus1_reject_edit_neighbor_contested=1, got %d", metrics.Resolver.RecentPlus1RejectEdit)
	}
	if metrics.Resolver.RecentPlus1RejectDistance != 1 {
		t.Fatalf("expected recent_plus1_reject_distance_or_family=1, got %d", metrics.Resolver.RecentPlus1RejectDistance)
	}
	if metrics.Resolver.RecentPlus1RejectOther != 1 {
		t.Fatalf("expected recent_plus1_reject_other=1, got %d", metrics.Resolver.RecentPlus1RejectOther)
	}
}
