package main

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestObserveCurrentPathLegacyUnknownNowP(t *testing.T) {
	var metrics replayABMetrics
	metrics.ObserveCurrentPath(replayConfidenceOutcome{
		Final:       "P",
		LegacyFinal: "?",
	})

	if metrics.CurrentPath.ConfidenceCounts.P != 1 {
		t.Fatalf("expected current-path P count 1, got %d", metrics.CurrentPath.ConfidenceCounts.P)
	}
	if metrics.CurrentPath.LegacyUnknownNowP != 1 {
		t.Fatalf("expected legacy_unknown_now_p 1, got %d", metrics.CurrentPath.LegacyUnknownNowP)
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
	if metrics.Resolver.LegacyUnknownNowP != 1 {
		t.Fatalf("expected resolver legacy_unknown_now_p 1, got %d", metrics.Resolver.LegacyUnknownNowP)
	}
}

func TestStabilizerDelayProxyOldVsNew(t *testing.T) {
	store := spot.NewRecentBandStore(10 * time.Minute)
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	s := spot.NewSpotNormalized("K1ABC", "W1XYZ", 7010.0, "CW")
	now := time.Now().UTC()

	if !stabilizerDelayProxyEligible(s, store, cfg) {
		t.Fatalf("expected stabilizer delay proxy eligibility")
	}
	oldDelay := wouldDelayTelnetByStabilizerWithConfidence(s, store, cfg, now, "?")
	newDelay := wouldDelayTelnetByStabilizerWithConfidence(s, store, cfg, now, "P")

	if !oldDelay {
		t.Fatalf("expected old mapping to delay")
	}
	if newDelay {
		t.Fatalf("expected new mapping P to bypass delay")
	}

	var proxy replayStabilizerDelayProxyMetrics
	proxy.Observe(oldDelay, newDelay)
	if proxy.WouldDelayOld != 1 || proxy.WouldDelayNew != 0 {
		t.Fatalf("unexpected delay counts old=%d new=%d", proxy.WouldDelayOld, proxy.WouldDelayNew)
	}
	if proxy.NewlyNotDelayedUnderNewRule != 1 {
		t.Fatalf("expected newly_not_delayed_under_new_rule=1, got %d", proxy.NewlyNotDelayedUnderNewRule)
	}
	if proxy.DelayDelta != -1 {
		t.Fatalf("expected delay_delta=-1, got %d", proxy.DelayDelta)
	}
}
