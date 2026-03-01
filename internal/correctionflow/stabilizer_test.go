package correctionflow

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func newTestRecentBandStore() *spot.RecentBandStore {
	return spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
}

func TestEvaluateStabilizerDelayUnknownNonRecent(t *testing.T) {
	store := newTestRecentBandStore()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		StabilizerMaxChecks:               5,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	s := spot.NewSpot("K1RISK", "W1XYZ", 7010.0, "CW")
	s.Confidence = "?"

	decision := EvaluateStabilizerDelay(s, store, cfg, time.Now().UTC(), spot.ResolverSnapshot{}, false)
	if !decision.ShouldDelay {
		t.Fatalf("expected unknown non-recent spot to be delayed")
	}
	if decision.Reason != StabilizerDelayReasonUnknownOrNonRecent {
		t.Fatalf("expected reason %q, got %q", StabilizerDelayReasonUnknownOrNonRecent.String(), decision.Reason.String())
	}
	if decision.MaxChecks != 5 {
		t.Fatalf("expected max checks 5, got %d", decision.MaxChecks)
	}
}

func TestEvaluateStabilizerDelayPDefaultPassThrough(t *testing.T) {
	store := newTestRecentBandStore()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		StabilizerMaxChecks:               4,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	s := spot.NewSpot("K1PASS", "W1XYZ", 7010.0, "CW")
	s.Confidence = "P"

	decision := EvaluateStabilizerDelay(s, store, cfg, time.Now().UTC(), spot.ResolverSnapshot{}, false)
	if decision.ShouldDelay {
		t.Fatalf("expected P pass-through when low-P policy is not configured")
	}
	if decision.Reason != StabilizerDelayReasonNone {
		t.Fatalf("expected none reason, got %q", decision.Reason.String())
	}
}

func TestEvaluateStabilizerDelayPLowConfidence(t *testing.T) {
	store := newTestRecentBandStore()
	now := time.Now().UTC()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		StabilizerMaxChecks:               5,
		StabilizerPDelayConfidencePercent: 25,
		StabilizerPDelayMaxChecks:         2,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	s := spot.NewSpot("K1PLOW", "W1XYZ", 7010.0, "CW")
	s.Confidence = "P"
	s.EnsureNormalized()
	store.Record("K1PLOW", s.BandNorm, "CW", "N0AAA", now.Add(-2*time.Minute))
	store.Record("K1PLOW", s.BandNorm, "CW", "N0BBB", now.Add(-1*time.Minute))

	snapshot := spot.ResolverSnapshot{
		State:                     spot.ResolverStateProbable,
		TotalWeightedSupportMilli: 1000,
		CandidateRanks: []spot.ResolverCandidateSupport{
			{Call: "K1PLOW", WeightedSupportMilli: 200},
		},
	}
	decision := EvaluateStabilizerDelay(s, store, cfg, now, snapshot, true)
	if !decision.ShouldDelay {
		t.Fatalf("expected low-confidence P policy delay")
	}
	if decision.Reason != StabilizerDelayReasonPLowConfidence {
		t.Fatalf("expected reason %q, got %q", StabilizerDelayReasonPLowConfidence.String(), decision.Reason.String())
	}
	if decision.MaxChecks != 2 {
		t.Fatalf("expected max checks 2, got %d", decision.MaxChecks)
	}
}

func TestEvaluateStabilizerDelayAmbiguousResolver(t *testing.T) {
	store := newTestRecentBandStore()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:            true,
		StabilizerMaxChecks:          6,
		StabilizerAmbiguousMaxChecks: 3,
	}
	s := spot.NewSpot("K1AMB", "W1XYZ", 7010.0, "CW")
	s.Confidence = "S"

	decision := EvaluateStabilizerDelay(s, store, cfg, time.Now().UTC(), spot.ResolverSnapshot{
		State: spot.ResolverStateSplit,
	}, true)
	if !decision.ShouldDelay {
		t.Fatalf("expected resolver split to trigger delay")
	}
	if decision.Reason != StabilizerDelayReasonAmbiguous {
		t.Fatalf("expected reason %q, got %q", StabilizerDelayReasonAmbiguous.String(), decision.Reason.String())
	}
	if decision.MaxChecks != 3 {
		t.Fatalf("expected ambiguous max checks 3, got %d", decision.MaxChecks)
	}
}

func TestEvaluateStabilizerDelayEditNeighborContested(t *testing.T) {
	store := newTestRecentBandStore()
	now := time.Now().UTC()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		StabilizerMaxChecks:               4,
		StabilizerEditNeighborEnabled:     true,
		StabilizerEditNeighborMaxChecks:   2,
		StabilizerEditNeighborMinSpotters: 2,
	}
	s := spot.NewSpot("K1ABC", "W1XYZ", 7010.0, "CW")
	s.Confidence = "S"
	s.EnsureNormalized()
	store.Record("K1ABC", "40m", "CW", "N0AAA", now.Add(-2*time.Minute))
	store.Record("K1ABC", "40m", "CW", "N0AAB", now.Add(-90*time.Second))
	store.Record("K1ABD", "40m", "CW", "N0AAC", now.Add(-80*time.Second))
	store.Record("K1ABD", "40m", "CW", "N0AAD", now.Add(-70*time.Second))

	decision := EvaluateStabilizerDelay(s, store, cfg, now, spot.ResolverSnapshot{}, false)
	if !decision.ShouldDelay {
		t.Fatalf("expected edit-neighbor contested delay")
	}
	if decision.Reason != StabilizerDelayReasonEditNeighbor {
		t.Fatalf("expected edit-neighbor reason, got %q", decision.Reason.String())
	}
	if decision.MaxChecks != 2 {
		t.Fatalf("expected edit-neighbor max checks 2, got %d", decision.MaxChecks)
	}
}

func TestEvaluateStabilizerDelayVPassesThrough(t *testing.T) {
	store := newTestRecentBandStore()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:            true,
		StabilizerMaxChecks:          6,
		StabilizerAmbiguousMaxChecks: 3,
	}
	s := spot.NewSpot("K1VOK", "W1XYZ", 7010.0, "CW")
	s.Confidence = "V"

	decision := EvaluateStabilizerDelay(s, store, cfg, time.Now().UTC(), spot.ResolverSnapshot{
		State: spot.ResolverStateSplit,
	}, true)
	if decision.ShouldDelay {
		t.Fatalf("did not expect V glyph to be delayed")
	}
	if decision.Reason != StabilizerDelayReasonNone {
		t.Fatalf("expected reason %q, got %q", StabilizerDelayReasonNone.String(), decision.Reason.String())
	}
}

func TestEvaluateStabilizerDelayEditNeighborSkipsWhenNeighborWeaker(t *testing.T) {
	store := newTestRecentBandStore()
	now := time.Now().UTC()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		StabilizerEditNeighborEnabled:     true,
		StabilizerEditNeighborMinSpotters: 2,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	s := spot.NewSpot("K1ABC", "W1XYZ", 7010.0, "CW")
	s.Confidence = "V"
	s.EnsureNormalized()
	store.Record("K1ABC", "40m", "CW", "N0AAA", now.Add(-2*time.Minute))
	store.Record("K1ABC", "40m", "CW", "N0AAB", now.Add(-90*time.Second))
	store.Record("K1ABC", "40m", "CW", "N0AAC", now.Add(-80*time.Second))
	store.Record("K1ABD", "40m", "CW", "N0AAD", now.Add(-70*time.Second))
	store.Record("K1ABD", "40m", "CW", "N0AAE", now.Add(-60*time.Second))

	decision := EvaluateStabilizerDelay(s, store, cfg, now, spot.ResolverSnapshot{}, false)
	if decision.ShouldDelay {
		t.Fatalf("did not expect edit-neighbor delay when neighbor support is weaker")
	}
}

func TestShouldRetryStabilizerDelay(t *testing.T) {
	decision := StabilizerDelayDecision{
		ShouldDelay: true,
		Reason:      StabilizerDelayReasonUnknownOrNonRecent,
		MaxChecks:   2,
	}
	if !ShouldRetryStabilizerDelay(decision, 1) {
		t.Fatalf("expected retry while checks remain")
	}
	if ShouldRetryStabilizerDelay(decision, 2) {
		t.Fatalf("did not expect retry at max checks")
	}
}

func TestHasRecentSupportForCallFamily(t *testing.T) {
	store := newTestRecentBandStore()
	now := time.Now().UTC()
	// Mirror family-aware admission by recording both family keys.
	store.Record("W1AW", "40m", "CW", "N0AAA", now.Add(-10*time.Minute))
	store.Record("W1AW", "40m", "CW", "N0BBB", now.Add(-5*time.Minute))
	store.Record("W1AW/5", "40m", "CW", "N0AAA", now.Add(-10*time.Minute))
	store.Record("W1AW/5", "40m", "CW", "N0BBB", now.Add(-5*time.Minute))
	if !HasRecentSupportForCallFamily(store, "W1AW", "40m", "CW", 2, now) {
		t.Fatalf("expected bare call to match slash family support")
	}
}
