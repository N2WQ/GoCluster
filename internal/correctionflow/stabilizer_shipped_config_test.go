package correctionflow

import (
	"path/filepath"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// Load the shipped settings without overlays so tuning cannot leave consumer
// budgets pinned to an older configuration. These are nominal stabilizer waits;
// queueing, scheduling and temporal holds can add elapsed time in production.
func TestShippedStabilizerReasonBudgets(t *testing.T) {
	loaded, err := config.Load(filepath.Join("..", "..", "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := loaded.CallCorrection
	now := time.Now().UTC().Truncate(time.Second)
	// Reuse independently encoded history with two known geographic cells;
	// the budget oracle must not depend on process-global grid initialization.
	store := b12ConsumerStore(t, b12ConsumerRows("K1ABD", 6, now.Add(-time.Minute)),
		cfg.CustomSCP.StabilizerMinScore, cfg.CustomSCP.StabilizerMinUniqueH3Cells)
	requirements := spot.CustomSCPRequirements{
		MinScore: cfg.CustomSCP.StabilizerMinScore, MinH3Cells: cfg.CustomSCP.StabilizerMinUniqueH3Cells,
	}
	if got := store.RecentSupportCountWithRequirements("K1ABD", "20m", "CW", requirements, now); got != 6 {
		t.Fatalf("edit-neighbor positive control=%d want 6 (unqualified count=%d, requirements=%+v)", got,
			store.RecentSupportCountWithRequirements("K1ABD", "20m", "CW", spot.CustomSCPRequirements{}, now), requirements)
	}
	for _, tc := range []struct {
		name, call, glyph string
		snapshot          spot.ResolverSnapshot
		snapshotOK        bool
		reason            StabilizerDelayReason
		checks            int
		wait              time.Duration
	}{
		{name: "base", call: "K1XYZ", glyph: "?", reason: StabilizerDelayReasonUnknownOrNonRecent, checks: 12, wait: 60 * time.Second},
		{name: "ambiguity", call: "K1ABC", glyph: "S", snapshot: spot.ResolverSnapshot{State: spot.ResolverStateSplit}, snapshotOK: true,
			reason: StabilizerDelayReasonAmbiguous, checks: 12, wait: 60 * time.Second},
		{name: "edit_neighbor", call: "K1ABC", glyph: "S", reason: StabilizerDelayReasonEditNeighbor, checks: 12, wait: 60 * time.Second},
		{name: "low_P", call: "K1ABC", glyph: "P", snapshot: spot.ResolverSnapshot{
			State: spot.ResolverStateConfident, SupportBasis: spot.ResolverSupportBasisWeighted,
			TotalReporters: 3, TotalWeightedSupportMilli: 1000,
			CandidateRanks: []spot.ResolverCandidateSupport{{Call: "K1ABD", Support: 1, WeightedSupportMilli: 800}, {Call: "K1ABC", Support: 2, WeightedSupportMilli: 200}},
		}, snapshotOK: true, reason: StabilizerDelayReasonPLowConfidence, checks: 3, wait: 15 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := spot.NewSpot(tc.call, "W2TT", 14020, "CW")
			s.Time, s.Confidence = now, tc.glyph
			interval := time.Duration(cfg.StabilizerDelaySeconds) * time.Second
			// The first completed check follows the first hold; each retry has
			// another full interval. Exercise both sides of the final boundary.
			for completed := 0; completed <= tc.checks+1; completed++ {
				at := now.Add(time.Duration(completed) * interval)
				decision := EvaluateStabilizerDelay(s, store, cfg, at, tc.snapshot, tc.snapshotOK)
				if !decision.ShouldDelay || decision.Reason != tc.reason || decision.MaxChecks != tc.checks {
					t.Fatalf("check %d decision=%+v, want hold reason=%s budget=%d", completed, decision, tc.reason, tc.checks)
				}
				if got, want := ShouldRetryStabilizerDelay(decision, completed), completed < tc.checks; got != want {
					t.Fatalf("retry after %d checks=%v want %v", completed, got, want)
				}
				if completed == tc.checks && at.Sub(now) != tc.wait {
					t.Fatalf("nominal stabilizer wait=%s want %s", at.Sub(now), tc.wait)
				}
			}
		})
	}
}
