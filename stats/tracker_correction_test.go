package stats

import "testing"

func TestTrackerObserveCallCorrectionDecision(t *testing.T) {
	tr := NewTracker()
	tr.ObserveCallCorrectionDecision("consensus", "applied", "", 1, false)
	tr.ObserveCallCorrectionDecision("consensus+prior_bonus", "applied", "", 2, true)
	tr.ObserveCallCorrectionDecision("anchor", "rejected", "min_reports", 1, false)

	if tr.CorrectionDecisionTotal() != 3 {
		t.Fatalf("expected total decisions 3, got %d", tr.CorrectionDecisionTotal())
	}
	if tr.CorrectionDecisionApplied() != 2 {
		t.Fatalf("expected applied decisions 2, got %d", tr.CorrectionDecisionApplied())
	}
	if tr.CorrectionDecisionRejected() != 1 {
		t.Fatalf("expected rejected decisions 1, got %d", tr.CorrectionDecisionRejected())
	}
	if tr.CorrectionFallbackApplied() != 1 {
		t.Fatalf("expected fallback applied decisions 1, got %d", tr.CorrectionFallbackApplied())
	}
	if tr.CorrectionPriorBonusUsed() != 1 {
		t.Fatalf("expected prior bonus usage 1, got %d", tr.CorrectionPriorBonusUsed())
	}

	reasons := tr.CorrectionDecisionReasons()
	if reasons["min_reports"] != 1 {
		t.Fatalf("expected min_reports rejection count 1, got %d", reasons["min_reports"])
	}
	appliedReasons := tr.CorrectionDecisionAppliedReasons()
	if appliedReasons["unknown"] != 2 {
		t.Fatalf("expected unknown applied-reason count 2, got %d", appliedReasons["unknown"])
	}
	paths := tr.CorrectionDecisionPaths()
	if paths["consensus"] != 1 || paths["consensus+prior_bonus"] != 1 || paths["anchor"] != 1 {
		t.Fatalf("unexpected correction decision path counts: %+v", paths)
	}
	ranks := tr.CorrectionDecisionRanks()
	if ranks["1"] != 2 || ranks["2"] != 1 {
		t.Fatalf("unexpected correction decision rank counts: %+v", ranks)
	}
}
